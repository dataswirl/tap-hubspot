"""REST client handling, including HubspotStream base class."""

from __future__ import annotations

import datetime as dt
import json
import typing as t
from collections.abc import Callable
from functools import cached_property

import requests
from singer_sdk import typing as th
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream

from tap_hubspot.auth import HubSpotOAuthAuthenticator

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
    from singer_sdk.pagination import BaseAPIPaginator

type _Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

type SearchToken = dict[str, t.Any]
# `str` for list endpoints, `SearchToken` for search endpoints
type PageToken = str | SearchToken | None


class HubspotStream(RESTStream):
    """tap-hubspot stream class."""

    @property
    def url_base(self) -> str:
        """Returns base url"""
        return "https://api.hubapi.com/"

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @cached_property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        if "refresh_token" in self.config:
            return HubSpotOAuthAuthenticator(
                self,
                auth_endpoint="https://api.hubapi.com/oauth/v1/token",
            )
        return BearerTokenAuthenticator(
            self,
            token=self.config.get("access_token") or "",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return super().get_new_paginator()

    def _get_paging_after(self, response: requests.Response) -> str | None:
        """Return the `paging.next.after` cursor, or None on the last page."""
        return (response.json().get("paging") or {}).get("next", {}).get("after")

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: PageToken,  # noqa: ARG002
    ) -> PageToken:
        """Return a token for identifying next page or None if no more pages."""
        return self._get_paging_after(response)

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: PageToken,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        params["limit"] = 100
        if next_page_token:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params


# HubSpot search endpoints return at most 10k results per query
# https://developers.hubspot.com/docs/api/crm/search#limitations
SEARCH_RESULTS_LIMIT = 10000
SEARCH_PAGE_SIZE = 100


def _to_epoch_ms(value: str) -> int:
    """Parse an ISO 8601 timestamp into epoch milliseconds (naive means UTC)."""
    ts = dt.datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.UTC)
    return int(ts.timestamp() * 1000)


class DynamicHubspotStream(HubspotStream):
    """DynamicHubspotStream"""

    def _get_datatype(self, data_type: str) -> th.JSONTypeHelper:  # noqa: ARG002
        # TODO: consider typing more precisely
        return th.StringType()

    @cached_property
    def hs_properties(self) -> dict[str, str]:
        """Return all available HubSpot properties for this object."""
        return self._get_available_properties()

    @cached_property
    def schema(self) -> dict:
        """Return a draft JSON schema for this stream."""
        hs_props = [
            th.Property(name, self._get_datatype(data_type))
            for name, data_type in self.hs_properties.items()
        ]
        schema = th.PropertiesList(
            th.Property("id", th.StringType),
            th.Property(
                "properties",
                th.ObjectType(*hs_props),
            ),
            th.Property("createdAt", th.DateTimeType),
            th.Property("updatedAt", th.DateTimeType),
            th.Property("archived", th.BooleanType),
        )
        if self.replication_key:
            schema.append(
                th.Property(
                    self.replication_key,
                    th.DateTimeType,
                ),
            )
        return schema.to_dict()

    def _get_available_properties(self) -> dict[str, str]:
        session = requests.Session()
        session.auth = self.authenticator

        resp = session.get(
            f"https://api.hubapi.com/crm/v3/properties/{self.name}",
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return {prop["name"]: prop["type"] for prop in results}

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: PageToken,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Note: objects with many properties produce a query string too long for
        HubSpot (HTTP 431). Prefer the search endpoint, see
        `DynamicIncrementalHubspotStream`.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params = super().get_url_params(context, next_page_token)
        if self.hs_properties:
            params["properties"] = ",".join(self.hs_properties)
        return params


class DynamicIncrementalHubspotStream(DynamicHubspotStream):
    """Dynamic stream that reads through the object's search endpoint.

    The search endpoint is a POST request that carries the property list in the
    JSON body, so it is used for every sync (incremental or full) to avoid
    HTTP 431 errors caused by long `properties` query strings.

    Search results are capped at 10k per query. When the cap is reached, the
    query is restarted with the `GTE` filter moved to the replication key value
    of the last record seen. Records sharing that boundary value are emitted
    again (duplicates on `id` are expected).

    Pagination tokens are dicts: `{"after": <str | None>, "since": <ms | None>}`.
    """

    incremental_path: str | None = None

    @property
    def _use_search(self) -> bool:
        return bool(self.incremental_path and self.replication_key)

    def _get_search_start_ms(self, context: Context | None) -> int:
        """Return the search lower bound: state, then start_date, then epoch 0."""
        start = self.get_starting_replication_key_value(context)
        return _to_epoch_ms(start) if start else 0

    def _get_record_replication_value(self, record: dict) -> str | None:
        props = record.get("properties") or {}
        return props.get(self.replication_key) or record.get("updatedAt")

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: PageToken,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        if self._use_search:
            return {}
        return super().get_url_params(context, next_page_token)

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: PageToken,
    ) -> PageToken:
        """Return the next search page token, restarting the query at the 10k cap."""
        after = self._get_paging_after(response)
        if not self._use_search or not after:
            return after

        if int(after) + SEARCH_PAGE_SIZE < SEARCH_RESULTS_LIMIT:
            since = (
                previous_token.get("since")
                if isinstance(previous_token, dict)
                else None
            )
            return {"after": after, "since": since}

        # Restart the query from the last replication key value seen.
        records = list(self.parse_response(response))
        last_value = (
            self._get_record_replication_value(records[-1]) if records else None
        )
        if not last_value:
            msg = (
                f"Cannot continue {self.name} past {SEARCH_RESULTS_LIMIT} search "
                f"results: last record has no '{self.replication_key}' value."
            )
            raise RuntimeError(msg)

        sent_body = json.loads(t.cast("bytes", response.request.body))
        current_since = int(sent_body["filterGroups"][0]["filters"][0]["value"])
        next_since = _to_epoch_ms(last_value)
        if next_since <= current_since:
            msg = (
                f"Cannot continue {self.name} past {SEARCH_RESULTS_LIMIT} search "
                f"results: all of them share {self.replication_key}={last_value}."
            )
            raise RuntimeError(msg)

        self.logger.info(
            "Reached the %d search results cap for %s, restarting from %s=%s",
            SEARCH_RESULTS_LIMIT,
            self.name,
            self.replication_key,
            last_value,
        )
        return {"after": None, "since": next_since}

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.
        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        if self.replication_key:
            row[self.replication_key] = (row.get("properties") or {}).get(
                self.replication_key,
            )
        return row

    def prepare_request(
        self,
        context: Context | None,
        next_page_token: PageToken,
    ) -> requests.PreparedRequest:
        """Prepare a POST request to the search endpoint, or the default request."""
        if not self._use_search:
            return super().prepare_request(context, next_page_token)
        # Search endpoints use POST requests
        return self.build_prepared_request(
            method="POST",
            url=f"{self.url_base}{self.incremental_path}",
            headers=self.http_headers,
            json=self.prepare_request_payload(context, next_page_token),
        )

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: PageToken,
    ) -> dict | None:
        """Prepare the JSON body for the search request.

        https://developers.hubspot.com/docs/api/crm/search

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        if not self._use_search:
            return None

        token = next_page_token if isinstance(next_page_token, dict) else {}
        since = token.get("since")
        if since is None:
            since = self._get_search_start_ms(context)

        body = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": self.replication_key,
                            "operator": "GTE",
                            # Timestamps need to be in milliseconds
                            # https://legacydocs.hubspot.com/docs/faq/how-should-timestamps-be-formatted-for-hubspots-apis
                            "value": str(since),
                        },
                    ],
                },
            ],
            "sorts": [
                {
                    # This is inside the properties object
                    "propertyName": self.replication_key,
                    "direction": "ASCENDING",
                },
            ],
            # Hubspot sets a limit of most 100 per request. Default is 10
            "limit": SEARCH_PAGE_SIZE,
            "properties": list(self.hs_properties),
        }
        if token.get("after"):
            body["after"] = token["after"]
        return body
