"""REST client handling, including HubspotStream base class."""

from __future__ import annotations

import datetime as dt
import typing as t
from collections.abc import Callable
from functools import cached_property

import requests
from singer_sdk import typing as th
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.streams import RESTStream
from singer_sdk.streams.core import REPLICATION_FULL_TABLE

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
        paging = response.json().get("paging") or {}
        return (paging.get("next") or {}).get("after")

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
        HubSpot (HTTP 431). Streams with an `incremental_path` use the search
        endpoint instead, see `DynamicIncrementalHubspotStream`.

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

    Search results are capped at 10k per query and only one sort is allowed, so
    the sync runs as a sequence of scans:

    - `id`: full scan sorted by `hs_object_id`, used for full-table syncs and for
      the first sync without state or start_date (includes records whose
      replication key is empty). At the cap, restarts with `hs_object_id GT`.
    - `key`: incremental scan sorted by the replication key, starting at the
      bookmark (`GTE`). At the cap, switches to a `tie` scan on the replication
      key value of the last record seen.
    - `tie`: scan of the records sharing that value, sorted by `hs_object_id`.
      When done, the `key` scan resumes with `GT` the tie value. Records with
      the tie value are emitted again (duplicates on `id` are expected).

    Pagination tokens are dicts:
    `{"phase": "id" | "key" | "tie", "value": <ms | None>, "last_id": <int | None>,
    "strict": <bool>, "after": <str | None>}`.
    """

    incremental_path: str | None = None
    _initial_search_token: SearchToken | None = None

    @property
    def _use_search(self) -> bool:
        return bool(self.incremental_path)

    @property
    def _is_full_table(self) -> bool:
        # Also covers a catalog that clears `replication_key`
        return (
            not self.replication_key
            or self.replication_method == REPLICATION_FULL_TABLE
        )

    def _initial_token(self, context: Context | None) -> SearchToken:
        """Return the first search token: `key` scan from the bookmark, else `id`."""
        start = (
            None
            if self._is_full_table
            else self.get_starting_replication_key_value(context)
        )
        if not start:
            return _search_token("id")
        return _search_token("key", value=_to_epoch_ms(start))

    def _get_record_replication_ms(self, record: dict) -> int:
        value = (record.get("properties") or {}).get(self.replication_key)
        if not value:
            # Cannot happen: `key` scans filter on the replication key
            msg = (
                f"Cannot continue {self.name} past {SEARCH_RESULTS_LIMIT} search "
                f"results: record {record.get('id')} has no "
                f"'{self.replication_key}' value."
            )
            raise RuntimeError(msg)
        return _to_epoch_ms(value)

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
        """Return the next search token, moving to the next scan at the 10k cap."""
        after = self._get_paging_after(response)
        if not self._use_search:
            return after

        token = (
            previous_token
            if isinstance(previous_token, dict)
            else self._initial_search_token
        )
        if token is None:
            msg = "Search token is missing: no search request was prepared."
            raise RuntimeError(msg)

        if after and int(after) + SEARCH_PAGE_SIZE < SEARCH_RESULTS_LIMIT:
            return {**token, "after": after}

        if not after:
            if token["phase"] == "tie":
                # All records sharing the tie value were read, resume after it
                return _search_token("key", value=token["value"], strict=True)
            return None

        last = response.json()["results"][-1]
        if token["phase"] == "key":
            next_token = _search_token(
                "tie",
                value=self._get_record_replication_ms(last),
            )
        else:
            next_token = {**token, "last_id": int(last["id"]), "after": None}
        self.logger.info(
            "Reached the %d search results cap for %s, continuing with %s",
            SEARCH_RESULTS_LIMIT,
            self.name,
            next_token,
        )
        return next_token

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

        if isinstance(next_page_token, dict):
            token = next_page_token
        else:
            token = self._initial_search_token = self._initial_token(context)

        filters = []
        if token["phase"] == "id":
            sort_by = "hs_object_id"
        elif token["phase"] == "key":
            operator = "GT" if token["strict"] else "GTE"
            filters.append(_filter(self.replication_key, operator, token["value"]))
            sort_by = self.replication_key
        else:
            filters.append(_filter(self.replication_key, "EQ", token["value"]))
            sort_by = "hs_object_id"
        if token["last_id"] is not None:
            filters.append(_filter("hs_object_id", "GT", token["last_id"]))

        body: dict[str, t.Any] = {
            # This is inside the properties object
            "sorts": [{"propertyName": sort_by, "direction": "ASCENDING"}],
            # Hubspot sets a limit of most 100 per request. Default is 10
            "limit": SEARCH_PAGE_SIZE,
            "properties": list(self.hs_properties),
        }
        if filters:
            body["filterGroups"] = [{"filters": filters}]
        if token["after"]:
            body["after"] = token["after"]
        return body


def _search_token(
    phase: t.Literal["id", "key", "tie"],
    *,
    value: int | None = None,
    strict: bool = False,
) -> SearchToken:
    return {
        "phase": phase,
        "value": value,
        "last_id": None,
        "strict": strict,
        "after": None,
    }


def _filter(property_name: str | None, operator: str, value: int) -> dict:
    return {
        "propertyName": property_name,
        "operator": operator,
        # Timestamps need to be in milliseconds
        # https://legacydocs.hubspot.com/docs/faq/how-should-timestamps-be-formatted-for-hubspots-apis
        "value": str(value),
    }
