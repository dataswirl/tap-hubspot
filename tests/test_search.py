"""Tests for search-based syncing of dynamic streams (mocked HTTP, no live API)."""

from __future__ import annotations

import datetime as dt
import json
import re

import pytest
import requests_mock as rm

from tap_hubspot.client import DynamicIncrementalHubspotStream
from tap_hubspot.tap import TapHubspot

BASE = "https://api.hubapi.com/crm/v3"
N_PROPS = 600
# Base timestamp for generated records: 2024-01-01T00:00:00Z
BASE_MS = 1704067200000


def _iso(ms: int) -> str:
    ts = dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC)
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _props_response(request, context):
    return {
        "results": [
            {"name": f"custom_property_number_{i:04d}", "type": "string"}
            for i in range(N_PROPS)
        ]
        + [
            {"name": "lastmodifieddate", "type": "datetime"},
            {"name": "hs_lastmodifieddate", "type": "datetime"},
        ],
    }


def _make_search_callback(timestamps: list[int], replication_key: str):
    """Emulate HubSpot search: GTE filter, ASC sort, `after` offset, 10k cap."""

    def callback(request, context):
        body = request.json()
        flt = body["filterGroups"][0]["filters"][0]
        assert flt["propertyName"] == replication_key
        assert flt["operator"] == "GTE"
        since = int(flt["value"])
        after = int(body.get("after", 0))
        if after + body["limit"] > 10000:
            context.status_code = 400
            return {"message": "after exceeds 10000"}
        matching = [(i, ts) for i, ts in enumerate(timestamps) if ts >= since]
        page = matching[after : after + body["limit"]]
        resp = {
            "total": len(matching),
            "results": [
                {
                    "id": str(i),
                    "properties": {replication_key: _iso(ts)},
                    "updatedAt": _iso(ts),
                }
                for i, ts in page
            ],
        }
        if after + body["limit"] < len(matching):
            resp["paging"] = {"next": {"after": str(after + body["limit"])}}
        return resp

    return callback


@pytest.fixture
def http():
    with rm.Mocker() as m:
        m.get(re.compile(f"{BASE}/properties/.*"), json=_props_response)
        yield m


def _tap(**config) -> TapHubspot:
    return TapHubspot(
        config={"access_token": "test-token", **config},
        parse_env_config=False,
        validate_config=False,
    )


def _search_bodies(http, path: str) -> list[dict]:
    return [
        r.json()
        for r in http.request_history
        if r.method == "POST" and r.url == f"{BASE}{path}"
    ]


def _sync(stream, capsys) -> list[dict]:
    stream.sync()
    out = capsys.readouterr().out
    messages = [json.loads(line) for line in out.splitlines() if line.startswith("{")]
    return [m["record"] for m in messages if m["type"] == "RECORD"]


def test_full_sync_without_state_uses_search(http, capsys):
    """No state and no start_date -> POST /search with epoch-0 filter."""
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(
            [BASE_MS + i for i in range(150)], "lastmodifieddate"
        ),
    )
    stream = _tap().streams["contacts"]

    records = _sync(stream, capsys)

    assert len(records) == 150
    assert not [
        r
        for r in http.request_history
        if "/objects/contacts" in r.url and r.method == "GET"
    ]
    bodies = _search_bodies(http, "/objects/contacts/search")
    assert len(bodies) == 2
    assert bodies[0]["filterGroups"][0]["filters"][0]["value"] == "0"
    assert "after" not in bodies[0]
    assert bodies[1]["after"] == "100"
    assert len(bodies[0]["properties"]) == N_PROPS + 2
    assert "custom_property_number_0599" in bodies[0]["properties"]


def test_no_long_get_requests_for_any_dynamic_stream(http, capsys):
    """Every dynamic stream syncs without long query strings."""
    http.post(
        re.compile(f"{BASE}/objects/.*/search"),
        json=lambda request, context: {
            "results": [
                {
                    "id": "1",
                    "properties": {
                        "lastmodifieddate": _iso(BASE_MS),
                        "hs_lastmodifieddate": _iso(BASE_MS),
                    },
                },
            ],
        },
    )
    tap = _tap()
    dynamic = [
        s
        for s in tap.streams.values()
        if isinstance(s, DynamicIncrementalHubspotStream)
    ]
    assert {s.name for s in dynamic} >= {"contacts", "companies", "deals"}

    for stream in dynamic:
        assert len(_sync(stream, capsys)) == 1, stream.name

    for request in http.request_history:
        assert len(request.url) < 8000, request.url
    searched = {r.path for r in http.request_history if r.method == "POST"}
    assert searched == {f"/crm/v3{s.incremental_path}" for s in dynamic}


def test_start_date_seeds_filter_and_bookmark_advances(http, capsys):
    timestamps = [BASE_MS + i * 1000 for i in range(250)]
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(timestamps, "lastmodifieddate"),
    )
    stream = _tap(start_date=_iso(BASE_MS + 50_000)).streams["contacts"]

    records = _sync(stream, capsys)

    bodies = _search_bodies(http, "/objects/contacts/search")
    assert bodies[0]["filterGroups"][0]["filters"][0]["value"] == str(BASE_MS + 50_000)
    assert len(records) == 200
    assert records[0]["lastmodifieddate"] == _iso(BASE_MS + 50_000)
    state = stream.get_context_state(None)
    assert state["replication_key"] == "lastmodifieddate"
    assert _parse_ms(state["replication_key_value"]) == timestamps[-1]


def test_existing_state_is_used_as_lower_bound(http, capsys):
    timestamps = [BASE_MS + i * 1000 for i in range(20)]
    http.post(
        f"{BASE}/objects/companies/search",
        json=_make_search_callback(timestamps, "hs_lastmodifieddate"),
    )
    tap = TapHubspot(
        config={"access_token": "test-token", "start_date": "2000-01-01T00:00:00Z"},
        state={
            "bookmarks": {
                "companies": {
                    "replication_key": "hs_lastmodifieddate",
                    "replication_key_value": _iso(BASE_MS + 10_000),
                },
            },
        },
        parse_env_config=False,
        validate_config=False,
    )
    stream = tap.streams["companies"]

    records = _sync(stream, capsys)

    bodies = _search_bodies(http, "/objects/companies/search")
    assert bodies[0]["filterGroups"][0]["filters"][0]["value"] == str(BASE_MS + 10_000)
    assert len(records) == 10
    assert (
        _parse_ms(stream.get_context_state(None)["replication_key_value"])
        == timestamps[-1]
    )


def test_search_cap_restarts_from_last_seen_value(http, capsys):
    timestamps = [BASE_MS + i for i in range(10_500)]
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(timestamps, "lastmodifieddate"),
    )
    stream = _tap().streams["contacts"]

    records = _sync(stream, capsys)

    bodies = _search_bodies(http, "/objects/contacts/search")
    afters = [b.get("after") for b in bodies]
    # 99 pages up to after=9800, then the query restarts without `after`
    assert afters[98] == "9800"
    assert afters[99] is None
    restart_filter = bodies[99]["filterGroups"][0]["filters"][0]["value"]
    assert restart_filter == str(timestamps[9899])
    assert all(int(a) < 9900 for a in afters if a)
    # Boundary record is emitted twice, nothing is missing
    assert len(records) == 10_501
    assert {r["id"] for r in records} == {str(i) for i in range(10_500)}
    assert (
        _parse_ms(stream.get_context_state(None)["replication_key_value"])
        == timestamps[-1]
    )


def test_search_cap_with_identical_boundary_value_raises(http, capsys):
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback([BASE_MS] * 10_500, "lastmodifieddate"),
    )
    stream = _tap().streams["contacts"]

    with pytest.raises(RuntimeError, match="all of them share lastmodifieddate"):
        _sync(stream, capsys)

    # First restart moves the filter forward (0 -> BASE_MS), the second one would not.
    bodies = _search_bodies(http, "/objects/contacts/search")
    assert len(bodies) == 99 * 2


def _parse_ms(value: str) -> int:
    ts = dt.datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.UTC)
    return int(ts.timestamp() * 1000)
