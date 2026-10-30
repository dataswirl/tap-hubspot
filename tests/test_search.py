"""Tests for search-based syncing of dynamic streams (mocked HTTP, no live API)."""

from __future__ import annotations

import datetime as dt
import json
import re
import typing as t

import pytest
import requests_mock as rm

from tap_hubspot.client import DynamicIncrementalHubspotStream
from tap_hubspot.tap import TapHubspot

if t.TYPE_CHECKING:
    from collections.abc import Sequence

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


def _matches(record: tuple[int, int | None], flt: dict, replication_key: str) -> bool:
    record_id, ts = record
    if flt["propertyName"] == "hs_object_id":
        value = record_id
    else:
        assert flt["propertyName"] == replication_key
        value = ts
    if value is None:
        return False
    target = int(flt["value"])
    return {
        "GT": value > target,
        "GTE": value >= target,
        "EQ": value == target,
    }[flt["operator"]]


def _make_search_callback(
    timestamps: Sequence[int | None],
    replication_key: str,
    *,
    paging_at_cap: bool = True,
):
    """Emulate HubSpot search: AND filters, one ASC sort, `after` offset, 10k cap.

    Record ids are 1-based positions in `timestamps`, a `None` timestamp means the
    replication key is empty. Ties on the replication key come back in descending
    id order, since HubSpot does not guarantee any order for them.
    With `paging_at_cap=False`, the page ending at the cap has no `paging`.
    """
    records = [(i + 1, ts) for i, ts in enumerate(timestamps)]

    def callback(request, context):
        body = request.json()
        groups = body.get("filterGroups", [])
        assert len(groups) <= 1
        filters = groups[0]["filters"] if groups else []
        (sort,) = body["sorts"]
        assert sort["direction"] == "ASCENDING"
        after = int(body.get("after", 0))
        if after + body["limit"] > 10000:
            context.status_code = 400
            return {"message": "after exceeds 10000"}
        matching = [
            r for r in records if all(_matches(r, f, replication_key) for f in filters)
        ]
        if sort["propertyName"] == "hs_object_id":
            matching.sort(key=lambda r: r[0])
        else:
            assert sort["propertyName"] == replication_key
            assert all(r[1] is not None for r in matching)
            matching.sort(key=lambda r: (r[1], -r[0]))
        page = matching[after : after + body["limit"]]
        resp = {
            "total": len(matching),
            "results": [
                {
                    "id": str(record_id),
                    "properties": {
                        replication_key: None if ts is None else _iso(ts),
                        "hs_object_id": str(record_id),
                    },
                }
                for record_id, ts in page
            ],
        }
        at_cap = after + body["limit"] >= 10000
        if after + body["limit"] < len(matching) and (paging_at_cap or not at_cap):
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


def test_full_sync_without_state_scans_by_id(http, capsys):
    """No state and no start_date -> POST /search sorted by id, no key filter."""
    timestamps: list[int | None] = [BASE_MS + i for i in range(150)]
    timestamps[10] = timestamps[120] = None
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(timestamps, "lastmodifieddate"),
    )
    stream = _tap().streams["contacts"]

    records = _sync(stream, capsys)

    assert not stream.is_sorted
    # Records with an empty replication key are not dropped
    assert sorted(int(r["id"]) for r in records) == list(range(1, 151))
    assert not [
        r
        for r in http.request_history
        if "/objects/contacts" in r.url and r.method == "GET"
    ]
    bodies = _search_bodies(http, "/objects/contacts/search")
    assert len(bodies) == 2
    assert "filterGroups" not in bodies[0]
    assert bodies[0]["sorts"] == [
        {"propertyName": "hs_object_id", "direction": "ASCENDING"},
    ]
    assert "after" not in bodies[0]
    assert bodies[1]["after"] == "100"
    assert len(bodies[0]["properties"]) == N_PROPS + 2
    assert "custom_property_number_0599" in bodies[0]["properties"]
    assert _parse_ms(stream.get_context_state(None)["replication_key_value"]) == (
        BASE_MS + 149
    )


def _tap_with_catalog_override(state: dict | None = None, **override) -> TapHubspot:
    catalog = _tap().catalog_dict
    (entry,) = [s for s in catalog["streams"] if s["tap_stream_id"] == "contacts"]
    entry.update(override)
    return TapHubspot(
        config={"access_token": "test-token", "start_date": _iso(BASE_MS + 100)},
        catalog=catalog,
        state=state,
        parse_env_config=False,
        validate_config=False,
    )


def test_full_table_override_ignores_state_and_start_date(http, capsys):
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(
            [BASE_MS + i for i in range(150)], "lastmodifieddate"
        ),
    )
    tap = _tap_with_catalog_override(
        state={
            "bookmarks": {
                "contacts": {
                    "replication_key": "lastmodifieddate",
                    "replication_key_value": _iso(BASE_MS + 140),
                },
            },
        },
        replication_method="FULL_TABLE",
    )
    stream = tap.streams["contacts"]

    records = _sync(stream, capsys)

    assert len(records) == 150
    bodies = _search_bodies(http, "/objects/contacts/search")
    assert all("filterGroups" not in b for b in bodies)


def test_cleared_replication_key_scans_by_id(http, capsys):
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(
            [BASE_MS + i for i in range(150)], "lastmodifieddate"
        ),
    )
    # Discovery writes `replication_method: INCREMENTAL`, so it is overridden too
    tap = _tap_with_catalog_override(
        replication_key=None,
        replication_method="FULL_TABLE",
    )
    stream = tap.streams["contacts"]

    records = _sync(stream, capsys)

    assert len(records) == 150
    assert all(r.method == "POST" for r in http.request_history if "/objects/" in r.url)
    bodies = _search_bodies(http, "/objects/contacts/search")
    assert all("filterGroups" not in b for b in bodies)
    assert not stream.get_context_state(None).get("replication_key_value")


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


def test_id_scan_cap_restarts_from_last_id(http, capsys):
    timestamps = [BASE_MS + i for i in range(10_500)]
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(timestamps, "lastmodifieddate"),
    )
    stream = _tap().streams["contacts"]

    records = _sync(stream, capsys)

    bodies = _search_bodies(http, "/objects/contacts/search")
    afters = [b.get("after") for b in bodies]
    # 100 pages up to after=9900, then the query restarts without `after`
    assert afters[99] == "9900"
    assert afters[100] is None
    assert bodies[100]["filterGroups"] == [
        {
            "filters": [
                {"propertyName": "hs_object_id", "operator": "GT", "value": "10000"},
            ],
        },
    ]
    assert all(int(a) <= 9900 for a in afters if a)
    # No duplicates, nothing is missing
    assert sorted(int(r["id"]) for r in records) == list(range(1, 10_501))


def test_key_scan_cap_switches_to_tie_scan(http, capsys):
    timestamps = [BASE_MS + i for i in range(10_500)]
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(timestamps, "lastmodifieddate"),
    )
    stream = _tap(start_date=_iso(BASE_MS)).streams["contacts"]

    records = _sync(stream, capsys)

    bodies = _search_bodies(http, "/objects/contacts/search")
    boundary = str(timestamps[9999])
    tie, resumed = bodies[100], bodies[101]
    assert tie["filterGroups"][0]["filters"] == [
        {"propertyName": "lastmodifieddate", "operator": "EQ", "value": boundary},
    ]
    assert tie["sorts"][0]["propertyName"] == "hs_object_id"
    assert "after" not in tie
    assert resumed["filterGroups"][0]["filters"] == [
        {"propertyName": "lastmodifieddate", "operator": "GT", "value": boundary},
    ]
    assert resumed["sorts"][0]["propertyName"] == "lastmodifieddate"
    # Boundary record is emitted twice, nothing is missing
    assert len(records) == 10_501
    assert {r["id"] for r in records} == {str(i) for i in range(1, 10_501)}
    assert (
        _parse_ms(stream.get_context_state(None)["replication_key_value"])
        == timestamps[-1]
    )


def test_search_cap_with_identical_boundary_value_completes(http, capsys):
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback([BASE_MS] * 10_500, "lastmodifieddate"),
    )
    stream = _tap(start_date=_iso(BASE_MS)).streams["contacts"]

    records = _sync(stream, capsys)

    bodies = _search_bodies(http, "/objects/contacts/search")
    # key scan (100 pages), tie scan (100 pages), tie restart past id 10000
    # (5 pages), then the key scan resumes after the tie value (1 empty page)
    assert len(bodies) == 100 + 100 + 5 + 1
    assert bodies[200]["filterGroups"][0]["filters"] == [
        {"propertyName": "lastmodifieddate", "operator": "EQ", "value": str(BASE_MS)},
        {"propertyName": "hs_object_id", "operator": "GT", "value": "10000"},
    ]
    assert bodies[-1]["filterGroups"][0]["filters"][0]["operator"] == "GT"
    assert {r["id"] for r in records} == {str(i) for i in range(1, 10_501)}


def test_paging_next_null_ends_pagination(http, capsys):
    http.post(
        f"{BASE}/objects/contacts/search",
        json={
            "results": [{"id": "1", "properties": {}}],
            "paging": {"next": None},
        },
    )
    stream = _tap().streams["contacts"]

    assert len(_sync(stream, capsys)) == 1
    assert len(_search_bodies(http, "/objects/contacts/search")) == 1


@pytest.mark.parametrize("start_date", [None, _iso(BASE_MS)])
def test_missing_paging_at_cap_continues_with_next_scan(http, capsys, start_date):
    timestamps = [BASE_MS + i for i in range(10_500)]
    http.post(
        f"{BASE}/objects/contacts/search",
        json=_make_search_callback(timestamps, "lastmodifieddate", paging_at_cap=False),
    )
    config = {"start_date": start_date} if start_date else {}
    stream = _tap(**config).streams["contacts"]

    records = _sync(stream, capsys)

    assert {r["id"] for r in records} == {str(i) for i in range(1, 10_501)}


def test_dynamic_stream_requires_incremental_path(http):
    class NoSearchStream(DynamicIncrementalHubspotStream):
        name = "contacts"

    with pytest.raises(TypeError, match="incremental_path"):
        NoSearchStream(_tap())


def test_interrupted_key_scan_keeps_resumable_bookmark(http, capsys):
    timestamps = [BASE_MS + i * 1000 for i in range(500)]
    search = _make_search_callback(timestamps, "lastmodifieddate")

    def failing_search(request, context):
        if request.json().get("after") == "200":
            context.status_code = 400
            return {"message": "boom"}
        return search(request, context)

    http.post(f"{BASE}/objects/contacts/search", json=failing_search)
    stream = _tap(start_date=_iso(BASE_MS)).streams["contacts"]

    with pytest.raises(Exception, match="400"):
        _sync(stream, capsys)

    assert stream.is_sorted
    state = stream.get_context_state(None)
    assert "progress_markers" not in state
    assert _parse_ms(state["replication_key_value"]) == timestamps[199]


def _parse_ms(value: str) -> int:
    ts = dt.datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.UTC)
    return int(ts.timestamp() * 1000)
