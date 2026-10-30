"""Tests standard tap features using the built-in SDK tests library."""

import os

import pytest
from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_hubspot.tap import TapHubspot

REQUIRED_ENV = (
    "TAP_HUBSPOT_CLIENT_ID",
    "TAP_HUBSPOT_CLIENT_SECRET",
    "TAP_HUBSPOT_REFRESH_TOKEN",
)

# These tests call the live HubSpot API
if missing := [name for name in REQUIRED_ENV if not os.environ.get(name)]:
    pytest.skip(
        reason=f"HubSpot credentials not set: {', '.join(missing)}",
        allow_module_level=True,
    )

SAMPLE_CONFIG = {
    "start_date": "2023-10-01T00:00:00.0Z",
    "client_id": os.environ["TAP_HUBSPOT_CLIENT_ID"],
    "client_secret": os.environ["TAP_HUBSPOT_CLIENT_SECRET"],
    "refresh_token": os.environ["TAP_HUBSPOT_REFRESH_TOKEN"],
}


TestTapHubspot = get_tap_test_class(
    tap_class=TapHubspot,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        max_records_limit=10,
        ignore_no_records_for_streams=[
            "calls",
            "communications",
            "emails",
            "meetings",
            "notes",
            "postal_mail",
        ],
    ),
)
