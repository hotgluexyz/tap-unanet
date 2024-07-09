"""Unanet tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_unanet.streams import (
    UnanetStream,
    GeneralLedgerStream,
    AccountsStream,
    CustomersStream,
    PersonsStream,
    PnLDetailStream,
)
from tap_unanet.client import UnanetConnector
STREAM_TYPES = [
    GeneralLedgerStream,
    AccountsStream,
    CustomersStream,
    PersonsStream,
    PnLDetailStream,
]


class TapUnanet(Tap):
    """Unanet tap class."""
    name = "tap-unanet"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "server",
            th.StringType,
            required=True,
            description="Server address"
        ),
        th.Property(
            "port",
            th.StringType,
            required=True,
            description="Server port"
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="Database name"
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="Username of the database"
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Password of the user of database"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        # conn = UnanetConnector(self)
        # return conn.discover_catalog_entries()
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
