"""Unanet tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  
from tap_unanet.streams import (
    GeneralLedgerStream,
    AccountsStream,
    CustomersStream,
    PersonsStream,
    PnLDetailStream,
    ProjectsStream,
)
STREAM_TYPES = [
    GeneralLedgerStream,
    AccountsStream,
    CustomersStream,
    PersonsStream,
    PnLDetailStream,
    ProjectsStream,
]


class TapUnanet(Tap):
    """Unanet tap class."""
    name = "tap-unanet"

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

if __name__ == "__main__":
    TapUnanet.cli()