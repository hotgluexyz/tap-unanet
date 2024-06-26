"""Custom client handling, including UnanetStream base class."""

import pyodbc
import logging
import singer
from typing import Any, Optional, List, Iterable, cast, Union, Type, Dict
from singer.schema import Schema
from singer_sdk import PluginBase, SQLTap, SQLStream, SQLConnector
from singer_sdk.streams import Stream
from singer_sdk import typing as th
from singer_sdk.helpers._singer import CatalogEntry, MetadataMapping


class Singleton(type):
    _instances = dict()

    def __call__(cls, *args: Any, **kwds: Any) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwds)
        return cls._instances[cls]


class OdbcClient(metaclass=Singleton):
    def __init__(
        self,
        server: str,
        port: str,
        database: str,
        username: str,
        password: str,
        logger: logging.Logger,
    ) -> None:
        self.logger = logger

        conn_str = ";".join([
            "DRIVER={CData Virtuality Unicode(x64)}",
            f"SERVER={server}",
            f"PORT={port}",
            f"DATABASE={database}",
            "SSLMODE=require",
            f"UID={username}",
            f"PWD={password}",
        ])

        self._connection = pyodbc.connect(conn_str)

        self.logger.info("ODBC connected")


    def run_query(self, query: str, *params):
        cursor = self._connection.cursor()

        cursor.execute(query, *params)

        row = cursor.fetchone()

        result = list()

        while row is not None:
            result.append(row)
            row = cursor.fetchone()

        cursor.close()

        return result


    def run_query_yield(self, query: str, *params):
        cursor = self._connection.cursor()

        cursor.execute(query, *params)

        row = cursor.fetchone()

        while row is not None:
            yield row
            row = cursor.fetchone()

        cursor.close()


    def get_schema_names(self) -> List[str]:
        return [
            schema_name
            for (schema_name,) in self.run_query_yield("SELECT name FROM sys.schemas")
        ]


    def get_table_names(self, schema_name: str) -> List[str]:
        return [
            table_name
            for (table_name,) in self.run_query_yield("SELECT \"Name\" FROM sys.tables WHERE \"SchemaName\" = ?", schema_name)
        ]


    def get_possible_primary_keys(self, schema_name: str, table_name: str) -> Optional[str]:
        cursor = self._connection.cursor()

        row = cursor.statistics(table=table_name, schema=schema_name, unique=True).fetchone()

        get_attribute_pos = lambda description, attr: next(
            (
                i
                for i, el in enumerate(description)
                if el[0] == attr
            ),
            None
        )

        possible_primary_keys: List[str] = list()

        while row is not None:
            description = row.cursor_description

            index_name_pos = get_attribute_pos(description, "index_name")
            column_name_pos = get_attribute_pos(description, "column_name")

            if index_name_pos is not None and column_name_pos is not None:
                if row[index_name_pos] == f"pk_{table_name}":
                    possible_primary_keys = [row[column_name_pos]]
                    break
                else:
                    possible_primary_keys.append(row[column_name_pos])

            row = cursor.fetchone()

        return possible_primary_keys

    def get_table_column_defs(self, schema_name: str, table_name: str) -> List[Any]:
        cursor = self._connection.cursor()

        row = cursor.execute(f"SELECT * FROM {schema_name}.{table_name} LIMIT 1").fetchone()

        if row is None:
            return []

        desc = row.cursor_description

        return [
            {
                "name": column_name,
                "type": type_code,
                "nullable": nullable
            }
            for (column_name, type_code, _, _, _, _, nullable) in desc
        ]


class UnanetConnector():
    def __init__(self, tap: PluginBase) -> None:
        self.logger: logging.Logger = tap.logger
        self.config: dict = tap.config
        self._odbc_client: OdbcClient = OdbcClient(
            self.config.get('server'),
            self.config.get('port'),
            self.config.get('database'),
            self.config.get('username'),
            self.config.get('password'),
            self.logger,
        )


    @staticmethod
    def get_fully_qualified_name(
        table_name: str,
        schema_name: Optional[str] = None,
        db_name: Optional[str] = None,
        delimiter: str = ".",
    ) -> str:
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema. Defaults to None.
            db_name: The name of the database. Defaults to None.
            delimiter: Generally: '.' for SQL names and '-' for Singer names.

        Raises:
            ValueError: If table_name is not provided or if neither schema_name or
                db_name are provided.

        Returns:
            The fully qualified name as a string.
        """
        if db_name and schema_name:
            result = delimiter.join([db_name, schema_name, table_name])
        elif db_name:
            result = delimiter.join([db_name, table_name])
        elif schema_name:
            result = delimiter.join([schema_name, table_name])
        elif table_name:
            result = table_name
        else:
            raise ValueError(
                "Could not generate fully qualified name for stream: "
                + ":".join(
                    [
                        db_name or "(unknown-db)",
                        schema_name or "(unknown-schema)",
                        table_name or "(unknown-table-name)",
                    ]
                )
            )

        return result

    
    def to_jsonschema_type(
        self,
        sql_type
    ) -> dict:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_jsonschema_type()` for strings and SQLAlchemy
        types.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.

        Args:
            sql_type: The string representation of the SQL type, a SQLAlchemy
                TypeEngine class or object, or a custom-specified object.

        Raises:
            ValueError: If the type received could not be translated to jsonschema.

        Returns:
            The JSON Schema representation of the provided type.
        """
        type_name = sql_type.__name__

        sqltype_lookup: Dict[str, dict] = {
            "timestamp": th.DateTimeType.type_dict,
            "datetime": th.DateTimeType.type_dict,
            "date": th.DateType.type_dict,
            "int": th.IntegerType.type_dict,
            "number": th.NumberType.type_dict,
            "decimal": th.NumberType.type_dict,
            "double": th.NumberType.type_dict,
            "float": th.NumberType.type_dict,
            "string": th.StringType.type_dict,
            "text": th.StringType.type_dict,
            "str": th.StringType.type_dict,
            "char": th.StringType.type_dict,
            "bool": th.BooleanType.type_dict,
            "variant": th.StringType.type_dict,
        }

        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                return jsonschema_type

        return sqltype_lookup["string"]


    def discover_catalog_entries(self):
        result: List[dict] = list()

        for schema_name in self._odbc_client.get_schema_names():
            if schema_name.lower() in ["pg_catalog", "sys"]:
                continue

            table_names = self._odbc_client.get_table_names(schema_name=schema_name)

            for table_name in table_names:
                self.logger.info(f"table {schema_name}.{table_name}")
                unique_stream_id = self.get_fully_qualified_name(
                    db_name=None,
                    schema_name=schema_name,
                    table_name=table_name,
                    delimiter="-"
                )

                possible_primary_keys = self._odbc_client.get_possible_primary_keys(
                    schema_name=schema_name,
                    table_name=table_name,
                )
                key_properties = possible_primary_keys or None

                table_schema = th.PropertiesList()

                for column_def in self._odbc_client.get_table_column_defs(schema_name=schema_name, table_name=table_name):
                    column_name = column_def["name"]
                    is_nullable = column_def["nullable"] or False

                    jsonschema_type: dict = self.to_jsonschema_type(column_def["type"])

                    table_schema.append(
                        th.Property(
                            name=column_name,
                            wrapped=th.CustomType(jsonschema_type),
                            required=not is_nullable,
                        )
                    )

                schema = table_schema.to_dict()

                addl_replication_methods: List[str] = [""]

                replication_method = next(reversed(["FULL_TABLE"] + addl_replication_methods))

                catalog_entry = CatalogEntry(
                    tap_stream_id=unique_stream_id,
                    stream=unique_stream_id,
                    table=table_name,
                    key_properties=key_properties,
                    schema=singer.Schema.from_dict(schema),
                    is_view=False,
                    replication_method=replication_method,
                    metadata=MetadataMapping.get_standard_metadata(
                        schema_name=schema_name,
                        schema=schema,
                        replication_method=replication_method,
                        key_properties=key_properties,
                        valid_replication_keys=None,
                    ),
                    database=None,
                    row_count=None,
                    stream_alias=None,
                    replication_key=None,
                )

                result.append(catalog_entry.to_dict())

                break

        return result



class UnanetStream(Stream):
    """Stream class for Unanet streams."""

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        raise NotImplementedError("The method is not yet implemented (TODO)")
