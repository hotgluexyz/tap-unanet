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
import singer_sdk.helpers._catalog as catalog
import json


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

        conn_str = ";".join(
            [
                "DRIVER={CData Virtuality Unicode(x64)}",
                f"SERVER={server}",
                f"PORT={port}",
                f"DATABASE={database}",
                "SSLMODE=require",
                f"UID={username}",
                f"PWD={password}",
            ]
        )

        self._connection = pyodbc.connect(conn_str)

        self.logger.info("ODBC connected")

    def run_query(self, query: str, *params):
        cursor = self._connection.cursor()
        self.logger.info(f"Running query: {query}")
        cursor.execute(query, *params)

        row = cursor.fetchone()

        result = list()

        while row is not None:
            result.append(row)
            row = cursor.fetchone()

        cursor.close()
        # self.logger.info(f"Query result: {result}")
        return result

    def run_query_all(self, query: str, *params):
        cursor = self._connection.cursor()
        cursor.execute(query, *params)
        rows = cursor.fetchall()
        for row in rows:
            yield row
        cursor.close()

    def run_query_yield(self, query: str, *params):
        cursor = self._connection.cursor()
        self.logger.info(f"query before: {query}")
        try:
            cursor.execute(query, *params)
            self.logger.info(f"Running query yield: {query}")

            row = cursor.fetchone()

            while row is not None:
                # self.logger.info(f"returning row: {row}")
                yield row
                row = cursor.fetchone()

            cursor.close()
        except Exception as e:
            self.logger.warn(f"Error in row: {row}")
            self.logger.error(f"Error in run_query_yield: {e}")

    def get_schema_names(self) -> List[str]:
        return [
            schema_name
            for (schema_name,) in self.run_query_yield("SELECT name FROM sys.schemas")
        ]

    def get_table_names(self, schema_name: str) -> List[str]:
        return [
            table_name
            for (table_name,) in self.run_query_yield(
                'SELECT "Name" FROM sys.tables WHERE "SchemaName" = ?', schema_name
            )
        ]

    def get_possible_primary_keys(
        self, schema_name: str, table_name: str
    ) -> Optional[str]:
        cursor = self._connection.cursor()

        row = cursor.statistics(
            table=table_name, schema=schema_name, unique=True
        ).fetchone()
        self.logger.info(
            f"Running statistics: schema_name: {schema_name}, table_name: {table_name}, row: {row}"
        )
        get_attribute_pos = lambda description, attr: next(
            (i for i, el in enumerate(description) if el[0] == attr), None
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

        row = cursor.execute(
            f"SELECT * FROM {schema_name}.{table_name} LIMIT 1"
        ).fetchone()
        self.logger.info(
            f"Running get_table_column_defs: {row}, schema_name: {schema_name}, table_name: {table_name}"
        )
        if row is None:
            return []

        desc = row.cursor_description

        return [
            {"name": column_name, "type": type_code, "nullable": nullable}
            for (column_name, type_code, _, _, _, _, nullable) in desc
        ]


class UnanetConnector:
    def __init__(self, tap: PluginBase) -> None:
        self.logger: logging.Logger = tap.logger
        self.config: dict = tap.config
        self._odbc_client: OdbcClient = OdbcClient(
            self.config.get("server"),
            self.config.get("port"),
            self.config.get("database"),
            self.config.get("username"),
            self.config.get("password"),
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
        logging.info(f"Fully qualified name: {result}")
        return result

    def to_jsonschema_type(self, sql_type) -> dict:
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
        self.logger.info(f"schema names: {self._odbc_client.get_schema_names()}")
        for schema_name in self._odbc_client.get_schema_names():
            if schema_name.lower() in ["pg_catalog", "sys"]:
                continue

            table_names = self._odbc_client.get_table_names(schema_name=schema_name)
            self.logger.info(f"tables {table_names}")
            with open(".secrets/tables-list.json", "w") as f:
                json.dump(table_names, f)
            table_names = [
                "person",
            ]
            for table_name in table_names:
                self.logger.info(f"table {schema_name}.{table_name}")
                unique_stream_id = self.get_fully_qualified_name(
                    db_name=None,
                    schema_name=schema_name,
                    table_name=table_name,
                    delimiter="-",
                )

                possible_primary_keys = self._odbc_client.get_possible_primary_keys(
                    schema_name=schema_name,
                    table_name=table_name,
                )
                key_properties = possible_primary_keys or None

                table_schema = th.PropertiesList()

                for column_def in self._odbc_client.get_table_column_defs(
                    schema_name=schema_name, table_name=table_name
                ):
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
                self.logger.info(f"schema: {schema}")
                with open(f".secrets/{table_name}-schema-entries.json", "a") as f:
                    json.dump(
                        {
                            "tap_stream_id": unique_stream_id,
                            "stream": unique_stream_id,
                            "schema_name": schema_name,
                            "table": table_name,
                            "key_properties": key_properties,
                            "schema": schema,
                        },
                        f,
                    )

                addl_replication_methods: List[str] = [""]

                replication_method = next(
                    reversed(["FULL_TABLE"] + addl_replication_methods)
                )

                # catalog_entry = CatalogEntry(
                #     tap_stream_id=unique_stream_id,
                #     stream=unique_stream_id,
                #     table=table_name,
                #     key_properties=key_properties,
                #     schema=singer.Schema.from_dict(schema),
                #     is_view=False,
                #     replication_method=replication_method,
                #     metadata=MetadataMapping.get_standard_metadata(
                #         schema_name=schema_name,
                #         schema=schema,
                #         replication_method=replication_method,
                #         key_properties=key_properties,
                #         valid_replication_keys=None,
                #     ),
                #     database=None,
                #     row_count=None,
                #     stream_alias=None,
                #     replication_key=None,
                #     parent_stream_type=None,
                #     parent_stream_id=None,
                # )
                # self.logger.info(f"catalog_entry: {catalog_entry}")
                # result.append(catalog_entry.to_dict()
                # break

        return result


class UnanetStream(Stream):
    """Stream class for Unanet streams."""

    conn = None
    finished = False
    page_size = 1000
    offset = 0
    page = 0
    total = None
    
    @property
    def schema_name(self):
        return self.config.get('schema_name')

    def next_page_token(self,context: Optional[dict] = None) -> Any:
        if self.total is None:
            self.total = self.get_total(context)
        offset = self.page_size * self.page
        self.page += 1
        self.offset = offset
        if offset >= self.total:
            self.finished = True
        return self.offset
    
    def get_total(self,context: Optional[dict] = None) -> Any:
        query = f"SELECT COUNT(*) AS total FROM {self.schema_name}.{self.table_name}"
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                query = query + f" WHERE {self.replication_key} >= '{start_date}'"
        connection = self.get_connection()
        total =  list(connection._odbc_client.run_query(query))
        if len(total) > 0:
            self.total = total[0][0]
        else:
            self.total = 0    
        self.logger.info(f"{self.name} total: {self.total}")
        return self.total

    def get_connection(self):
        if not self.conn:
            self.conn = UnanetConnector(self)
        return self.conn

    def get_selected_schema(self) -> dict:
        """Return a copy of the Stream JSON schema, dropping any fields not selected.

        Returns:
            A dictionary containing a copy of the Stream JSON schema, filtered
            to any selection criteria.
        """
        return catalog.get_selected_schema(
            stream_name=self.name,
            schema=self.schema,
            mask=self.mask,
            logger=self.logger,
        )

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        try:
            selected_column_names = self.get_selected_schema()["properties"].keys()
            self.logger.info(f"selected_column_names: {selected_column_names}")
            combined_dict = dict(zip(selected_column_names, row))
            return combined_dict
        except Exception as e:
            self.logger.error(f"Error in post_process: {e}")
            print(f"Error in post_process: {row}")
            return None

    def request_records(self, context: dict | None) -> Iterable[dict]:
        while not self.finished:
            selected_column_names = list(self.get_selected_schema()["properties"].keys())
            selected_column_names = ", ".join(selected_column_names)
            
            query = f"SELECT {selected_column_names} FROM {self.schema_name}.{self.table_name}"
            if self.replication_key:
                start_date = self.get_starting_timestamp(context)
                if start_date:
                    start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    query = query + f" WHERE {self.replication_key} >= '{start_date}'"
                query = query + f" ORDER BY {self.replication_key}"
            offset = self.next_page_token()
            query = (
                query + f" OFFSET {offset} ROWS FETCH NEXT {self.page_size} ROWS ONLY"
            )
            self.logger.info(f"Running get_records: {query}")
            connection = self.get_connection()
            yield from connection._odbc_client.run_query_yield(query)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        for record in self.request_records(context):
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            yield transformed_record
