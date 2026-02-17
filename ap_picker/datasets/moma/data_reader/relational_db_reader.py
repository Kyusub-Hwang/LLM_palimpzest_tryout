from typing import Any, Dict, Iterator, LiteralString, cast

from psycopg import connect
from psycopg.sql import SQL


class RelationalDbReader:
    """Reader for streaming data from relational databases."""

    def __init__(self, db_name: str):
        # TODO: Remove harcode
        self.connection_url = f"postgresql://provdemo:provdemo@postgres:5432/{db_name}"

    def read_stream(self, query: str = "", batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """
        Stream rows from database query or table.

        Args:
            query: SQL query to execute. If None, reads from table_name
            batch_size: Number of rows to fetch at a time
        """

        if query == "":
            raise ValueError(
                "Query must be provided for relational database reader")

        with connect(self.connection_url) as conn:
            with conn.cursor() as cur:
                # NOTE: This executes user-provided queries, this serice MUST
                # only have read-only rights
                cur.execute(SQL(cast(LiteralString, query)))

                columns = [desc[0] for desc in cur.description]

                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break

                    for row in rows:
                        yield dict(zip(columns, row))

    def get_schema(self) -> Dict[str, Any]:
        """Get schema information from the database."""

        with connect(self.connection_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""            
            SELECT
                table_schema,
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name, ordinal_position;
            """)

                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                schema = []
                for row in rows:
                    schema.append(dict(zip(columns, row)))
        return schema
