from typing import Any, Dict, Iterator, LiteralString, Optional, cast

from psycopg import connect
from psycopg.sql import SQL


class RelationalDbReader:
    """Reader for streaming data from relational databases."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def read_stream(self, query: Optional[str] = None, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """
        Stream rows from database query or table.

        Args:
            query: SQL query to execute. If None, reads from table_name
            batch_size: Number of rows to fetch at a time
        """
        assert query is not None, "Query must be provided for relational database reader"

        with connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                # NOTE: This executes user-provided queries, this serice MUST
                # only have read-only rights
                cur.execute(SQL(cast(LiteralString, query)))

                assert cur.description is not None, "Query did not return any columns"
                columns = [desc[0] for desc in cur.description]

                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break

                    for row in rows:
                        yield dict(zip(columns, row))

    def get_schema(self) -> Dict[str, Any]:
        """Get schema information from the database, including foreign keys."""
        with connect(self.connection_string) as conn:
            dump = {}

            with conn.cursor() as cur:
                # 1️⃣ get all user tables
                cur.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type='BASE TABLE'
                    AND table_schema NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY table_schema, table_name;
                """)
                tables = cur.fetchall()

                for schema_name, table_name in tables:
                    full_name = f"{schema_name}.{table_name}"

                    # 2️⃣ get column definitions
                    cur.execute("""
                        SELECT
                            column_name,
                            data_type,
                            is_nullable,
                            column_default
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position;
                    """, (schema_name, table_name))
                    columns = cur.fetchall()

                    col_defs = []
                    for col_name, data_type, is_nullable, col_default in columns:
                        col_line = f'"{col_name}" {data_type.upper()}'
                        if is_nullable == "NO":
                            col_line += " NOT NULL"
                        if col_default is not None:
                            col_line += f" DEFAULT {col_default}"
                        col_defs.append(col_line)

                    # 3️⃣ get primary key / unique constraints
                    cur.execute("""
                        SELECT
                            tc.constraint_type,
                            kcu.column_name,
                            tc.constraint_name
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema = %s
                        AND tc.table_name = %s
                        AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE')
                        ORDER BY kcu.ordinal_position;
                    """, (schema_name, table_name))
                    constraints = cur.fetchall()

                    cons_defs = []
                    pk_columns = [col_name for ctype, col_name,
                                  _ in constraints if ctype == 'PRIMARY KEY']
                    if pk_columns:
                        cons_defs.append(
                            f"PRIMARY KEY ({', '.join([f'\"{c}\"' for c in pk_columns])})")

                    unique_columns = [col_name for ctype, col_name,
                                      _ in constraints if ctype == 'UNIQUE']
                    for col in unique_columns:
                        cons_defs.append(f"UNIQUE (\"{col}\")")

                    # 4️⃣ get foreign keys
                    cur.execute("""
                        SELECT
                            kcu.column_name,
                            ccu.table_schema AS foreign_table_schema,
                            ccu.table_name AS foreign_table,
                            ccu.column_name AS foreign_column,
                            tc.constraint_name
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = %s
                        AND tc.table_name = %s;
                    """, (schema_name, table_name))
                    fks = cur.fetchall()
                    for col, f_schema, f_table, f_col, cons_name in fks:
                        cons_defs.append(
                            f'CONSTRAINT "{cons_name}" FOREIGN KEY ("{col}") REFERENCES "{f_schema}"."{f_table}"("{f_col}")')

                    # 5️⃣ combine all
                    all_defs = col_defs + cons_defs
                    create_table_sql = f'CREATE TABLE "{schema_name}"."{table_name}" (\n    ' + ",\n    ".join(
                        all_defs) + "\n);"

                    dump[full_name] = create_table_sql

            return dump
