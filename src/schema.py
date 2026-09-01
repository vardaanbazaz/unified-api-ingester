"""Single source of truth for the `breweries` table schema.

Column order here is authoritative: it drives the DuckDB DDL, the INSERT
column list, and the UPDATE SET column list in load.py, and the column
name/dtype declarations transform.py uses to build its output DataFrame.
"""

from typing import List, NamedTuple, Optional


class ColumnDef(NamedTuple):
    """One column's definition. `primary_key` is the flag UPDATE_COLUMNS
    derives from — never compare a column name to a literal string to
    decide primary-key-ness."""

    name: str
    sql_type: str
    dtype: Optional[str] = None  # pandas dtype; None if not in transform.py's column_type_map
    primary_key: bool = False


COLUMN_SCHEMA: List[ColumnDef] = [
    ColumnDef("id", "VARCHAR", "string", primary_key=True),
    ColumnDef("name", "VARCHAR", "string"),
    ColumnDef("brewery_type", "VARCHAR", "string"),
    ColumnDef("address_1", "VARCHAR", "string"),
    ColumnDef("address_2", "VARCHAR", "string"),
    ColumnDef("address_3", "VARCHAR", "string"),
    ColumnDef("city", "VARCHAR", "string"),
    ColumnDef("state_province", "VARCHAR", "string"),
    ColumnDef("postal_code", "VARCHAR", "string"),
    ColumnDef("country", "VARCHAR", "string"),
    ColumnDef("longitude", "DOUBLE", "float64"),
    ColumnDef("latitude", "DOUBLE", "float64"),
    ColumnDef("phone", "VARCHAR", "string"),
    ColumnDef("website_url", "VARCHAR", "string"),
    ColumnDef("state", "VARCHAR", "string"),
    ColumnDef("street", "VARCHAR", "string"),
]

# ingested_at is injected by transform() itself (a timestamp, set outside
# column_type_map) and is not part of PayloadTransformer's type map — it's
# appended here only so DDL/INSERT column lists include it.
INGESTED_AT_COLUMN = ColumnDef("ingested_at", "TIMESTAMPTZ")

# Full DDL column list, in DDL order, including id and trailing ingested_at.
DDL_COLUMNS: List[ColumnDef] = COLUMN_SCHEMA + [INGESTED_AT_COLUMN]

# Columns transform.py's PayloadTransformer.column_type_map should expose.
# Derived from dtype presence, not from excluding a hardcoded column name.
TRANSFORM_COLUMN_TYPES = {c.name: c.dtype for c in COLUMN_SCHEMA if c.dtype is not None}

# INSERT column list: every DDL column, including id. Used to build an
# explicit `INSERT INTO t (col1, col2, ...) SELECT col1, col2, ... FROM view`
# instead of positional `SELECT *`.
INSERT_COLUMNS: List[str] = [c.name for c in DDL_COLUMNS]

# UPDATE SET column list: every DDL column whose `primary_key` flag is
# False. id is never reassigned in ON CONFLICT DO UPDATE because it's
# flagged primary_key=True on its ColumnDef above, not because its name
# happens to be "id".
UPDATE_COLUMNS: List[str] = [c.name for c in DDL_COLUMNS if not c.primary_key]


def build_ddl(table_name: str) -> str:
    """Render CREATE TABLE IF NOT EXISTS DDL for the given table name."""
    cols_sql = ",\n            ".join(f"{c.name} {c.sql_type}" for c in DDL_COLUMNS)
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {cols_sql}
        );
        """
