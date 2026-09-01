# Schema Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a single source of truth for the `breweries` table schema (`src/schema.py`), consumed by `load.py` for DDL and INSERT/UPDATE column lists, and by `transform.py` for its column name/dtype declarations only — without touching `transform.py`'s numeric coercion or `.str.strip()` logic.

**Architecture:** `src/schema.py` declares an ordered list of `(column_name, sql_type, dtype)` tuples plus derived constants (`DDL`, `INSERT_COLUMNS`, `UPDATE_COLUMNS`). `load.py`'s `create_table_if_not_exists` and `load()` are rewritten to consume those constants instead of hardcoding the DDL string and the `cols` list. `transform.py`'s `column_type_map` is replaced with a value derived from `schema.py`, but the body of `transform()` (the `pd.to_numeric` coercion loop and the `.astype("string").str.strip()` loop) is left byte-for-byte as-is.

**Tech Stack:** Python, pandas, DuckDB, unittest (existing `tests/test_pipeline.py` pattern).

**Spec:** This document (no separate upstream spec; requirements captured below from user confirmation in-session).

## Global Constraints

- `id` stays the DuckDB `PRIMARY KEY`; column order in `schema.py` must match the existing DDL order in `load.py:53-73` exactly, so no migration is needed for existing `.duckdb` files.
- `transform.py`'s coercion logic (`pd.to_numeric` for lat/long, `.str.strip()` for string columns) must not move into `schema.py` — confirmed explicitly, not a proposal to revisit.
- No behavior change to public method signatures (`DuckDBLoader.load`, `DuckDBLoader.create_table_if_not_exists`, `PayloadTransformer.transform`) — this is an internal refactor, not a feature change.
- Existing tests in `tests/test_pipeline.py` must keep passing unmodified except where a task explicitly adds new assertions.

---

## Design: `src/schema.py`

```python
"""Single source of truth for the `breweries` table schema.

Column order here is authoritative: it drives the DuckDB DDL, the INSERT
column list, and the UPDATE SET column list in load.py, and the column
name/dtype declarations transform.py uses to build its output DataFrame.
"""

from typing import List, Tuple

# (column_name, sql_type, pandas_dtype)
# pandas_dtype is None for columns transform.py sets outside column_type_map
# (currently only "ingested_at", set separately as a timestamp).
COLUMN_SCHEMA: List[Tuple[str, str, str]] = [
    ("id", "VARCHAR", "string"),
    ("name", "VARCHAR", "string"),
    ("brewery_type", "VARCHAR", "string"),
    ("address_1", "VARCHAR", "string"),
    ("address_2", "VARCHAR", "string"),
    ("address_3", "VARCHAR", "string"),
    ("city", "VARCHAR", "string"),
    ("state_province", "VARCHAR", "string"),
    ("postal_code", "VARCHAR", "string"),
    ("country", "VARCHAR", "string"),
    ("longitude", "DOUBLE", "float64"),
    ("latitude", "DOUBLE", "float64"),
    ("phone", "VARCHAR", "string"),
    ("website_url", "VARCHAR", "string"),
    ("state", "VARCHAR", "string"),
    ("street", "VARCHAR", "string"),
]

# Columns transform.py's PayloadTransformer.column_type_map should expose.
# Excludes ingested_at: that column is injected by transform() itself,
# outside column_type_map, and is not part of the DDL-authoritative list
# below either (it's appended separately in both load.py and transform.py).
TRANSFORM_COLUMN_TYPES = {name: dtype for name, _, dtype in COLUMN_SCHEMA}

# Full DDL column list, in DDL order, including id and trailing ingested_at.
DDL_COLUMNS: List[Tuple[str, str]] = [(name, sql) for name, sql, _ in COLUMN_SCHEMA] + [
    ("ingested_at", "TIMESTAMPTZ")
]

# INSERT column list: every DDL column, including id. Used to build an
# explicit `INSERT INTO t (col1, col2, ...) SELECT col1, col2, ... FROM view`
# instead of positional `SELECT *`.
INSERT_COLUMNS: List[str] = [name for name, _ in DDL_COLUMNS]

# UPDATE SET column list: every DDL column except id (id is the conflict
# target and is never reassigned in ON CONFLICT DO UPDATE).
UPDATE_COLUMNS: List[str] = [name for name in INSERT_COLUMNS if name != "id"]


def build_ddl(table_name: str) -> str:
    """Render CREATE TABLE IF NOT EXISTS DDL for the given table name."""
    cols_sql = ",\n            ".join(f"{name} {sql}" for name, sql in DDL_COLUMNS)
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {cols_sql}
        );
        """
```

Rationale for shape:
- One ordered list (`COLUMN_SCHEMA`) is the only place a new column gets added. Everything else derives from it, so DDL, INSERT, UPDATE, and transform's type map cannot drift apart silently.
- `ingested_at` stays a special case appended after the derived list in both `load.py` (already true today, DDL line 71) and `transform.py` (already true today, `transform.py:81`) — this refactor does not change where that column is set, only where its *name and position* are declared for DDL/INSERT purposes.
- `build_ddl()` lives in `schema.py`, not `load.py`, so the DDL string and the column-order source can never diverge.

## Decision: INSERT column list — positional `SELECT *` vs. explicit column list

**Recommendation: switch to an explicit column list**, i.e.:

```python
insert_cols_sql = ", ".join(INSERT_COLUMNS)
upsert_query = f"""
INSERT INTO {self.table_name} ({insert_cols_sql})
SELECT {insert_cols_sql} FROM df_temp_view
ON CONFLICT (id) DO UPDATE SET
{update_clause};
"""
```

**Tradeoff:**

| | Positional (`SELECT *`, current) | Explicit column list (proposed) |
|---|---|---|
| Correctness | Silently relies on `transform.py`'s DataFrame column order matching DDL order. If `column_type_map` in `transform.py` is ever reordered, or a column is added/removed on one side but not the other, DuckDB does **not** error — it inserts values into the wrong columns. Nearly every column here is `VARCHAR`, so a swap (e.g. `city`/`state_province`) type-checks fine and just silently corrupts data. | Column identity is explicit at both the INSERT and SELECT list. A DataFrame column reorder has zero effect on which value lands in which column. A column *count* mismatch (missing/extra column) still raises a clear DuckDB binder error instead of silently misassigning. |
| Verbosity | Shorter query string. | Slightly longer (two extra column lists), fully generated from `schema.py`, so no hand-maintenance cost. |
| Coupling to transform.py | Tight, implicit — depends on `PayloadTransformer.transform()`'s final column order (`transform.py:76-77`) never changing. | Loose — `load.py` only requires that `df_temp_view` *contains* the named columns, not that they appear in any particular order. |
| Performance | Identical — DuckDB positional and named INSERT/SELECT compile to the same plan for this data volume. | Identical. |

The current code already reorders columns deliberately (`transform.py:76-77`, `df = df[target_columns].copy()`), which shows the positional dependency is real, not hypothetical — it exists purely so `SELECT *` lines up with the DDL. That coupling is exactly what an explicit column list removes. Given nearly all columns share the same SQL type, a silent-corruption failure mode is worse than the trivial verbosity cost, so explicit columns win.

## Design: `transform.py` change (structural only)

Only this changes:

```python
# before
self.column_type_map = {
    "id": "string",
    "name": "string",
    ... # 16 hardcoded entries
}

# after
from src.schema import TRANSFORM_COLUMN_TYPES

self.column_type_map = dict(TRANSFORM_COLUMN_TYPES)
```

Everything else in `transform.py` — the `pd.to_numeric(df[col], errors="coerce")` loop (`transform.py:66-68`), the `.astype("string").str.strip()` loop (`transform.py:70-73`), `_create_empty_dataframe`, column reordering, `ingested_at` injection, `dropna(subset=["id"])` — is untouched. This is a rename/import of the *declaration*, not a change to any transformation behavior.

---

## Task 1: Create `src/schema.py`

**Files:**
- Create: `src/schema.py`
- Test: `tests/test_schema.py`

**Interfaces:**
- Produces: `COLUMN_SCHEMA: List[Tuple[str, str, str]]`, `TRANSFORM_COLUMN_TYPES: Dict[str, str]`, `DDL_COLUMNS: List[Tuple[str, str]]`, `INSERT_COLUMNS: List[str]`, `UPDATE_COLUMNS: List[str]`, `build_ddl(table_name: str) -> str`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_schema.py
import unittest

from src import schema


class TestSchema(unittest.TestCase):
    def test_insert_columns_includes_id_and_ingested_at(self):
        self.assertIn("id", schema.INSERT_COLUMNS)
        self.assertIn("ingested_at", schema.INSERT_COLUMNS)
        self.assertEqual(schema.INSERT_COLUMNS[0], "id")
        self.assertEqual(schema.INSERT_COLUMNS[-1], "ingested_at")

    def test_update_columns_excludes_id_only(self):
        self.assertNotIn("id", schema.UPDATE_COLUMNS)
        self.assertEqual(len(schema.UPDATE_COLUMNS), len(schema.INSERT_COLUMNS) - 1)
        self.assertEqual(set(schema.INSERT_COLUMNS) - set(schema.UPDATE_COLUMNS), {"id"})

    def test_build_ddl_contains_all_columns_and_primary_key(self):
        ddl = schema.build_ddl("breweries")
        self.assertIn("CREATE TABLE IF NOT EXISTS breweries", ddl)
        for name, _ in schema.DDL_COLUMNS:
            self.assertIn(name, ddl)
        self.assertIn("id VARCHAR", ddl)

    def test_transform_column_types_matches_column_schema_names(self):
        schema_names = {name for name, _, _ in schema.COLUMN_SCHEMA}
        self.assertEqual(set(schema.TRANSFORM_COLUMN_TYPES.keys()), schema_names)
        self.assertNotIn("ingested_at", schema.TRANSFORM_COLUMN_TYPES)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_schema.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.schema'`

- [ ] **Step 3: Write `src/schema.py`**

Use the full module body from the "Design: `src/schema.py`" section above.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_schema.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add src/schema.py tests/test_schema.py
git commit -m "feat(schema): add single-source-of-truth breweries table schema"
```

---

## Task 2: Refactor `load.py` to consume `schema.py`

**Files:**
- Modify: `src/load.py:47-75` (`create_table_if_not_exists`), `src/load.py:98-111` (INSERT/UPDATE query build)
- Test: `tests/test_pipeline.py` (extend `TestDuckDBLoader`)

**Interfaces:**
- Consumes: `src.schema.build_ddl`, `src.schema.INSERT_COLUMNS`, `src.schema.UPDATE_COLUMNS` (from Task 1).
- Produces: no change to `DuckDBLoader.load(df) -> int` or `create_table_if_not_exists(conn) -> None` signatures.

- [ ] **Step 1: Write the failing test — column-order regression**

This is the test that proves the explicit-column-list fix actually matters: it feeds `load()` a DataFrame whose columns are in a different order than the DDL, something `SELECT *` would silently misassign.

```python
# add to tests/test_pipeline.py, inside class TestDuckDBLoader

def test_load_is_order_independent(self):
    """Column order in the DataFrame must not affect which value lands where."""
    import pandas as pd
    from src.load import DuckDBLoader

    loader = DuckDBLoader(db_path=self.db_path, table_name=self.table_name, config=self.config)

    df = pd.DataFrame([{
        "id": "brewery-1",
        "name": "Test Brewery",
        "brewery_type": "micro",
        "address_1": None, "address_2": None, "address_3": None,
        "city": "Testville",
        "state_province": "TS",
        "postal_code": "00000",
        "country": "USA",
        "longitude": -122.4,
        "latitude": 37.7,
        "phone": None,
        "website_url": None,
        "state": "TS",
        "street": None,
        "ingested_at": pd.Timestamp.now(tz="UTC"),
    }])

    # Shuffle columns so order no longer matches the DDL.
    shuffled = df[list(reversed(df.columns))]

    loader.load(shuffled)

    import duckdb
    conn = duckdb.connect(self.db_path)
    row = conn.execute(
        f"SELECT id, city, state_province FROM {self.table_name} WHERE id = 'brewery-1'"
    ).fetchone()
    conn.close()

    self.assertEqual(row[0], "brewery-1")
    self.assertEqual(row[1], "Testville")
    self.assertEqual(row[2], "TS")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_pipeline.py -k test_load_is_order_independent -v`
Expected: FAIL — either a DuckDB binder/type error, or (worse, silently) the assertions on `row[1]`/`row[2]` mismatch because values landed in swapped columns under the current `SELECT *` implementation.

- [ ] **Step 3: Rewrite `create_table_if_not_exists`**

```python
def create_table_if_not_exists(self, conn: duckdb.DuckDBPyConnection) -> None:
    """Create target database table with primary key if it does not exist.

    Args:
        conn: Active DuckDB connection instance.
    """
    ddl = schema.build_ddl(self.table_name)
    conn.execute(ddl)
    logger.info("Verified table structure for '%s'", self.table_name)
```

Add `from src import schema` to `load.py`'s imports.

- [ ] **Step 4: Rewrite the INSERT/UPDATE query build in `load()`**

```python
# Perform UPSERT (ON CONFLICT DO UPDATE)
insert_cols_sql = ", ".join(schema.INSERT_COLUMNS)
update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in schema.UPDATE_COLUMNS)

upsert_query = f"""
INSERT INTO {self.table_name} ({insert_cols_sql})
SELECT {insert_cols_sql} FROM df_temp_view
ON CONFLICT (id) DO UPDATE SET
{update_clause};
"""
```

This replaces the hardcoded `cols` list (`load.py:99-103`) and the old `SELECT * FROM df_temp_view` body.

- [ ] **Step 5: Run full loader test suite**

Run: `python -m pytest tests/test_pipeline.py -k TestDuckDBLoader -v`
Expected: PASS, including the new `test_load_is_order_independent` and the existing idempotency test.

- [ ] **Step 6: Commit**

```bash
git add src/load.py tests/test_pipeline.py
git commit -m "refactor(load): derive DDL and use explicit INSERT column list from schema.py"
```

---

## Task 3: Point `transform.py`'s `column_type_map` at `schema.py`

**Files:**
- Modify: `src/transform.py:18-37` (`__init__`) only
- Test: `tests/test_pipeline.py` (extend `TestPayloadTransformer`)

**Interfaces:**
- Consumes: `src.schema.TRANSFORM_COLUMN_TYPES` (from Task 1).
- Produces: no change to `PayloadTransformer.transform(raw_records) -> pd.DataFrame` or `_create_empty_dataframe()`.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/test_pipeline.py, inside class TestPayloadTransformer

def test_column_type_map_matches_schema(self):
    from src import schema
    from src.transform import PayloadTransformer

    transformer = PayloadTransformer()
    self.assertEqual(transformer.column_type_map, schema.TRANSFORM_COLUMN_TYPES)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_pipeline.py -k test_column_type_map_matches_schema -v`
Expected: FAIL — `transformer.column_type_map` is a locally hardcoded dict, not the same object/values source as `schema.TRANSFORM_COLUMN_TYPES` (fails if the two are ever out of sync; passes trivially only once Task 3's change lands).

- [ ] **Step 3: Replace the hardcoded dict**

```python
from src.schema import TRANSFORM_COLUMN_TYPES

# ...

class PayloadTransformer:
    def __init__(self) -> None:
        """Initialize the payload transformer with predefined schema expectations."""
        self.column_type_map = dict(TRANSFORM_COLUMN_TYPES)
```

No other line in `transform.py` changes — the `pd.to_numeric` loop, the `.str.strip()` loop, `_create_empty_dataframe`, column reordering, and `ingested_at` injection stay exactly as they are today.

- [ ] **Step 4: Run full transformer test suite**

Run: `python -m pytest tests/test_pipeline.py -k TestPayloadTransformer -v`
Expected: PASS, including the new test and all existing coercion/strip/empty-DataFrame tests unchanged.

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest tests/ -v`
Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/transform.py tests/test_pipeline.py
git commit -m "refactor(transform): source column_type_map from schema.py, no logic change"
```

---

## Self-Review

- **Coverage:** `schema.py` creation (Task 1), `load.py` DDL + INSERT/UPDATE derivation (Task 2), `transform.py` column-map sourcing (Task 3) — all three pieces from the in-session discussion are covered. The positional-vs-explicit INSERT decision is resolved and implemented in Task 2, Step 4.
- **Untouched-logic constraint:** Task 3 explicitly leaves the `pd.to_numeric`/`.str.strip()` loops alone; no task moves that logic into `schema.py`.
- **No placeholders:** every step has literal code, not a description of code.
- **Type/name consistency check:** `INSERT_COLUMNS`, `UPDATE_COLUMNS`, `DDL_COLUMNS`, `build_ddl`, `TRANSFORM_COLUMN_TYPES` are named identically everywhere they're referenced across Tasks 1–3.
