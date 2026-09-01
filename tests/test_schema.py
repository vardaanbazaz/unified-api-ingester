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
        for col in schema.DDL_COLUMNS:
            self.assertIn(col.name, ddl)
        self.assertIn("id VARCHAR PRIMARY KEY", ddl)

    def test_transform_column_types_matches_column_schema_names(self):
        schema_names = {c.name for c in schema.COLUMN_SCHEMA}
        self.assertEqual(set(schema.TRANSFORM_COLUMN_TYPES.keys()), schema_names)
        self.assertNotIn("ingested_at", schema.TRANSFORM_COLUMN_TYPES)

    def test_update_columns_derived_from_primary_key_flag_not_name(self):
        pk_names = {c.name for c in schema.DDL_COLUMNS if c.primary_key}
        self.assertEqual(pk_names, {"id"})
        self.assertEqual(set(schema.UPDATE_COLUMNS), set(schema.INSERT_COLUMNS) - pk_names)


if __name__ == "__main__":
    unittest.main()
