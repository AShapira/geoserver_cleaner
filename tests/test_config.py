import json
import os
import tempfile
import unittest
from unittest.mock import patch

from app.config import Settings


class SettingsConfigTests(unittest.TestCase):
    def test_external_path_mappings_are_parsed_from_json_object(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_updates = {
                "APP_DATABASE_PATH": os.path.join(temp_dir, "geoserver_cleaner.sqlite3"),
                "GEOSERVER_DATA_DIR": temp_dir,
                "GEOSERVER_EXTERNAL_PATH_MAPPINGS": json.dumps(
                    {
                        r"C:\data\osm": temp_dir,
                    }
                ),
            }
            with patch.dict(os.environ, env_updates, clear=True):
                settings = Settings.from_env()

        self.assertEqual(len(settings.external_path_mappings), 1)
        mapping = settings.external_path_mappings[0]
        self.assertEqual(mapping.geoserver_root, r"C:\data\osm")
        self.assertEqual(mapping.cleaner_root, os.path.abspath(temp_dir))

    def test_invalid_external_path_mapping_json_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_updates = {
                "APP_DATABASE_PATH": os.path.join(temp_dir, "geoserver_cleaner.sqlite3"),
                "GEOSERVER_DATA_DIR": temp_dir,
                "GEOSERVER_EXTERNAL_PATH_MAPPINGS": "{not-json}",
            }
            with patch.dict(os.environ, env_updates, clear=True):
                with self.assertRaisesRegex(ValueError, "must be valid JSON"):
                    Settings.from_env()

    def test_non_object_external_path_mapping_json_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_updates = {
                "APP_DATABASE_PATH": os.path.join(temp_dir, "geoserver_cleaner.sqlite3"),
                "GEOSERVER_DATA_DIR": temp_dir,
                "GEOSERVER_EXTERNAL_PATH_MAPPINGS": '["bad"]',
            }
            with patch.dict(os.environ, env_updates, clear=True):
                with self.assertRaisesRegex(ValueError, "must be a JSON object"):
                    Settings.from_env()


if __name__ == "__main__":
    unittest.main()
