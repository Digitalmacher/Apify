"""Tests for Apify dataset record shape (schema-safe string values)."""

import json
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from sven_scraping_projects.pipelines import (
    ApifyPipeline,
    _add_legacy_dataset_aliases,
    _canonicalize_item,
    _flatten_for_apify_dataset_schema,
    _normalize_for_dataset,
    _stringify_apify_dataset_record,
)


def _full_pipeline_item(item_dict):
    item_dict = dict(item_dict)
    canonical = _canonicalize_item(item_dict)
    flattened = _flatten_for_apify_dataset_schema(canonical)
    normalized = _normalize_for_dataset(flattened)
    with_aliases = _add_legacy_dataset_aliases(normalized)
    return _stringify_apify_dataset_record(with_aliases)


class TestApifyDatasetRecord(unittest.TestCase):
    def test_all_values_are_strings(self):
        rec = _full_pipeline_item(
            {
                "source": "zahnaerzte_hh",
                "name": "Dr. Test",
                "zip": 20095,
                "street": "Foo 1",
                "city": "Hamburg",
                "specialization": "Implantologie",
                "url": "https://example.com/x",
            }
        )
        for k, v in rec.items():
            self.assertIsInstance(v, str, msg=f"key {k!r} must be str, got {type(v)}")

    def test_actor_json_schema_accepts_record(self):
        actor_path = Path(__file__).resolve().parents[1] / ".actor" / "actor.json"
        self.assertTrue(actor_path.is_file(), "missing .actor/actor.json")
        meta = json.loads(actor_path.read_text(encoding="utf-8"))
        schema = meta["storages"]["dataset"]["fields"]

        sample = _full_pipeline_item(
            {
                "source": "uke",
                "name": "Jane Doe",
                "url": "https://uke.de/p",
                "telephone": "040 123",
                "specialties": "A, B",
                "languages": "Deutsch, Englisch",
            }
        )
        # Minimal draft-07 check: object, known props are strings
        self.assertEqual(schema.get("type"), "object")
        props = schema.get("properties", {})
        for key, val in sample.items():
            self.assertIsInstance(val, str)
            if key in props:
                self.assertEqual(props[key].get("type"), "string")

    def test_raw_source_fields_is_json_string_not_nested_object(self):
        rec = _full_pipeline_item(
            {
                "source": "kvhh",
                "url": "https://kvhh.net/x",
                "first_name": "A",
                "last_name": "B",
                "specialization": "Cardiology",
            }
        )
        raw = rec.get("raw_source_fields", "")
        self.assertIsInstance(raw, str)
        parsed = json.loads(raw)
        self.assertIsInstance(parsed, dict)

    def test_academic_titles_removed_from_name_and_added_to_job_title(self):
        rec = _full_pipeline_item(
            {
                "source": "kvhh",
                "url": "https://kvhh.net/x",
                "name": "Dr. med. Anna Schmidt",
                "position": "Allgemeinmedizin",
            }
        )
        self.assertEqual(rec.get("display_name"), "Anna Schmidt")
        self.assertIn("Dr. med.", rec.get("name_title", ""))
        self.assertIn("Dr. med.", rec.get("job_title", ""))
        self.assertIn("Allgemeinmedizin", rec.get("job_title", ""))
        self.assertNotIn("Dr.", rec.get("name", ""))

    def test_organization_name_not_treated_as_person_titles(self):
        rec = _full_pipeline_item(
            {
                "source": "apothekerkammer-hamburg",
                "entity_type": "organization",
                "name": "Apotheke am Markt 1",
                "url": "https://portal.example/a",
            }
        )
        self.assertEqual(rec.get("display_name"), "Apotheke am Markt 1")

    def test_batching_math(self):
        # Not calling Apify; just ensuring expected number of batches.
        total = 1201
        batch_size = 500
        batches = [list(range(total))[i : i + batch_size] for i in range(0, total, batch_size)]
        self.assertEqual(len(batches), 3)
        self.assertEqual(len(batches[0]), 500)
        self.assertEqual(len(batches[1]), 500)
        self.assertEqual(len(batches[2]), 201)

    def test_streaming_push_batches_before_close(self):
        """
        Regression test for the real Apify failure mode:
        - Actor can get SIGTERM during migrations.
        - If we only push in close_spider, we may lose most items.
        This test ensures we push in streaming batches while crawling.
        """
        import sven_scraping_projects.pipelines as pipelines_mod

        pushed_chunks = []

        async def _push_data(chunk):
            # Simulate network delay to ensure worker batching/flush is exercised.
            await __import__("asyncio").sleep(0.01)
            pushed_chunks.append(list(chunk))

        class _Log:
            def info(self, *args, **kwargs):
                return None
            def warning(self, *args, **kwargs):
                return None
            def error(self, *args, **kwargs):
                return None

        # Monkeypatch Actor used by the pipeline module
        pipelines_mod.Actor = SimpleNamespace(push_data=_push_data, log=_Log())

        spider = SimpleNamespace(name="uke")
        p = ApifyPipeline()
        p.open_spider(spider)

        # Force streaming mode on in this unit test.
        p._apify_available = True

        # Send enough items to require multiple batches.
        for i in range(1200):
            p.process_item({"url": f"https://example.com/{i}", "name": f"Name {i}"}, spider)

        # Wait briefly for worker to flush at least one batch before close_spider.
        deadline = time.time() + 2.0
        while time.time() < deadline and len(pushed_chunks) < 1:
            time.sleep(0.02)

        self.assertGreaterEqual(
            len(pushed_chunks),
            1,
            "Expected at least one streaming push before close_spider",
        )

        # Now finalize; should flush remaining data without errors.
        d = p.close_spider(spider)
        # close_spider returns a Deferred in Apify mode; we just wait a moment for the join.
        time.sleep(0.5)

        total_pushed = sum(len(c) for c in pushed_chunks)
        self.assertEqual(total_pushed, 1200)


if __name__ == "__main__":
    unittest.main()
