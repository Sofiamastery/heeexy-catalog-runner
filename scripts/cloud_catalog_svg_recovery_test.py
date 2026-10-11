import copy
import json
import unittest
from unittest.mock import Mock

from scripts import cloud_catalog_svg_recovery as s


def row():
    return {"identity": "s" * 32, "source_file_id": "s" * 32, "source_kind": "loose_svg",
        "screening_evidence": s.LEGACY_SCREENING, "ai_provenance": "Human-original/non-generative SVG campaign",
        "metadata_recovery_status": "complete_recovered", "rights_gate": "Campaign-gated; preserve evidence",
        "title": "Original floral design", "description": "An original floral illustration for craft projects.",
        "keywords": "floral, illustration, craft", "category": "Arts"}


def parse(rows, claimed=None):
    return s.parse_ledger(("\n".join(json.dumps(item) for item in rows)).encode(), "example-store", claimed or set())


class SvgRecoveryTests(unittest.TestCase):
    def test_exact_screened_source_preserves_original_metadata(self):
        item = row()
        seeds, holds, stats = parse([item])
        self.assertEqual(holds, [])
        self.assertEqual(stats["ready_metadata_rows"], 1)
        self.assertEqual(seeds[0].svg_file_ids, (item["source_file_id"],))
        self.assertEqual(seeds[0].png_file_ids, ())  # Existing cloud renderer generates its PNG.
        self.assertEqual(seeds[0].metadata_evidence[0].description, item["description"])

    def test_existing_source_is_not_republished_or_rewritten(self):
        seeds, holds, stats = parse([row()], {"s" * 32})
        self.assertEqual((seeds, holds), ([], []))
        self.assertEqual(stats["existing_source_rows"], 1)

    def test_unverified_and_incomplete_sources_remain_holds(self):
        for key, value in (("description", ""), ("keywords", ""), ("screening_evidence", "local=clear; visual=unscreened"),
                           ("rights_gate", "blocked"), ("historical_hold_reason", "copyright review"),
                           ("source_kind", "archive_svg_member"), ("identity", "other")):
            with self.subTest(key=key):
                item = {**row(), key: value}
                seeds, holds, _ = parse([item])
                self.assertEqual(seeds, [])
                self.assertEqual(len(holds), 1)

    def test_duplicate_identity_fails_closed(self):
        with self.assertRaisesRegex(s.m.ManifestBuilderError, "identity_invalid"):
            parse([row(), copy.deepcopy(row())])

    def test_optional_configuration_is_explicit_and_partial_config_fails(self):
        client = Mock()
        records, report = s.discover(client, {}, [], lambda record: set())
        self.assertEqual(records, [])
        self.assertFalse(report["configured"])
        client.assert_not_called()
        with self.assertRaisesRegex(s.m.ManifestBuilderError, "configuration_invalid"):
            s.discover(client, {"HEEEXY_SVG_RECOVERY_LEDGER_ID": "s" * 32}, [], lambda record: set())


if __name__ == "__main__":
    unittest.main()
