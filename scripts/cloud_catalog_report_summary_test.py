import contextlib
import io
import json
import os
import unittest
from unittest import mock

from scripts import cloud_catalog_report_summary as summary


REPORTS_ID = "reports_folder_id_0000000000000001"
OAUTH = ("client-id", "client-secret", "refresh-token")


def report_payload():
    return {
        "schema_version": 3,
        "record_type": "heeexy_catalog_inventory_report",
        "generated_at": "2026-10-08T12:34:56Z",
        "manifest": {
            "filename": "private.jsonl",
            "sha256": "do-not-print",
            "size_bytes": 1234,
            "accepted_products": 12,
        },
        "inventory": {"products_discovered": 14, "svg_files": 9},
        "metadata": {"files_read": 2, "rows_parsed": 18},
        "reconciliation": {
            "accepted": 12,
            "held": 2,
            "hold_counts": {"metadata_missing": 2},
            "unmatched_metadata_rows": 4,
            "ambiguous_metadata_rows": 1,
            "warnings": {"duplicate_metadata": 3},
            "holds": [
                {
                    "title": "private product title",
                    "source_file_id": "private-drive-file-id",
                    "reasons": ["metadata_missing"],
                }
            ],
        },
        "publication_gate": {
            "eligible": False,
            "expected_min_products": 20,
            "expected_min_accepted": 20,
            "global_inventory_target": 20,
            "global_product_target": 20,
            "curated_seed_target": 20,
            "global_inventory_assets": 14,
            "authoritative_seeds": 14,
            "curated_seed_count": 14,
            "global_product_gap": 6,
            "inventory_shortfall": 6,
            "curated_seed_shortfall": 6,
            "accepted_shortfall": 8,
            "complete_inventory_coverage": False,
            "complete_seed_disposition": True,
            "full_catalog_complete": False,
            "pilot_manifest_eligible": False,
            "reviewed_holds": 2,
            "reviewed_hold_limit": 40,
            "unresolved_holds": 0,
            "cross_product_collisions": 0,
            "blocking_issue_count": 0,
            "inventory_blockers": 0,
            "metadata_file_blockers": 0,
        },
        "oauth": "must-never-print",
    }


class ReportSummaryTests(unittest.TestCase):
    def test_parser_emits_only_allowlisted_aggregates(self):
        parsed = summary.parse_report_summary(
            json.dumps(report_payload()).encode("utf-8")
        )

        self.assertEqual(parsed["manifest"], {"accepted_products": 12, "size_bytes": 1234})
        self.assertEqual(parsed["reconciliation"]["hold_counts"], {"metadata_missing": 2})
        rendered = json.dumps(parsed, sort_keys=True)
        self.assertNotIn("holds", parsed["reconciliation"])
        self.assertNotIn("private product title", rendered)
        self.assertNotIn("private-drive-file-id", rendered)
        self.assertNotIn("do-not-print", rendered)
        self.assertNotIn("must-never-print", rendered)
        self.assertNotIn("filename", rendered)

        legacy = report_payload()
        for field in (
            "global_inventory_target",
            "global_product_target",
            "curated_seed_target",
            "global_inventory_assets",
            "authoritative_seeds",
            "curated_seed_count",
            "global_product_gap",
            "curated_seed_shortfall",
            "complete_seed_disposition",
            "full_catalog_complete",
            "pilot_manifest_eligible",
            "reviewed_holds",
            "reviewed_hold_limit",
            "unresolved_holds",
            "cross_product_collisions",
        ):
            legacy["publication_gate"].pop(field, None)
        parsed_legacy = summary.parse_report_summary(json.dumps(legacy).encode("utf-8"))
        self.assertEqual(parsed_legacy["publication_gate"]["authoritative_seeds"], 14)
        self.assertFalse(
            parsed_legacy["publication_gate"]["complete_seed_disposition"]
        )
        self.assertEqual(parsed_legacy["publication_gate"]["reviewed_holds"], 0)
        self.assertEqual(parsed_legacy["publication_gate"]["reviewed_hold_limit"], 0)
        self.assertEqual(parsed_legacy["publication_gate"]["unresolved_holds"], 2)

    def test_parser_rejects_oversize_invalid_and_non_numeric_counters(self):
        with self.assertRaisesRegex(summary.ReportSummaryError, "report_size_invalid"):
            summary.parse_report_summary(b"")
        with mock.patch.object(summary, "MAX_REPORT_BYTES", 2):
            with self.assertRaisesRegex(summary.ReportSummaryError, "report_size_invalid"):
                summary.parse_report_summary(b"{}\n")
        invalid = report_payload()
        invalid["inventory"]["products_discovered"] = "14"
        with self.assertRaisesRegex(summary.ReportSummaryError, "report_inventory_invalid"):
            summary.parse_report_summary(json.dumps(invalid).encode("utf-8"))

    def test_drive_read_uses_generated_role_and_64_mib_limit(self):
        payload = json.dumps(report_payload()).encode("utf-8")

        class FakeDrive:
            def __init__(self, *oauth):
                self.oauth = oauth

            def read_generated_bytes(self, **kwargs):
                self.test_case.assertEqual(self.oauth, OAUTH)
                self.test_case.assertEqual(kwargs["parent_id"], REPORTS_ID)
                self.test_case.assertEqual(kwargs["role"], summary.manifest.REPORT_ROLE)
                self.test_case.assertEqual(kwargs["maximum_bytes"], 64 * 1024 * 1024)
                return payload

        FakeDrive.test_case = self
        parsed = summary.read_report_summary(REPORTS_ID, OAUTH, drive_factory=FakeDrive)
        self.assertEqual(parsed["reconciliation"]["held"], 2)

        class MissingDrive(FakeDrive):
            def read_generated_bytes(self, **kwargs):
                return None

        with self.assertRaisesRegex(summary.ReportSummaryError, "generated_report_not_found"):
            summary.read_report_summary(REPORTS_ID, OAUTH, drive_factory=MissingDrive)

    def test_config_requires_report_folder_and_all_oauth_values(self):
        environment = {
            "HEEEXY_REPORTS_FOLDER_ID": REPORTS_ID,
            "GOOGLE_DRIVE_OAUTH_CLIENT_ID": OAUTH[0],
            "GOOGLE_DRIVE_OAUTH_CLIENT_SECRET": OAUTH[1],
            "GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN": OAUTH[2],
        }
        self.assertEqual(summary.load_summary_config(environment), (REPORTS_ID, OAUTH))
        for key in tuple(environment):
            invalid = dict(environment)
            invalid[key] = ""
            with self.assertRaises(summary.ReportSummaryError):
                summary.load_summary_config(invalid)

    def test_main_refuses_local_runtime_before_drive_access(self):
        stderr = io.StringIO()
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch.object(summary, "read_report_summary") as read_report,
            contextlib.redirect_stderr(stderr),
        ):
            result = summary.main([])

        self.assertEqual(result, 2)
        self.assertFalse(read_report.called)
        self.assertEqual(
            stderr.getvalue().strip(),
            "manifest_report_summary_status=failed code=cloud_runtime_required",
        )

    def test_successful_main_prints_only_publication_boolean(self):
        stdout = io.StringIO()
        private_summary = {
            "publication_gate": {"eligible": True},
            "manifest": {"accepted_products": 999_999},
            "private_file_id": "must-never-print",
        }
        with (
            mock.patch.object(summary.pipeline, "enforce_cloud_runtime"),
            mock.patch.object(summary, "load_summary_config", return_value=(REPORTS_ID, OAUTH)),
            mock.patch.object(summary, "read_report_summary", return_value=private_summary),
            contextlib.redirect_stdout(stdout),
        ):
            result = summary.main([])

        self.assertEqual(result, 0)
        self.assertEqual(
            stdout.getvalue().strip(),
            "manifest_report_summary_status=ok publication_eligible=true",
        )
        self.assertNotIn("999999", stdout.getvalue())
        self.assertNotIn("must-never-print", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
