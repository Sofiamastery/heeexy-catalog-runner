from __future__ import annotations

import contextlib
import io
from pathlib import Path
import tempfile
import unittest

from scripts import cloud_catalog_batch_summary as summary


def safe_line(**overrides: int) -> str:
    values = {field: 0 for field in summary.SUMMARY_FIELDS}
    values.update(manifest_rows=1_000, manifest_accepted=1_000)
    values.update(overrides)
    return "pipeline_status=ok " + " ".join(
        f"{field}={values[field]}" for field in summary.SUMMARY_FIELDS
    )


class CloudCatalogBatchSummaryTests(unittest.TestCase):
    def test_short_new_batch_continues_to_revision_audit(self) -> None:
        _, selected, continue_required = summary.parse_summary(
            safe_line(selected=12, published=12, verification_deferred=1).encode(), 500)
        self.assertEqual(selected, 12)
        self.assertTrue(continue_required)
        for payload in (safe_line(verification_deferred=1), safe_line(selected=1, published=1, verification_deferred=2)):
            with self.assertRaises(summary.BatchSummaryError):
                summary.parse_summary(payload.encode(), 500)

    def test_valid_summary_exposes_only_bounded_continuation_outputs(self) -> None:
        line, selected, batch_full = summary.parse_summary(
            (safe_line(selected=500, published=499, held=1, failed=1) + "\n").encode(),
            500,
        )
        self.assertTrue(line.startswith("pipeline_status=ok "))
        self.assertEqual(selected, 500)
        self.assertTrue(batch_full)

        _, selected, batch_full = summary.parse_summary(
            safe_line(selected=12, published=12).encode(),
            500,
        )
        self.assertEqual(selected, 12)
        self.assertFalse(batch_full)

        # A permanent artifact-fingerprint failure can be held while scanning,
        # before the bounded batch itself is selected.
        _, selected, batch_full = summary.parse_summary(
            safe_line(selected=50, published=50, held=2, failed=2).encode(),
            50,
        )
        self.assertEqual(selected, 50)
        self.assertTrue(batch_full)

        _, selected, batch_full = summary.parse_summary(
            safe_line(
                manifest_rows=0,
                manifest_accepted=0,
                discovery_accepted=12,
                selected=12,
                published=12,
            ).encode(),
            50,
        )
        self.assertEqual(selected, 12)
        self.assertFalse(batch_full)

    def test_summary_fails_closed_on_unexpected_or_malformed_output(self) -> None:
        invalid_payloads = (
            b"",
            b"source_filename=private.png\n",
            (safe_line(selected=1) + " extra=value\n").encode(),
            safe_line(selected=1).replace("selected=1", "selected=-1").encode(),
            (safe_line(selected=1) + "\n" + safe_line(selected=1)).encode(),
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload[:30]):
                with self.assertRaises(summary.BatchSummaryError):
                    summary.parse_summary(payload, 50)

        with self.assertRaisesRegex(summary.BatchSummaryError, "pipeline_selected_invalid"):
            summary.parse_summary(safe_line(selected=51).encode(), 50)

    def test_summary_fails_closed_on_impossible_success_counters(self) -> None:
        impossible = (
            safe_line(selected=50, candidate_conflicts=1, published=50),
            safe_line(selected=50, published=51),
            safe_line(selected=50, published=49, held=1, failed=0),
            safe_line(selected=50, published=49, held=0, failed=0),
            safe_line(
                selected=2,
                published=2,
                manifest_rows=1,
                manifest_accepted=1,
            ),
            safe_line(manifest_rows=10, manifest_accepted=9, manifest_ignored=0),
        )
        for payload in impossible:
            with self.subTest(payload=payload):
                with self.assertRaisesRegex(
                    summary.BatchSummaryError,
                    "pipeline_summary_semantics_invalid",
                ):
                    summary.parse_summary(payload.encode(), 50)

    def test_batch_size_is_strictly_bounded(self) -> None:
        self.assertEqual(summary.parse_batch_size("500"), 500)
        for value in ("0", "501", "050", "-1", "1.0", "secret"):
            with self.subTest(value=value):
                with self.assertRaises(summary.BatchSummaryError):
                    summary.parse_batch_size(value)

    def test_cli_prints_only_validated_summary_and_writes_safe_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary_file = root / "summary.txt"
            output_file = root / "output.txt"
            expected = safe_line(selected=3, published=3)
            summary_file.write_text(expected + "\n", encoding="utf-8")
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = summary.main(
                    (
                        "--summary-file",
                        str(summary_file),
                        "--github-output",
                        str(output_file),
                        "--batch-size",
                        "3",
                    )
                )
            self.assertEqual(result, 0)
            self.assertEqual(
                stdout.getvalue(),
                "catalog_batch_status=ok selected=3 batch_full=true\n",
            )
            self.assertEqual(
                output_file.read_text(encoding="utf-8"),
                "selected=3\nbatch_size=3\nbatch_full=yes\n",
            )


class CloudCatalogWorkflowStaticTests(unittest.TestCase):
    def test_manual_drain_is_opt_in_bounded_and_has_isolated_permission(self) -> None:
        workflow = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "catalog-batch.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("drain_catalog:", workflow)
        self.assertIn("default: false", workflow)
        self.assertIn("needs.ingest.outputs.batch_full == 'yes'", workflow)
        self.assertIn("actions: write", workflow)
        self.assertIn("SAFE_BATCH_SIZE: ${{ inputs.batch_size }}", workflow)
        self.assertNotIn("needs.ingest.outputs.batch_size", workflow)
        self.assertNotIn("batch_size: ${{ steps.process_batch.outputs.batch_size }}", workflow)
        self.assertIn('SAFE_BATCH_SIZE" =~ ^(25|50|100|250|500)$', workflow)
        self.assertIn("EXPECTED_REPOSITORY", workflow)
        self.assertNotIn("actions/cache", workflow)
        self.assertNotIn("actions/upload-artifact", workflow)


if __name__ == "__main__":
    unittest.main()
