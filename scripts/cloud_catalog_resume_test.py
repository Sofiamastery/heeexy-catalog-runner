from collections import Counter
import contextlib
import io
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from scripts import cloud_catalog_pipeline as p


class ResumeTests(unittest.TestCase):
    def test_metadata_lookahead_preserves_cache_and_does_not_hide_failed_lookup(self):
        candidates = [SimpleNamespace(source_id=key) for key in ("cached", "new", "new", "failed")]
        calls = []
        class Drive:
            def get_metadata(self, key, **kwargs):
                calls.append(key)
                if key == "failed":
                    raise p.PipelineError("drive_metadata_failed", permanent=True)
                return {"id": key, "version": "new-version"}
        cache = {"cached": {"id": "cached", "version": "original-version"}}
        with patch.object(p, "candidate_source_references", side_effect=lambda c: [("svg", c.source_id)]):
            p.warm_candidate_metadata(Drive, candidates, cache)
        self.assertCountEqual(calls, ["new", "failed"])
        self.assertNotIn("failed", cache)  # Serial selector must still handle it.
        self.assertEqual(cache["cached"]["version"], "original-version")
        self.assertEqual(cache["new"]["version"], "new-version")

    def test_one_retry_then_stop_no_infinite_loop(self):
        error = p.PipelineError("drive_download_failed", diagnostic="drive_http=503 drive_reason=unknown")
        with patch.object(p, "run_pipeline", side_effect=[error, Counter(published=2)]) as run, patch.object(p.time, "sleep"):
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(p.run_with_bounded_resume(None, ()), Counter(published=2))
            self.assertEqual(run.call_count, 2)
        with patch.object(p, "run_pipeline", side_effect=error) as run, patch.object(p.time, "sleep"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(p.PipelineError):
                p.run_with_bounded_resume(None, ())
            self.assertEqual(run.call_count, 2)

    def test_auth_permission_unknown_and_permanent_never_retry(self):
        for error in (p.PipelineError("drive_oauth_failed"), p.PipelineError("drive_download_failed", diagnostic="drive_http=403 drive_reason=unknown"),
                      p.PipelineError("unexpected_failure"), p.PipelineError("drive_download_failed", permanent=True, diagnostic="drive_http=503 drive_reason=unknown")):
            self.assertFalse(p.batch_error_can_resume(error))

    def test_coverage_conserves_manifest_and_never_claims_whole_library(self):
        candidates = [SimpleNamespace(product_key=key) for key in ("a", "b", "c")]
        state = {"completed": {"a": {}, "old": {}}, "holds": {"b": {"reason": "png_dimensions_invalid"}}}
        result = p.checkpoint_coverage(candidates, state)
        self.assertEqual(result["manifest_candidates"], 3)
        self.assertEqual(result["checkpoint_completed"], 1)
        self.assertEqual(result["checkpoint_unseen"], 1)
        self.assertEqual(result["status"], "more_work")
        state["completed"]["c"] = {}
        result = p.checkpoint_coverage(candidates, state)
        self.assertEqual(result["status"], "manifest_has_holds")
        self.assertFalse(result["full_library_complete"])

    def test_progress_is_counts_only(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            p.safe_progress("publish", 10, 500)
        self.assertEqual(output.getvalue(), "catalog_progress phase=publish processed=10 total=500\n")


if __name__ == "__main__":
    unittest.main()
