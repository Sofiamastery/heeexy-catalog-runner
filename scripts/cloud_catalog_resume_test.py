from collections import Counter
import contextlib
import io
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from scripts import cloud_catalog_pipeline as p


class ResumeTests(unittest.TestCase):
    def test_short_batch_publishes_before_old_audit_and_next_run_finishes_audit(self):
        candidates = [SimpleNamespace(product_key="old", fingerprint="seed-old"),
                      SimpleNamespace(product_key="new", fingerprint="seed-new")]
        state = {"completed": {"old": {"fingerprint": "artifact-old"}}, "holds": {}}
        drive = Mock(spec=p.DriveClient)
        drive.load_state.return_value = (state, "checkpoint")
        drive.save_state.return_value = "checkpoint"
        config = SimpleNamespace(product_zips_folder_id="zips", reports_folder_id="reports",
            manifest_file_id="manifest", png_only_root_id=None, runner_temp=tempfile.gettempdir(),
            batch_size=500, retry_holds=False)
        def fingerprint(client, candidate, cache):
            return "artifact-" + candidate.product_key, {}
        def outcomes(config, oauth, pending, root, client):
            return iter((index, None) for index in range(len(pending)))
        with patch.object(p, "DriveClient", return_value=drive), \
             patch.object(p, "read_manifest_from_drive", return_value=(candidates, Counter(rows=2, accepted=2))), \
             patch.object(p, "merge_candidates", return_value=(candidates, 0)), \
             patch.object(p, "warm_candidate_metadata"), \
             patch.object(p, "resolve_candidate_artifact_fingerprint", side_effect=fingerprint) as resolver, \
             patch.object(p, "publish_outcomes", side_effect=outcomes), contextlib.redirect_stderr(io.StringIO()):
            first = p.run_pipeline(config, ())
            self.assertEqual(first["selected"], 1)
            self.assertEqual(first["published"], 1)
            self.assertEqual(first["verification_deferred"], 1)
            self.assertEqual(resolver.call_count, 1)
            second = p.run_pipeline(config, ())
            self.assertEqual(second["selected"], 0)
            self.assertEqual(second["verification_deferred"], 0)
            self.assertEqual(second["already_completed"], 2)
            self.assertEqual(resolver.call_count, 3)

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
