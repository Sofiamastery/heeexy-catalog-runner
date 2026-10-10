from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "catalog-batch.yml"
PRIVATE_SOURCE_NAME_HASHES = frozenset(
    {
        "9af93a220e5380917e7b3970de0315edbf7c5cc196c9f15ed1e7e7f501f6909a",
        "77052b60e5d488bfc89b8d8c7d0ff85d3906b5d03d8aee2cf724d0893eaea0fd",
        "c405e57407a43e4cb23b8848efbf60a2544a810d27503e56dd128e94686c3991",
        "66f45101e770ce1a82ee286c1570e955613b12cb02b24b2abd3fdc65602e8b46",
    }
)


def _fold_literal_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _fold_literal_string(node.left)
        right = _fold_literal_string(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _normalized_source_name(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.casefold()).split())


class PublicRepositorySafetyTests(unittest.TestCase):
    def test_repository_contains_no_catalog_data_files(self) -> None:
        forbidden_suffixes = {
            ".csv",
            ".eps",
            ".jpeg",
            ".jpg",
            ".json",
            ".jsonl",
            ".pdf",
            ".png",
            ".svg",
            ".tsv",
            ".webp",
            ".xls",
            ".xlsx",
            ".zip",
            ".ai",
        }
        committed_candidates = [
            path.relative_to(ROOT).as_posix()
            for path in ROOT.rglob("*")
            if path.is_file()
            and ".git" not in path.parts
            and path.suffix.lower() in forbidden_suffixes
        ]
        self.assertEqual(committed_candidates, [])

    def test_private_pinned_ledger_filenames_are_not_published(self) -> None:
        candidates: set[str] = set()
        for path in ROOT.rglob("*"):
            if (
                not path.is_file()
                or ".git" in path.parts
                or path.suffix.lower() not in {".py", ".md", ".yml", ".yaml", ".txt"}
            ):
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            candidates.update(
                re.findall(
                    r"[A-Za-z0-9_.-]{1,255}\.(?:jsonl|zip|xlsx|xls|csv|md)",
                    text,
                    re.IGNORECASE,
                )
            )
            if path.suffix.lower() == ".py":
                tree = ast.parse(text, filename=str(path))
                for node in ast.walk(tree):
                    value = _fold_literal_string(node)
                    if value:
                        candidates.add(value)

        for candidate in candidates:
            values = {candidate.casefold(), _normalized_source_name(candidate)}
            digests = {
                hashlib.sha256(value.encode("utf-8")).hexdigest()
                for value in values
            }
            self.assertTrue(digests.isdisjoint(PRIVATE_SOURCE_NAME_HASHES))

    def test_repository_contains_no_literal_catalog_external_ids(self) -> None:
        repository_text = "\n".join(
            path.read_text(encoding="utf-8", errors="ignore")
            for path in ROOT.rglob("*")
            if path.is_file()
            and ".git" not in path.parts
            and path.suffix.lower() in {".py", ".md", ".yml", ".yaml", ".txt"}
        )
        self.assertEqual(
            re.findall(r"[\"']([0-9]{8,20})[\"']", repository_text), []
        )

    def test_workflow_has_only_reviewed_triggers_and_no_repository_variables(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("schedule:", workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertNotIn("pull_request_target:", workflow)
        self.assertNotIn("vars.", workflow)

    def test_actions_are_immutable_and_artifact_persistence_is_absent(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        uses = re.findall(r"^\s*uses:\s*([^\s#]+)", workflow, re.MULTILINE)
        self.assertGreater(len(uses), 0)
        for action in uses:
            self.assertRegex(action, r"^[^@]+@[0-9a-f]{40}$")
        self.assertNotIn("actions/cache", workflow)
        self.assertNotIn("actions/upload-artifact", workflow)
        self.assertNotIn("actions/download-artifact", workflow)

    def test_fork_guard_secret_scope_and_bounded_continuation_are_present(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("secrets.EXPECTED_REPOSITORY", workflow)
        self.assertIn('GITHUB_REPOSITORY" != "$EXPECTED_REPOSITORY', workflow)
        self.assertIn("github.ref == 'refs/heads/main'", workflow)
        self.assertIn("needs.ingest.outputs.batch_full == 'true'", workflow)
        self.assertIn("actions: write", workflow)
        self.assertIn("SAFE_BATCH_SIZE", workflow)
        self.assertIn('ACTIONS_RETENTION_DAYS" != "1', workflow)
        self.assertIn("environment: production-catalog", workflow)
        continuation = workflow.split("continue_catalog:", maxsplit=1)[1]
        self.assertNotIn("secrets.", continuation)

    def test_private_configuration_is_secret_backed(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        secret_names = {
            "CATALOG_MANIFEST_DRIVE_FILE_ID",
            "METADATA_IMPORT_FOLDER_ID",
            "CATALOG_METADATA_FOLDER_ID",
            "PRODUCT_ZIPS_FOLDER_ID",
            "REPORTS_FOLDER_ID",
            "F16_ROOT_FOLDER_ID",
            "PNG_ONLY_ROOT_FOLDER_ID",
            "EXPECTED_MIN_PRODUCT_COUNT",
            "EXPECTED_MIN_ACCEPTED_COUNT",
            "CATALOG_SOURCE_NAMESPACE",
            "DREAMSTIME_LEDGER_ZIP_DRIVE_FILE_ID",
            "DREAMSTIME_LEDGER_ZIP_NAME",
            "DREAMSTIME_LEDGER_ZIP_SHA256",
            "DREAMSTIME_LEDGER_MEMBER_SHA256",
            "AI_PNG_METADATA_JSONL_DRIVE_FILE_ID",
            "AI_PNG_METADATA_JSONL_NAME",
            "AI_PNG_METADATA_JSONL_SHA256",
            "SITE_BASE_URL",
            "CATALOG_INGEST_SECRET",
            "GOOGLE_DRIVE_OAUTH_CLIENT_ID",
            "GOOGLE_DRIVE_OAUTH_CLIENT_SECRET",
            "GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN",
        }
        for name in secret_names:
            self.assertIn(f"${{{{ secrets.{name} }}}}", workflow)

    def test_ingest_keeps_oidc_default_and_allows_a_masked_static_fallback(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        pipeline = (ROOT / "scripts" / "cloud_catalog_pipeline.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("id-token: write", workflow)
        self.assertIn("ACTIONS_ID_TOKEN_REQUEST_URL", workflow)
        self.assertIn('oidc_audience="heeexy-catalog-ingest"', workflow)
        self.assertIn("@uri", workflow)
        self.assertIn("CATALOG_INGEST_SECRET", workflow)
        self.assertIn("CATALOG_INGEST_SECRET", pipeline)
        self.assertIn('if [[ -z "${CATALOG_INGEST_SECRET:-}" ]]', workflow)
        self.assertNotIn("add-mask::${CATALOG_INGEST_SECRET}", workflow)
        self.assertNotIn('echo "$CATALOG_INGEST_SECRET"', workflow)
        self.assertIn('"X-GitHub-OIDC-Token"', pipeline)
        self.assertIn('"Authorization"', pipeline)
        self.assertIn("validated_ingest_secret", pipeline)

    def test_metadata_job_is_read_only_and_manifest_identity_is_not_exported(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        manifest_source = (ROOT / "scripts" / "cloud_catalog_manifest.py").read_text(
            encoding="utf-8"
        )
        manifest_job = workflow.split("  manifest:\n", maxsplit=1)[1].split(
            "\n  ingest:", maxsplit=1
        )[0]
        self.assertIn("rebuild_manifest:", workflow)
        self.assertIn("permissions:\n      contents: read", manifest_job)
        self.assertNotIn("id-token: write", manifest_job)
        self.assertNotIn("GITHUB_ENV", manifest_source)
        self.assertIn("expected_file_id=config.catalog_manifest_drive_file_id", manifest_source)
        self.assertIn("blocking_previous_manifest_missing_products", manifest_source)
        self.assertIn("scan_png_only_root_and_discover_incremental_seeds", manifest_source)
        self.assertIn("incremental_sidecar_missing", manifest_source)
        self.assertNotIn("manifest_file_id}\n", manifest_source)


if __name__ == "__main__":
    unittest.main()
