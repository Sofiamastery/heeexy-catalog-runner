import hashlib
import base64
import io
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from concurrent.futures import Future
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))

from cloud_catalog_pipeline import (  # noqa: E402
    ARTIFACT_RECIPE_VERSION,
    CATALOG_SCHEMA_VERSION,
    CHECKER_DARK,
    CHECKER_LIGHT,
    CHECKER_SQUARE,
    DARK_BROWN,
    DRIVE_RECONCILE_DELAYS,
    DRIVE_RESUMABLE_RETRY_DELAYS,
    DriveClient,
    GitHubOidcTokenProvider,
    StaticIngestSecretProvider,
    MAX_ARCHIVE_ENTRIES,
    WHITE,
    PipelineError,
    Candidate,
    artifact_fingerprint,
    candidate_from_record,
    candidate_from_sidecar,
    classify_drive_error,
    build_ingest_auth_provider,
    create_previews,
    discover_png_only_candidates,
    drive_binary_metadata_matches,
    drive_error_is_retryable,
    enforce_archive_entry_count,
    enforce_cloud_runtime,
    ingest_auth_headers,
    merge_candidates,
    parse_manifest_jsonl,
    read_manifest_from_drive,
    publish_outcomes,
    publish_in_isolated_worker,
    preview_padding,
    prioritize_unseen_candidates,
    hold_has_untried_repair,
    png_dimensions_allowed,
    bounded_preview_source,
    verify_png,
    public_site_metadata,
    safe_summary,
    source_revision_descriptor,
    validate_delivery_zip_archive,
    validate_ingest_response_payload,
    validated_ingest_oidc_token,
    validated_ingest_secret,
    unverified_oidc_expiry,
    validate_svg_bytes,
    visible_content_bounds,
    write_deterministic_zip_member,
)


PNG_ID = "1abcdefghijklmnopqrstuvwxyzABCDE"
SVG_ID = "2abcdefghijklmnopqrstuvwxyzABCDE"
ROOT_ID = "3abcdefghijklmnopqrstuvwxyzABCDE"


def whitelist_record(**overrides):
    record = {
        "record_type": "whitelist",
        "is_product_primary_record": True,
        "product_group_id": f"drive:{SVG_ID}",
        "canonical_local_id": SVG_ID,
        "known_png_drive_file_ids": [PNG_ID],
        "known_svg_drive_file_ids": [SVG_ID],
        "metadata": {
            "title": "Peace Love Basketball Graphic",
            "description": "An existing marketplace description that is long enough.",
            "categories": ["Arts"],
            "keywords": ["basketball", "peace", "Basketball", "sports design"],
        },
    }
    record.update(overrides)
    return record


class CloudCatalogPipelineTests(unittest.TestCase):
    def fake_product_executor(self, results):
        calls = []
        class Executor:
            def __init__(self, **kwargs):
                self.options = kwargs
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def submit(self, fn, config, oauth, candidate, run_root, metadata):
                calls.append(candidate)
                future = Future()
                result = results[candidate]
                if isinstance(result, Exception):
                    future.set_exception(result)
                else:
                    future.set_result(result)
                return future
        return Executor, calls

    def test_parallel_outcomes_are_bounded_and_preserve_permanent_hold(self):
        executor, calls = self.fake_product_executor({0: None, 1: ("svg_unsafe", True, ""), 2: None})
        pending = [(index, "fingerprint", {}) for index in range(3)]
        observed_sizes = []
        def done_together(futures, **kwargs):
            observed_sizes.append(len(futures))
            return set(futures), set()
        with mock.patch("cloud_catalog_pipeline.ProcessPoolExecutor", executor), mock.patch("cloud_catalog_pipeline.wait", done_together):
            outcomes = list(publish_outcomes(SimpleNamespace(product_workers=2), (), pending, Path("unused"), None))
        self.assertEqual(calls, [0, 1, 2])
        self.assertEqual(observed_sizes, [2, 1])
        self.assertEqual([index for index, _ in outcomes], [0, 1, 2])
        self.assertTrue(outcomes[1][1].permanent)
        self.assertEqual(outcomes[1][1].code, "svg_unsafe")

    def test_parallel_transient_failure_drains_started_success_without_new_work(self):
        executor, calls = self.fake_product_executor({0: ("catalog_ingest_network_failed", False, ""), 1: None, 2: None})
        pending = [(index, "fingerprint", {}) for index in range(3)]
        with mock.patch("cloud_catalog_pipeline.ProcessPoolExecutor", executor), mock.patch("cloud_catalog_pipeline.wait", side_effect=lambda futures, **kwargs: ({next(iter(futures))}, set())):
            outcomes = list(publish_outcomes(SimpleNamespace(product_workers=2), (), pending, Path("unused"), None))
        self.assertEqual(calls, [0, 1])
        self.assertEqual([index for index, _ in outcomes], [0, 1])
        self.assertIsNone(outcomes[1][1])
        self.assertFalse(outcomes[0][1].permanent)

    def test_parallel_unexpected_failure_is_sanitized_and_stops(self):
        executor, calls = self.fake_product_executor({0: RuntimeError("private-token-url"), 1: None, 2: None})
        pending = [(index, "fingerprint", {}) for index in range(3)]
        with mock.patch("cloud_catalog_pipeline.ProcessPoolExecutor", executor), mock.patch("cloud_catalog_pipeline.wait", side_effect=lambda futures, **kwargs: (set(futures), set())):
            outcomes = list(publish_outcomes(SimpleNamespace(product_workers=2), (), pending, Path("unused"), None))
        self.assertEqual(calls, [0, 1])
        self.assertEqual(outcomes[0][1].code, "product_worker_failed")
        self.assertNotIn("private-token", str(outcomes))

    def test_product_worker_refuses_local_before_client_or_assets(self):
        with mock.patch.dict("os.environ", {}, clear=True), mock.patch("cloud_catalog_pipeline.DriveClient") as client, mock.patch("cloud_catalog_pipeline.prepare_and_publish_candidate") as publish:
            result = publish_in_isolated_worker(None, (), None, None, {})
        self.assertEqual(result, ("cloud_runtime_required", False, ""))
        client.assert_not_called()
        publish.assert_not_called()

    def test_manifest_snapshot_retries_only_bounded_revision_races(self):
        drive = mock.Mock()
        def write_snapshot(file_id, path, maximum_bytes):
            path.write_text(json.dumps(whitelist_record()) + "\n", encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory, mock.patch("cloud_catalog_pipeline.time.sleep"):
            drive.download_to.side_effect = [PipelineError("drive_source_changed_during_download"), None]
            # The second successful call must write the synthetic snapshot.
            calls = 0
            def race_then_snapshot(*args):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise PipelineError("drive_source_changed_during_download")
                write_snapshot(*args)
            drive.download_to.side_effect = race_then_snapshot
            candidates, _ = read_manifest_from_drive(drive, ROOT_ID, Path(directory))
            self.assertEqual(len(candidates), 1)
            self.assertEqual(calls, 2)
            drive.reset_mock()
            drive.download_to.side_effect = PipelineError("drive_download_size_mismatch")
            with self.assertRaisesRegex(PipelineError, "drive_download_size_mismatch"):
                read_manifest_from_drive(drive, ROOT_ID, Path(directory))
            self.assertEqual(drive.download_to.call_count, 3)
            drive.reset_mock()
            drive.download_to.side_effect = PipelineError("drive_permission_check_failed")
            with self.assertRaisesRegex(PipelineError, "drive_permission_check_failed"):
                read_manifest_from_drive(drive, ROOT_ID, Path(directory))
            self.assertEqual(drive.download_to.call_count, 1)

    def test_drive_diagnostics_expose_only_allowlisted_status_and_reasons(self):
        error = RuntimeError("secret-token and private-file-url must never appear")
        error.resp = type("Response", (), {"status": 403})()
        error.content = json.dumps({"error": {"message": "secret-token", "errors": [
            {"reason": "storageQuotaExceeded"}, {"reason": "private-file-url"}
        ]}}).encode()
        result = classify_drive_error("drive_upload_failed", error, candidate_scoped=False)
        self.assertEqual(result.code, "drive_upload_failed")
        self.assertFalse(result.permanent)
        self.assertEqual(result.diagnostic, "drive_http=403 drive_reason=storagequotaexceeded")
        error.resp.status = 987654321
        error.content = b"not json secret-token"
        self.assertEqual(
            classify_drive_error("drive_upload_failed", error, candidate_scoped=False).diagnostic,
            "drive_http=none drive_reason=unknown",
        )

    def test_unseen_candidates_are_prioritized_without_dropping_verification_work(self):
        def candidate(product_key):
            return Candidate(
                product_key=product_key,
                slug=product_key,
                title=f"Title {product_key}",
                description="An existing marketplace description that is long enough.",
                category="Arts",
                tags=("design",),
                source_png_id=PNG_ID,
                source_svg_id=None,
                source_archive_id=None,
                archive_png_member=None,
                archive_svg_member=None,
                fingerprint=product_key * 8,
            )

        candidates = [candidate("done"), candidate("held"), candidate("new-a"), candidate("new-b")]
        ordered = prioritize_unseen_candidates(
            candidates,
            {"done": {"fingerprint": "old"}},
            {"held": {"fingerprint": "old"}},
            retry_holds=False,
        )

        self.assertEqual(
            [item.product_key for item in ordered],
            ["new-a", "new-b", "done", "held"],
        )

    def test_retry_holds_prioritizes_held_candidates_with_new_work(self):
        def candidate(product_key):
            return Candidate(
                product_key=product_key,
                slug=product_key,
                title=f"Title {product_key}",
                description="An existing marketplace description that is long enough.",
                category="Arts",
                tags=("design",),
                source_png_id=PNG_ID,
                source_svg_id=None,
                source_archive_id=None,
                archive_png_member=None,
                archive_svg_member=None,
                fingerprint=product_key * 8,
            )

        candidates = [candidate("done"), candidate("held"), candidate("new")]
        ordered = prioritize_unseen_candidates(
            candidates,
            {"done": {"fingerprint": "old"}},
            {"held": {"fingerprint": "old"}},
            retry_holds=True,
        )

        self.assertEqual(
            [item.product_key for item in ordered],
            ["held", "new", "done"],
        )

    def test_resumable_upload_retries_the_same_session(self):
        payload = b"delivery-zip"
        metadata = {
            "id": "4abcdefghijklmnopqrstuvwxyzABCDE",
            "mimeType": "application/zip",
            "size": str(len(payload)),
            "md5Checksum": hashlib.md5(payload, usedforsecurity=False).hexdigest(),
            "trashed": False,
            "createdTime": "2026-10-08T00:00:00Z",
        }
        state = {"chunks": 0, "creates": 0}

        class FakeRequest:
            def next_chunk(self, *, num_retries):
                self.num_retries = num_retries
                state["chunks"] += 1
                if state["chunks"] == 1:
                    raise TimeoutError("transient")
                if state["chunks"] == 2:
                    return None, None
                return None, metadata

        request = FakeRequest()

        class FakeFiles:
            def create(self, **kwargs):
                state["creates"] += 1
                return request

        client = DriveClient.__new__(DriveClient)
        client.service = type("FakeService", (), {"files": lambda self: FakeFiles()})()
        client._find_by_properties = lambda parent_id, properties: []
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "delivery.zip"
            path.write_bytes(payload)
            with (
                mock.patch(
                    "googleapiclient.http.MediaFileUpload",
                    return_value=object(),
                ) as media_upload,
                mock.patch("cloud_catalog_pipeline.time.sleep") as sleep,
            ):
                result = client.upload_content_addressed(
                    path,
                    parent_id=ROOT_ID,
                    name="delivery.zip",
                    mime_type="application/zip",
                    role="delivery_zip_v1",
                    product_hash="a" * 64,
                    content_sha256=hashlib.sha256(payload).hexdigest(),
                )
        self.assertEqual(result, metadata)
        self.assertEqual(state, {"chunks": 3, "creates": 1})
        self.assertEqual(request.num_retries, 3)
        self.assertEqual(media_upload.call_count, 1)
        sleep.assert_called_once_with(DRIVE_RESUMABLE_RETRY_DELAYS[0])

    def test_ambiguous_upload_reconciles_without_a_second_create(self):
        payload = b"reconciled-zip"
        metadata = {
            "id": "5abcdefghijklmnopqrstuvwxyzABCDE",
            "mimeType": "application/zip",
            "size": str(len(payload)),
            "md5Checksum": hashlib.md5(payload, usedforsecurity=False).hexdigest(),
            "trashed": False,
            "createdTime": "2026-10-08T00:00:00Z",
        }
        state = {"chunks": 0, "creates": 0, "finds": 0}

        class FakeRequest:
            def next_chunk(self, *, num_retries):
                state["chunks"] += 1
                raise TimeoutError("ambiguous")

        request = FakeRequest()

        class FakeFiles:
            def create(self, **kwargs):
                state["creates"] += 1
                return request

        client = DriveClient.__new__(DriveClient)
        client.service = type("FakeService", (), {"files": lambda self: FakeFiles()})()

        def find(parent_id, properties):
            state["finds"] += 1
            return [metadata] if state["finds"] == 3 else []

        client._find_by_properties = find
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "delivery.zip"
            path.write_bytes(payload)
            with (
                mock.patch("googleapiclient.http.MediaFileUpload", return_value=object()),
                mock.patch("cloud_catalog_pipeline.time.sleep") as sleep,
            ):
                result = client.upload_content_addressed(
                    path,
                    parent_id=ROOT_ID,
                    name="delivery.zip",
                    mime_type="application/zip",
                    role="delivery_zip_v1",
                    product_hash="b" * 64,
                    content_sha256=hashlib.sha256(payload).hexdigest(),
                )
        self.assertEqual(result, metadata)
        self.assertEqual(state["creates"], 1)
        self.assertEqual(state["chunks"], len(DRIVE_RESUMABLE_RETRY_DELAYS) + 1)
        self.assertEqual(state["finds"], 3)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            [*DRIVE_RESUMABLE_RETRY_DELAYS, *DRIVE_RECONCILE_DELAYS[:2]],
        )

    def test_local_entrypoint_is_refused_before_io(self):
        with self.assertRaisesRegex(PipelineError, "cloud_runtime_required"):
            enforce_cloud_runtime({})

    def test_existing_metadata_and_svg_png_pair_are_preserved(self):
        candidate = candidate_from_record(whitelist_record())
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.title, "Peace Love Basketball Graphic")
        self.assertEqual(candidate.description, "An existing marketplace description that is long enough.")
        self.assertEqual(candidate.tags, ("basketball", "peace", "sports design"))
        self.assertEqual(candidate.source_png_id, PNG_ID)
        self.assertEqual(candidate.source_svg_id, SVG_ID)
        self.assertRegex(candidate.product_key, r"^drive:catalog:[0-9a-f]{64}$")

    def test_complete_bounded_seo_metadata_is_preserved_and_never_silently_cut(self):
        full = whitelist_record(
            metadata={
                "title": "T" * 256,
                "description": "D" * 4_000,
                "categories": ["C" * 100],
                "keywords": [f"{index:02d}-" + "K" * 253 for index in range(50)],
            }
        )
        candidate = candidate_from_record(full)
        assert candidate is not None
        self.assertEqual(len(candidate.title), 256)
        self.assertEqual(len(candidate.description), 4_000)
        self.assertEqual(len(candidate.category), 100)
        self.assertEqual(len(candidate.tags), 50)
        self.assertTrue(all(len(tag) == 256 for tag in candidate.tags))

        cases = (
            ({**full["metadata"], "title": "T" * 257}, "title_exceeds_limit"),
            ({**full["metadata"], "description": "D" * 4_001}, "description_exceeds_limit"),
            ({**full["metadata"], "keywords": ["K" * 257]}, "tag_exceeds_limit"),
            (
                {**full["metadata"], "keywords": [f"keyword-{index}" for index in range(51)]},
                "tags_exceed_limit",
            ),
        )
        for metadata, code in cases:
            with self.subTest(code=code), self.assertRaisesRegex(PipelineError, code):
                candidate_from_record(whitelist_record(metadata=metadata))

    def test_category_is_optional_and_remains_empty(self):
        metadata = {**whitelist_record()["metadata"]}
        metadata.pop("categories")
        candidate = candidate_from_record(whitelist_record(metadata=metadata))
        assert candidate is not None
        self.assertEqual(candidate.category, "")

    def test_public_site_metadata_removes_only_exact_ai_disclosures(self):
        metadata = {
            **whitelist_record()["metadata"],
            "description": (
                "A detailed transparent sports illustration for creative projects. "
                "tHiS IlLuStRaTiOn WaS GeNeRaTeD wItH aI."
            ),
            "categories": ["aI GENERATED IMAGES"],
            "keywords": [
                "basketball",
                "AI Generated",
                "generated WITH ai",
                "AI GENERATED IMAGES",
                "AI generated art",
            ],
        }
        candidate = candidate_from_record(whitelist_record(metadata=metadata))
        assert candidate is not None

        public = public_site_metadata(candidate)

        self.assertEqual(
            public["description"],
            "A detailed transparent sports illustration for creative projects.",
        )
        self.assertEqual(public["category"], "")
        self.assertEqual(public["tags"], ["basketball", "AI generated art"])
        self.assertEqual(candidate.description, metadata["description"])
        self.assertEqual(candidate.category, "aI GENERATED IMAGES")
        self.assertIn("AI Generated", candidate.tags)

    def test_public_site_metadata_does_not_broadly_rewrite_related_copy(self):
        description = (
            "This illustration was generated with AI. "
            "A later sentence makes the disclosure non-trailing."
        )
        metadata = {
            **whitelist_record()["metadata"],
            "description": description,
            "categories": ["AI Generated Images & More"],
            "keywords": ["AI generated art", "basketball"],
        }
        candidate = candidate_from_record(whitelist_record(metadata=metadata))
        assert candidate is not None

        public = public_site_metadata(candidate)

        self.assertEqual(public["description"], description)
        self.assertEqual(public["category"], "AI Generated Images & More")
        self.assertEqual(public["tags"], ["AI generated art", "basketball"])

    def test_public_site_metadata_fails_closed_if_sanitization_empties_required_copy(self):
        description_only = candidate_from_record(
            whitelist_record(
                metadata={
                    "title": "Existing marketplace title",
                    "description": "This illustration was generated with AI.",
                    "categories": ["Arts"],
                    "keywords": ["basketball"],
                }
            )
        )
        assert description_only is not None
        with self.assertRaisesRegex(PipelineError, "description_missing") as description_error:
            public_site_metadata(description_only)
        self.assertTrue(description_error.exception.permanent)

        keywords_only = candidate_from_record(
            whitelist_record(
                metadata={
                    "title": "Existing marketplace title",
                    "description": "An existing marketplace description that is long enough.",
                    "categories": ["Arts"],
                    "keywords": ["AI generated", "Generated with AI", "AI Generated Images"],
                }
            )
        )
        assert keywords_only is not None
        with self.assertRaisesRegex(PipelineError, "tags_missing") as tags_error:
            public_site_metadata(keywords_only)
        self.assertTrue(tags_error.exception.permanent)

    def test_svg_without_png_is_a_valid_render_candidate(self):
        candidate = candidate_from_record(
            whitelist_record(known_png_drive_file_ids=[], known_svg_drive_file_ids=[SVG_ID])
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertIsNone(candidate.source_png_id)
        self.assertEqual(candidate.source_svg_id, SVG_ID)

    def test_archive_member_record_is_a_valid_cloud_candidate(self):
        candidate = candidate_from_record(
            {
                "record_type": "heeexy_catalog_product",
                "product_identity": "archive-product-1",
                "metadata": whitelist_record()["metadata"],
                "assets": {
                    "archive_drive_file_id": "4abcdefghijklmnopqrstuvwxyzABCDE",
                    "archive_svg_member": "bundle/design.svg",
                },
            }
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertIsNone(candidate.source_png_id)
        self.assertEqual(candidate.archive_svg_member, "bundle/design.svg")

    def test_alias_and_quarantine_rows_are_not_products(self):
        alias = whitelist_record(is_product_primary_record=False)
        quarantine = {"record_type": "quarantine", "category": "review"}
        candidates, stats = parse_manifest_jsonl(
            io.StringIO("\n".join((__import__("json").dumps(alias), __import__("json").dumps(quarantine))))
        )
        self.assertEqual(candidates, [])
        self.assertEqual(stats["ignored"], 2)

    def test_conflicting_duplicate_identity_is_held_not_selected(self):
        import json

        first = whitelist_record()
        second = whitelist_record(
            metadata={
                **whitelist_record()["metadata"],
                "title": "A Different Existing Title",
            }
        )
        candidates, stats = parse_manifest_jsonl(
            io.StringIO(f"{json.dumps(first)}\n{json.dumps(second)}\n")
        )
        self.assertEqual(candidates, [])
        self.assertEqual(stats["held_manifest_identity_conflict"], 1)

    def test_sidecar_requires_and_reuses_supplied_metadata(self):
        csv_bytes = (
            "DeliveryFile,Title,Description,Category,Keywords\n"
            "design.png,Existing Title,Existing description with enough words,AI Generated Images,alpha;beta\n"
        ).encode()
        candidate = candidate_from_sidecar(
            csv_bytes,
            "3abcdefghijklmnopqrstuvwxyzABCDE",
            [{"id": PNG_ID, "name": "design.png", "mimeType": "image/png"}],
        )
        self.assertEqual(candidate.title, "Existing Title")
        self.assertEqual(candidate.tags, ("alpha", "beta"))

    def test_sidecar_delivery_filename_must_match_exactly(self):
        csv_bytes = (
            "DeliveryFile,Title,Description,Category,Keywords\n"
            "expected.png,Existing Title,Existing description with enough words,Arts,alpha;beta\n"
        ).encode()
        with self.assertRaisesRegex(PipelineError, "sidecar_delivery_file_not_exact"):
            candidate_from_sidecar(
                csv_bytes,
                ROOT_ID,
                [{"id": PNG_ID, "name": "different.png", "mimeType": "image/png"}],
            )

    def test_duplicate_sidecars_are_held_instead_of_overwriting(self):
        csv_bytes = (
            "DeliveryFile,Title,Description,Category,Keywords\n"
            "design.png,Existing Title,Existing description with enough words,Arts,alpha;beta\n"
        ).encode()

        class FakeDrive:
            def list_named_files(self, _name):
                return [
                    {"id": "4abcdefghijklmnopqrstuvwxyzABCDE", "parents": [ROOT_ID]},
                    {"id": "5abcdefghijklmnopqrstuvwxyzABCDE", "parents": [ROOT_ID]},
                ]

            def download_bytes(self, _file_id, _maximum, *, candidate_scoped=False):
                self.assert_candidate_scoped = candidate_scoped
                return csv_bytes, {}

            def list_children(self, _folder_id, *, candidate_scoped=False):
                self.assert_candidate_scoped = candidate_scoped
                return [{"id": PNG_ID, "name": "design.png", "mimeType": "image/png"}]

        candidates, stats = discover_png_only_candidates(FakeDrive(), ROOT_ID)  # type: ignore[arg-type]
        self.assertEqual(candidates, [])
        self.assertEqual(stats["held_sidecar_duplicate"], 1)
        self.assertEqual(stats["accepted"], 0)

    def test_svg_policy_accepts_static_svg_and_rejects_external_href(self):
        validate_svg_bytes(
            b'<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20"><path d="M0 0h20v20z"/></svg>'
        )
        with self.assertRaisesRegex(PipelineError, "svg_active_or_external_content"):
            validate_svg_bytes(
                b'<svg xmlns="http://www.w3.org/2000/svg"><image href="https://example.invalid/a.png"/></svg>'
            )

    def test_svg_policy_allows_local_fragments_and_rejects_css_and_xml_base(self):
        validate_svg_bytes(
            b'<svg xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="g"/></defs>'
            b'<path fill="url(#g)" d="M0 0h10v10z"/><use href="#g"/></svg>'
        )
        with self.assertRaisesRegex(PipelineError, "svg_active_or_external_content"):
            validate_svg_bytes(
                b'<svg xmlns="http://www.w3.org/2000/svg"><path style="fill:url(https://example.invalid/a)"/></svg>'
            )
        with self.assertRaisesRegex(PipelineError, "svg_active_or_external_content"):
            validate_svg_bytes(
                b'<svg xmlns="http://www.w3.org/2000/svg" xml:base="https://example.invalid/"><use href="#x"/></svg>'
            )
        with self.assertRaisesRegex(PipelineError, "svg_active_or_external_content"):
            validate_svg_bytes(
                b'<?xml-stylesheet href="https://example.invalid/a.css"?>'
                b'<svg xmlns="http://www.w3.org/2000/svg"/>'
            )

    def test_svg_foreign_namespaces_are_confined_to_metadata(self):
        validate_svg_bytes(
            b'<svg xmlns="http://www.w3.org/2000/svg" '
            b'xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
            b'<metadata><rdf:RDF/></metadata><path d="M0 0h1v1z"/></svg>'
        )
        with self.assertRaisesRegex(PipelineError, "svg_namespace_not_allowed"):
            validate_svg_bytes(
                b'<svg xmlns="http://www.w3.org/2000/svg" '
                b'xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
                b'<rdf:RDF/></svg>'
            )

    def test_c2pa_is_allowed_only_as_inert_metadata(self):
        prefix = b'<svg xmlns="http://www.w3.org/2000/svg" xmlns:c2pa="http://c2pa.org/manifest">'
        validate_svg_bytes(prefix + b'<metadata><c2pa:manifest>provenance</c2pa:manifest></metadata></svg>')
        with self.assertRaisesRegex(PipelineError, "svg_namespace_not_allowed"):
            validate_svg_bytes(prefix + b'<c2pa:manifest/></svg>')
        with self.assertRaisesRegex(PipelineError, "svg_active_or_external_content"):
            validate_svg_bytes(prefix + b'<metadata><c2pa:manifest onload="bad()"/></metadata></svg>')
        with self.assertRaisesRegex(PipelineError, "svg_active_or_external_content"):
            validate_svg_bytes(prefix + b'<metadata><c2pa:manifest href="https://example.invalid/data"/></metadata></svg>')

    def test_diagnosed_hold_repair_retries_once_only(self):
        self.assertTrue(hold_has_untried_repair({"reason": "svg_namespace_not_allowed"}))
        self.assertFalse(hold_has_untried_repair({"reason": "svg_namespace_not_allowed", "repair_version": "c2pa-inert-metadata-v1"}))
        self.assertTrue(hold_has_untried_repair({"reason": "png_decode_failed"}))
        self.assertFalse(hold_has_untried_repair({"reason": "png_decode_failed", "repair_version": "svg-preview-large-original-v1"}))
        self.assertFalse(hold_has_untried_repair({"reason": "source_asset_missing"}))

    def test_large_original_bounds_and_bounded_svg_preview(self):
        self.assertFalse(png_dimensions_allowed(18_184, 18_184))
        self.assertTrue(png_dimensions_allowed(18_184, 18_184, 400_000_000))
        self.assertFalse(png_dimensions_allowed(20_001, 100, 400_000_000))
        original, svg, work = Path("original.png"), Path("original.svg"), Path("work")
        with mock.patch("cloud_catalog_pipeline.render_svg") as render:
            result = bounded_preview_source(original, svg, (18_184, 18_184), work)
            self.assertNotEqual(result, original)
            render.assert_called_once_with(svg, result, work)
        with self.assertRaisesRegex(PipelineError, "large_png_requires_svg_preview"):
            bounded_preview_source(original, None, (18_184, 18_184), work)
        self.assertEqual(bounded_preview_source(original, None, (3000, 3000), work), original)

    def test_large_delivery_verification_never_decodes_pixels_or_disables_preview_guard(self):
        from PIL import Image
        image = mock.MagicMock()
        image.format = "PNG"
        image.size = (18_184, 18_184)
        image.__enter__.return_value = image
        with mock.patch("PIL.Image.open", return_value=image):
            self.assertEqual(verify_png(Path("original.png"), allow_large_delivery=True), image.size)
            image.verify.assert_called_once()
            image.load.assert_not_called()
            image.convert.assert_not_called()
            self.assertEqual(Image.MAX_IMAGE_PIXELS, 100_000_000)
            with self.assertRaisesRegex(PipelineError, "png_dimensions_invalid"):
                verify_png(Path("original.png"))
        image.verify.side_effect = ValueError("synthetic invalid CRC")
        with mock.patch("PIL.Image.open", return_value=image):
            with self.assertRaisesRegex(PipelineError, "png_decode_failed"):
                verify_png(Path("original.png"), allow_large_delivery=True)

    def test_drive_http_errors_separate_file_holds_from_service_stops(self):
        class FakeResponse:
            def __init__(self, status):
                self.status = status

        class FakeHttpError(Exception):
            def __init__(self, status, reason):
                self.resp = FakeResponse(status)
                self.content = json.dumps(
                    {"error": {"errors": [{"reason": reason}]}}
                ).encode()

        for status in (403, 404, 410):
            error = classify_drive_error(
                "drive_download_failed",
                FakeHttpError(status, "insufficientFilePermissions"),
                candidate_scoped=True,
            )
            self.assertTrue(error.permanent)
        for status, reason in ((429, "rateLimitExceeded"), (503, "backendError")):
            error = classify_drive_error(
                "drive_download_failed",
                FakeHttpError(status, reason),
                candidate_scoped=True,
            )
            self.assertFalse(error.permanent)
        rate_limited_403 = classify_drive_error(
            "drive_download_failed",
            FakeHttpError(403, "userRateLimitExceeded"),
            candidate_scoped=True,
        )
        self.assertFalse(rate_limited_403.permanent)
        oauth = classify_drive_error(
            "drive_download_failed",
            FakeHttpError(401, "authError"),
            candidate_scoped=True,
        )
        self.assertEqual(oauth.code, "drive_oauth_failed")
        self.assertFalse(oauth.permanent)
        missing_scope = classify_drive_error(
            "drive_download_failed",
            FakeHttpError(403, "insufficientPermissions"),
            candidate_scoped=True,
        )
        self.assertEqual(missing_scope.code, "drive_oauth_failed")
        self.assertTrue(drive_error_is_retryable(TimeoutError("transport")))
        self.assertTrue(
            drive_error_is_retryable(FakeHttpError(503, "backendError"))
        )
        self.assertFalse(
            drive_error_is_retryable(FakeHttpError(400, "invalidArgument"))
        )
        self.assertFalse(
            drive_error_is_retryable(FakeHttpError(403, "storageQuotaExceeded"))
        )
        self.assertFalse(
            drive_error_is_retryable(FakeHttpError(403, "dailyLimitExceeded"))
        )

    def test_artifact_fingerprint_tracks_source_revision_and_recipe(self):
        self.assertEqual(CATALOG_SCHEMA_VERSION, 2)
        self.assertEqual(ARTIFACT_RECIPE_VERSION, "catalog-artifacts-v3")
        candidate = candidate_from_record(whitelist_record())
        assert candidate is not None
        first = source_revision_descriptor(
            "png",
            {
                "id": PNG_ID,
                "mimeType": "image/png",
                "size": "123",
                "version": "7",
                "modifiedTime": "2026-10-08T00:00:00Z",
                "md5Checksum": "a" * 32,
                "trashed": False,
            },
        )
        changed = {**first, "version": "8", "checksums": {"md5Checksum": "b" * 32}}
        baseline = artifact_fingerprint(candidate, [first])
        self.assertNotEqual(baseline, artifact_fingerprint(candidate, [changed]))
        self.assertNotEqual(
            baseline,
            artifact_fingerprint(
                candidate, [first], recipe_version="catalog-artifacts-v2"
            ),
        )
        self.assertNotEqual(
            baseline,
            artifact_fingerprint(candidate, [first], recipe_version="future-recipe"),
        )

    def test_drive_content_address_reuse_requires_real_size_mime_and_md5(self):
        metadata = {
            "mimeType": "application/zip",
            "size": "12",
            "md5Checksum": "a" * 32,
            "trashed": False,
        }
        self.assertTrue(
            drive_binary_metadata_matches(
                metadata,
                expected_size=12,
                expected_mime_type="application/zip",
                expected_md5="a" * 32,
            )
        )
        self.assertFalse(
            drive_binary_metadata_matches(
                {**metadata, "size": "13"},
                expected_size=12,
                expected_mime_type="application/zip",
                expected_md5="a" * 32,
            )
        )

    def test_delivery_zip_members_are_deterministic_and_verified_in_memory(self):
        def build_zip() -> bytes:
            output = io.BytesIO()
            expected = {}
            with zipfile.ZipFile(
                output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
            ) as archive:
                expected["design.png"] = write_deterministic_zip_member(
                    archive, "design.png", io.BytesIO(b"png-bytes")
                )
                expected["design.svg"] = write_deterministic_zip_member(
                    archive, "design.svg", io.BytesIO(b"<svg/>")
                )
            payload = output.getvalue()
            with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
                validate_delivery_zip_archive(archive, expected)
            return payload

        self.assertEqual(build_zip(), build_zip())

    def test_delivery_zip_rejects_unexpected_members_and_archive_count_is_bounded(self):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            write_deterministic_zip_member(archive, "design.png", io.BytesIO(b"png"))
            write_deterministic_zip_member(archive, "extra.txt", io.BytesIO(b"extra"))
        with zipfile.ZipFile(io.BytesIO(output.getvalue()), "r") as archive:
            with self.assertRaisesRegex(PipelineError, "delivery_zip_members_invalid"):
                validate_delivery_zip_archive(archive, {"design.png": 3})
        enforce_archive_entry_count(MAX_ARCHIVE_ENTRIES)
        with self.assertRaisesRegex(PipelineError, "archive_entry_count_invalid"):
            enforce_archive_entry_count(MAX_ARCHIVE_ENTRIES + 1)

    def test_ingest_success_response_must_match_published_product(self):
        validate_ingest_response_payload(
            {
                "ok": True,
                "product": {"id": "prd_123", "slug": "expected-slug", "status": "published"},
            },
            "expected-slug",
        )
        with self.assertRaisesRegex(PipelineError, "catalog_ingest_response_invalid"):
            validate_ingest_response_payload(
                {
                    "ok": True,
                    "product": {"id": "prd_123", "slug": "wrong", "status": "published"},
                },
                "expected-slug",
            )

    def test_ingest_authentication_accepts_only_jwt_shaped_oidc_tokens(self):
        token = f"{'a' * 24}.{'b' * 48}.{'c' * 32}"
        self.assertEqual(validated_ingest_oidc_token(token), token)
        self.assertEqual(
            ingest_auth_headers(oidc_token=token), {"X-GitHub-OIDC-Token": token}
        )
        invalid_tokens = (
            "",
            "static-secret",
            "a.b.c",
            f"{'a' * 24}.bad value.{'c' * 32}",
        )
        for invalid in invalid_tokens:
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(
                    PipelineError, "catalog_ingest_oidc_token_invalid"
                ):
                    validated_ingest_oidc_token(invalid)

    def test_static_ingest_fallback_rejects_weak_secrets(self):
        weak_values = (
            "",
            "too-short",
            "A" * 64,
            hashlib.sha256(b"valid-shape-but-surrounded").hexdigest() + " ",
            "abc123_-" * 65,
        )
        for index, weak in enumerate(weak_values):
            with self.subTest(case=index):
                with self.assertRaisesRegex(
                    PipelineError, "catalog_ingest_secret_invalid"
                ):
                    validated_ingest_secret(weak)

    def test_static_ingest_fallback_selects_bearer_without_printing_values(self):
        secret = hashlib.sha256(b"deterministic-test-only-ingest-secret").hexdigest()
        oidc_token = f"{'a' * 24}.{'b' * 48}.{'c' * 32}"
        with mock.patch("builtins.print") as print_mock:
            provider = build_ingest_auth_provider(
                {
                    "CATALOG_INGEST_SECRET": secret,
                    "GITHUB_ACTIONS_OIDC_TOKEN": oidc_token,
                }
            )
            headers = ingest_auth_headers(
                oidc_token=oidc_token,
                ingest_secret=secret,
            )
        print_mock.assert_not_called()
        self.assertIsInstance(provider, StaticIngestSecretProvider)
        self.assertEqual(set(headers), {"Authorization"})
        self.assertTrue(headers["Authorization"].startswith("Bearer "))

    def test_oidc_remains_default_when_static_fallback_is_absent(self):
        def encode(value):
            return base64.urlsafe_b64encode(
                json.dumps(value, separators=(",", ":")).encode("utf-8")
            ).decode("ascii").rstrip("=")

        oidc_token = (
            f"{encode({'alg': 'RS256', 'typ': 'JWT'})}."
            f"{encode({'exp': 2_000_000_000})}.{'s' * 32}"
        )
        with mock.patch("builtins.print") as print_mock:
            provider = build_ingest_auth_provider(
                {"GITHUB_ACTIONS_OIDC_TOKEN": oidc_token}
            )
            headers = ingest_auth_headers(oidc_token=oidc_token)
        print_mock.assert_not_called()
        self.assertIsInstance(provider, GitHubOidcTokenProvider)
        self.assertEqual(set(headers), {"X-GitHub-OIDC-Token"})

    def test_long_batch_refreshes_expiring_oidc_token_in_memory(self):
        def encode(value):
            return base64.urlsafe_b64encode(
                json.dumps(value, separators=(",", ":")).encode("utf-8")
            ).decode("ascii").rstrip("=")

        def token(expiry):
            return f"{encode({'alg': 'RS256', 'typ': 'JWT'})}.{encode({'exp': expiry})}.{'s' * 32}"

        class Response:
            status_code = 200

            def __init__(self, value):
                self.value = value

            def json(self):
                return {"value": self.value}

        class Client:
            def __init__(self, value):
                self.value = value
                self.calls = []

            def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                return Response(self.value)

        expiring = token(1_050)
        refreshed = token(2_000)
        provider = GitHubOidcTokenProvider(expiring)
        client = Client(refreshed)
        result = provider.current_token(
            client,
            now_epoch=1_000,
            environment={
                "ACTIONS_ID_TOKEN_REQUEST_URL": (
                    "https://pipelines.actions.githubusercontent.com/token?api-version=1"
                ),
                "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "r" * 64,
            },
        )
        self.assertEqual(result, refreshed)
        self.assertEqual(unverified_oidc_expiry(result), 2_000)
        self.assertEqual(len(client.calls), 1)
        request_url, request_options = client.calls[0]
        self.assertIn("audience=heeexy-catalog-ingest", request_url)
        self.assertEqual(
            request_options["headers"], {"Authorization": f"bearer {'r' * 64}"}
        )
        self.assertFalse(request_options["allow_redirects"])

        # The refreshed in-memory token is reused while it remains valid.
        self.assertEqual(
            provider.current_token(client, now_epoch=1_100, environment={}), refreshed
        )
        self.assertEqual(len(client.calls), 1)

    def test_merge_is_deterministic_and_flags_conflict(self):
        first = candidate_from_record(whitelist_record())
        second = candidate_from_record(whitelist_record())
        assert first is not None and second is not None
        merged, conflicts = merge_candidates(([first], [second]))
        self.assertEqual(len(merged), 1)
        self.assertEqual(conflicts, 0)

    def test_summary_never_contains_asset_names_or_error_details(self):
        summary = safe_summary({"selected": 3, "published": 2, "failed_secret-url-name.png": 1})
        self.assertIn("failed=1", summary)
        self.assertNotIn("secret-url-name", summary)
        self.assertEqual(WHITE, (255, 255, 255))
        self.assertEqual(DARK_BROWN, (42, 36, 32))

    def test_preview_bounds_keep_every_visible_edge_and_add_safety_space(self):
        from PIL import Image

        source = Image.new("RGBA", (1_000, 400), (0, 0, 0, 0))
        artwork = Image.new("RGBA", (240, 90), (220, 25, 35, 255))
        source.alpha_composite(artwork, (0, 310))

        self.assertEqual(visible_content_bounds(source), (0, 310, 240, 400))
        self.assertEqual(preview_padding((240, 90)), 48)
        self.assertEqual(preview_padding((1_104, 20)), 88)
        with self.assertRaisesRegex(PipelineError, "png_content_empty"):
            visible_content_bounds(Image.new("RGBA", (40, 40), (0, 0, 0, 0)))

    def test_three_previews_are_distinct_opaque_and_checkerboard_is_baked_in(self):
        from PIL import Image, ImageDraw

        with tempfile.TemporaryDirectory() as temp_name:
            work_dir = Path(temp_name)
            source_path = work_dir / "source.png"
            source = Image.new("RGBA", (192, 160), (0, 0, 0, 0))
            source_draw = ImageDraw.Draw(source)
            source_draw.rectangle((0, 0, 191, 159), fill=(230, 35, 80, 255))
            # A large transparent window proves that the same checker grid
            # continues behind artwork, rather than restarting inside it.
            source_draw.rectangle((48, 40, 143, 119), fill=(0, 0, 0, 0))
            source.save(source_path, format="PNG")

            white, dark, transparency = create_previews(source_path, work_dir)
            previews = (white, dark, transparency)
            self.assertEqual(len({preview.sha256 for preview in previews}), 3)
            self.assertEqual(len({(preview.width, preview.height) for preview in previews}), 1)

            loaded = []
            for preview in previews:
                with Image.open(preview.path) as image:
                    image.load()
                    self.assertEqual(image.mode, "RGB")
                    self.assertNotIn("A", image.getbands())
                    loaded.append(image.copy())

            def close_to(actual, expected, tolerance=10):
                return all(abs(left - right) <= tolerance for left, right in zip(actual, expected))

            self.assertTrue(close_to(loaded[0].getpixel((0, 0)), WHITE))
            self.assertTrue(close_to(loaded[1].getpixel((0, 0)), DARK_BROWN))

            checker = loaded[2]

            def expected_checker(point):
                x, y = point
                parity = (x // CHECKER_SQUARE + y // CHECKER_SQUARE) % 2
                return CHECKER_DARK if parity else CHECKER_LIGHT

            width, height = checker.size
            # Corners and padding edges all follow the same 32px origin/grid.
            outer_points = (
                (4, 4),
                (CHECKER_SQUARE + 4, 4),
                (CHECKER_SQUARE * 2 + 4, 4),
                (4, CHECKER_SQUARE + 4),
                (width - 5, 4),
                (4, height - 5),
                (width - 5, height - 5),
            )
            # With 48px padding, these three points are inside the transparent
            # window but retain the canvas-wide 32px checker phase.
            transparent_window_points = ((112, 112), (144, 112), (176, 112))
            for point in (*outer_points, *transparent_window_points):
                with self.subTest(point=point):
                    self.assertTrue(
                        close_to(checker.getpixel(point), expected_checker(point))
                    )


if __name__ == "__main__":
    unittest.main()
