import hashlib
import io
import json
import csv
import unittest
import zipfile
from collections import Counter
from unittest import mock

from scripts import cloud_catalog_manifest as manifest
from scripts.cloud_catalog_manifest import (
    AssetGroup,
    DriveAsset,
    MANIFEST_SCHEMA_VERSION,
    ManifestBuilderError,
    MetadataEvidence,
    ProductSeed,
    archive_groups_from_evidence,
    build_inventory_report,
    evidence_from_grid,
    group_inventory_assets,
    load_builder_config,
    manifest_product_identities,
    normalize_basename,
    extract_drive_ids,
    parse_markdown_grids,
    parse_csv_grid,
    parse_metadata_file,
    parse_xlsx_grids,
    parse_ai_png_product_seeds,
    parse_dreamstime_product_seeds,
    extract_dreamstime_ledger_member,
    merge_authoritative_product_seeds,
    reconcile_seed_catalog,
    reconcile_catalog,
    read_metadata_imports,
    render_manifest_jsonl,
    scan_png_only_root_and_discover_incremental_seeds,
    scan_drive_root,
    sanitize_incremental_etsy_description,
)
from scripts.cloud_catalog_pipeline import (
    candidate_from_record,
    metadata_text_length,
    parse_manifest_jsonl,
)


PNG_ID = "png_source_file_id_000000000001"
SVG_ID = "svg_source_file_id_000000000001"
PNG_ONLY_ID = "png_source_file_id_000000000002"
SVG_ONLY_ID = "svg_source_file_id_000000000002"
ARCHIVE_ID = "archive_source_file_id_000000001"
ROOT_ID = "source_root_folder_id_00000000001"
PNG_ROOT_ID = "png_only_root_folder_id_000000001"
PARENT_ID = "source_parent_folder_id_000000001"
SYNTHETIC_ARCHIVE_IMAGE_ID = "9" * 18


def asset(file_id, name, *, parent=PARENT_ID, root=ROOT_ID, cohort="f16"):
    return DriveAsset(
        file_id=file_id,
        name=name,
        mime_type={".png": "image/png", ".svg": "image/svg+xml", ".zip": "application/zip"}[
            "." + name.rsplit(".", 1)[-1].lower()
        ],
        size_bytes=123,
        parent_id=parent,
        root_id=root,
        cohort=cohort,
    )


def evidence(
    *,
    drive_ids=(),
    title="Complete Synthetic Alpha SVG Design",
    description="A complete existing marketplace description with useful product details.",
    tags=("synthetic alpha", "test asset", "vector test"),
    categories=("Sports",),
    source_names=(),
    dreamstime_ids=(),
    source_label="reconciliation.xlsx",
    section="Shutterstock EPS",
    row_number=2,
):
    return MetadataEvidence(
        source_label=source_label,
        section=section,
        row_number=row_number,
        title=title,
        description=description,
        categories=categories,
        tags=tags,
        drive_ids=drive_ids,
        source_names=source_names,
        dreamstime_ids=dreamstime_ids,
    )


def synthetic_xlsx(rows):
    strings = []
    string_index = {}

    def shared(value):
        if value not in string_index:
            string_index[value] = len(strings)
            strings.append(value)
        return string_index[value]

    cells = []
    for row_index, row in enumerate(rows, start=1):
        row_cells = []
        for column_index, value in enumerate(row):
            number = column_index + 1
            letters = ""
            while number:
                number, remainder = divmod(number - 1, 26)
                letters = chr(65 + remainder) + letters
            row_cells.append(
                f'<c r="{letters}{row_index}" t="s"><v>{shared(value)}</v></c>'
            )
        cells.append(f'<row r="{row_index}">{"".join(row_cells)}</row>')
    shared_xml = "".join(f"<si><t>{value}</t></si>" for value in strings)
    workbook = io.BytesIO()
    with zipfile.ZipFile(workbook, "w") as archive:
        archive.writestr(
            "xl/workbook.xml",
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            'Target="worksheets/sheet1.xml"/></Relationships>',
        )
        archive.writestr(
            "xl/sharedStrings.xml",
            '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f"{shared_xml}</sst>",
        )
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f'<sheetData>{"".join(cells)}</sheetData></worksheet>',
        )
    return workbook.getvalue()


def rewrite_xlsx_member(payload, member_name, transform):
    rebuilt = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(payload), "r") as source:
        members = [(name, source.read(name)) for name in source.namelist()]
    with zipfile.ZipFile(rebuilt, "w") as target:
        for name, value in members:
            target.writestr(name, transform(value) if name == member_name else value)
    return rebuilt.getvalue()


def jsonl_bytes(*records):
    return b"".join(
        json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        for record in records
    )


def incremental_sidecar_bytes(
    delivery_file="New Design.png",
    *,
    title="Reviewed New Transparent PNG Design",
    description="A complete reviewed marketplace description with useful product details.",
    tags="transparent png, digital design, commercial use",
    owner_approval="approved_exact_synthetic_fixture",
    delivery_kind="transparent_png",
    etsy_stage="ACTIVE_VERIFIED",
    png_bytes="123",
    **overrides,
):
    row = {
        "DesignID": "synthetic-test-design",
        "Topic": "Graphics",
        "Slug": "synthetic-test-design",
        "DeliveryFile": delivery_file,
        "DeliveryKind": delivery_kind,
        "AIGenerated": "yes",
        "PNG_SHA256": "",
        "PNG_Bytes": png_bytes,
        "Width": "100",
        "Height": "100",
        "Mode": "RGBA",
        "AlphaMin": "0",
        "AlphaMax": "255",
        "OwnerApproval": owner_approval,
        "EtsyStage": etsy_stage,
        "EtsyTitle": title,
        "EtsyDescription": description,
        "EtsyTags": tags,
        "DriveDestinationFolderID": "CONNECTOR_UPLOAD_PENDING",
        "DriveProductFolderID": "CONNECTOR_UPLOAD_PENDING",
        "DrivePNGFileID": "CONNECTOR_UPLOAD_PENDING",
        "DriveUploadStatus": "CONNECTOR_UPLOAD_PENDING",
    }
    row.update(overrides)
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    return output.getvalue().encode("utf-8")


def dreamstime_primary(
    group_id,
    *,
    png_ids=(),
    svg_ids=(),
    source_name="design.svg",
    title="Authoritative Product Title",
    description="An authoritative existing product description with enough detail.",
    categories=("Vectors",),
    keywords=("authoritative", "design"),
    **overrides,
):
    row = {
        "record_type": "whitelist",
        "is_product_primary_record": True,
        "product_group_id": group_id,
        "canonical_local_id": (svg_ids or png_ids or ("",))[0],
        "drive_file_id": (svg_ids or png_ids or ("",))[0],
        "known_png_drive_file_ids": list(png_ids),
        "known_svg_drive_file_ids": list(svg_ids),
        "source_name": source_name,
        "metadata": {
            "title": title,
            "description": description,
            "categories": list(categories),
            "keywords": list(keywords),
        },
    }
    row.update(overrides)
    return row


def ai_png_identity(
    source_id,
    *,
    title="AI Product Title",
    description="An existing AI product description with enough useful detail.",
    categories=("AI Generated",),
    keywords=("ai", "png"),
    **overrides,
):
    row = {
        "record_type": "local_source_identity",
        "source_file_id": source_id,
        "source_extension": ".png",
        "source_name": "ai-design.png",
        "title": title,
        "description": description,
        "categories": list(categories),
        "keywords": list(keywords),
    }
    row.update(overrides)
    return row


class ManifestParsingTests(unittest.TestCase):
    def test_xlsx_is_parsed_in_memory_and_preserves_full_metadata(self):
        description = "Full description " + "detail " * 40
        payload = synthetic_xlsx(
            [
                ["Canonical source ID", "Buyer title", "Buyer description", "Keywords", "Category"],
                [SVG_ID, "Complete title", description, "alpha, beta, gamma", "Vectors"],
            ]
        )
        grids = parse_xlsx_grids(payload)
        self.assertEqual(grids[0][0], "Data")
        parsed = parse_metadata_file("stock_pipeline_reconciliation.xlsx", payload)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].description, description.strip())
        self.assertEqual(parsed[0].tags, ("alpha", "beta", "gamma"))
        self.assertEqual(parsed[0].drive_ids, (SVG_ID,))

    def test_csv_and_markdown_tables_are_supported(self):
        csv_payload = (
            "Drive File ID,Title,Description,Keywords,Categories\n"
            f'{PNG_ID},Full title,"Long existing description for this product","one,two",Graphics\n'
        ).encode()
        parsed_csv = parse_metadata_file("metadata.csv", csv_payload)
        self.assertEqual(parsed_csv[0].title, "Full title")
        self.assertEqual(parsed_csv[0].drive_ids, (PNG_ID,))

        markdown = (
            "# Report\n\n"
            "| Drive File ID | Title | Description | Keywords | Categories |\n"
            "| --- | --- | --- | --- | --- |\n"
            f"| {SVG_ID} | SVG title | A complete existing SVG description | tag one, tag two | Vectors |\n"
        ).encode()
        grids = parse_markdown_grids(markdown)
        self.assertEqual(len(grids), 1)
        parsed_md = parse_metadata_file("metadata.md", markdown)
        self.assertEqual(parsed_md[0].tags, ("tag one", "tag two"))

    def test_unsafe_xlsx_xml_is_rejected(self):
        payload = synthetic_xlsx([["Title"], ["Hello"]])
        with zipfile.ZipFile(io.BytesIO(payload), "r") as source:
            files = {name: source.read(name) for name in source.namelist()}
        files["xl/workbook.xml"] = b"<!DOCTYPE x [<!ENTITY a 'x'>]><x>&a;</x>"
        rebuilt = io.BytesIO()
        with zipfile.ZipFile(rebuilt, "w") as target:
            for name, value in files.items():
                target.writestr(name, value)
        with self.assertRaisesRegex(ManifestBuilderError, "metadata_xml_unsafe"):
            parse_xlsx_grids(rebuilt.getvalue())

    def test_xlsx_rejects_out_of_contract_dimensions_and_sparse_cells(self):
        payload = synthetic_xlsx([["Title", "Description"]])
        xfe = rewrite_xlsx_member(
            payload,
            "xl/worksheets/sheet1.xml",
            lambda value: value.replace(b'r="A1"', b'r="XFE1"', 1),
        )
        with self.assertRaisesRegex(ManifestBuilderError, "xlsx_column_limit"):
            parse_xlsx_grids(xfe)

        oversized_dimension = rewrite_xlsx_member(
            payload,
            "xl/worksheets/sheet1.xml",
            lambda value: value.replace(
                b"<sheetData>", b'<dimension ref="A1:A500001"/><sheetData>', 1
            ),
        )
        with self.assertRaisesRegex(ManifestBuilderError, "xlsx_row_limit"):
            parse_xlsx_grids(oversized_dimension)

        sparse = rewrite_xlsx_member(
            synthetic_xlsx([["Title"]]),
            "xl/worksheets/sheet1.xml",
            lambda value: value.replace(b'r="A1"', b'r="K1"', 1),
        )
        with mock.patch.object(manifest, "MAX_XLSX_CELLS", 10):
            with self.assertRaisesRegex(ManifestBuilderError, "xlsx_cell_limit"):
                parse_xlsx_grids(sparse)

    def test_xlsx_actual_cell_count_is_bounded(self):
        payload = synthetic_xlsx([["same", "same"]])
        with mock.patch.object(manifest, "MAX_XLSX_CELLS", 1):
            with self.assertRaisesRegex(ManifestBuilderError, "xlsx_cell_limit"):
                parse_xlsx_grids(payload)

    def test_csv_and_markdown_row_caps_fire_before_unbounded_grids_accumulate(self):
        with self.assertRaisesRegex(ManifestBuilderError, "metadata_row_limit"):
            parse_csv_grid(b"Title\nOne\nTwo\n", maximum_rows=2)

        markdown = (
            "| Title | Description |\n"
            "| --- | --- |\n"
            "| One | First row description |\n"
            "| Two | Second row description |\n"
        ).encode()
        with self.assertRaisesRegex(ManifestBuilderError, "metadata_row_limit"):
            parse_markdown_grids(markdown, maximum_rows=2)


class AuthoritativeSeedLedgerTests(unittest.TestCase):
    def test_seed_jsonl_line_row_and_encoding_limits_fail_closed(self):
        payload = jsonl_bytes(dreamstime_primary("group-one", svg_ids=(SVG_ID,)))
        with mock.patch.object(manifest, "MAX_LEDGER_LINE_BYTES", 16):
            with self.assertRaisesRegex(ManifestBuilderError, "line_limit"):
                parse_dreamstime_product_seeds(payload, "heeexy-store")
        with mock.patch.object(manifest, "MAX_LEDGER_ROWS", 1):
            with self.assertRaisesRegex(ManifestBuilderError, "row_limit"):
                parse_dreamstime_product_seeds(payload + payload, "heeexy-store")
        with self.assertRaisesRegex(ManifestBuilderError, "encoding_invalid"):
            parse_dreamstime_product_seeds(b"\xff\xfe\xfd\n", "heeexy-store")

    def test_pinned_zip_member_hash_and_structure_are_verified_in_memory(self):
        member = jsonl_bytes(dreamstime_primary("group-one", svg_ids=(SVG_ID,)))
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("sole-ledger-member.jsonl", member)
        payload = buffer.getvalue()
        self.assertEqual(
            extract_dreamstime_ledger_member(
                payload,
                expected_zip_sha256=hashlib.sha256(payload).hexdigest(),
                expected_member_sha256=hashlib.sha256(member).hexdigest(),
            ),
            member,
        )
        with self.assertRaisesRegex(ManifestBuilderError, "zip_hash_mismatch"):
            extract_dreamstime_ledger_member(
                payload,
                expected_zip_sha256="0" * 64,
                expected_member_sha256=hashlib.sha256(member).hexdigest(),
            )
        with self.assertRaisesRegex(ManifestBuilderError, "member_hash_mismatch"):
            extract_dreamstime_ledger_member(
                payload,
                expected_zip_sha256=hashlib.sha256(payload).hexdigest(),
                expected_member_sha256="0" * 64,
            )
        with mock.patch.object(manifest, "MAX_LEDGER_COMPRESSION_RATIO", 1):
            with self.assertRaisesRegex(ManifestBuilderError, "ratio_limit"):
                extract_dreamstime_ledger_member(
                    payload,
                    expected_zip_sha256=hashlib.sha256(payload).hexdigest(),
                    expected_member_sha256=hashlib.sha256(member).hexdigest(),
                )
        with mock.patch.object(manifest, "MAX_LEDGER_MEMBER_BYTES", len(member) - 1):
            with self.assertRaisesRegex(ManifestBuilderError, "member_size_invalid"):
                extract_dreamstime_ledger_member(
                    payload,
                    expected_zip_sha256=hashlib.sha256(payload).hexdigest(),
                    expected_member_sha256=hashlib.sha256(member).hexdigest(),
                )

        extra = io.BytesIO()
        with zipfile.ZipFile(extra, "w") as archive:
            archive.writestr("sole-ledger-member.jsonl", member)
            archive.writestr("unexpected.jsonl", member)
        extra_payload = extra.getvalue()
        with self.assertRaisesRegex(ManifestBuilderError, "member_invalid"):
            extract_dreamstime_ledger_member(
                extra_payload,
                expected_zip_sha256=hashlib.sha256(extra_payload).hexdigest(),
                expected_member_sha256=hashlib.sha256(member).hexdigest(),
            )

        unsafe = io.BytesIO()
        with zipfile.ZipFile(unsafe, "w") as archive:
            archive.writestr("../ledger.jsonl", member)
        unsafe_payload = unsafe.getvalue()
        with self.assertRaisesRegex(ManifestBuilderError, "entry_unsafe"):
            extract_dreamstime_ledger_member(
                unsafe_payload,
                expected_zip_sha256=hashlib.sha256(unsafe_payload).hexdigest(),
                expected_member_sha256=hashlib.sha256(member).hexdigest(),
            )

        encrypted = bytearray(payload)
        local = encrypted.find(b"PK\x03\x04")
        central = encrypted.find(b"PK\x01\x02")
        self.assertGreaterEqual(local, 0)
        self.assertGreaterEqual(central, 0)
        encrypted[local + 6 : local + 8] = (
            int.from_bytes(encrypted[local + 6 : local + 8], "little") | 1
        ).to_bytes(2, "little")
        encrypted[central + 8 : central + 10] = (
            int.from_bytes(encrypted[central + 8 : central + 10], "little") | 1
        ).to_bytes(2, "little")
        encrypted_payload = bytes(encrypted)
        with self.assertRaisesRegex(ManifestBuilderError, "entry_unsafe"):
            extract_dreamstime_ledger_member(
                encrypted_payload,
                expected_zip_sha256=hashlib.sha256(encrypted_payload).hexdigest(),
                expected_member_sha256=hashlib.sha256(member).hexdigest(),
            )

    def test_exact_union_merges_only_source_id_overlap_and_ignores_non_products(self):
        dream_payload = jsonl_bytes(
            dreamstime_primary(
                "group-one", png_ids=(PNG_ID,), svg_ids=(SVG_ID,), title="Primary One"
            ),
            dreamstime_primary("group-two", svg_ids=(SVG_ONLY_ID,), title="Primary Two"),
            {"record_type": "whitelist", "is_product_primary_record": False},
            {"record_type": "quarantine", "product_group_id": "ignored"},
        )
        ai_payload = jsonl_bytes(
            ai_png_identity(PNG_ID, title="Supplement must not replace primary"),
            ai_png_identity(PNG_ONLY_ID, title="Unique AI Product", categories=()),
            {"record_type": "quarantine", "source_file_id": "ignored"},
        )
        dream, dream_stats = parse_dreamstime_product_seeds(
            dream_payload, "heeexy-store"
        )
        ai, ai_stats = parse_ai_png_product_seeds(ai_payload, "heeexy-store")
        union, merge_stats = merge_authoritative_product_seeds(dream, ai)
        self.assertEqual(len(dream), 2)
        self.assertEqual(len(ai), 2)
        self.assertEqual(len(union), 3)
        self.assertEqual(dream_stats["non_primary_rows_ignored"], 1)
        self.assertEqual(dream_stats["quarantine_rows_ignored"], 1)
        self.assertEqual(ai_stats["quarantine_rows_ignored"], 1)
        self.assertEqual(merge_stats["ai_exact_overlaps_merged"], 1)
        self.assertEqual(merge_stats["ai_unique_seeds_added"], 1)
        overlap = next(seed for seed in union if PNG_ID in seed.png_file_ids)
        self.assertEqual(len(overlap.metadata_evidence), 2)
        repeated, _ = merge_authoritative_product_seeds(
            list(reversed(dream)), list(reversed(ai))
        )
        self.assertEqual(
            [(seed.identity, seed.source_file_ids) for seed in union],
            [(seed.identity, seed.source_file_ids) for seed in repeated],
        )

    def test_ai_identity_is_stable_when_later_promoted_to_dreamstime_group(self):
        ai, _ = parse_ai_png_product_seeds(
            jsonl_bytes(ai_png_identity(PNG_ONLY_ID)), "heeexy-store"
        )
        dream, _ = parse_dreamstime_product_seeds(
            jsonl_bytes(
                dreamstime_primary(
                    "future-dreamstime-group-convention",
                    png_ids=(PNG_ONLY_ID,),
                    source_name="ai-design.png",
                )
            ),
            "heeexy-store",
        )
        self.assertEqual(ai[0].seed_key, f"drive:{PNG_ONLY_ID}")
        self.assertNotEqual(dream[0].identity, ai[0].identity)
        promoted, _ = merge_authoritative_product_seeds(dream, ai)
        self.assertEqual(len(promoted), 1)
        self.assertEqual(promoted[0].seed_key, f"drive:{PNG_ONLY_ID}")
        self.assertEqual(promoted[0].identity, ai[0].identity)

    def test_optional_category_does_not_demote_authoritative_dreamstime_metadata(self):
        dream, _ = parse_dreamstime_product_seeds(
            jsonl_bytes(
                dreamstime_primary(
                    f"drive:{PNG_ID}",
                    png_ids=(PNG_ID,),
                    svg_ids=(),
                    source_name="design.png",
                    title="Authoritative Without Category",
                    categories=(),
                )
            ),
            "heeexy-store",
        )
        ai, _ = parse_ai_png_product_seeds(
            jsonl_bytes(ai_png_identity(PNG_ID, title="Lower Priority With Category")),
            "heeexy-store",
        )
        seeds, _ = merge_authoritative_product_seeds(dream, ai)
        result, _ = reconcile_seed_catalog(
            seeds, [asset(PNG_ID, "design.png")], "heeexy-store"
        )
        self.assertEqual(result.holds, [])
        self.assertEqual(
            result.records[0]["metadata"]["title"],
            "Authoritative Without Category",
        )
        self.assertEqual(result.records[0]["metadata"]["categories"], [])

    def test_ai_png_overlap_with_dreamstime_svg_role_is_held(self):
        dream, _ = parse_dreamstime_product_seeds(
            jsonl_bytes(
                dreamstime_primary(
                    "role-conflict",
                    png_ids=(),
                    svg_ids=(PNG_ID,),
                    source_name="design.svg",
                )
            ),
            "heeexy-store",
        )
        ai, _ = parse_ai_png_product_seeds(
            jsonl_bytes(ai_png_identity(PNG_ID)), "heeexy-store"
        )
        seeds, _ = merge_authoritative_product_seeds(dream, ai)
        self.assertEqual(len(seeds), 1)
        self.assertIn(
            "source_id_role_conflict_between_ledgers", seeds[0].hold_reasons
        )

    def test_synthetic_and_multiple_same_role_ids_are_held_as_seed_dispositions(self):
        payload = jsonl_bytes(
            dreamstime_primary(
                "multiple-pngs",
                png_ids=(PNG_ID, PNG_ONLY_ID),
                svg_ids=(),
                source_name="one.png",
            ),
            dreamstime_primary(
                "synthetic",
                png_ids=("svg-png-render-v1-not-a-real-drive-id",),
                svg_ids=(),
                source_name="fake.png",
            ),
        )
        seeds, _ = parse_dreamstime_product_seeds(payload, "heeexy-store")
        assets = [
            asset(PNG_ID, "one.png"),
            asset(PNG_ONLY_ID, "two.png"),
        ]
        result, stats = reconcile_seed_catalog(seeds, assets, "heeexy-store")
        self.assertEqual(result.records, [])
        self.assertEqual(len(result.holds), 2)
        reasons = {reason for hold in result.holds for reason in hold["reasons"]}
        self.assertIn("multiple_png_source_ids", reasons)
        self.assertIn("synthetic_or_invalid_source_id", reasons)
        self.assertEqual(stats["disposed_seed_products"], len(seeds))

    def test_inventory_can_verify_seed_ids_but_cannot_create_product_identity(self):
        dream, _ = parse_dreamstime_product_seeds(
            jsonl_bytes(
                dreamstime_primary(
                    "one", png_ids=(PNG_ID,), svg_ids=(SVG_ID,), title="Seed Product"
                )
            ),
            "heeexy-store",
        )
        ai, _ = parse_ai_png_product_seeds(
            jsonl_bytes(ai_png_identity(PNG_ONLY_ID, categories=())), "heeexy-store"
        )
        seeds, _ = merge_authoritative_product_seeds(dream, ai)
        unrelated_id = "unreferenced_png_drive_file_id_00001"
        inventory = [
            asset(PNG_ID, "seed.png"),
            asset(SVG_ID, "seed.svg"),
            asset(PNG_ONLY_ID, "ai.png", root=PNG_ROOT_ID, cohort="png_only"),
            asset(unrelated_id, "raw-scan-must-not-be-product.png"),
        ]
        result, stats = reconcile_seed_catalog(seeds, inventory, "heeexy-store")
        self.assertEqual(len(result.records), 2)
        self.assertEqual(result.holds, [])
        self.assertEqual(len(result.records) + len(result.holds), len(seeds))
        self.assertEqual(stats["unreferenced_inventory_assets"], 1)
        self.assertNotIn(
            unrelated_id,
            {
                value
                for record in result.records
                for value in record["assets"].values()
                if isinstance(value, str)
            },
        )

    def test_curated_pilot_gate_allows_reviewed_holds_without_claiming_global_complete(self):
        reconciliation = manifest.ReconciliationResult(
            records=[{}] * 760,
            holds=[
                {"reasons": ["description_missing", "tags_missing"]}
                for _ in range(35)
            ],
        )
        reconciliation.holds.extend(
            {"reasons": ["description_missing"]} for _ in range(4)
        )
        reconciliation.holds.append({"reasons": ["multiple_svg_source_ids"]})
        report = build_inventory_report(
            inventory_stats=Counter(
                authoritative_seeds=800,
                global_inventory_assets=1_100,
            ),
            metadata_stats=Counter(dreamstime_primary_seeds=740),
            reconciliation=reconciliation,
            manifest_payload=b"curated-pilot\n",
            expected_min_products=1_000,
            expected_min_accepted=800,
            generated_at="2026-10-08T00:00:00Z",
        )
        gate = report["publication_gate"]
        self.assertTrue(gate["eligible"])
        self.assertTrue(gate["pilot_manifest_eligible"])
        self.assertTrue(gate["complete_seed_disposition"])
        self.assertFalse(gate["full_catalog_complete"])
        self.assertFalse(gate["complete_inventory_coverage"])
        self.assertEqual(gate["global_product_target"], 1_000)
        self.assertEqual(gate["curated_seed_count"], 800)
        self.assertEqual(gate["global_product_gap"], 200)
        self.assertEqual(gate["reviewed_holds"], 40)
        self.assertEqual(gate["reviewed_hold_limit"], 40)
        self.assertEqual(gate["unresolved_holds"], 0)

        reconciliation.holds[0] = {
            "reasons": ["source_id_shared_by_primary_groups"]
        }
        collision = build_inventory_report(
            inventory_stats=Counter(authoritative_seeds=800),
            metadata_stats=Counter(),
            reconciliation=reconciliation,
            manifest_payload=b"curated-pilot\n",
            expected_min_products=1_000,
            expected_min_accepted=800,
            generated_at="2026-10-08T00:00:00Z",
        )
        self.assertFalse(collision["publication_gate"]["eligible"])
        self.assertEqual(
            collision["publication_gate"]["cross_product_collisions"], 1
        )

        reconciliation.holds[0] = {
            "reasons": ["png_source_missing_from_inventory"]
        }
        missing_asset = build_inventory_report(
            inventory_stats=Counter(authoritative_seeds=800),
            metadata_stats=Counter(),
            reconciliation=reconciliation,
            manifest_payload=b"curated-pilot\n",
            expected_min_products=1_000,
            expected_min_accepted=800,
            generated_at="2026-10-08T00:00:00Z",
        )
        self.assertFalse(missing_asset["publication_gate"]["eligible"])
        self.assertEqual(missing_asset["publication_gate"]["reviewed_holds"], 39)
        self.assertEqual(missing_asset["publication_gate"]["unresolved_holds"], 1)

        too_many_reviewed = manifest.ReconciliationResult(
            records=[{}] * 759,
            holds=[
                {"reasons": ["description_missing"]} for _ in range(41)
            ],
        )
        overflow = build_inventory_report(
            inventory_stats=Counter(authoritative_seeds=800),
            metadata_stats=Counter(),
            reconciliation=too_many_reviewed,
            manifest_payload=b"curated-pilot\n",
            expected_min_products=1_000,
            expected_min_accepted=800,
            generated_at="2026-10-08T00:00:00Z",
        )
        self.assertFalse(overflow["publication_gate"]["eligible"])
        self.assertEqual(overflow["publication_gate"]["reviewed_holds"], 41)


class InventoryAndReconciliationTests(unittest.TestCase):
    def test_groups_svg_png_twins_and_keeps_svg_and_png_only_products(self):
        assets = [
            asset(PNG_ID, "Hockey_Mom.png"),
            asset(SVG_ID, "hockey-mom.svg"),
            asset(PNG_ONLY_ID, "AI Flower.png", root=PNG_ROOT_ID, cohort="png_only"),
            asset(SVG_ONLY_ID, "Vintage Truck.svg"),
        ]
        groups, stats = group_inventory_assets(assets, "heeexy-store")
        self.assertEqual(len(groups), 3)
        by_basename = {group.basename_normalized: group for group in groups}
        self.assertEqual(len(by_basename["hockey mom"].png_assets), 1)
        self.assertEqual(len(by_basename["hockey mom"].svg_assets), 1)
        self.assertEqual(stats["svg_png_products"], 1)
        self.assertEqual(stats["svg_only_products"], 1)
        self.assertEqual(stats["png_only_products"], 1)

        repeated, _ = group_inventory_assets(list(reversed(assets)), "heeexy-store")
        self.assertEqual([group.identity for group in groups], [group.identity for group in repeated])

        svg_only, _ = group_inventory_assets(
            [asset(SVG_ONLY_ID, "Vintage Truck.svg")], "heeexy-store"
        )
        rendered_added, _ = group_inventory_assets(
            [
                asset(SVG_ONLY_ID, "Vintage Truck.svg"),
                asset("png_source_file_id_000000000099", "Vintage Truck.png"),
            ],
            "heeexy-store",
        )
        self.assertEqual(svg_only[0].identity, rendered_added[0].identity)

    def test_reconciliation_preserves_full_metadata_and_current_pipeline_accepts_it(self):
        self.assertEqual(MANIFEST_SCHEMA_VERSION, 3)
        groups, _ = group_inventory_assets(
            [asset(PNG_ID, "Hockey Mom.png"), asset(SVG_ID, "Hockey Mom.svg")],
            "heeexy-store",
        )
        direct = evidence(drive_ids=(SVG_ID,))
        current_dreamstime = evidence(
            drive_ids=(),
            title=direct.title,
            description="",
            tags=(),
            categories=(),
            dreamstime_ids=(SYNTHETIC_ARCHIVE_IMAGE_ID,),
        )
        result = reconcile_catalog(groups, [direct, current_dreamstime], "heeexy-store")
        self.assertEqual(len(result.records), 1)
        self.assertEqual(result.holds, [])
        record = result.records[0]
        self.assertEqual(record["schema_version"], MANIFEST_SCHEMA_VERSION)
        self.assertEqual(record["metadata"]["title"], direct.title)
        self.assertEqual(record["metadata"]["description"], direct.description)
        self.assertEqual(record["metadata"]["keywords"], list(direct.tags))
        self.assertEqual(
            record["external_refs"][0]["ref_value"], SYNTHETIC_ARCHIVE_IMAGE_ID
        )
        candidate = candidate_from_record(record)
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.title, direct.title)

        candidates, stats = parse_manifest_jsonl(
            render_manifest_jsonl(result.records).decode().splitlines()
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(stats["accepted"], 1)

    def test_canonical_source_priority_selects_one_complete_row_with_provenance(self):
        groups, _ = group_inventory_assets([asset(PNG_ID, "One.png")], "heeexy-store")
        low_priority = evidence(
            drive_ids=(PNG_ID,),
            title="A much longer marketplace title that must not win by text length",
            description="A lower priority marketplace description with complete product details.",
            tags=("low priority", "marketplace"),
            source_label="uploads_sample_09_2026.xlsx",
        )
        canonical = evidence(
            drive_ids=(PNG_ID,),
            title="Canonical title",
            description="The reviewed canonical description for this exact product asset.",
            tags=("canonical", "reviewed"),
            source_label="stock_pipeline_reconciliation_20260909.xlsx",
        )
        result = reconcile_catalog(groups, [low_priority, canonical], "heeexy-store")
        self.assertEqual(result.holds, [])
        self.assertEqual(result.records[0]["metadata"]["title"], "Canonical title")
        self.assertEqual(
            result.records[0]["metadata"]["description"], canonical.description
        )
        self.assertEqual(result.records[0]["metadata"]["keywords"], list(canonical.tags))
        self.assertEqual(
            result.records[0]["metadata_provenance"]["source_label"],
            canonical.source_label,
        )
        self.assertEqual(
            result.records[0]["metadata_provenance"]["match_confidence"], 300
        )
        self.assertEqual(
            result.records[0]["metadata_provenance"]["alternate_source_refs"],
            [low_priority.source_ref],
        )

    def test_complete_seo_without_category_beats_title_only_without_conflict(self):
        groups, _ = group_inventory_assets([asset(PNG_ID, "One.png")], "heeexy-store")
        complete_without_category = evidence(
            drive_ids=(PNG_ID,),
            title="Complete SEO title",
            categories=(),
            source_label="stock_pipeline_reconciliation_20260909.xlsx",
            row_number=2,
        )
        title_only = evidence(
            drive_ids=(PNG_ID,),
            title="Title-only alternate",
            description="",
            tags=(),
            categories=(),
            source_label="stock_pipeline_reconciliation_20260909.xlsx",
            row_number=3,
        )
        result = reconcile_catalog(
            groups,
            [title_only, complete_without_category],
            "heeexy-store",
        )
        self.assertEqual(result.holds, [])
        self.assertEqual(result.records[0]["metadata"]["title"], "Complete SEO title")
        self.assertEqual(result.records[0]["metadata"]["categories"], [])

    def test_exact_drive_id_match_precedes_higher_priority_basename_match(self):
        groups, _ = group_inventory_assets([asset(PNG_ID, "One.png")], "heeexy-store")
        exact = evidence(
            drive_ids=(PNG_ID,),
            title="Exact ID metadata",
            source_label="uploads_sample_09_2026.xlsx",
        )
        basename_only = evidence(
            drive_ids=(),
            source_names=("One.png",),
            title="Higher source priority but weaker basename match",
            source_label="stock_pipeline_reconciliation_20260909.xlsx",
        )
        result = reconcile_catalog(groups, [basename_only, exact], "heeexy-store")
        self.assertEqual(result.holds, [])
        self.assertEqual(result.records[0]["metadata"]["title"], exact.title)
        self.assertEqual(
            result.records[0]["metadata_provenance"]["match_confidence"], 300
        )

    def test_equal_priority_canonical_metadata_conflict_holds_product(self):
        groups, _ = group_inventory_assets([asset(PNG_ID, "One.png")], "heeexy-store")
        first = evidence(
            drive_ids=(PNG_ID,),
            title="Reviewed product title one",
            source_label="stock_pipeline_reconciliation_20260909.xlsx",
            row_number=2,
        )
        second = evidence(
            drive_ids=(PNG_ID,),
            title="Reviewed product title two",
            source_label="stock_pipeline_reconciliation_20260909.xlsx",
            row_number=3,
        )
        result = reconcile_catalog(groups, [first, second], "heeexy-store")
        self.assertEqual(result.records, [])
        self.assertIn("metadata_priority_conflict", result.holds[0]["reasons"])

    def test_utf16_metadata_lengths_match_javascript_zod_semantics(self):
        accepted_count = manifest.MAX_TITLE // 2
        self.assertEqual(metadata_text_length("😀" * accepted_count), manifest.MAX_TITLE)
        self.assertEqual(
            metadata_text_length("😀" * (accepted_count + 1)), manifest.MAX_TITLE + 2
        )
        groups, _ = group_inventory_assets([asset(PNG_ID, "Emoji.png")], "heeexy-store")

        accepted = reconcile_catalog(
            groups,
            [evidence(drive_ids=(PNG_ID,), title="😀" * accepted_count)],
            "heeexy-store",
        )
        self.assertEqual(len(accepted.records), 1)

        rejected = reconcile_catalog(
            groups,
            [evidence(drive_ids=(PNG_ID,), title="😀" * (accepted_count + 1))],
            "heeexy-store",
        )
        self.assertEqual(rejected.records, [])
        self.assertIn("title_exceeds_pipeline_limit", rejected.holds[0]["reasons"])

    def test_svg_only_is_emitted_with_render_needed_and_png_only_is_accepted(self):
        assets = [
            asset(SVG_ONLY_ID, "Vintage Truck.svg"),
            asset(PNG_ONLY_ID, "AI Flower.png", root=PNG_ROOT_ID, cohort="png_only"),
        ]
        groups, _ = group_inventory_assets(assets, "heeexy-store")
        rows = [
            evidence(drive_ids=(SVG_ONLY_ID,), title="Vintage Truck Design"),
            evidence(drive_ids=(PNG_ONLY_ID,), title="AI Flower Design"),
        ]
        result = reconcile_catalog(groups, rows, "heeexy-store")
        self.assertEqual(len(result.records), 2)
        by_title = {row["metadata"]["title"]: row for row in result.records}
        self.assertTrue(by_title["Vintage Truck Design"]["assets"]["render_needed"])
        self.assertIsNone(by_title["Vintage Truck Design"]["assets"]["png_drive_file_id"])
        self.assertFalse(by_title["AI Flower Design"]["assets"]["render_needed"])

    def test_missing_or_oversized_metadata_is_held_not_truncated(self):
        groups, _ = group_inventory_assets([asset(PNG_ID, "One.png")], "heeexy-store")
        missing = evidence(drive_ids=(PNG_ID,), description="", tags=())
        result = reconcile_catalog(groups, [missing], "heeexy-store")
        self.assertEqual(result.records, [])
        self.assertEqual(
            set(result.holds[0]["reasons"]), {"description_missing", "tags_missing"}
        )

        full_keyword_set = evidence(
            drive_ids=(PNG_ID,),
            tags=tuple(f"keyword {index}" for index in range(50)),
        )
        result = reconcile_catalog(groups, [full_keyword_set], "heeexy-store")
        self.assertEqual(len(result.records), 1)
        self.assertEqual(len(result.records[0]["metadata"]["keywords"]), 50)

        too_many = evidence(
            drive_ids=(PNG_ID,),
            tags=tuple(f"keyword {index}" for index in range(51)),
        )
        result = reconcile_catalog(groups, [too_many], "heeexy-store")
        self.assertEqual(result.records, [])
        self.assertIn("tags_exceed_pipeline_limit", result.holds[0]["reasons"])

    def test_duplicate_same_kind_assets_are_held(self):
        duplicate_id = "png_source_file_id_000000000003"
        groups, _ = group_inventory_assets(
            [asset(PNG_ID, "One.png"), asset(duplicate_id, "one.png")],
            "heeexy-store",
        )
        result = reconcile_catalog(groups, [evidence(drive_ids=(PNG_ID,))], "heeexy-store")
        self.assertEqual(result.records, [])
        self.assertIn("png_identity_ambiguous", result.holds[0]["reasons"])

    def test_archive_backed_products_use_exact_members_without_opening_zip(self):
        archive = asset(ARCHIVE_ID, "vectors.zip")
        row = MetadataEvidence(
            source_label="reconciliation.xlsx",
            section="archives",
            row_number=2,
            title="Archive Vector Design",
            description="A complete archived vector design description for buyers.",
            categories=("Vectors",),
            tags=("archive vector", "svg", "png"),
            drive_ids=(ARCHIVE_ID,),
            archive_drive_id=ARCHIVE_ID,
            archive_png_member="batch/design.png",
            archive_svg_member="batch/design.svg",
        )
        groups, stats = archive_groups_from_evidence([row], [archive], "heeexy-store")
        self.assertEqual(len(groups), 1)
        self.assertEqual(stats["archive_products_discovered"], 1)
        result = reconcile_catalog(groups, [row], "heeexy-store")
        self.assertEqual(len(result.records), 1)
        self.assertEqual(result.records[0]["assets"]["archive_svg_member"], "batch/design.svg")

    def test_multiple_products_in_one_archive_match_by_full_exact_member_path(self):
        archive = asset(ARCHIVE_ID, "vectors.zip")
        rows = [
            MetadataEvidence(
                source_label="reconciliation.xlsx",
                section="archives",
                row_number=index + 2,
                title=f"Archive Vector Design {index}",
                description=f"A complete archived vector design description number {index}.",
                categories=("Vectors",),
                tags=(f"archive {index}", "svg", "png"),
                drive_ids=(ARCHIVE_ID,),
                archive_drive_id=ARCHIVE_ID,
                archive_png_member=f"folder-{index}/design.png",
                archive_svg_member=f"folder-{index}/design.svg",
            )
            for index in (1, 2)
        ]
        groups, stats = archive_groups_from_evidence(rows, [archive], "heeexy-store")
        self.assertEqual(len(groups), 2)
        self.assertEqual(stats["archive_products_discovered"], 2)
        self.assertEqual(len({group.identity for group in groups}), 2)
        result = reconcile_catalog(groups, rows, "heeexy-store")
        self.assertEqual(len(result.records), 2)
        self.assertEqual(result.ambiguous_metadata_rows, 0)

    def test_mismatched_archive_member_pair_is_a_blocking_inventory_issue(self):
        archive = asset(ARCHIVE_ID, "vectors.zip")
        row = MetadataEvidence(
            source_label="reconciliation.xlsx",
            section="archives",
            row_number=2,
            title="Archive Vector Design",
            description="A complete archived vector design description for buyers.",
            categories=("Vectors",),
            tags=("archive vector", "svg", "png"),
            drive_ids=(ARCHIVE_ID,),
            archive_drive_id=ARCHIVE_ID,
            archive_png_member="one/design.png",
            archive_svg_member="two/design.svg",
        )
        groups, stats = archive_groups_from_evidence([row], [archive], "heeexy-store")
        self.assertEqual(groups, [])
        self.assertEqual(stats["blocking_archive_member_pair_mismatch"], 1)

    def test_archive_qualified_miss_never_falls_back_to_direct_or_basename_matching(self):
        groups, _ = group_inventory_assets(
            [asset(PNG_ID, "Hockey Mom.png")], "heeexy-store"
        )
        archive_qualified = MetadataEvidence(
            source_label="stock_pipeline_reconciliation_20260909.xlsx",
            section="archives",
            row_number=2,
            title="Archive-qualified title",
            description="A complete description that belongs only to the archive member.",
            categories=("Vectors",),
            tags=("archive", "qualified"),
            drive_ids=(PNG_ID,),
            source_names=("Hockey Mom.png",),
            archive_drive_id=ARCHIVE_ID,
            archive_png_member="missing/design.png",
        )
        result = reconcile_catalog(groups, [archive_qualified], "heeexy-store")
        self.assertEqual(result.records, [])
        self.assertEqual(result.unmatched_metadata_rows, 1)
        self.assertIn("title_missing", result.holds[0]["reasons"])

    def test_manifest_is_deterministic_and_report_exposes_holds_and_gate(self):
        groups, stats = group_inventory_assets(
            [asset(PNG_ID, "B.png"), asset(PNG_ONLY_ID, "A.png")], "heeexy-store"
        )
        rows = [
            evidence(drive_ids=(PNG_ID,), title="B Product"),
            evidence(drive_ids=(PNG_ONLY_ID,), title="A Product", description=""),
        ]
        result = reconcile_catalog(groups, rows, "heeexy-store")
        first = render_manifest_jsonl(result.records)
        second = render_manifest_jsonl(list(reversed(result.records)))
        self.assertEqual(first, second)
        report = build_inventory_report(
            inventory_stats=stats,
            metadata_stats=Counter(rows_parsed=2),
            reconciliation=result,
            manifest_payload=first,
            expected_min_products=2,
            expected_min_accepted=2,
            generated_at="2026-10-08T00:00:00Z",
        )
        self.assertFalse(report["publication_gate"]["eligible"])
        self.assertEqual(report["publication_gate"]["accepted_shortfall"], 1)
        self.assertEqual(report["manifest"]["sha256"], hashlib.sha256(first).hexdigest())
        self.assertEqual(report["reconciliation"]["hold_counts"]["description_missing"], 1)

        complete_rows = [
            evidence(drive_ids=(PNG_ID,), title="B Product"),
            evidence(drive_ids=(PNG_ONLY_ID,), title="A Product"),
        ]
        complete = reconcile_catalog(groups, complete_rows, "heeexy-store")
        complete_payload = render_manifest_jsonl(complete.records)
        authoritative_stats = Counter(
            stats, authoritative_seeds=2, global_inventory_assets=5
        )
        eligible = build_inventory_report(
            inventory_stats=authoritative_stats,
            metadata_stats=Counter(rows_parsed=2),
            reconciliation=complete,
            manifest_payload=complete_payload,
            expected_min_products=2,
            expected_min_accepted=2,
            generated_at="2026-10-08T00:00:00Z",
        )
        self.assertTrue(eligible["publication_gate"]["eligible"])
        self.assertTrue(eligible["publication_gate"]["complete_inventory_coverage"])
        self.assertEqual(eligible["publication_gate"]["global_inventory_assets"], 5)
        self.assertEqual(eligible["publication_gate"]["authoritative_seeds"], 2)
        self.assertTrue(eligible["publication_gate"]["complete_seed_disposition"])

        blocked = build_inventory_report(
            inventory_stats=stats,
            metadata_stats=Counter(rows_parsed=2, held_metadata_file_xlsx_invalid=1),
            reconciliation=complete,
            manifest_payload=complete_payload,
            expected_min_products=2,
            expected_min_accepted=2,
            generated_at="2026-10-08T00:00:00Z",
        )
        self.assertFalse(blocked["publication_gate"]["eligible"])
        self.assertEqual(blocked["publication_gate"]["metadata_file_blockers"], 1)

        identities = manifest_product_identities(complete_payload)
        self.assertEqual(
            identities,
            frozenset(record["product_identity"] for record in complete.records),
        )
        with self.assertRaisesRegex(ManifestBuilderError, "previous_manifest_invalid"):
            manifest_product_identities(complete_payload + complete_payload)

        continuity_stats = Counter(stats)
        continuity_stats["blocking_previous_manifest_missing_products"] = 1
        continuity_blocked = build_inventory_report(
            inventory_stats=continuity_stats,
            metadata_stats=Counter(rows_parsed=2),
            reconciliation=complete,
            manifest_payload=complete_payload,
            expected_min_products=2,
            expected_min_accepted=2,
            generated_at="2026-10-08T00:00:00Z",
        )
        self.assertFalse(continuity_blocked["publication_gate"]["eligible"])

        shortcut_stats = Counter(stats)
        shortcut_stats["f16_blocking_shortcuts_encountered"] = 1
        shortcut_blocked = build_inventory_report(
            inventory_stats=shortcut_stats,
            metadata_stats=Counter(rows_parsed=2),
            reconciliation=complete,
            manifest_payload=complete_payload,
            expected_min_products=2,
            expected_min_accepted=2,
            generated_at="2026-10-08T00:00:00Z",
        )
        self.assertFalse(shortcut_blocked["publication_gate"]["eligible"])
        self.assertEqual(shortcut_blocked["publication_gate"]["inventory_blockers"], 1)


class MetadataOnlyDriveTests(unittest.TestCase):
    def test_drive_frontier_query_maps_parents_and_fails_closed_when_incomplete(self):
        class FakeRequest:
            def __init__(self, response):
                self.response = response

            def execute(self, num_retries):
                self.test_case.assertEqual(num_retries, 3)
                return self.response

        class FakeFiles:
            def __init__(self, test_case, responses):
                self.test_case = test_case
                self.responses = iter(responses)
                self.calls = []

            def list(self, **kwargs):
                self.calls.append(kwargs)
                request = FakeRequest(next(self.responses))
                request.test_case = self.test_case
                return request

        class FakeService:
            def __init__(self, files):
                self.files_resource = files

            def files(self):
                return self.files_resource

        files = FakeFiles(
            self,
            [
                {
                    "files": [
                        {
                            "id": PNG_ID,
                            "name": "Root.png",
                            "mimeType": "image/png",
                            "size": "123",
                            "parents": [ROOT_ID],
                        }
                    ],
                    "nextPageToken": "page-2",
                    "incompleteSearch": False,
                },
                {
                    "files": [
                        {
                            "id": PNG_ONLY_ID,
                            "name": "PNG only.png",
                            "mimeType": "image/png",
                            "size": "456",
                            "parents": [PNG_ROOT_ID],
                        }
                    ],
                    "incompleteSearch": False,
                },
            ],
        )
        drive = manifest.ManifestDriveClient.__new__(manifest.ManifestDriveClient)
        drive.service = FakeService(files)

        children = drive.list_children_many([ROOT_ID, PNG_ROOT_ID])

        self.assertEqual([item["id"] for item in children[ROOT_ID]], [PNG_ID])
        self.assertEqual([item["id"] for item in children[PNG_ROOT_ID]], [PNG_ONLY_ID])
        self.assertEqual([call["pageToken"] for call in files.calls], [None, "page-2"])
        self.assertIn(f"'{ROOT_ID}' in parents", files.calls[0]["q"])
        self.assertIn(f"'{PNG_ROOT_ID}' in parents", files.calls[0]["q"])
        self.assertIn("incompleteSearch", files.calls[0]["fields"])
        self.assertIn("parents", files.calls[0]["fields"])

        incomplete_files = FakeFiles(
            self,
            [{"files": [], "incompleteSearch": True}],
        )
        drive.service = FakeService(incomplete_files)
        with self.assertRaises(ManifestBuilderError) as context:
            drive.list_children_many([ROOT_ID])
        self.assertEqual(context.exception.code, "drive_list_incomplete")

        mismatched_files = FakeFiles(
            self,
            [
                {
                    "files": [
                        {
                            "id": PNG_ID,
                            "name": "Out of scope.png",
                            "mimeType": "image/png",
                            "size": "123",
                            "parents": [PARENT_ID],
                        }
                    ],
                    "incompleteSearch": False,
                }
            ],
        )
        drive.service = FakeService(mismatched_files)
        with self.assertRaises(ManifestBuilderError) as context:
            drive.list_children_many([ROOT_ID])
        self.assertEqual(context.exception.code, "drive_list_parent_invalid")

        multiple_parent_files = FakeFiles(
            self,
            [
                {
                    "files": [
                        {
                            "id": PNG_ID,
                            "name": "Ambiguous parent.png",
                            "mimeType": "image/png",
                            "size": "123",
                            "parents": [ROOT_ID, PNG_ROOT_ID],
                        }
                    ],
                    "incompleteSearch": False,
                }
            ],
        )
        drive.service = FakeService(multiple_parent_files)
        with self.assertRaises(ManifestBuilderError) as context:
            drive.list_children_many([ROOT_ID, PNG_ROOT_ID])
        self.assertEqual(context.exception.code, "drive_list_parent_invalid")

    def test_frontier_batches_respect_count_and_query_length_limits(self):
        frontier = [
            (f"folder_{index:02d}_" + "a" * 188, 1)
            for index in range(manifest.MAX_DRIVE_PARENT_BATCH)
        ]

        batches = list(manifest._frontier_batches(frontier))

        self.assertGreater(len(batches), 1)
        self.assertEqual(sum(len(batch) for batch in batches), len(frontier))
        for batch in batches:
            self.assertLessEqual(len(batch), manifest.MAX_DRIVE_PARENT_BATCH)
            query = manifest._drive_parent_query([folder_id for folder_id, _ in batch])
            self.assertLessEqual(len(query), manifest.MAX_DRIVE_PARENT_QUERY_CHARS)

    def test_source_scan_batches_wide_frontier_and_excludes_png_only_root(self):
        folder_ids = [
            f"wide_source_folder_{index:03d}_0000000000001"
            for index in range(30)
        ]

        class FakeDrive:
            def __init__(self):
                self.batches = []

            def get_metadata(self, file_id):
                return {"id": file_id, "mimeType": "application/vnd.google-apps.folder"}

            def list_children_many(self, parent_ids):
                self.batches.append(tuple(parent_ids))
                output = {}
                for parent_id in parent_ids:
                    if parent_id == ROOT_ID:
                        output[parent_id] = [
                            {
                                "id": child_id,
                                "name": f"Folder {index}",
                                "mimeType": "application/vnd.google-apps.folder",
                                "parents": [ROOT_ID],
                            }
                            for index, child_id in enumerate(folder_ids)
                        ] + [
                            {
                                "id": PNG_ROOT_ID,
                                "name": "PNG Only",
                                "mimeType": "application/vnd.google-apps.folder",
                                "parents": [ROOT_ID],
                            }
                        ]
                    else:
                        index = folder_ids.index(parent_id)
                        output[parent_id] = [
                            {
                                "id": f"wide_png_file_{index:03d}_000000000000001",
                                "name": f"Design {index}.png",
                                "mimeType": "image/png",
                                "size": "123",
                                "parents": [parent_id],
                            }
                        ]
                return output

            def download_bytes(self, *args, **kwargs):
                raise AssertionError("source assets must never be downloaded by the builder")

        drive = FakeDrive()
        assets, stats = scan_drive_root(
            drive,
            ROOT_ID,
            "f16",
            excluded_folder_ids=frozenset({PNG_ROOT_ID}),
        )

        self.assertEqual(len(assets), 30)
        self.assertEqual(stats["folders_scanned"], 31)
        self.assertEqual(stats["excluded_nested_roots"], 1)
        self.assertEqual([len(batch) for batch in drive.batches], [1, 25, 5])
        self.assertNotIn(PNG_ROOT_ID, [folder for batch in drive.batches for folder in batch])

    def test_source_scan_lists_only_and_never_downloads_asset_bytes(self):
        class FakeDrive:
            def __init__(self):
                self.download_calls = 0

            def get_metadata(self, file_id):
                return {"id": file_id, "mimeType": "application/vnd.google-apps.folder"}

            def list_children_many(self, folder_ids):
                output = {}
                for folder_id in folder_ids:
                    if folder_id == ROOT_ID:
                        output[folder_id] = [
                        {
                            "id": PNG_ID,
                            "name": "Design.png",
                            "mimeType": "image/png",
                            "size": "123",
                            "version": "1",
                            "parents": [folder_id],
                        },
                        {
                            "id": "nested_folder_id_000000000001",
                            "name": "Nested",
                            "mimeType": "application/vnd.google-apps.folder",
                            "parents": [folder_id],
                        },
                    ]
                    else:
                        output[folder_id] = [
                            {
                                "id": SVG_ID,
                                "name": "Nested.svg",
                                "mimeType": "image/svg+xml",
                                "size": "456",
                                "version": "1",
                                "parents": [folder_id],
                            }
                        ]
                return output

            def download_bytes(self, *args, **kwargs):
                self.download_calls += 1
                raise AssertionError("source assets must never be downloaded by the builder")

        drive = FakeDrive()
        assets, stats = scan_drive_root(drive, ROOT_ID, "f16")
        self.assertEqual(len(assets), 2)
        self.assertEqual(stats["png_files"], 1)
        self.assertEqual(stats["svg_files"], 1)
        self.assertEqual(drive.download_calls, 0)

    def test_source_scan_blocks_shortcuts_wrong_mime_and_zero_byte_assets(self):
        wrong_mime_id = "wrong_mime_file_id_00000000001"
        zero_byte_id = "zero_byte_file_id_000000000001"
        shortcut_id = "shortcut_file_id_0000000000001"

        class FakeDrive:
            def get_metadata(self, file_id):
                return {"id": file_id, "mimeType": "application/vnd.google-apps.folder"}

            def list_children_many(self, folder_ids):
                return {
                    folder_id: [
                        {
                            "id": PNG_ID,
                            "name": "Valid.png",
                            "mimeType": "image/png",
                            "size": "123",
                            "parents": [folder_id],
                        },
                        {
                            "id": wrong_mime_id,
                            "name": "Wrong.png",
                            "mimeType": "text/plain",
                            "size": "123",
                            "parents": [folder_id],
                        },
                        {
                            "id": zero_byte_id,
                            "name": "Empty.svg",
                            "mimeType": "image/svg+xml",
                            "size": "0",
                            "parents": [folder_id],
                        },
                        {
                            "id": shortcut_id,
                            "name": "Linked design",
                            "mimeType": "application/vnd.google-apps.shortcut",
                            "parents": [folder_id],
                        },
                    ]
                    for folder_id in folder_ids
                }

        assets, stats = scan_drive_root(FakeDrive(), ROOT_ID, "f16")
        self.assertEqual([item.file_id for item in assets], [PNG_ID])
        self.assertEqual(stats["blocking_asset_mime_invalid"], 1)
        self.assertEqual(stats["blocking_asset_size_invalid"], 1)
        self.assertEqual(stats["blocking_shortcuts_encountered"], 1)

    def test_metadata_reader_downloads_only_supported_metadata_files_to_memory(self):
        metadata_id = "metadata_workbook_file_id_0000001"
        payload = synthetic_xlsx(
            [
                ["Canonical source ID", "Buyer title", "Buyer description", "Keywords", "Category"],
                [SVG_ID, "Title", "A sufficiently long description", "one, two", "Vectors"],
            ]
        )

        class FakeDrive:
            def __init__(self):
                self.downloaded = []

            def get_metadata(self, file_id):
                return {"id": file_id, "mimeType": "application/vnd.google-apps.folder"}

            def list_children_many(self, folder_ids):
                return {
                    folder_id: [
                        {
                            "id": metadata_id,
                            "name": "metadata.xlsx",
                            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            "size": str(len(payload)),
                            "parents": [folder_id],
                        },
                        {
                            "id": PNG_ID,
                            "name": "product.png",
                            "mimeType": "image/png",
                            "size": "999999",
                            "parents": [folder_id],
                        },
                    ]
                    for folder_id in folder_ids
                }

            def download_bytes(self, file_id, maximum):
                self.downloaded.append((file_id, maximum))
                return payload, {"id": file_id, "size": str(len(payload))}

        drive = FakeDrive()
        rows, stats = read_metadata_imports(drive, ROOT_ID)
        self.assertEqual(len(rows), 1)
        self.assertEqual(stats["files_read"], 1)
        self.assertEqual(drive.downloaded, [(metadata_id, 128 * 1024 * 1024)])

    def test_metadata_reader_batches_wide_frontier_without_downloading_products(self):
        folder_ids = [
            f"wide_metadata_folder_{index:03d}_000000000001"
            for index in range(30)
        ]

        class FakeDrive:
            def __init__(self):
                self.batches = []

            def get_metadata(self, file_id):
                return {"id": file_id, "mimeType": "application/vnd.google-apps.folder"}

            def list_children_many(self, parent_ids):
                self.batches.append(tuple(parent_ids))
                return {
                    parent_id: (
                        [
                            {
                                "id": child_id,
                                "name": f"Metadata folder {index}",
                                "mimeType": "application/vnd.google-apps.folder",
                                "parents": [ROOT_ID],
                            }
                            for index, child_id in enumerate(folder_ids)
                        ]
                        if parent_id == ROOT_ID
                        else []
                    )
                    for parent_id in parent_ids
                }

            def download_bytes(self, *args, **kwargs):
                raise AssertionError("no product or metadata bytes should be downloaded")

        drive = FakeDrive()
        rows, stats = read_metadata_imports(drive, ROOT_ID)

        self.assertEqual(rows, [])
        self.assertEqual(stats["folders_scanned"], 31)
        self.assertEqual(stats["items_seen"], 0)
        self.assertEqual([len(batch) for batch in drive.batches], [1, 25, 5])

    def test_metadata_reader_enforces_depth_folder_and_item_limits(self):
        nested_id = "nested_metadata_folder_id_00000001"

        class NestedDrive:
            def get_metadata(self, file_id):
                return {"id": file_id, "mimeType": "application/vnd.google-apps.folder"}

            def list_children_many(self, folder_ids):
                return {
                    folder_id: (
                        [
                            {
                                "id": nested_id,
                                "name": "Nested",
                                "mimeType": "application/vnd.google-apps.folder",
                                "parents": [folder_id],
                            }
                        ]
                        if folder_id == ROOT_ID
                        else []
                    )
                    for folder_id in folder_ids
                }

        with mock.patch.object(manifest, "MAX_DRIVE_DEPTH", 0):
            with self.assertRaises(ManifestBuilderError) as context:
                read_metadata_imports(NestedDrive(), ROOT_ID)
        self.assertEqual(context.exception.code, "metadata_tree_depth_limit")

        with mock.patch.object(manifest, "MAX_INVENTORY_FOLDERS", 1):
            with self.assertRaises(ManifestBuilderError) as context:
                read_metadata_imports(NestedDrive(), ROOT_ID)
        self.assertEqual(context.exception.code, "metadata_folder_limit")

        class ItemDrive:
            def get_metadata(self, file_id):
                return {"id": file_id, "mimeType": "application/vnd.google-apps.folder"}

            def list_children_many(self, folder_ids):
                return {
                    folder_id: [
                        {
                            "id": f"non_metadata_file_id_{index:03d}_00001",
                            "name": f"Ignored {index}.txt",
                            "mimeType": "text/plain",
                            "size": "1",
                            "parents": [folder_id],
                        }
                        for index in range(2)
                    ]
                    for folder_id in folder_ids
                }

        with mock.patch.object(manifest, "MAX_INVENTORY_ITEMS", 1):
            with self.assertRaises(ManifestBuilderError) as context:
                read_metadata_imports(ItemDrive(), ROOT_ID)
        self.assertEqual(context.exception.code, "metadata_item_limit")


class IncrementalPngOnlySidecarTests(unittest.TestCase):
    FOLDER_A = "incremental_product_folder_a_00001"
    FOLDER_B = "incremental_product_folder_b_00001"
    PNG_A = "incremental_png_file_a_0000000001"
    PNG_B = "incremental_png_file_b_0000000001"
    SVG_A = "incremental_svg_file_a_0000000001"
    SIDECAR_A = "incremental_sidecar_file_a_000001"
    SIDECAR_B = "incremental_sidecar_file_b_000001"

    class FakeDrive:
        def __init__(self, children, payloads):
            self.children = children
            self.payloads = payloads
            self.downloaded = []

        def get_metadata(self, file_id):
            if file_id == PNG_ROOT_ID:
                return {
                    "id": file_id,
                    "mimeType": "application/vnd.google-apps.folder",
                }
            raise AssertionError(f"unexpected metadata read: {file_id}")

        def list_children_many(self, folder_ids):
            return {folder_id: list(self.children.get(folder_id, ())) for folder_id in folder_ids}

        def download_bytes(self, file_id, maximum, *, candidate_scoped=False):
            self.downloaded.append((file_id, maximum, candidate_scoped))
            if file_id not in self.payloads:
                raise AssertionError("product asset bytes must never be downloaded")
            payload, metadata = self.payloads[file_id]
            return payload, dict(metadata)

    @classmethod
    def folder(cls, folder_id, name, parent_id=PNG_ROOT_ID):
        return {
            "id": folder_id,
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }

    @classmethod
    def png(cls, file_id, name, folder_id):
        return {
            "id": file_id,
            "name": name,
            "mimeType": "image/png",
            "size": "123",
            "sha256Checksum": "a" * 64,
            "parents": [folder_id],
        }

    @classmethod
    def svg(cls, file_id, name, folder_id):
        return {
            "id": file_id,
            "name": name,
            "mimeType": "image/svg+xml",
            "size": "123",
            "parents": [folder_id],
        }

    @classmethod
    def sidecar(cls, file_id, folder_id, payload):
        return {
            "id": file_id,
            "name": "design_metadata_and_specs.csv",
            "mimeType": "text/csv",
            "size": str(len(payload)),
            "parents": [folder_id],
        }

    def scan(self, children, payloads):
        drive = self.FakeDrive(children, payloads)
        assets, seeds, stats = scan_png_only_root_and_discover_incremental_seeds(
            drive,
            PNG_ROOT_ID,
            pinned_source_ids=frozenset(),
            source_namespace="heeexy-store",
        )
        return drive, assets, seeds, stats

    def test_new_exact_parent_sidecar_becomes_authoritative_seed_without_asset_download(self):
        payload = incremental_sidecar_bytes(
            PNG_SHA256="a" * 64,
            DriveProductFolderID=self.FOLDER_A,
            DrivePNGFileID=self.PNG_A,
        )
        sidecar = self.sidecar(self.SIDECAR_A, self.FOLDER_A, payload)
        children = {
            PNG_ROOT_ID: [self.folder(self.FOLDER_A, "New Product")],
            self.FOLDER_A: [
                self.png(self.PNG_A, "New Design.png", self.FOLDER_A),
                self.svg(self.SVG_A, "New Design.svg", self.FOLDER_A),
                sidecar,
            ],
        }
        drive, assets, seeds, stats = self.scan(
            children, {self.SIDECAR_A: (payload, sidecar)}
        )
        self.assertEqual(len(seeds), 1)
        self.assertEqual(seeds[0].hold_reasons, ())
        self.assertEqual(seeds[0].png_file_ids, (self.PNG_A,))
        self.assertEqual(seeds[0].svg_file_ids, (self.SVG_A,))
        self.assertEqual(
            drive.downloaded,
            [(self.SIDECAR_A, manifest.MAX_INCREMENTAL_SIDECAR_BYTES, True)],
        )
        self.assertEqual(stats["accepted_product_folders"], 1)
        reconciled, _ = reconcile_seed_catalog(seeds, assets, "heeexy-store")
        self.assertEqual(len(reconciled.records), 1)
        self.assertEqual(reconciled.holds, [])

    def _scan_with_approval(self, owner_approval, etsy_stage="ACTIVE_VERIFIED"):
        payload = incremental_sidecar_bytes(
            PNG_SHA256="a" * 64,
            DriveProductFolderID=self.FOLDER_A,
            DrivePNGFileID=self.PNG_A,
            owner_approval=owner_approval,
            etsy_stage=etsy_stage,
        )
        sidecar = self.sidecar(self.SIDECAR_A, self.FOLDER_A, payload)
        children = {
            PNG_ROOT_ID: [self.folder(self.FOLDER_A, "New Product")],
            self.FOLDER_A: [self.png(self.PNG_A, "New Design.png", self.FOLDER_A), sidecar],
        }
        _, _, seeds, _ = self.scan(children, {self.SIDECAR_A: (payload, sidecar)})
        return seeds[0]

    def test_pending_owner_approval_is_held_without_auto_approve_policy(self):
        import os
        os.environ.pop("HEEEXY_PNG_ONLY_AUTO_APPROVE", None)
        seed = self._scan_with_approval("pending", "draft")
        self.assertIn("incremental_sidecar_owner_approval_invalid", seed.hold_reasons)

    def test_owner_auto_approve_policy_accepts_pending_png_only_uploads(self):
        import os
        os.environ["HEEEXY_PNG_ONLY_AUTO_APPROVE"] = "true"
        try:
            self.assertEqual(self._scan_with_approval("", "").hold_reasons, ())
            self.assertEqual(self._scan_with_approval("pending", "draft").hold_reasons, ())
            self.assertEqual(self._scan_with_approval("waiting_for_owner", "listed").hold_reasons, ())
            # Explicit refusals are still held.
            self.assertIn(
                "incremental_sidecar_owner_approval_invalid",
                self._scan_with_approval("rejected").hold_reasons,
            )
            self.assertIn(
                "incremental_sidecar_etsy_stage_invalid",
                self._scan_with_approval("approved", "removed").hold_reasons,
            )
        finally:
            os.environ.pop("HEEEXY_PNG_ONLY_AUTO_APPROVE", None)

    def test_studio_json_metadata_sidecar_publishes_under_auto_approve(self):
        import os
        payload = json.dumps({
            "id": "LC023",
            "filename": "New Design.png",
            "canonical_png": "New Design.png",
            "dimensions": "3000x3000",
            "etsy_state": "not_uploaded",
            "release_status": "prepared_for_authorized_release",
            "etsy_title": "Safari Birthday 10 Bow Charm PNG, Jungle Animal Birthday Clipart, Transparent Sublimation Digital Download",
            "etsy_description": "Original Safari Birthday 10 bow-charm artwork with coordinated topic-specific charms, long layered ribbon tails and a polished glossy finish for shirts and gifts.",
            "etsy_tags": "safari birthday 10, safari birthday, birthday bow png, jungle birthday, bow charm png, transparent png",
        }).encode()
        sidecar = {
            "id": self.SIDECAR_A,
            "name": "New Design_metadata.json",
            "mimeType": "application/json",
            "size": str(len(payload)),
            "parents": [self.FOLDER_A],
        }
        children = {
            PNG_ROOT_ID: [self.folder(self.FOLDER_A, "LC023_safari_birthday_10")],
            self.FOLDER_A: [self.png(self.PNG_A, "New Design.png", self.FOLDER_A), sidecar],
        }
        os.environ.pop("HEEEXY_PNG_ONLY_AUTO_APPROVE", None)
        _, _, seeds, stats = self.scan(children, {self.SIDECAR_A: (payload, sidecar)})
        self.assertEqual(seeds, [])
        self.assertEqual(stats["quarantine_incremental_sidecar_missing"], 1)
        os.environ["HEEEXY_PNG_ONLY_AUTO_APPROVE"] = "true"
        try:
            _, assets, seeds, stats = self.scan(children, {self.SIDECAR_A: (payload, sidecar)})
            self.assertEqual(len(seeds), 1)
            self.assertEqual(seeds[0].hold_reasons, ())
            self.assertEqual(stats["json_sidecars_discovered"], 1)
            reconciled, _ = reconcile_seed_catalog(seeds, assets, "heeexy-store")
            self.assertEqual(len(reconciled.records), 1)
            self.assertTrue(reconciled.records[0]["metadata"]["title"].startswith("Safari Birthday 10"))
        finally:
            os.environ.pop("HEEEXY_PNG_ONLY_AUTO_APPROVE", None)

    def test_append_only_manifest_keeps_previous_records_and_adds_new_ones(self):
        def record(identity, title):
            return {
                "schema_version": manifest.MANIFEST_SCHEMA_VERSION,
                "record_type": "heeexy_catalog_product",
                "product_identity": identity,
                "title": title,
            }
        old_a = "gdrive:v3:" + "a" * 64
        old_b = "gdrive:v3:" + "b" * 64
        new_c = "gdrive:v3:" + "c" * 64
        previous = manifest.render_manifest_jsonl([record(old_a, "A"), record(old_b, "B")])
        # Current build lost B (e.g. legacy inventory issue) and found new C.
        payload, added = manifest.append_only_manifest(
            previous, [record(old_a, "A changed"), record(new_c, "C")]
        )
        self.assertEqual(added, 1)
        rows = [json.loads(line) for line in payload.decode().splitlines()]
        self.assertEqual([row["product_identity"] for row in rows], [old_a, old_b, new_c])
        # Previously published rows are kept exactly as they were.
        self.assertEqual(rows[0]["title"], "A")
        with self.assertRaises(manifest.ManifestBuilderError):
            manifest.append_only_manifest(b"", [record(new_c, "C")])

    def test_missing_sidecar_is_quarantined_outside_authoritative_gate(self):
        missing_children = {
            PNG_ROOT_ID: [self.folder(self.FOLDER_A, "Missing")],
            self.FOLDER_A: [self.png(self.PNG_A, "New Design.png", self.FOLDER_A)],
        }
        drive, assets, seeds, stats = self.scan(missing_children, {})
        self.assertEqual([asset.file_id for asset in assets], [self.PNG_A])
        self.assertEqual(seeds, [])
        self.assertEqual(drive.downloaded, [])
        self.assertEqual(stats["quarantined_direct_candidate_folders"], 1)
        self.assertEqual(stats["quarantine_incremental_sidecar_missing"], 1)
        self.assertEqual(stats["incremental_seeds"], 0)
        gate_inventory = Counter(authoritative_seeds=1, global_inventory_assets=1)
        gate_inventory.update(stats)
        report = build_inventory_report(
            inventory_stats=gate_inventory,
            metadata_stats=Counter(),
            reconciliation=manifest.ReconciliationResult(records=[{}]),
            manifest_payload=b"trusted-product\n",
            expected_min_products=1,
            expected_min_accepted=1,
            generated_at="2026-10-10T00:00:00Z",
        )
        self.assertEqual(report["publication_gate"]["unresolved_holds"], 0)
        self.assertEqual(report["publication_gate"]["blocking_issue_count"], 0)
        self.assertTrue(report["publication_gate"]["eligible"])

    def test_duplicate_and_malformed_canonical_sidecars_remain_blockers(self):
        cases = []

        valid = incremental_sidecar_bytes()
        sidecar_a = self.sidecar(self.SIDECAR_A, self.FOLDER_A, valid)
        sidecar_b = self.sidecar(self.SIDECAR_B, self.FOLDER_A, valid)
        duplicate_children = {
            PNG_ROOT_ID: [self.folder(self.FOLDER_A, "Duplicate")],
            self.FOLDER_A: [
                self.png(self.PNG_A, "New Design.png", self.FOLDER_A),
                sidecar_a,
                sidecar_b,
            ],
        }
        cases.append(
            (
                "duplicate",
                duplicate_children,
                {
                    self.SIDECAR_A: (valid, sidecar_a),
                    self.SIDECAR_B: (valid, sidecar_b),
                },
                "incremental_sidecar_duplicate",
            )
        )

        malformed = incremental_sidecar_bytes(owner_approval="not-approved")
        malformed_sidecar = self.sidecar(self.SIDECAR_A, self.FOLDER_A, malformed)
        malformed_children = {
            PNG_ROOT_ID: [self.folder(self.FOLDER_A, "Malformed")],
            self.FOLDER_A: [
                self.png(self.PNG_A, "New Design.png", self.FOLDER_A),
                malformed_sidecar,
            ],
        }
        cases.append(
            (
                "malformed",
                malformed_children,
                {self.SIDECAR_A: (malformed, malformed_sidecar)},
                "incremental_sidecar_owner_approval_invalid",
            )
        )

        for label, children, payloads, reason in cases:
            with self.subTest(label=label):
                _, assets, seeds, _ = self.scan(children, payloads)
                self.assertEqual(len(seeds), 1)
                self.assertIn(reason, seeds[0].hold_reasons)
                reconciled, inventory_stats = reconcile_seed_catalog(
                    seeds, assets, "heeexy-store"
                )
                report = build_inventory_report(
                    inventory_stats=inventory_stats,
                    metadata_stats=Counter(),
                    reconciliation=reconciled,
                    manifest_payload=b"",
                    expected_min_products=1,
                    expected_min_accepted=1,
                    generated_at="2026-10-10T00:00:00Z",
                )
                self.assertEqual(report["publication_gate"]["unresolved_holds"], 1)
                self.assertFalse(report["publication_gate"]["eligible"])

    def test_nested_canonical_folder_remains_recursive_inventory_only(self):
        payload = incremental_sidecar_bytes(
            DriveProductFolderID=self.FOLDER_B,
            DrivePNGFileID=self.PNG_B,
        )
        sidecar = self.sidecar(self.SIDECAR_B, self.FOLDER_B, payload)
        children = {
            PNG_ROOT_ID: [self.folder(self.FOLDER_A, "Legacy Container")],
            self.FOLDER_A: [
                self.folder(self.FOLDER_B, "Nested Product", self.FOLDER_A)
            ],
            self.FOLDER_B: [
                self.png(self.PNG_B, "New Design.png", self.FOLDER_B),
                sidecar,
            ],
        }
        drive, assets, seeds, stats = self.scan(
            children, {self.SIDECAR_B: (payload, sidecar)}
        )
        self.assertEqual([asset.file_id for asset in assets], [self.PNG_B])
        self.assertEqual(seeds, [])
        self.assertEqual(drive.downloaded, [])
        self.assertEqual(stats["coverage_nested_candidate_folders"], 1)
        self.assertEqual(stats["coverage_non_direct_sidecars"], 1)
        self.assertEqual(stats["incremental_seeds"], 0)

    def test_direct_multi_file_folder_is_quarantined_without_a_seed(self):
        payload = incremental_sidecar_bytes()
        sidecar = self.sidecar(self.SIDECAR_A, self.FOLDER_A, payload)
        children = {
            PNG_ROOT_ID: [self.folder(self.FOLDER_A, "Ambiguous")],
            self.FOLDER_A: [
                self.png(self.PNG_A, "New Design.png", self.FOLDER_A),
                self.png(self.PNG_B, "Other Design.png", self.FOLDER_A),
                sidecar,
            ],
        }
        drive, assets, seeds, stats = self.scan(
            children, {self.SIDECAR_A: (payload, sidecar)}
        )
        self.assertEqual(len(assets), 2)
        self.assertEqual(seeds, [])
        self.assertEqual(drive.downloaded, [])
        self.assertEqual(stats["quarantined_direct_candidate_folders"], 1)
        self.assertEqual(
            stats["quarantine_incremental_png_identity_ambiguous"], 1
        )
        self.assertEqual(stats["incremental_seeds"], 0)

    def test_sidecar_never_matches_png_from_another_folder(self):
        payload = incremental_sidecar_bytes("Shared.png")
        sidecar = self.sidecar(self.SIDECAR_B, self.FOLDER_B, payload)
        children = {
            PNG_ROOT_ID: [
                self.folder(self.FOLDER_A, "A"),
                self.folder(self.FOLDER_B, "B"),
            ],
            self.FOLDER_A: [self.png(self.PNG_A, "Shared.png", self.FOLDER_A)],
            self.FOLDER_B: [
                self.png(self.PNG_B, "Different.png", self.FOLDER_B),
                sidecar,
            ],
        }
        _, _, seeds, stats = self.scan(
            children, {self.SIDECAR_B: (payload, sidecar)}
        )
        self.assertEqual(len(seeds), 1)
        reasons = {reason for seed in seeds for reason in seed.hold_reasons}
        self.assertIn("incremental_sidecar_delivery_file_not_exact", reasons)
        self.assertFalse(any(not seed.hold_reasons for seed in seeds))
        self.assertEqual(stats["quarantine_incremental_sidecar_missing"], 1)

    def test_description_sanitizer_preserves_core_and_removes_reviewed_suffixes(self):
        core = (
            "A polished transparent PNG for shirts, signs, and printable crafts. "
            "Do not resell, share or redistribute the standalone digital file."
        )
        full = (
            core
            + " The composition is AI-assisted and individually reviewed for transparency, framing and file integrity.\n\n"
            + "AI DISCLOSURE\nThis artwork used assisted tools.\n\n"
            + "Etsy Purchase Protection\nMarketplace terms apply.\n\n"
            + "Explore our Entire Shop Mega Bundle with 170,000+ designs and all shop files at a major bundle discount:\n"
            + "https://example.invalid/promo"
        )
        self.assertEqual(sanitize_incremental_etsy_description(full), core)
        for suffix in (
            "\nEtsy Purchase Protection\nMarketplace terms apply.",
            "\nExplore our Entire Shop Mega Bundle with 170,000+ designs.\nhttps://example.invalid",
            "\nAI DISCLOSURE\nDisclosure text.\nEtsy Purchase Protection\nTerms.",
        ):
            with self.subTest(suffix=suffix.splitlines()[1]):
                self.assertEqual(
                    sanitize_incremental_etsy_description(core + suffix), core
                )
        embedded = core + " A guide mentions AI disclosure requirements for sellers."
        self.assertEqual(sanitize_incremental_etsy_description(embedded), embedded)

    def test_authoritative_gate_accepts_valid_incremental_growth_above_reviewed_floor(self):
        report = build_inventory_report(
            inventory_stats=Counter(authoritative_seeds=3, global_inventory_assets=3),
            metadata_stats=Counter(),
            reconciliation=manifest.ReconciliationResult(records=[{}, {}, {}]),
            manifest_payload=b"three-products\n",
            expected_min_products=2,
            expected_min_accepted=2,
            generated_at="2026-10-10T00:00:00Z",
        )
        self.assertTrue(report["publication_gate"]["eligible"])


class UtilityTests(unittest.TestCase):
    def test_private_manifest_identity_is_pinned_before_continuity_read(self):
        class FakeDrive:
            def __init__(self, entries):
                self.entries = entries
                self.downloaded = []

            def _find_by_properties(self, parent_id, properties):
                return self.entries

            def download_bytes(self, file_id, maximum_bytes):
                self.downloaded.append((file_id, maximum_bytes))
                return b'{"record_type":"heeexy_catalog_product"}\n', {}

        expected_id = "catalog_manifest_drive_file_id_001"
        drive = FakeDrive([{"id": expected_id}])
        payload = manifest.ManifestDriveClient.read_generated_bytes(
            drive,
            parent_id="catalog_metadata_folder_id_000001",
            role=manifest.MANIFEST_ROLE,
            maximum_bytes=1_024,
            expected_file_id=expected_id,
        )
        self.assertTrue(payload)
        self.assertEqual(drive.downloaded, [(expected_id, 1_024)])

        for entries, error_code in (
            ([], "generated_output_expected_missing"),
            ([{"id": "different_manifest_drive_file_id_01"}], "generated_output_identity_mismatch"),
        ):
            with self.subTest(error_code=error_code):
                with self.assertRaisesRegex(ManifestBuilderError, error_code):
                    manifest.ManifestDriveClient.read_generated_bytes(
                        FakeDrive(entries),
                        parent_id="catalog_metadata_folder_id_000001",
                        role=manifest.MANIFEST_ROLE,
                        maximum_bytes=1_024,
                        expected_file_id=expected_id,
                    )

    def test_builder_requires_pinned_authoritative_ledger_ids_and_hashes(self):
        environment = {
            "HEEEXY_METADATA_IMPORT_FOLDER_ID": "metadata_import_folder_id_000001",
            "HEEEXY_CATALOG_METADATA_FOLDER_ID": "catalog_metadata_folder_id_000001",
            "HEEEXY_CATALOG_MANIFEST_DRIVE_FILE_ID": "catalog_manifest_drive_file_id_001",
            "HEEEXY_REPORTS_FOLDER_ID": "reports_folder_id_00000000000001",
            "HEEEXY_F16_ROOT_FOLDER_ID": ROOT_ID,
            "HEEEXY_PNG_ONLY_ROOT_FOLDER_ID": PNG_ROOT_ID,
            "HEEEXY_EXPECTED_MIN_PRODUCT_COUNT": "1000",
            "HEEEXY_EXPECTED_MIN_ACCEPTED_COUNT": "800",
            "HEEEXY_CATALOG_SOURCE_NAMESPACE": "heeexy-store",
            "HEEEXY_DREAMSTIME_LEDGER_ZIP_DRIVE_FILE_ID": "dreamstime_ledger_zip_file_id_001",
            "HEEEXY_DREAMSTIME_LEDGER_ZIP_NAME": "primary-ledger.zip",
            "HEEEXY_DREAMSTIME_LEDGER_ZIP_SHA256": "a" * 64,
            "HEEEXY_DREAMSTIME_LEDGER_MEMBER_SHA256": "b" * 64,
            "HEEEXY_AI_PNG_METADATA_JSONL_DRIVE_FILE_ID": "ai_png_jsonl_drive_file_id_00001",
            "HEEEXY_AI_PNG_METADATA_JSONL_NAME": "supplemental-ledger.jsonl",
            "HEEEXY_AI_PNG_METADATA_JSONL_SHA256": "c" * 64,
            "GOOGLE_DRIVE_OAUTH_CLIENT_ID": "client",
            "GOOGLE_DRIVE_OAUTH_CLIENT_SECRET": "secret",
            "GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN": "refresh",
        }
        config, _ = load_builder_config(environment)
        self.assertEqual(config.global_inventory_target, 1_000)
        self.assertEqual(config.curated_seed_target, 800)
        self.assertEqual(
            config.catalog_manifest_drive_file_id,
            "catalog_manifest_drive_file_id_001",
        )
        self.assertEqual(config.dreamstime_ledger_zip_sha256, "a" * 64)
        missing_hash = dict(environment)
        missing_hash.pop("HEEEXY_AI_PNG_METADATA_JSONL_SHA256")
        with self.assertRaisesRegex(ManifestBuilderError, "ai_png_jsonl_sha256_invalid"):
            load_builder_config(missing_hash)

    def test_basename_normalization_is_format_independent(self):
        self.assertEqual(normalize_basename("Folder/My_Design.SVG"), "my design")
        self.assertEqual(normalize_basename("my-design.png"), "my design")

    def test_drive_ids_are_read_from_raw_values_and_drive_urls(self):
        self.assertEqual(extract_drive_ids(SVG_ID), (SVG_ID,))
        self.assertEqual(
            extract_drive_ids(f"https://drive.google.com/file/d/{PNG_ID}/view"),
            (PNG_ID,),
        )

    def test_grid_header_discovery_tolerates_preamble_rows(self):
        grid = [
            ["Export generated 2026-10-08"],
            ["Canonical source ID", "Buyer title", "Buyer description", "Keywords", "Category"],
            [SVG_ID, "Title", "A sufficiently long description", "one, two", "Vectors"],
        ]
        parsed = evidence_from_grid(grid, "source.xlsx", "Sheet")
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].drive_ids, (SVG_ID,))


if __name__ == "__main__":
    unittest.main()
