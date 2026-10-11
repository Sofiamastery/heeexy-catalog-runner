"""Append verified PNG-only metadata without replacing the historical catalog.

All source assets stay in Drive/the hosted runner. Recursive discovery is
metadata-only; optional held-file diagnostics download a bounded sample only
inside RUNNER_TEMP. Private reports retain unresolved identities explicitly.
"""
from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
import threading
import xml.etree.ElementTree as ET

try:
    from scripts import cloud_catalog_manifest as m
    from scripts import cloud_catalog_pipeline as p
    from scripts import cloud_catalog_svg_recovery as svg_recovery
except ImportError:
    import cloud_catalog_manifest as m
    import cloud_catalog_pipeline as p
    import cloud_catalog_svg_recovery as svg_recovery

REPORT_ROLE = "catalog_recovery_report_v1"
REPORT_NAME = "catalog-recovery-report.json"
MAX_SIDECAR_BYTES = 256 * 1024


def source_ids(record):
    assets = record.get("assets", {})
    sources = {value for key, value in assets.items()
            if key in {"png_drive_file_id", "svg_drive_file_id"} and value}
    archive_id = assets.get("archive_drive_file_id")
    if archive_id:
        sources.update(f"archive:{archive_id}:{assets[key]}" for key in ("archive_svg_member", "archive_png_member") if assets.get(key))
    return sources


def append_only_union(previous, additions):
    """No deletion, metadata replacement, duplicate identity or source claims."""
    identities = set()
    sources = set()
    for record in previous:
        identity = record.get("product_identity")
        if not identity or identity in identities:
            raise m.ManifestBuilderError("previous_identity_invalid")
        identities.add(identity)
        sources.update(source_ids(record))
        if p.candidate_from_record(record) is None:
            raise m.ManifestBuilderError("previous_candidate_invalid")
    output = list(previous)
    for record in sorted(additions, key=lambda item: item["product_identity"]):
        if record["product_identity"] in identities or source_ids(record) & sources:
            raise m.ManifestBuilderError("append_identity_collision")
        if p.candidate_from_record(record) is None:
            raise m.ManifestBuilderError("append_candidate_invalid")
        output.append(record)
        identities.add(record["product_identity"])
        sources.update(source_ids(record))
    return output


def json_product_record(payload, sidecar, children, root_id, namespace):
    """Exact same-parent filename AND SHA-256, never fuzzy or title matching.

    Marketplace-specific queue states are not website publication approval.
    The owner explicitly requested this PNG-only collection for the website.
    Explicit global rights blocks nevertheless remain holds.
    """
    data = json.loads(payload.decode("utf-8-sig"))
    if not isinstance(data, dict) or not isinstance(data.get("etsy"), dict):
        raise m.ManifestBuilderError("json_metadata_schema_unknown")
    for key in ("rights_gate", "screening_status", "publication_status"):
        if str(data.get(key, "")).casefold() in {"block", "blocked", "review", "rejected"}:
            raise m.ManifestBuilderError("json_explicit_rights_hold")
    etsy = data["etsy"]
    filename = etsy.get("Filename", "")
    design_id = data.get("id", "")
    if (not isinstance(filename, str) or PurePosixPath(filename).name != filename
            or "\\" in filename or not filename.lower().endswith(".png")
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", str(design_id))
            or etsy.get("ID") != design_id
            or sidecar["name"] != f"{design_id}_metadata.json"):
        raise m.ManifestBuilderError("json_exact_identity_invalid")
    pngs = [item for item in children if item.get("name") == filename
            and item.get("mimeType") == p.PNG_MIME]
    if len(pngs) != 1:
        raise m.ManifestBuilderError("json_png_name_not_unique")
    png = pngs[0]
    if png.get("parents") != sidecar.get("parents") or len(png.get("parents", [])) != 1:
        raise m.ManifestBuilderError("json_parent_mismatch")
    expected_hash = str(data.get("canonical_sha256", "")).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        raise m.ManifestBuilderError("json_png_hash_missing")
    if png.get("sha256Checksum", "").lower() != expected_hash:
        raise m.ManifestBuilderError("json_png_hash_mismatch_or_unavailable")
    svg_name = filename[:-4] + ".svg"
    svgs = [item for item in children if item.get("name") == svg_name]
    if len(svgs) > 1 or any(item.get("mimeType") not in p.SVG_MIMES for item in svgs):
        raise m.ManifestBuilderError("json_svg_pair_ambiguous")
    record = {
        "record_type": "heeexy_catalog_product", "schema_version": 3,
        "product_identity": m._seed_identity(namespace, "drive:" + png["id"]),
        "assets": {"png_drive_file_id": png["id"],
                   "svg_drive_file_id": svgs[0]["id"] if svgs else None,
                   "render_needed": False},
        "metadata": {"title": etsy.get("Title", ""),
                     "description": m.sanitize_incremental_etsy_description(etsy.get("Description", "")),
                     "keywords": list(m.split_terms(etsy.get("Tags", ""))),
                     "categories": [data["category"]] if data.get("category") else []},
        "source": {"namespace": namespace, "provider": "google_drive", "cohort": "png_only",
                   "container_id": root_id, "folder_id": png["parents"][0]},
        "metadata_provenance": {"source_file_id": sidecar["id"],
                                "source_sha256": hashlib.sha256(payload).hexdigest(),
                                "match": "same_parent_exact_filename_and_sha256"},
    }
    p.candidate_from_record(record)
    return record


def compatible_csv_payload(payload, png_asset):
    """Adapt two observed historical metadata schemas, not arbitrary drafts.

    Original files stay unchanged. Title/description/tags are not rewritten.
    Owner approval dates and completed visual-QA evidence are preserved in the
    original private sidecar; this adapter only normalizes contract fields.
    """
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")), strict=True)
    headers = reader.fieldnames or []
    if len({name.casefold() for name in headers}) != len(headers):
        raise m.ManifestBuilderError("csv_duplicate_headers")
    rows = [row for row in reader if any(row.values())]
    if len(rows) != 1 or None in rows[0]:
        raise m.ManifestBuilderError("csv_single_row_required")
    row = rows[0]
    approval = row.get("OwnerApproval", "")
    if re.fullmatch(r"explicit_(?:upload_instruction_)?\d{4}-\d{2}-\d{2}", approval):
        try:
            datetime.strptime(approval[-10:], "%Y-%m-%d")
        except ValueError as error:
            raise m.ManifestBuilderError("csv_approval_date_invalid") from error
        row["OwnerApproval"] = "owner_approved"
    if "DeliveryKind" not in row and {"FinalPNG_SHA256", "FinalPNGBytes", "VisualQA", "GlobalMarketplaceStatus"} <= row.keys():
        if (row["VisualQA"] != "PASS" or row["GlobalMarketplaceStatus"] != "READY_AFTER_FINAL_VISUAL_QA"
                or row.get("Mode") != "RGBA" or row.get("Background") != "Genuine transparency"):
            raise m.ManifestBuilderError("legacy_csv_visual_qa_not_passed")
        if not png_asset.sha256_checksum or row["FinalPNG_SHA256"].lower() != png_asset.sha256_checksum.lower():
            raise m.ManifestBuilderError("legacy_csv_png_hash_mismatch")
        link = row.get("DigitalFileLink", "")
        link_match = re.fullmatch(r"https://drive\.google\.com/uc\?export=download&id=([A-Za-z0-9_-]{20,200})", link)
        if not link_match or link_match.group(1) != png_asset.file_id:
            raise m.ManifestBuilderError("legacy_csv_drive_identity_mismatch")
        if row.get("DeliveryFile") != png_asset.name or int(row["FinalPNGBytes"]) != png_asset.size_bytes:
            raise m.ManifestBuilderError("legacy_csv_file_metadata_mismatch")
        # This owner-approved website import does not imply an active Etsy
        # listing. Preserve the actual marketplace state; invent no stage.
        return m.MetadataEvidence(source_label=p.METADATA_FILENAME, section="legacy-final-visual-qa", row_number=2,
            title=m.normalize_text(row.get("EtsyTitle")),
            description=m.sanitize_incremental_etsy_description(row.get("EtsyDescription", "")),
            tags=m.split_terms(row.get("EtsyTags", "")), drive_ids=(png_asset.file_id,),
            source_names=(png_asset.name,))
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    return output.getvalue().encode("utf-8")


def scan_tree(client_for_thread, root_id):
    """Bounded parallel metadata list calls; separate Drive client per thread."""
    folders = {root_id: {"id": root_id, "name": "root", "depth": 0}}
    children_by_parent = {}
    stats = Counter()
    frontier = [root_id]
    def batch_read(batch):
        return client_for_thread().list_children_many(batch)
    with ThreadPoolExecutor(max_workers=4) as executor:
        while frontier:
            batches = [frontier[i:i + 25] for i in range(0, len(frontier), 25)]
            next_frontier = []
            for result in executor.map(batch_read, batches):
                for parent, children in result.items():
                    children_by_parent[parent] = children
                    for child in children:
                        stats["items"] += 1
                        if stats["items"] > m.MAX_INVENTORY_ITEMS:
                            raise m.ManifestBuilderError("inventory_item_limit")
                        if child.get("mimeType") == m.FOLDER_MIME and child["id"] not in folders:
                            depth = folders[parent]["depth"] + 1
                            if depth > m.MAX_DRIVE_DEPTH or len(folders) >= m.MAX_INVENTORY_FOLDERS:
                                raise m.ManifestBuilderError("inventory_folder_limit")
                            folders[child["id"]] = {**child, "depth": depth}
                            next_frontier.append(child["id"])
                        elif child.get("mimeType") == m.SHORTCUT_MIME:
                            stats["shortcuts_not_followed"] += 1
                        elif child.get("mimeType") == p.PNG_MIME:
                            stats["png_files"] += 1
            frontier = next_frontier
    stats["folders"] = len(folders)
    return folders, children_by_parent, stats


def discover(drive, root_id, namespace, children_by_parent, previous):
    known = set().union(*(source_ids(record) for record in previous))
    additions = []
    holds = []
    claimed = set(known)
    total_bytes = 0
    for parent, children in sorted(children_by_parent.items()):
        csvs = [item for item in children if item.get("name") == p.METADATA_FILENAME]
        jsons = [item for item in children if str(item.get("name", "")).endswith("_metadata.json")]
        relevant = [item for item in children if str(item.get("name", "")).lower().endswith((".png", ".svg"))]
        if relevant and all(item["id"] in known for item in relevant):
            continue
        for sidecar in csvs + jsons:
            try:
                size = int(sidecar.get("size", 0))
                if not 0 < size <= MAX_SIDECAR_BYTES:
                    raise m.ManifestBuilderError("sidecar_size_limit")
                total_bytes += size
                if total_bytes > m.MAX_INCREMENTAL_SIDECAR_TOTAL_BYTES:
                    raise m.ManifestBuilderError("sidecar_total_limit")
                payload, _ = drive.download_bytes(sidecar["id"], MAX_SIDECAR_BYTES)
                if sidecar in jsons:
                    if sidecar.get("mimeType") not in {"application/json", "text/plain"}:
                        raise m.ManifestBuilderError("json_mime_invalid")
                    record = json_product_record(payload, sidecar, children, root_id, namespace)
                else:
                    if len(csvs) != 1:
                        raise m.ManifestBuilderError("csv_sidecar_ambiguous")
                    assets = [m._asset_from_metadata(item, parent, root_id, "png_only") for item in relevant]
                    assets = [asset for asset in assets if asset]
                    pngs = [asset for asset in assets if asset.extension == ".png"]
                    svgs = [asset for asset in assets if asset.extension == ".svg"]
                    if len(pngs) != 1 or len(svgs) > 1:
                        raise m.ManifestBuilderError("csv_product_assets_ambiguous")
                    if svgs and pngs[0].basename_normalized != svgs[0].basename_normalized:
                        raise m.ManifestBuilderError("csv_svg_pair_mismatch")
                    if sidecar.get("mimeType") not in m.INCREMENTAL_SIDECAR_MIMES or sidecar.get("parents") != [parent]:
                        raise m.ManifestBuilderError("csv_sidecar_parent_or_mime_invalid")
                    compatible = compatible_csv_payload(payload, pngs[0])
                    evidence = compatible if isinstance(compatible, m.MetadataEvidence) else m.parse_incremental_product_sidecar(
                        compatible, folder_id=parent, sidecar_metadata=sidecar,
                        png_asset=pngs[0], svg_asset=svgs[0] if svgs else None)
                    seed = m.ProductSeed(identity=m._seed_identity(namespace, "drive:" + pngs[0].file_id),
                        seed_key="drive:" + pngs[0].file_id,
                        png_file_ids=(pngs[0].file_id,), svg_file_ids=tuple(x.file_id for x in svgs),
                        metadata_evidence=(evidence,), source_refs=(evidence.source_ref,))
                    result, _ = m.reconcile_seed_catalog([seed], assets, namespace)
                    if len(result.records) != 1 or result.holds:
                        raise m.ManifestBuilderError("csv_reconciliation_held")
                    record = result.records[0]
                    record["metadata_provenance"]["raw_sidecar_sha256"] = hashlib.sha256(payload).hexdigest()
                ids = source_ids(record)
                if ids <= known:
                    continue
                if ids & claimed:
                    raise m.ManifestBuilderError("incremental_source_claim_conflict")
                claimed.update(ids)
                additions.append(record)
            except (m.ManifestBuilderError, p.PipelineError, ValueError, KeyError, TypeError) as error:
                if isinstance(error, p.PipelineError) and not error.permanent:
                    raise
                holds.append({"folder_id": parent, "sidecar_id": sidecar["id"],
                              "reason": getattr(error, "code", "sidecar_invalid")})
    return additions, holds


def discover_parallel(client_for_thread, root_id, namespace, children_by_parent, previous):
    """Fetch independent sidecars concurrently, then reconcile claims serially."""
    work = [(parent, items) for parent, items in sorted(children_by_parent.items())
            if any(item.get("name") == p.METADATA_FILENAME
                   or str(item.get("name", "")).endswith("_metadata.json") for item in items)]
    declared_total = sum(int(item.get("size") or 0) for _, items in work for item in items
                         if item.get("name") == p.METADATA_FILENAME
                         or str(item.get("name", "")).endswith("_metadata.json"))
    if declared_total > m.MAX_INCREMENTAL_SIDECAR_TOTAL_BYTES:
        raise m.ManifestBuilderError("sidecar_total_limit")
    def inspect(entry):
        parent, items = entry
        return discover(client_for_thread(), root_id, namespace, {parent: items}, previous)
    additions, holds = [], []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for proposed, rejected in executor.map(inspect, work):
            additions.extend(proposed)
            holds.extend(rejected)
    # Cross-folder conflicts are never resolved by which thread finishes first.
    append_only_union(previous, additions)
    return additions, holds


def diagnose_held(drive, previous, state, runner_temp):
    """Inspect at most three files per held reason; no validation is weakened."""
    diagnostics = []
    counts = Counter()
    for record in previous:
        candidate = p.candidate_from_record(record)
        hold = state["holds"].get(candidate.product_key)
        if not hold or counts[hold["reason"]] >= 3:
            continue
        counts[hold["reason"]] += 1
        info = {"product_key": candidate.product_key, "reason": hold["reason"],
                "has_svg": bool(candidate.source_svg_id), "has_png": bool(candidate.source_png_id)}
        with tempfile.TemporaryDirectory(prefix="catalog-diagnostic-", dir=runner_temp) as directory:
            try:
                if hold["reason"] == "svg_namespace_not_allowed" and candidate.source_svg_id:
                    payload, _ = drive.download_bytes(candidate.source_svg_id, p.MAX_SVG_BYTES)
                    root = ET.fromstring(payload)
                    info["foreign_elements"] = sorted({str(node.tag) for node in root.iter()
                        if str(node.tag).startswith("{") and not str(node.tag).startswith("{http://www.w3.org/2000/svg}")})[:30]
                elif candidate.source_png_id:
                    target = Path(directory) / "source.png"
                    drive.download_to(candidate.source_png_id, target, p.MAX_SOURCE_BYTES, candidate_scoped=True)
                    from PIL import Image
                    Image.MAX_IMAGE_PIXELS = p.MAX_IMAGE_PIXELS
                    try:
                        with Image.open(target) as image:
                            info.update(format=image.format, dimensions=list(image.size))
                            image.verify()
                        info["verify"] = "ok"
                    except Exception as error:
                        info["decode_exception_type"] = type(error).__name__
                        # Bounded diagnostic text is written ONLY to private Drive.
                        info["decode_exception"] = str(error)[:300]
            except Exception as error:
                info["diagnostic_error"] = getattr(error, "code", type(error).__name__)
        diagnostics.append(info)
    return diagnostics


def main():
    try:
        runner_temp = p.enforce_cloud_runtime()
        parser = argparse.ArgumentParser()
        parser.add_argument("--publish", action="store_true")
        parser.add_argument("--diagnose-holds", action="store_true")
        args = parser.parse_args()
        env = os.environ
        names = ("GOOGLE_DRIVE_OAUTH_CLIENT_ID", "GOOGLE_DRIVE_OAUTH_CLIENT_SECRET", "GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN")
        oauth = tuple(env[name] for name in names)
        root_id = env["HEEEXY_PNG_ONLY_ROOT_FOLDER_ID"]
        manifest_id = env["HEEEXY_CATALOG_MANIFEST_DRIVE_FILE_ID"]
        reports_id = env["HEEEXY_REPORTS_FOLDER_ID"]
        metadata_id = env["HEEEXY_CATALOG_METADATA_FOLDER_ID"]
        namespace = env["HEEEXY_CATALOG_SOURCE_NAMESPACE"]
        local = threading.local()
        def client():
            if not hasattr(local, "drive"):
                local.drive = m.ManifestDriveClient(*oauth)
            return local.drive
        drive = client()
        m.MANIFEST_DRIVE_LIST_FIELDS += ",createdTime,modifiedTime"
        drive.assert_private_writable_folder(reports_id)
        drive.assert_private_writable_folder(metadata_id)
        original = drive.read_generated_bytes(parent_id=metadata_id, role=m.MANIFEST_ROLE,
                    maximum_bytes=p.MAX_MANIFEST_BYTES, expected_file_id=manifest_id)
        if not original:
            raise m.ManifestBuilderError("previous_manifest_missing")
        m.manifest_product_identities(original)
        previous = [json.loads(line) for line in original.decode("utf-8").splitlines() if line.strip()]
        baseline = drive.get_metadata(manifest_id).get("modifiedTime", "")
        state, _ = drive.load_state(reports_id)
        folders, children, stats = scan_tree(client, root_id)
        additions, holds = discover_parallel(client, root_id, namespace, children, previous)
        png_additions = list(additions)
        svg_additions, svg_report = svg_recovery.discover(client, env, previous + additions, source_ids)
        additions.extend(svg_additions)
        union = append_only_union(previous, additions)
        png_inventory = {item["id"] for items in children.values() for item in items if item.get("mimeType") == p.PNG_MIME}
        known_png = set()
        published_png = set()
        held_png = set()
        for record in previous:
            candidate = p.candidate_from_record(record)
            if candidate.source_png_id in png_inventory:
                known_png.add(candidate.source_png_id)
                if candidate.product_key in state["completed"]:
                    published_png.add(candidate.source_png_id)
                elif candidate.product_key in state["holds"]:
                    held_png.add(candidate.source_png_id)
        new_png = {record["assets"]["png_drive_file_id"] for record in png_additions}
        report = {"generated_at": datetime.now(timezone.utc).isoformat(), "mode": "publish" if args.publish else "audit",
            "inventory": dict(stats), "previous_records": len(previous), "new_verified_records": len(additions),
            "union_records": len(union), "previous_completed": len(state["completed"]),
            "previous_holds": len(state["holds"]), "pipeline_hold_reasons": dict(Counter(item["reason"] for item in state["holds"].values())),
            "png_reconciliation": {"known_source_png": len(known_png), "completed_source_png": len(published_png),
                "held_source_png": len(held_png), "new_verified_png": len(new_png),
                "unassociated_png_files_not_unique_products": len(png_inventory - known_png - new_png)},
            "sidecar_holds": holds, "sidecar_hold_reasons": dict(Counter(item["reason"] for item in holds)),
            "additions": additions,
            "svg_recovery": svg_report,
            "unassociated_png_ids": sorted(png_inventory - known_png - new_png),
            "full_library_complete": False, "published": False}
        report["manifest_baseline_modified_time"] = baseline
        report["files_created_after_manifest"] = sum(
            bool(baseline) and item.get("createdTime", "") > baseline
            for items in children.values() for item in items
            if item.get("mimeType") == p.PNG_MIME)
        report["new_verified_bow_charm"] = sum(
            "bow charm" in record["metadata"]["title"].casefold()
            for record in additions)
        if args.diagnose_holds:
            report["held_diagnostics"] = diagnose_held(drive, previous, state, runner_temp)
        def save_report():
            drive.upsert_generated_bytes(parent_id=reports_id, name=REPORT_NAME, mime_type="application/json",
                role=REPORT_ROLE, payload=(p.canonical_json(report) + "\n").encode())
        save_report()
        if args.publish and additions:
            current = drive.read_generated_bytes(parent_id=metadata_id, role=m.MANIFEST_ROLE,
                maximum_bytes=p.MAX_MANIFEST_BYTES, expected_file_id=manifest_id)
            if current != original:
                raise m.ManifestBuilderError("manifest_changed_during_recovery")
            # All prior objects are preserved unchanged; known incomplete legacy
            # coverage is reported, never converted into a completeness claim.
            drive.upsert_generated_bytes(parent_id=metadata_id, name=m.MANIFEST_FILENAME, mime_type=m.MANIFEST_MIME,
                role=m.MANIFEST_ROLE, payload=m.render_manifest_jsonl(union), expected_file_id=manifest_id)
            report["published"] = True
            save_report()
        if args.publish:
            available = bool(additions) or any(
                (candidate.product_key not in state["completed"]
                 and (candidate.product_key not in state["holds"]
                      or p.hold_has_untried_repair(state["holds"][candidate.product_key])))
                for candidate in (p.candidate_from_record(record) for record in previous))
            output_path = env.get("GITHUB_OUTPUT")
            if output_path:
                with open(output_path, "a", encoding="utf-8") as output:
                    output.write("work_available=" + str(available).lower() + "\n")
        print("catalog_recovery_status=ok", flush=True)
    except Exception as error:
        code = getattr(error, "code", "unexpected_failure")
        if not re.fullmatch(r"[a-z0-9_]{1,100}", str(code)):
            code = "unexpected_failure"
        print(f"catalog_recovery_status=failed code={code}", file=__import__("sys").stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
