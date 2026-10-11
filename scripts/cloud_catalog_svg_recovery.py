"""Append screened, exact-ID loose SVGs from a pinned private metadata ledger.

No artwork bytes are read here. Archives and incomplete/unscreened rows remain
explicit private holds. Existing catalog records are never replaced.
"""
from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import re

try:
    from scripts import cloud_catalog_manifest as m
    from scripts import cloud_catalog_pipeline as p
except ImportError:
    import cloud_catalog_manifest as m
    import cloud_catalog_pipeline as p

MAX_LEDGER_BYTES = 64 * 1024 * 1024
MAX_LEDGER_ROWS = 30_000
LEGACY_SCREENING = "legacy screened human SVG cohort; owner-attested human Illustrator original"


def parse_ledger(payload, namespace, claimed):
    if not payload or len(payload) > MAX_LEDGER_BYTES:
        raise m.ManifestBuilderError("svg_recovery_ledger_size_invalid")
    seeds, holds, stats = [], [], Counter()
    seen = set()
    for row_number, line in enumerate(payload.decode("utf-8-sig").splitlines(), 1):
        if not line.strip():
            continue
        stats["ledger_rows"] += 1
        if stats["ledger_rows"] > MAX_LEDGER_ROWS:
            raise m.ManifestBuilderError("svg_recovery_row_limit")
        row = json.loads(line)
        if not isinstance(row, dict):
            raise m.ManifestBuilderError("svg_recovery_row_invalid")
        identity = row.get("identity")
        if not isinstance(identity, str) or not identity or identity in seen:
            raise m.ManifestBuilderError("svg_recovery_identity_invalid")
        seen.add(identity)
        source_id = m.valid_drive_id(row.get("source_file_id"))
        if source_id and source_id in claimed:
            stats["existing_source_rows"] += 1
            continue
        reasons = []
        if row.get("source_kind") != "loose_svg":
            reasons.append("archive_requires_exact_member_reconciliation")
        elif not source_id or identity != source_id or row.get("archive_file_id") or row.get("archive_member"):
            reasons.append("svg_recovery_exact_identity_invalid")
        # A text value saying needs_screen is not a completed safety review.
        if row.get("screening_evidence") != LEGACY_SCREENING:
            reasons.append("svg_screening_not_verified")
        if row.get("ai_provenance") != "Human-original/non-generative SVG campaign":
            reasons.append("svg_provenance_not_verified")
        if row.get("metadata_recovery_status") != "complete_recovered":
            reasons.append("svg_metadata_incomplete")
        if re.search(r"\b(block(?:ed)?|reject(?:ed)?|trademark|copyright)\b",
                     str(row.get("rights_gate", "")) + " " + str(row.get("historical_hold_reason", "")), re.I):
            reasons.append("svg_explicit_rights_hold")
        if not row.get("rights_gate"):
            reasons.append("svg_rights_evidence_missing")
        title = m.normalize_text(row.get("title"))
        description = m.normalize_text(row.get("description"))
        tags = m.split_terms(row.get("keywords"))
        categories = m.split_terms(row.get("category"))
        reasons.extend(m._seed_metadata_contract_reasons(title, description, categories[0] if categories else "", tags))
        if reasons:
            holds.append({"identity": identity, "row": row_number, "reasons": sorted(set(reasons))})
            continue
        key = f"drive:{source_id}"
        evidence = m.MetadataEvidence(source_label="screened_svg_recovery", section="metadata", row_number=row_number,
            title=title, description=description, tags=tags, categories=categories, drive_ids=(source_id,))
        seeds.append(m.ProductSeed(identity=m._seed_identity(namespace, key), seed_key=key,
            svg_file_ids=(source_id,), metadata_evidence=(evidence,), source_refs=(evidence.source_ref,)))
    stats["ready_metadata_rows"] = len(seeds)
    stats["held_metadata_rows"] = len(holds)
    if stats["ledger_rows"] != stats["existing_source_rows"] + len(seeds) + len(holds):
        raise m.ManifestBuilderError("svg_recovery_disposition_incomplete")
    return seeds, holds, stats


def discover(client_for_thread, env, previous, source_ids):
    file_id = env.get("HEEEXY_SVG_RECOVERY_LEDGER_ID", "")
    digest = env.get("HEEEXY_SVG_RECOVERY_LEDGER_SHA256", "")
    if not file_id and not digest:
        return [], {"configured": False, "full_library_complete": False}
    if not m.valid_drive_id(file_id) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise m.ManifestBuilderError("svg_recovery_configuration_invalid")
    payload, _ = client_for_thread().download_bytes(file_id, MAX_LEDGER_BYTES)
    if hashlib.sha256(payload).hexdigest() != digest:
        raise m.ManifestBuilderError("svg_recovery_ledger_hash_mismatch")
    claimed = set().union(*(source_ids(record) for record in previous)) if previous else set()
    namespace = env["HEEEXY_CATALOG_SOURCE_NAMESPACE"]
    seeds, holds, stats = parse_ledger(payload, namespace, claimed)
    def verify(seed):
        try:
            source_id = seed.svg_file_ids[0]
            item = client_for_thread().get_metadata(source_id, candidate_scoped=True)
            parents = item.get("parents", [])
            if len(parents) != 1:
                raise m.ManifestBuilderError("svg_recovery_parent_invalid")
            asset = m._asset_from_metadata(item, parents[0], parents[0], "screened_svg_recovery")
            if asset is None or asset.file_id != source_id or asset.extension != ".svg":
                raise m.ManifestBuilderError("svg_recovery_source_role_invalid")
            result, _ = m.reconcile_seed_catalog([seed], [asset], namespace)
            return result.records, result.holds
        except (m.ManifestBuilderError, p.PipelineError) as error:
            if isinstance(error, p.PipelineError) and not error.permanent:
                raise
            return [], [{"identity": seed.identity, "reasons": [error.code]}]
    records = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for accepted, rejected in executor.map(verify, seeds):
            records.extend(accepted)
            holds.extend(rejected)
    report = {"configured": True, "counts": dict(stats), "new_verified_records": len(records),
        "holds": holds, "hold_reasons": dict(Counter(reason for hold in holds for reason in hold["reasons"])),
        "full_library_complete": False,
        "scope": "Recovered metadata rows only. Unrecovered identities and archive members require separate reconciliation."}
    return records, report
