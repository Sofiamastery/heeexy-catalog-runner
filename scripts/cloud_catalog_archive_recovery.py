"""Recover exact ZIP members from the pinned, previously screened SVG ledger.

Only central-directory reads happen here, on the hosted runner. A basename is
never searched across a ZIP: the existing canonical path must match exactly.
The asset pipeline validates and extracts the chosen SVG and renders its PNG.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import io
import json
from pathlib import PurePosixPath
import re
import zipfile

try:
    from scripts import cloud_catalog_manifest as m
    from scripts import cloud_catalog_pipeline as p
except ImportError:
    import cloud_catalog_manifest as m
    import cloud_catalog_pipeline as p

ARCHIVE_SCREENING = "screen_state=archive_screen_terminal; local=clear; visual=unscreened; lineage=No AI lineage recorded"
MAX_INDEX_BYTES = 16 * 1024 * 1024


def archive_claim(archive_id, member):
    return f"archive:{archive_id}:{member}"


def exact_member(row, archive_name):
    """Resolve only the full existing path, never extend the abbreviated label."""
    path = str(row.get("canonical_folder", ""))
    marker = archive_name + "/"
    parts = path.split(marker)
    if len(parts) != 2 or not parts[0].endswith("/"):
        raise m.ManifestBuilderError("archive_canonical_path_ambiguous")
    member = parts[1]
    if (not member.lower().endswith(".svg") or "\\" in member
            or p.normalized_archive_member(member) != member
            or PurePosixPath(member).name != row.get("source_name")):
        raise m.ManifestBuilderError("archive_canonical_member_invalid")
    # The old product key used the lowercase stem, not a ZIP path.
    label = str(row.get("archive_member", ""))
    if label not in {member, PurePosixPath(member).stem.casefold()}:
        raise m.ManifestBuilderError("archive_legacy_identity_mismatch")
    return member


def eligible_rows(payload):
    rows, holds = [], []
    seen = set()
    for line in payload.decode("utf-8-sig").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("source_kind") != "archive_svg_member":
            continue
        identity = row.get("identity")
        if not identity or identity in seen:
            raise m.ManifestBuilderError("archive_ledger_identity_invalid")
        seen.add(identity)
        archive_id = m.valid_drive_id(row.get("archive_file_id"))
        reasons = []
        if not archive_id or identity != f"{archive_id}|{row.get('archive_member', '')}" or row.get("source_file_id"):
            reasons.append("archive_ledger_identity_invalid")
        # This specific terminal local verdict came from the historical strict
        # render/OCR IP screen. 'needs_screen' or merely 'local=clear' is NOT it.
        if row.get("screening_evidence") != ARCHIVE_SCREENING:
            reasons.append("archive_screening_not_verified")
        if row.get("ai_provenance") != "Human-original/non-generative SVG campaign":
            reasons.append("archive_provenance_not_verified")
        if row.get("metadata_recovery_status") != "complete_recovered":
            reasons.append("archive_metadata_incomplete")
        if not row.get("rights_gate") or re.search(
                r"\b(block(?:ed)?|reject(?:ed)?|trademark|copyright)\b",
                str(row.get("rights_gate", "")) + " " + str(row.get("historical_hold_reason", "")), re.I):
            reasons.append("archive_rights_hold")
        categories = m.split_terms(row.get("category"))
        reasons.extend(m._seed_metadata_contract_reasons(m.normalize_text(row.get("title")),
            m.normalize_text(row.get("description")), categories[0] if categories else "",
            m.split_terms(row.get("keywords"))))
        if reasons:
            holds.append({"identity": identity, "reasons": sorted(set(reasons))})
        else:
            rows.append(row)
    return rows, holds


class IndexReader(io.RawIOBase):
    """Bounded seekable reader: ZIP directory only, no archive extraction."""
    def __init__(self, size, fetch, budget=MAX_INDEX_BYTES):
        self.size, self.fetch, self.pos, self.fetched = size, fetch, 0, 0
        self.budget = budget

    def seekable(self):
        return True

    def readable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, offset, whence=0):
        target = offset + (self.pos if whence == 1 else self.size if whence == 2 else 0)
        if whence not in (0, 1, 2) or target < 0 or target > self.size:
            raise ValueError("archive_seek_invalid")
        self.pos = target
        return target

    def read(self, size=-1):
        size = self.size - self.pos if size < 0 else min(size, self.size - self.pos)
        if not size:
            return b""
        if size < 0 or self.fetched + size > self.budget:
            raise m.ManifestBuilderError("archive_index_read_limit")
        result = self.fetch(self.pos, self.pos + size - 1)
        if len(result) != size:
            raise m.ManifestBuilderError("archive_range_length_invalid")
        self.pos += size
        self.fetched += size
        return result


@contextmanager
def cloud_reader(drive, metadata, budget=MAX_INDEX_BYTES):
    p.enforce_cloud_runtime()
    from google.auth.transport.requests import AuthorizedSession
    size = int(metadata.get("size") or 0)
    if not 1 <= size <= p.MAX_ARCHIVE_BYTES:
        raise m.ManifestBuilderError("archive_size_invalid")
    with AuthorizedSession(drive.service._http.credentials) as session:
        def fetch(start, end):
            with session.get(f"https://www.googleapis.com/drive/v3/files/{metadata['id']}",
                    params={"alt": "media", "supportsAllDrives": "true"},
                    headers={"Range": f"bytes={start}-{end}"}, timeout=90, stream=True) as response:
                if response.status_code != 206:
                    # Do not accept a silently ignored Range/full-archive body.
                    raise m.ManifestBuilderError("archive_range_response_invalid")
                if response.headers.get("Content-Range") != f"bytes {start}-{end}/{size}":
                    raise m.ManifestBuilderError("archive_range_identity_invalid")
                data = bytearray()
                for block in response.iter_content(64 * 1024):
                    data.extend(block)
                    if len(data) > end - start + 1:
                        raise m.ManifestBuilderError("archive_range_overflow")
                return bytes(data)
        yield IndexReader(size, fetch, budget)
        after = drive.get_metadata(metadata["id"], candidate_scoped=True)
        if any(after.get(key) != metadata.get(key) for key in ("id", "size", "md5Checksum", "modifiedTime")):
            raise m.ManifestBuilderError("archive_changed_during_index")


def cloud_index(drive, metadata):
    try:
        with cloud_reader(drive, metadata) as reader, zipfile.ZipFile(reader) as archive:
            entries = archive.infolist()
            p.enforce_archive_entry_count(len(entries))
    except (zipfile.BadZipFile, ValueError) as error:
        raise m.ManifestBuilderError("archive_index_invalid") from error
    return entries


def extract_selected(drive, metadata, *, png_member, svg_member, png_destination, svg_destination):
    """Same extraction guards and CRC checks, without downloading unrelated members."""
    budget = MAX_INDEX_BYTES + (p.MAX_SOURCE_BYTES if png_member else 0) + (p.MAX_SVG_BYTES if svg_member else 0)
    try:
        with cloud_reader(drive, metadata, budget) as reader:
            result = p.extract_archive_members(reader, png_member=png_member, svg_member=svg_member,
                png_destination=png_destination, svg_destination=svg_destination)
        return result
    except m.ManifestBuilderError as error:
        # Explicitly safe diagnostic code only; never provider text/URLs.
        raise p.PipelineError(error.code, permanent=True) from error


def make_record(row, metadata, member, namespace, digest):
    record = {"schema_version": 3, "record_type": "heeexy_catalog_product",
        "product_identity": m._seed_identity(namespace, archive_claim(metadata["id"], member)),
        "assets": {"archive_drive_file_id": metadata["id"], "archive_svg_member": member,
                   "archive_png_member": None, "render_needed": True},
        "metadata": {"title": m.normalize_text(row["title"]), "description": m.normalize_text(row["description"]),
                     "keywords": list(m.split_terms(row["keywords"])), "categories": list(m.split_terms(row["category"]))},
        "metadata_provenance": {"source": "pinned_archive_recovery", "ledger_sha256": digest,
            "ledger_identity": row["identity"], "screening_evidence": row["screening_evidence"],
            "match": "canonical_full_path_exact_central_directory", "archive_md5": metadata.get("md5Checksum")},
        "source": {"provider": "google_drive", "namespace": namespace, "cohort": "screened_svg_archive",
                   "container_id": metadata["parents"][0], "folder_id": metadata["id"]}}
    p.candidate_from_record(record)
    return record


def discover(drive_for_thread, payload, namespace, claimed, index_reader=cloud_index):
    rows, holds = eligible_rows(payload)
    groups = defaultdict(list)
    for row in rows:
        groups[row["archive_file_id"]].append(row)
    digest = hashlib.sha256(payload).hexdigest()
    def verify(group):
        archive_id, group_rows = group
        records, rejected, existing = [], [], 0
        try:
            drive = drive_for_thread()
            metadata = drive.get_metadata(archive_id, candidate_scoped=True)
            if (metadata.get("id") != archive_id or metadata.get("trashed") or len(metadata.get("parents", [])) != 1
                    or metadata.get("mimeType") not in {p.ZIP_MIME, "application/x-zip-compressed", "application/octet-stream"}
                    or not str(metadata.get("name", "")).lower().endswith(".zip")):
                raise m.ManifestBuilderError("archive_source_role_invalid")
            pending = []
            for row in group_rows:
                try:
                    member = exact_member(row, metadata["name"])
                    if archive_claim(archive_id, member) in claimed:
                        existing += 1
                    else:
                        pending.append((row, member))
                except (m.ManifestBuilderError, p.PipelineError) as error:
                    rejected.append({"identity": row["identity"], "reasons": [error.code]})
            if not pending:
                return records, rejected, existing
            entries = index_reader(drive, metadata)
            by_name = defaultdict(list)
            for entry in entries:
                by_name[entry.filename].append(entry)
            selected = Counter(member for _, member in pending)
            for row, member in pending:
                matches = by_name[member]
                if len(matches) != 1 or selected[member] != 1:
                    rejected.append({"identity": row["identity"], "reasons": ["archive_exact_member_not_unique"]})
                    continue
                info = matches[0]
                mode = info.external_attr >> 16
                if (info.is_dir() or info.flag_bits & 1 or (mode & 0o170000) == 0o120000
                        or not 0 < info.file_size <= p.MAX_SVG_BYTES or info.compress_size <= 0
                        or info.file_size / info.compress_size > 1000):
                    rejected.append({"identity": row["identity"], "reasons": ["archive_member_unsafe"]})
                    continue
                records.append(make_record(row, metadata, member, namespace, digest))
        except (m.ManifestBuilderError, p.PipelineError) as error:
            if isinstance(error, p.PipelineError) and not error.permanent:
                raise
            return [], [{"identity": row["identity"], "reasons": [error.code]} for row in group_rows], 0
        return records, rejected, existing
    records, existing = [], 0
    with ThreadPoolExecutor(max_workers=4) as pool:
        for accepted, rejected, count in pool.map(verify, sorted(groups.items())):
            records.extend(accepted)
            holds.extend(rejected)
            existing += count
    return records, {"eligible_metadata_rows": len(rows), "archives": len(groups), "existing_rows": existing,
        "new_verified_records": len(records), "holds": holds,
        "hold_reasons": dict(Counter(reason for hold in holds for reason in hold["reasons"])),
        "full_library_complete": False, "scope": "Exact previously screened archive rows with complete original metadata only"}
