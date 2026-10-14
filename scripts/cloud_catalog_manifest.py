#!/usr/bin/env python3
"""Build the private HEEEXY catalog manifest on a GitHub-hosted runner.

This entry point is metadata-only.  It lists source files through the Google
Drive API, reads the small metadata workbooks/documents from the private import
folder into memory, and writes a deterministic JSONL index plus an inventory
report back to private Drive folders.  It never downloads PNG, SVG, or ZIP
product bytes and it never writes source or metadata files to the workstation.

The pure parsing, matching, and reconciliation helpers are importable by unit
tests.  ``main`` refuses to parse arguments or open Drive unless the same
GitHub-hosted-runner attestation used by the asset pipeline is present.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import posixpath
import re
import stat
import sys
import unicodedata
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Mapping, Sequence
import xml.etree.ElementTree as ET

try:  # Imported as ``scripts.cloud_catalog_manifest`` by unit tests.
    from scripts import cloud_catalog_pipeline as pipeline
except ImportError:  # Executed directly from the scripts directory.
    import cloud_catalog_pipeline as pipeline  # type: ignore[no-redef]


MANIFEST_SCHEMA_VERSION = 3
MANIFEST_ROLE = "catalog_manifest_v3"
REPORT_ROLE = "catalog_manifest_v3_report"
MANIFEST_FILENAME = "heeexy-catalog-manifest-v3.jsonl"
REPORT_FILENAME = "heeexy-catalog-manifest-v3-report.json"
MANIFEST_MIME = "application/x-ndjson"
REPORT_MIME = "application/json"
FOLDER_MIME = "application/vnd.google-apps.folder"
SHORTCUT_MIME = "application/vnd.google-apps.shortcut"
ZIP_MIMES = {"application/zip", "application/x-zip-compressed"}
MAX_METADATA_FILE_BYTES = 128 * 1024 * 1024
MAX_METADATA_TOTAL_BYTES = 512 * 1024 * 1024
MAX_XLSX_ENTRIES = 2_000
MAX_XLSX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
MAX_XLSX_COLUMNS = 16_384
MAX_XLSX_ROWS = 500_000
MAX_XLSX_CELLS = 5_000_000
MAX_METADATA_ROWS = 500_000
MAX_INVENTORY_ITEMS = 250_000
MAX_INVENTORY_FOLDERS = 50_000
MAX_DRIVE_DEPTH = 64
MAX_DRIVE_PARENT_BATCH = 25
MAX_DRIVE_PARENT_QUERY_CHARS = 4_096
MAX_LEDGER_ZIP_BYTES = 128 * 1024 * 1024
MAX_LEDGER_MEMBER_BYTES = 256 * 1024 * 1024
MAX_LEDGER_ZIP_ENTRIES = 32
MAX_LEDGER_COMPRESSION_RATIO = 1_000
MAX_LEDGER_LINE_BYTES = 1_000_000
MAX_LEDGER_ROWS = 100_000
MAX_INCREMENTAL_SIDECAR_BYTES = min(pipeline.MAX_METADATA_BYTES, 256 * 1024)
MAX_INCREMENTAL_SIDECAR_TOTAL_BYTES = 128 * 1024 * 1024
PRIMARY_LEDGER_SOURCE_LABEL = "pinned-primary-ledger"
PNG_LEDGER_SOURCE_LABEL = "pinned-png-ledger"
INCREMENTAL_SIDECAR_MIMES = frozenset(
    {
        "text/csv",
        "application/csv",
        "application/vnd.ms-excel",
    }
)
INCREMENTAL_SIDECAR_REQUIRED_COLUMNS = frozenset(
    {
        "deliveryfile",
        "deliverykind",
        "ownerapproval",
        "etsystage",
        "etsytitle",
        "etsydescription",
        "etsytags",
    }
)
INCREMENTAL_OWNER_APPROVAL_RE = re.compile(
    r"^approved_exact_[a-z0-9._-]{1,120}$", re.IGNORECASE
)
INCREMENTAL_AUTO_APPROVABLE_VALUES = frozenset(
    {"", "pending", "owner_pending", "needs_review", "review", "awaiting_approval"}
)
# Under the owner's auto-approve policy only an explicit refusal still holds.
INCREMENTAL_REFUSAL_RE = re.compile(r"(reject|denied|deny|declin|block|remove|delete|do_not|dont|not_approved|^no$)")
# Columns that must exist even under auto-approve; the rest may be absent.
INCREMENTAL_AUTO_APPROVE_REQUIRED_COLUMNS = frozenset(
    {"deliveryfile", "etsytitle", "etsydescription", "etsytags"}
)


def _png_only_auto_approve() -> bool:
    return os.environ.get("HEEEXY_PNG_ONLY_AUTO_APPROVE", "").strip().lower() == "true"
INCREMENTAL_PENDING_DRIVE_VALUES = frozenset(
    {"", "connector_upload_pending", "pending"}
)
REVIEWED_SEED_HOLD_REASONS = frozenset(
    {
        # These are the finite, manually inspected defects in the two pinned
        # bootstrap ledgers.  Every operational/inventory/identity failure is
        # deliberately excluded so it can never be mistaken for a reviewed
        # pilot hold.
        "description_missing",
        "tags_missing",
        "multiple_svg_source_ids",
    }
)
MAX_REVIEWED_SEED_HOLDS = 40
MAX_TITLE = pipeline.MAX_TITLE_CHARS
MAX_DESCRIPTION = pipeline.MAX_DESCRIPTION_CHARS
MAX_CATEGORY = pipeline.MAX_CATEGORY_CHARS
MAX_TAGS = pipeline.MAX_TAGS
MAX_TAG = pipeline.MAX_TAG_CHARS
DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{20,200}$")
DRIVE_ID_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9_-])([A-Za-z0-9_-]{20,200})(?![A-Za-z0-9_-])")
MANIFEST_IDENTITY_RE = re.compile(r"^gdrive:v3:[0-9a-f]{64}$")
CELL_REFERENCE_RE = re.compile(r"^([A-Z]+)([1-9][0-9]*)$")
DIMENSION_REFERENCE_RE = re.compile(
    r"^([A-Z]+)([1-9][0-9]*)(?::([A-Z]+)([1-9][0-9]*))?$"
)
MARKDOWN_SEPARATOR_RE = re.compile(r"^:?-{3,}:?$")
ASSET_EXTENSIONS = {".png", ".svg", ".zip"}
METADATA_EXTENSIONS = {".xlsx", ".csv", ".md", ".jsonl"}
MANIFEST_DRIVE_LIST_FIELDS = (
    "id,name,mimeType,size,md5Checksum,sha256Checksum,version,trashed,parents"
)


class ManifestBuilderError(RuntimeError):
    """Sanitized failure with a stable code safe for CI logs."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class DriveAsset:
    file_id: str
    name: str
    mime_type: str
    size_bytes: int
    parent_id: str
    root_id: str
    cohort: str
    md5_checksum: str = ""
    sha256_checksum: str = ""
    version: str = ""

    @property
    def extension(self) -> str:
        return PurePosixPath(self.name.replace("\\", "/")).suffix.casefold()

    @property
    def basename_normalized(self) -> str:
        return normalize_basename(self.name)


@dataclass(frozen=True)
class AssetGroup:
    identity: str
    root_id: str
    parent_id: str
    cohort: str
    basename_normalized: str
    png_assets: tuple[DriveAsset, ...] = ()
    svg_assets: tuple[DriveAsset, ...] = ()
    archive_asset: DriveAsset | None = None
    archive_png_member: str | None = None
    archive_svg_member: str | None = None

    @property
    def source_assets(self) -> tuple[DriveAsset, ...]:
        return self.png_assets + self.svg_assets + ((self.archive_asset,) if self.archive_asset else ())


@dataclass(frozen=True)
class MetadataEvidence:
    source_label: str
    section: str
    row_number: int
    title: str = ""
    description: str = ""
    categories: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    drive_ids: tuple[str, ...] = ()
    source_names: tuple[str, ...] = ()
    dreamstime_ids: tuple[str, ...] = ()
    archive_drive_id: str | None = None
    archive_png_member: str | None = None
    archive_svg_member: str | None = None

    @property
    def source_ref(self) -> str:
        return f"{self.source_label}:{self.section}:{self.row_number}"


@dataclass(frozen=True)
class MatchedEvidence:
    evidence: MetadataEvidence
    confidence: int


@dataclass(frozen=True)
class ResolvedMetadata:
    title: str
    description: str
    category: str
    tags: tuple[str, ...]
    variants: Mapping[str, int]
    provenance: Mapping[str, Any]
    priority_conflict: bool = False


@dataclass
class ReconciliationResult:
    records: list[dict[str, Any]] = field(default_factory=list)
    holds: list[dict[str, Any]] = field(default_factory=list)
    warnings: Counter[str] = field(default_factory=Counter)
    unmatched_metadata_rows: int = 0
    ambiguous_metadata_rows: int = 0


@dataclass(frozen=True)
class ProductSeed:
    """One authoritative product identity before Drive inventory verification.

    Bootstrap seeds come from the two pinned reviewed ledgers. A later PNG Only
    product may add a seed only when one exact-parent metadata sidecar explicitly
    binds its one PNG (and optional SVG). Asset names alone never create a seed.
    """

    identity: str
    seed_key: str
    png_file_ids: tuple[str, ...] = ()
    svg_file_ids: tuple[str, ...] = ()
    metadata_evidence: tuple[MetadataEvidence, ...] = ()
    source_refs: tuple[str, ...] = ()
    hold_reasons: tuple[str, ...] = ()

    @property
    def source_file_ids(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(self.png_file_ids + self.svg_file_ids))


@dataclass(frozen=True)
class BuilderConfig:
    metadata_import_folder_id: str
    catalog_metadata_folder_id: str
    catalog_manifest_drive_file_id: str
    reports_folder_id: str
    f16_root_id: str
    png_only_root_id: str
    expected_min_products: int
    expected_min_accepted: int
    source_namespace: str
    dreamstime_ledger_zip_file_id: str
    dreamstime_ledger_zip_name: str
    dreamstime_ledger_zip_sha256: str
    dreamstime_ledger_member_sha256: str
    ai_png_jsonl_file_id: str
    ai_png_jsonl_name: str
    ai_png_jsonl_sha256: str

    @property
    def global_inventory_target(self) -> int:
        return self.expected_min_products

    @property
    def global_product_target(self) -> int:
        return self.expected_min_products

    @property
    def curated_seed_target(self) -> int:
        return self.expected_min_accepted


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = "".join(" " if ord(char) < 32 or ord(char) == 127 else char for char in text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_header(value: Any) -> str:
    text = normalize_text(value).casefold().replace("_", " ")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def normalize_basename(value: Any) -> str:
    name = normalize_text(value).replace("\\", "/")
    leaf = PurePosixPath(name).name
    stem = PurePosixPath(leaf).stem if PurePosixPath(leaf).suffix else leaf
    normalized = unicodedata.normalize("NFKC", stem).casefold()
    normalized = re.sub(r"[\s_-]+", " ", normalized)
    return normalized.strip(" .")


def stable_digest(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


def valid_drive_id(value: Any) -> str | None:
    candidate = normalize_text(value)
    if not DRIVE_ID_RE.fullmatch(candidate):
        return None
    if candidate.casefold().startswith("svg-png-render-v1-"):
        return None
    return candidate


def valid_sha256(value: Any) -> str | None:
    candidate = normalize_text(value).casefold()
    return candidate if re.fullmatch(r"[0-9a-f]{64}", candidate) else None


def _drive_parent_query(folder_ids: Sequence[str]) -> str:
    return (
        "("
        + " or ".join(f"'{folder_id}' in parents" for folder_id in folder_ids)
        + ") and trashed = false"
    )


def _frontier_batches(
    frontier: Sequence[tuple[str, int]],
) -> Iterator[tuple[tuple[str, int], ...]]:
    """Split a verified folder frontier by count and conservative query length."""

    batch: list[tuple[str, int]] = []
    for entry in frontier:
        candidate = batch + [entry]
        candidate_query = _drive_parent_query([folder_id for folder_id, _ in candidate])
        if batch and (
            len(candidate) > MAX_DRIVE_PARENT_BATCH
            or len(candidate_query) > MAX_DRIVE_PARENT_QUERY_CHARS
        ):
            yield tuple(batch)
            batch = [entry]
            candidate_query = _drive_parent_query([entry[0]])
        else:
            batch = candidate
        if len(candidate_query) > MAX_DRIVE_PARENT_QUERY_CHARS:
            raise ManifestBuilderError("drive_parent_query_too_long")
    if batch:
        yield tuple(batch)


def extract_drive_ids(value: Any) -> tuple[str, ...]:
    text = normalize_text(value)
    output: list[str] = []
    direct = valid_drive_id(text)
    candidates = ([direct] if direct else []) + DRIVE_ID_TOKEN_RE.findall(text)
    for token in candidates:
        candidate = valid_drive_id(token)
        if candidate and candidate not in output:
            output.append(candidate)
    return tuple(output)


def split_terms(value: Any) -> tuple[str, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        raw_values: Iterable[Any] = value
    else:
        text = normalize_text(value)
        if text.startswith("[") and text.endswith("]"):
            try:
                decoded = json.loads(text)
                raw_values = decoded if isinstance(decoded, list) else [text]
            except json.JSONDecodeError:
                raw_values = re.split(r"[,;|]", text)
        else:
            raw_values = re.split(r"[,;|]", text)
    output: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        item = normalize_text(raw)
        key = item.casefold()
        if not item or key in seen:
            continue
        output.append(item)
        seen.add(key)
    return tuple(output)


def _column_index(reference: str) -> int:
    match = CELL_REFERENCE_RE.fullmatch(reference)
    if not match:
        raise ManifestBuilderError("xlsx_cell_reference_invalid")
    value = 0
    for char in match.group(1):
        value = value * 26 + (ord(char) - ord("A") + 1)
        if value > MAX_XLSX_COLUMNS:
            raise ManifestBuilderError("xlsx_column_limit")
    return value - 1


def _cell_coordinates(reference: str) -> tuple[int, int]:
    match = CELL_REFERENCE_RE.fullmatch(reference)
    if not match:
        raise ManifestBuilderError("xlsx_cell_reference_invalid")
    if len(match.group(2)) > len(str(MAX_XLSX_ROWS)):
        raise ManifestBuilderError("xlsx_row_limit")
    row = int(match.group(2))
    if row > MAX_XLSX_ROWS:
        raise ManifestBuilderError("xlsx_row_limit")
    return _column_index(reference), row - 1


def _validate_xlsx_dimension(reference: str) -> None:
    match = DIMENSION_REFERENCE_RE.fullmatch(reference)
    if not match:
        raise ManifestBuilderError("xlsx_dimension_invalid")
    start_reference = f"{match.group(1)}{match.group(2)}"
    end_reference = (
        f"{match.group(3)}{match.group(4)}" if match.group(3) and match.group(4) else start_reference
    )
    start_column, start_row = _cell_coordinates(start_reference)
    end_column, end_row = _cell_coordinates(end_reference)
    if end_column < start_column or end_row < start_row:
        raise ManifestBuilderError("xlsx_dimension_invalid")
    width = end_column - start_column + 1
    height = end_row - start_row + 1
    if width * height > MAX_XLSX_CELLS:
        raise ManifestBuilderError("xlsx_cell_limit")


def _safe_xml(payload: bytes) -> ET.Element:
    lowered = payload[:65_536].lower()
    if (
        b"<!doctype" in lowered
        or b"<!entity" in lowered
        or b"<!DOCTYPE" in payload
        or b"<!ENTITY" in payload
    ):
        raise ManifestBuilderError("metadata_xml_unsafe")
    try:
        return ET.fromstring(payload)
    except ET.ParseError as error:
        raise ManifestBuilderError("metadata_xml_invalid") from error


def _xlsx_scalar(value: str) -> str:
    text = value.strip()
    if re.fullmatch(r"-?\d+(?:\.0+)?", text):
        return text.split(".", 1)[0]
    return text


def _read_xlsx_member(archive: zipfile.ZipFile, name: str) -> bytes:
    try:
        return archive.read(name)
    except (KeyError, OSError, RuntimeError, zipfile.BadZipFile) as error:
        raise ManifestBuilderError("xlsx_member_read_failed") from error


def parse_xlsx_grids(payload: bytes) -> list[tuple[str, list[list[str]]]]:
    """Read data-only XLSX cells without executing formulas or external links."""

    try:
        archive = zipfile.ZipFile(io.BytesIO(payload), "r")
    except zipfile.BadZipFile as error:
        raise ManifestBuilderError("xlsx_invalid") from error
    with archive:
        infos = archive.infolist()
        if len(infos) > MAX_XLSX_ENTRIES:
            raise ManifestBuilderError("xlsx_entry_limit")
        if len({item.filename for item in infos}) != len(infos):
            raise ManifestBuilderError("xlsx_duplicate_entry")
        for item in infos:
            path = PurePosixPath(item.filename.replace("\\", "/"))
            if path.is_absolute() or ".." in path.parts or item.flag_bits & 0x1:
                raise ManifestBuilderError("xlsx_entry_unsafe")
            if item.file_size and (
                item.compress_size < 1 or item.file_size / item.compress_size > 1_000
            ):
                raise ManifestBuilderError("xlsx_compression_ratio_limit")
        if sum(item.file_size for item in infos) > MAX_XLSX_UNCOMPRESSED_BYTES:
            raise ManifestBuilderError("xlsx_uncompressed_limit")
        names = {item.filename for item in infos}
        required = {"xl/workbook.xml", "xl/_rels/workbook.xml.rels"}
        if not required.issubset(names):
            raise ManifestBuilderError("xlsx_structure_invalid")

        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in names:
            root = _safe_xml(_read_xlsx_member(archive, "xl/sharedStrings.xml"))
            for item in root.iter():
                if item.tag.rsplit("}", 1)[-1] == "si":
                    if len(shared_strings) >= MAX_XLSX_CELLS:
                        raise ManifestBuilderError("xlsx_cell_limit")
                    shared_strings.append(
                        "".join(
                            child.text or ""
                            for child in item.iter()
                            if child.tag.rsplit("}", 1)[-1] == "t"
                        )
                    )

        relationships = _safe_xml(_read_xlsx_member(archive, "xl/_rels/workbook.xml.rels"))
        targets: dict[str, str] = {}
        for relation in relationships:
            relation_id = relation.attrib.get("Id", "")
            target = relation.attrib.get("Target", "")
            if relation.attrib.get("TargetMode") == "External" or not relation_id or not target:
                continue
            if target.startswith("/"):
                normalized = target.lstrip("/")
            else:
                normalized = posixpath.normpath(posixpath.join("xl", target))
            if normalized.startswith("../"):
                raise ManifestBuilderError("xlsx_relationship_unsafe")
            targets[relation_id] = normalized

        workbook = _safe_xml(_read_xlsx_member(archive, "xl/workbook.xml"))
        sheets: list[tuple[str, str]] = []
        for node in workbook.iter():
            if node.tag.rsplit("}", 1)[-1] != "sheet":
                continue
            relationship_id = next(
                (value for key, value in node.attrib.items() if key.rsplit("}", 1)[-1] == "id"),
                "",
            )
            target = targets.get(relationship_id)
            if target and target in names:
                sheets.append((normalize_text(node.attrib.get("name")) or "Sheet", target))

        output: list[tuple[str, list[list[str]]]] = []
        row_total = 0
        cell_total = 0
        materialized_cell_total = 0
        for sheet_name, target in sheets:
            root = _safe_xml(_read_xlsx_member(archive, target))
            for node in root.iter():
                if node.tag.rsplit("}", 1)[-1] == "dimension":
                    reference = node.attrib.get("ref", "")
                    if reference:
                        _validate_xlsx_dimension(reference)
                    break
            grid: list[list[str]] = []
            sheet_row_count = 0
            for row_node in root.iter():
                if row_node.tag.rsplit("}", 1)[-1] != "row":
                    continue
                sheet_row_count += 1
                row_reference = row_node.attrib.get("r", "")
                if row_reference:
                    if not row_reference.isdigit():
                        raise ManifestBuilderError("xlsx_row_reference_invalid")
                    if len(row_reference) > len(str(MAX_XLSX_ROWS)):
                        raise ManifestBuilderError("xlsx_row_limit")
                    try:
                        declared_row = int(row_reference)
                    except ValueError as error:
                        raise ManifestBuilderError("xlsx_row_reference_invalid") from error
                    if declared_row < 1 or declared_row > MAX_XLSX_ROWS:
                        raise ManifestBuilderError("xlsx_row_limit")
                elif sheet_row_count > MAX_XLSX_ROWS:
                    raise ManifestBuilderError("xlsx_row_limit")
                values: dict[int, str] = {}
                fallback_index = 0
                cell_count = 0
                for cell in row_node:
                    if cell.tag.rsplit("}", 1)[-1] != "c":
                        continue
                    cell_count += 1
                    if cell_count > MAX_XLSX_COLUMNS:
                        raise ManifestBuilderError("xlsx_column_limit")
                    cell_total += 1
                    if cell_total > MAX_XLSX_CELLS:
                        raise ManifestBuilderError("xlsx_cell_limit")
                    reference = cell.attrib.get("r", "")
                    if reference:
                        index, referenced_row = _cell_coordinates(reference)
                        if row_reference and referenced_row + 1 != declared_row:
                            raise ManifestBuilderError("xlsx_cell_row_mismatch")
                    else:
                        index = fallback_index
                        if index >= MAX_XLSX_COLUMNS:
                            raise ManifestBuilderError("xlsx_column_limit")
                    fallback_index = index + 1
                    cell_type = cell.attrib.get("t", "")
                    raw_value = ""
                    if cell_type == "inlineStr":
                        raw_value = "".join(
                            child.text or ""
                            for child in cell.iter()
                            if child.tag.rsplit("}", 1)[-1] == "t"
                        )
                    else:
                        value_node = next(
                            (
                                child
                                for child in cell
                                if child.tag.rsplit("}", 1)[-1] == "v"
                            ),
                            None,
                        )
                        if value_node is not None and value_node.text is not None:
                            raw_value = value_node.text
                        if cell_type == "s" and raw_value:
                            try:
                                raw_value = shared_strings[int(raw_value)]
                            except (ValueError, IndexError) as error:
                                raise ManifestBuilderError("xlsx_shared_string_invalid") from error
                        elif cell_type == "b":
                            raw_value = "TRUE" if raw_value == "1" else "FALSE"
                        else:
                            raw_value = _xlsx_scalar(raw_value)
                    if raw_value:
                        values[index] = normalize_text(raw_value)
                if values:
                    width = max(values) + 1
                    materialized_cell_total += width
                    if materialized_cell_total > MAX_XLSX_CELLS:
                        raise ManifestBuilderError("xlsx_cell_limit")
                    if row_total >= MAX_METADATA_ROWS:
                        raise ManifestBuilderError("metadata_row_limit")
                    grid.append([values.get(index, "") for index in range(width)])
                    row_total += 1
            if grid:
                output.append((sheet_name, grid))
        return output


def _decode_text_payload(payload: bytes) -> str:
    encoding = "utf-16" if payload.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    try:
        return payload.decode(encoding)
    except UnicodeError as error:
        raise ManifestBuilderError("metadata_encoding_invalid") from error


def _iter_text_lines(payload: bytes) -> Iterator[str]:
    encoding = "utf-16" if payload.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    stream = io.TextIOWrapper(io.BytesIO(payload), encoding=encoding, newline="")
    try:
        yield from stream
    except UnicodeError as error:
        raise ManifestBuilderError("metadata_encoding_invalid") from error
    finally:
        stream.close()


def iter_csv_rows(
    payload: bytes, *, maximum_rows: int = MAX_METADATA_ROWS
) -> Iterator[list[str]]:
    """Yield bounded normalized CSV rows without retaining a second raw grid."""

    encoding = "utf-16" if payload.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    stream = io.TextIOWrapper(io.BytesIO(payload), encoding=encoding, newline="")
    try:
        sample = stream.read(65_536)
        stream.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel
        emitted = 0
        for row in csv.reader(stream, dialect=dialect):
            normalized = [normalize_text(cell) for cell in row]
            if not any(normalized):
                continue
            if emitted >= maximum_rows:
                raise ManifestBuilderError("metadata_row_limit")
            emitted += 1
            yield normalized
    except UnicodeError as error:
        raise ManifestBuilderError("metadata_encoding_invalid") from error
    except csv.Error as error:
        raise ManifestBuilderError("metadata_csv_invalid") from error
    finally:
        stream.close()


def parse_csv_grid(
    payload: bytes, *, maximum_rows: int = MAX_METADATA_ROWS
) -> list[list[str]]:
    return list(iter_csv_rows(payload, maximum_rows=maximum_rows))


def _split_markdown_row(line: str) -> list[str]:
    text = line.strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|") and not text.endswith("\\|"):
        text = text[:-1]
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "|":
            cells.append(normalize_text("".join(current)))
            current = []
        else:
            current.append(char)
    current.append("\\" if escaped else "")
    cells.append(normalize_text("".join(current)))
    return cells


def parse_markdown_grids(
    payload: bytes, *, maximum_rows: int = MAX_METADATA_ROWS
) -> list[tuple[str, list[list[str]]]]:
    output: list[tuple[str, list[list[str]]]] = []
    table_number = 0
    row_total = 0
    pending_header: str | None = None
    active_grid: list[list[str]] | None = None
    for raw_line in _iter_text_lines(payload):
        line = raw_line.rstrip("\r\n")
        if active_grid is not None:
            if "|" in line:
                row = _split_markdown_row(line)
                if any(row):
                    if row_total >= maximum_rows:
                        raise ManifestBuilderError("metadata_row_limit")
                    active_grid.append(row)
                    row_total += 1
                continue
            output.append((f"table-{table_number}", active_grid))
            active_grid = None
            pending_header = None
            continue
        if pending_header is None:
            if "|" in line:
                pending_header = line
            continue
        header = _split_markdown_row(pending_header)
        separator = _split_markdown_row(line) if "|" in line else []
        if (
            len(header) >= 2
            and len(separator) == len(header)
            and all(
                MARKDOWN_SEPARATOR_RE.fullmatch(cell.replace(" ", ""))
                for cell in separator
            )
        ):
            table_number += 1
            if row_total >= maximum_rows:
                raise ManifestBuilderError("metadata_row_limit")
            active_grid = [header]
            row_total += 1
            pending_header = None
        else:
            pending_header = line if "|" in line else None
    if active_grid is not None:
        output.append((f"table-{table_number}", active_grid))
    return output


TITLE_ALIASES = {
    "title",
    "buyer title",
    "product title",
    "dreamstime title",
    "headline",
    "image title",
}
DESCRIPTION_ALIASES = {
    "description",
    "buyer description",
    "product description",
    "caption",
    "image description",
}
KEYWORD_ALIASES = {
    "keywords",
    "keyword",
    "tags",
    "buyer keywords",
    "optimized keywords",
    "search terms",
}
CATEGORY_ALIASES = {
    "category",
    "categories",
    "dreamstime category",
    "dreamstime categories",
}
SOURCE_NAME_ALIASES = {
    "source name",
    "source filename",
    "filename",
    "file name",
    "remote filename",
    "submission filename",
    "primary jpg",
    "additional file",
    "delivery file",
    "png file",
    "svg file",
}
DREAMSTIME_ID_ALIASES = {
    "image id",
    "dreamstime id",
    "dreamstime image id",
}
ARCHIVE_ID_ALIASES = {
    "archive drive file id",
    "archive zip drive file id",
    "zip drive file id",
}
ARCHIVE_PNG_MEMBER_ALIASES = {"archive png member", "png archive member"}
ARCHIVE_SVG_MEMBER_ALIASES = {"archive svg member", "svg archive member"}
KNOWN_HEADERS = (
    TITLE_ALIASES
    | DESCRIPTION_ALIASES
    | KEYWORD_ALIASES
    | CATEGORY_ALIASES
    | SOURCE_NAME_ALIASES
    | DREAMSTIME_ID_ALIASES
    | ARCHIVE_ID_ALIASES
    | ARCHIVE_PNG_MEMBER_ALIASES
    | ARCHIVE_SVG_MEMBER_ALIASES
)


def _first_value(row: Mapping[str, str], aliases: set[str]) -> str:
    for alias in sorted(aliases):
        value = row.get(alias, "")
        if value:
            return value
    return ""


def _all_values(row: Mapping[str, str], aliases: set[str]) -> tuple[str, ...]:
    return tuple(value for key, value in row.items() if key in aliases and value)


def _header_row_index(grid: Sequence[Sequence[str]]) -> int | None:
    best: tuple[int, int] | None = None
    for index, row in enumerate(grid[:50]):
        normalized = [normalize_header(cell) for cell in row]
        score = sum(1 for cell in normalized if cell in KNOWN_HEADERS)
        if any("drive" in cell and "id" in cell for cell in normalized):
            score += 1
        if score and (best is None or score > best[0]):
            best = (score, index)
    return best[1] if best else None


def evidence_from_grid(
    grid: Iterable[Sequence[str]], source_label: str, section: str
) -> list[MetadataEvidence]:
    iterator = iter(grid)
    prefix: list[Sequence[str]] = []
    for _ in range(50):
        try:
            prefix.append(next(iterator))
        except StopIteration:
            break
    header_index = _header_row_index(prefix)
    if header_index is None:
        return []
    headers: list[str] = []
    seen_headers: Counter[str] = Counter()
    for offset, cell in enumerate(prefix[header_index]):
        base = normalize_header(cell) or f"column {offset + 1}"
        seen_headers[base] += 1
        headers.append(base if seen_headers[base] == 1 else f"{base} {seen_headers[base]}")
    output: list[MetadataEvidence] = []
    rows = chain(prefix[header_index + 1 :], iterator)
    for row_number, values in enumerate(rows, start=header_index + 2):
        row = {
            headers[index]: normalize_text(value)
            for index, value in enumerate(values[: len(headers)])
            if normalize_text(value)
        }
        if not row:
            continue
        title = _first_value(row, TITLE_ALIASES)
        description = _first_value(row, DESCRIPTION_ALIASES)
        categories = split_terms(_first_value(row, CATEGORY_ALIASES))
        tags = split_terms(_first_value(row, KEYWORD_ALIASES))
        source_names = tuple(
            dict.fromkeys(
                normalize_text(value)
                for value in _all_values(row, SOURCE_NAME_ALIASES)
                if normalize_text(value)
            )
        )
        drive_ids: list[str] = []
        for key, value in row.items():
            if "id" in key or "file" in key or "source" in key or "additional" in key:
                drive_ids.extend(extract_drive_ids(value))
        drive_ids = list(dict.fromkeys(drive_ids))
        dreamstime_ids: list[str] = []
        for value in _all_values(row, DREAMSTIME_ID_ALIASES):
            candidate = normalize_text(value)
            if candidate.isdigit() and candidate not in dreamstime_ids:
                dreamstime_ids.append(candidate)
        archive_id = valid_drive_id(_first_value(row, ARCHIVE_ID_ALIASES))
        archive_png_member = normalize_text(_first_value(row, ARCHIVE_PNG_MEMBER_ALIASES)) or None
        archive_svg_member = normalize_text(_first_value(row, ARCHIVE_SVG_MEMBER_ALIASES)) or None
        if not any(
            (
                title,
                description,
                categories,
                tags,
                source_names,
                drive_ids,
                dreamstime_ids,
                archive_id,
            )
        ):
            continue
        output.append(
            MetadataEvidence(
                source_label=source_label,
                section=section,
                row_number=row_number,
                title=title,
                description=description,
                categories=categories,
                tags=tags,
                drive_ids=tuple(drive_ids),
                source_names=source_names,
                dreamstime_ids=tuple(dreamstime_ids),
                archive_drive_id=archive_id,
                archive_png_member=archive_png_member,
                archive_svg_member=archive_svg_member,
            )
        )
    return output


def evidence_from_jsonl(payload: bytes, source_label: str) -> list[MetadataEvidence]:
    output: list[MetadataEvidence] = []
    for row_number, line in enumerate(_iter_text_lines(payload), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ManifestBuilderError("metadata_jsonl_invalid") from error
        if not isinstance(record, Mapping):
            continue
        metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else record
        assets = record.get("assets") if isinstance(record.get("assets"), Mapping) else {}
        categories = metadata.get("categories") or metadata.get("category")
        tags = metadata.get("keywords") or metadata.get("tags")
        drive_ids: list[str] = []
        for value in (
            record.get("canonical_local_id"),
            record.get("known_png_drive_file_ids"),
            record.get("known_svg_drive_file_ids"),
            assets.get("png_drive_file_id"),
            assets.get("svg_drive_file_id"),
        ):
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                for item in value:
                    drive_ids.extend(extract_drive_ids(item))
            else:
                drive_ids.extend(extract_drive_ids(value))
        archive_id = valid_drive_id(
            assets.get("archive_drive_file_id")
            or record.get("archive_zip_drive_file_id")
            or record.get("archive_drive_file_id")
        )
        external_refs = record.get("external_refs")
        dreamstime_ids: list[str] = []
        if isinstance(external_refs, Sequence) and not isinstance(external_refs, (str, bytes)):
            for reference in external_refs:
                if not isinstance(reference, Mapping):
                    continue
                if reference.get("provider") == "dreamstime" and reference.get("ref_type") == "image_id":
                    value = normalize_text(reference.get("ref_value"))
                    if value.isdigit():
                        dreamstime_ids.append(value)
        source_names = tuple(
            value
            for value in (
                normalize_text(record.get("source_name")),
                normalize_text(record.get("filename")),
            )
            if value
        )
        if len(output) >= MAX_METADATA_ROWS:
            raise ManifestBuilderError("metadata_row_limit")
        output.append(
            MetadataEvidence(
                source_label=source_label,
                section="jsonl",
                row_number=row_number,
                title=normalize_text(metadata.get("title")),
                description=normalize_text(metadata.get("description")),
                categories=split_terms(categories),
                tags=split_terms(tags),
                drive_ids=tuple(dict.fromkeys(drive_ids)),
                source_names=source_names,
                dreamstime_ids=tuple(dict.fromkeys(dreamstime_ids)),
                archive_drive_id=archive_id,
                archive_png_member=normalize_text(
                    assets.get("archive_png_member") or record.get("archive_png_member")
                )
                or None,
                archive_svg_member=normalize_text(
                    assets.get("archive_svg_member") or record.get("archive_svg_member")
                )
                or None,
            )
        )
    return output


def parse_metadata_file(name: str, payload: bytes) -> list[MetadataEvidence]:
    extension = PurePosixPath(name).suffix.casefold()
    if extension == ".xlsx":
        evidence: list[MetadataEvidence] = []
        for section, grid in parse_xlsx_grids(payload):
            evidence.extend(evidence_from_grid(grid, name, section))
        return evidence
    if extension == ".csv":
        return evidence_from_grid(iter_csv_rows(payload), name, "csv")
    if extension == ".md":
        evidence = []
        for section, grid in parse_markdown_grids(payload):
            evidence.extend(evidence_from_grid(grid, name, section))
        return evidence
    if extension == ".jsonl":
        return evidence_from_jsonl(payload, name)
    return []


def _bounded_jsonl_objects(payload: bytes, error_prefix: str) -> Iterator[tuple[int, Mapping[str, Any]]]:
    """Yield a bounded JSONL ledger without retaining a second decoded copy."""

    stream = io.BytesIO(payload)
    row_number = 0
    while True:
        raw_bytes = stream.readline(MAX_LEDGER_LINE_BYTES + 1)
        if not raw_bytes:
            break
        row_number += 1
        if row_number > MAX_LEDGER_ROWS:
            raise ManifestBuilderError(f"{error_prefix}_row_limit")
        if len(raw_bytes) > MAX_LEDGER_LINE_BYTES:
            raise ManifestBuilderError(f"{error_prefix}_line_limit")
        try:
            raw_line = raw_bytes.decode("utf-8-sig" if row_number == 1 else "utf-8")
        except UnicodeError as error:
            raise ManifestBuilderError(f"{error_prefix}_encoding_invalid") from error
        if not raw_line.strip():
            continue
        try:
            record = json.loads(raw_line)
        except json.JSONDecodeError as error:
            raise ManifestBuilderError(f"{error_prefix}_json_invalid") from error
        if not isinstance(record, Mapping):
            raise ManifestBuilderError(f"{error_prefix}_row_not_object")
        yield row_number, record


def extract_dreamstime_ledger_member(
    payload: bytes, *, expected_zip_sha256: str, expected_member_sha256: str
) -> bytes:
    """Verify and read the one allowlisted JSONL member from a ledger ZIP."""

    if len(payload) < 1 or len(payload) > MAX_LEDGER_ZIP_BYTES:
        raise ManifestBuilderError("dreamstime_ledger_zip_size_invalid")
    if hashlib.sha256(payload).hexdigest() != expected_zip_sha256:
        raise ManifestBuilderError("dreamstime_ledger_zip_hash_mismatch")
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload), "r")
    except zipfile.BadZipFile as error:
        raise ManifestBuilderError("dreamstime_ledger_zip_invalid") from error
    with archive:
        infos = archive.infolist()
        if not infos or len(infos) > MAX_LEDGER_ZIP_ENTRIES:
            raise ManifestBuilderError("dreamstime_ledger_zip_entry_limit")
        names = [item.filename for item in infos]
        if len(set(names)) != len(names):
            raise ManifestBuilderError("dreamstime_ledger_zip_duplicate_entry")
        data_members: list[zipfile.ZipInfo] = []
        uncompressed_total = 0
        for item in infos:
            normalized = item.filename.replace("\\", "/")
            path = PurePosixPath(normalized)
            mode = item.external_attr >> 16
            if (
                path.is_absolute()
                or ".." in path.parts
                or any(not part or len(part) > 255 for part in path.parts)
                or item.flag_bits & 0x1
                or stat.S_ISLNK(mode)
            ):
                raise ManifestBuilderError("dreamstime_ledger_zip_entry_unsafe")
            if item.is_dir():
                continue
            if item.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                raise ManifestBuilderError("dreamstime_ledger_zip_compression_invalid")
            if item.file_size < 1 or item.file_size > MAX_LEDGER_MEMBER_BYTES:
                raise ManifestBuilderError("dreamstime_ledger_member_size_invalid")
            if item.compress_size < 1 or item.file_size / item.compress_size > MAX_LEDGER_COMPRESSION_RATIO:
                raise ManifestBuilderError("dreamstime_ledger_zip_ratio_limit")
            uncompressed_total += item.file_size
            if uncompressed_total > MAX_LEDGER_MEMBER_BYTES:
                raise ManifestBuilderError("dreamstime_ledger_zip_uncompressed_limit")
            data_members.append(item)
        # The ZIP itself and the sole member are pinned by reviewed hashes. Do
        # not publish the private member filename in this public code-only
        # runner; accepting exactly one safe regular member is both sufficient
        # and stricter than matching a disclosed name.
        if len(data_members) != 1:
            raise ManifestBuilderError("dreamstime_ledger_member_invalid")
        try:
            member = archive.read(data_members[0])
        except (OSError, RuntimeError, zipfile.BadZipFile) as error:
            raise ManifestBuilderError("dreamstime_ledger_member_read_failed") from error
    if len(member) != data_members[0].file_size:
        raise ManifestBuilderError("dreamstime_ledger_member_size_mismatch")
    if hashlib.sha256(member).hexdigest() != expected_member_sha256:
        raise ManifestBuilderError("dreamstime_ledger_member_hash_mismatch")
    return member


def _strict_drive_id_values(value: Any) -> tuple[tuple[str, ...], bool]:
    """Return unique real Drive IDs and whether a supplied token was rejected."""

    if value is None or value == "":
        return (), False
    raw_values: Sequence[Any]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        raw_values = value
    else:
        raw_values = (value,)
    output: list[str] = []
    rejected = False
    for raw in raw_values:
        if raw is None or normalize_text(raw) == "":
            continue
        candidate = valid_drive_id(raw)
        if candidate:
            if candidate not in output:
                output.append(candidate)
        else:
            rejected = True
    return tuple(sorted(output)), rejected


def _seed_evidence(
    record: Mapping[str, Any], *, source_label: str, row_number: int, drive_ids: Sequence[str]
) -> MetadataEvidence:
    nested = record.get("metadata")
    metadata = nested if isinstance(nested, Mapping) else record
    categories = metadata.get("categories") or metadata.get("category") or ()
    keywords = metadata.get("keywords") or metadata.get("tags") or ()
    source_names = tuple(
        dict.fromkeys(
            value
            for value in (
                normalize_text(record.get("source_name")),
                normalize_text(record.get("source_path")),
                normalize_text(record.get("folder")),
            )
            if value
        )
    )
    dreamstime_ids: list[str] = []
    for value in (
        record.get("archive_image_id"),
        record.get("archive_image_ids_in_product"),
        record.get("dreamstime_archive_image_ids"),
    ):
        values = value if isinstance(value, Sequence) and not isinstance(value, str) else (value,)
        for raw in values:
            candidate = normalize_text(raw)
            if candidate.isdigit() and candidate not in dreamstime_ids:
                dreamstime_ids.append(candidate)
    return MetadataEvidence(
        source_label=source_label,
        section="authoritative-ledger",
        row_number=row_number,
        title=normalize_text(metadata.get("title") or record.get("title")),
        description=normalize_text(metadata.get("description") or record.get("description")),
        categories=split_terms(categories),
        tags=split_terms(keywords),
        drive_ids=tuple(drive_ids),
        source_names=source_names,
        dreamstime_ids=tuple(dreamstime_ids),
    )


def _seed_identity(source_namespace: str, seed_key: str) -> str:
    """Ledger-independent identity stable across AI-to-Dreamstime promotion."""

    return f"gdrive:v3:{stable_digest(source_namespace, 'canonical-product', seed_key)}"


def parse_dreamstime_product_seeds(
    payload: bytes,
    source_namespace: str,
    *,
    source_label: str = PRIMARY_LEDGER_SOURCE_LABEL,
) -> tuple[list[ProductSeed], Counter[str]]:
    """Parse only primary whitelist groups from the reviewed Dreamstime ledger."""

    grouped: dict[str, list[tuple[int, Mapping[str, Any]]]] = defaultdict(list)
    stats: Counter[str] = Counter()
    for row_number, record in _bounded_jsonl_objects(payload, "dreamstime_ledger"):
        stats["rows_seen"] += 1
        record_type = normalize_text(record.get("record_type")).casefold()
        if record_type == "quarantine":
            stats["quarantine_rows_ignored"] += 1
            continue
        if record_type != "whitelist" or record.get("is_product_primary_record") is not True:
            stats["non_primary_rows_ignored"] += 1
            continue
        group_key = normalize_text(record.get("product_group_id"))
        if not group_key:
            group_key = f"missing-product-group:{row_number}"
        grouped[group_key].append((row_number, record))

    seeds: list[ProductSeed] = []
    for group_key, rows in sorted(grouped.items()):
        row_number, record = rows[0]
        png_ids, bad_png = _strict_drive_id_values(record.get("known_png_drive_file_ids"))
        svg_ids, bad_svg = _strict_drive_id_values(record.get("known_svg_drive_file_ids"))
        fallback_values = (
            record.get("canonical_local_id"),
            record.get("drive_file_id"),
            record.get("matched_local_id"),
        )
        fallback_ids, bad_fallback = _strict_drive_id_values(fallback_values)
        if not png_ids and not svg_ids and fallback_ids:
            extension = PurePosixPath(normalize_text(record.get("source_name"))).suffix.casefold()
            if extension == ".png":
                png_ids = fallback_ids
            elif extension == ".svg":
                svg_ids = fallback_ids
        reasons: list[str] = []
        if group_key.startswith("missing-product-group:"):
            reasons.append("product_group_id_missing")
        if len(rows) != 1:
            reasons.append("multiple_primary_records")
        if bad_png or bad_svg or bad_fallback:
            reasons.append("synthetic_or_invalid_source_id")
        if not png_ids and not svg_ids:
            reasons.append("source_asset_id_missing")
        if len(png_ids) > 1:
            reasons.append("multiple_png_source_ids")
        if len(svg_ids) > 1:
            reasons.append("multiple_svg_source_ids")
        all_ids = tuple(dict.fromkeys(png_ids + svg_ids))
        evidence = tuple(
            _seed_evidence(
                item,
                source_label=source_label,
                row_number=item_row,
                drive_ids=all_ids,
            )
            for item_row, item in rows
        )
        refs = tuple(item.source_ref for item in evidence)
        seeds.append(
            ProductSeed(
                identity=_seed_identity(source_namespace, group_key),
                seed_key=group_key,
                png_file_ids=png_ids,
                svg_file_ids=svg_ids,
                metadata_evidence=evidence,
                source_refs=refs,
                hold_reasons=tuple(sorted(set(reasons))),
            )
        )
    stats["primary_groups"] = len(seeds)
    return seeds, stats


def parse_ai_png_product_seeds(
    payload: bytes,
    source_namespace: str,
    *,
    source_label: str = PNG_LEDGER_SOURCE_LABEL,
) -> tuple[list[ProductSeed], Counter[str]]:
    """Parse the supplemental AI-PNG ledger as exact source identities."""

    grouped: dict[str, list[tuple[int, Mapping[str, Any]]]] = defaultdict(list)
    stats: Counter[str] = Counter()
    for row_number, record in _bounded_jsonl_objects(payload, "ai_png_ledger"):
        stats["rows_seen"] += 1
        record_type = normalize_text(record.get("record_type")).casefold()
        if record_type == "quarantine":
            stats["quarantine_rows_ignored"] += 1
            continue
        if record_type != "local_source_identity":
            stats["non_identity_rows_ignored"] += 1
            continue
        raw_source_id = normalize_text(record.get("source_file_id"))
        group_key = raw_source_id or f"missing-source-file-id:{row_number}"
        grouped[group_key].append((row_number, record))

    seeds: list[ProductSeed] = []
    for group_key, rows in sorted(grouped.items()):
        row_number, record = rows[0]
        source_id = valid_drive_id(record.get("source_file_id"))
        seed_key = f"drive:{source_id}" if source_id else f"invalid-ai-source:{group_key}"
        reasons: list[str] = []
        if not source_id:
            reasons.append("synthetic_or_invalid_source_id")
        extension = normalize_text(record.get("source_extension")).casefold().lstrip(".")
        if extension and extension != "png":
            reasons.append("ai_source_extension_not_png")
        if len(rows) > 1:
            signatures = {
                _metadata_signature(
                    _seed_evidence(
                        item,
                        source_label=source_label,
                        row_number=item_row,
                        drive_ids=((source_id,) if source_id else ()),
                    )
                )
                for item_row, item in rows
            }
            if len(signatures) > 1:
                reasons.append("supplemental_metadata_conflict")
        evidence = tuple(
            _seed_evidence(
                item,
                source_label=source_label,
                row_number=item_row,
                drive_ids=((source_id,) if source_id else ()),
            )
            for item_row, item in rows
        )
        seeds.append(
            ProductSeed(
                identity=_seed_identity(source_namespace, seed_key),
                seed_key=seed_key,
                png_file_ids=((source_id,) if source_id else ()),
                metadata_evidence=evidence,
                source_refs=tuple(item.source_ref for item in evidence),
                hold_reasons=tuple(sorted(set(reasons))),
            )
        )
    stats["source_identities"] = len(seeds)
    return seeds, stats


def merge_authoritative_product_seeds(
    dreamstime_seeds: Sequence[ProductSeed], ai_png_seeds: Sequence[ProductSeed]
) -> tuple[list[ProductSeed], Counter[str]]:
    """Return the deterministic exact union, merging only exact source-ID overlap."""

    stats: Counter[str] = Counter()
    dream = {seed.identity: seed for seed in dreamstime_seeds}
    source_to_dream: dict[str, list[str]] = defaultdict(list)
    for seed in dream.values():
        for source_id in seed.source_file_ids:
            source_to_dream[source_id].append(seed.identity)

    conflicting_dream: set[str] = set()
    for identities in source_to_dream.values():
        if len(identities) > 1:
            conflicting_dream.update(identities)
    for identity in conflicting_dream:
        seed = dream[identity]
        dream[identity] = replace(
            seed,
            hold_reasons=tuple(
                sorted(set(seed.hold_reasons) | {"source_id_shared_by_primary_groups"})
            ),
        )

    supplemental: list[ProductSeed] = []
    for ai_seed in sorted(ai_png_seeds, key=lambda item: item.identity):
        source_id = ai_seed.png_file_ids[0] if len(ai_seed.png_file_ids) == 1 else None
        overlaps = source_to_dream.get(source_id or "", [])
        if len(overlaps) == 1:
            identity = overlaps[0]
            primary = dream[identity]
            role_conflict = source_id not in primary.png_file_ids
            dream[identity] = ProductSeed(
                # The exact Drive source ID is the durable identity.  Keep the
                # supplemental identity when that source is later promoted to
                # a Dreamstime primary group, even if Dreamstime changes its
                # product_group_id convention.
                identity=ai_seed.identity,
                seed_key=ai_seed.seed_key,
                png_file_ids=primary.png_file_ids,
                svg_file_ids=primary.svg_file_ids,
                metadata_evidence=primary.metadata_evidence + ai_seed.metadata_evidence,
                source_refs=tuple(dict.fromkeys(primary.source_refs + ai_seed.source_refs)),
                hold_reasons=tuple(
                    sorted(
                        set(primary.hold_reasons)
                        | set(ai_seed.hold_reasons)
                        | (
                            {"source_id_role_conflict_between_ledgers"}
                            if role_conflict
                            else set()
                        )
                    )
                ),
            )
            stats["ai_exact_overlaps_merged"] += 1
        elif len(overlaps) > 1:
            for identity in overlaps:
                primary = dream[identity]
                dream[identity] = replace(
                    primary,
                    hold_reasons=tuple(
                        sorted(
                            set(primary.hold_reasons)
                            | {"supplemental_overlap_ambiguous"}
                        )
                    ),
                )
            stats["ai_ambiguous_overlaps_held"] += 1
        else:
            supplemental.append(ai_seed)
            stats["ai_unique_seeds_added"] += 1
    union = sorted([*dream.values(), *supplemental], key=lambda item: item.identity)
    if len({seed.identity for seed in union}) != len(union):
        raise ManifestBuilderError("authoritative_seed_identity_conflict")
    stats["dreamstime_primary_seeds"] = len(dreamstime_seeds)
    stats["ai_png_seeds"] = len(ai_png_seeds)
    stats["authoritative_seed_union"] = len(union)
    return union, stats


def group_inventory_assets(
    assets: Sequence[DriveAsset], source_namespace: str
) -> tuple[list[AssetGroup], Counter[str]]:
    grouped: dict[tuple[str, str, str], dict[str, list[DriveAsset]]] = defaultdict(
        lambda: {".png": [], ".svg": []}
    )
    stats: Counter[str] = Counter()
    for asset in sorted(assets, key=lambda item: (item.file_id, item.parent_id, item.name.casefold())):
        if asset.extension not in {".png", ".svg"}:
            stats[f"inventory_{asset.extension.lstrip('.') or 'other'}_files"] += 1
            continue
        basename = asset.basename_normalized
        if not basename:
            stats["held_empty_basename"] += 1
            continue
        grouped[(asset.root_id, asset.parent_id, basename)][asset.extension].append(asset)
    output: list[AssetGroup] = []
    for (root_id, parent_id, basename), members in sorted(grouped.items()):
        cohorts = sorted({item.cohort for values in members.values() for item in values})
        cohort = cohorts[0] if len(cohorts) == 1 else "mixed"
        identity_digest = stable_digest(source_namespace, root_id, parent_id, basename)
        output.append(
            AssetGroup(
                identity=f"gdrive:v3:{identity_digest}",
                root_id=root_id,
                parent_id=parent_id,
                cohort=cohort,
                basename_normalized=basename,
                png_assets=tuple(sorted(members[".png"], key=lambda item: item.file_id)),
                svg_assets=tuple(sorted(members[".svg"], key=lambda item: item.file_id)),
            )
        )
    stats["products_discovered"] = len(output)
    stats["png_products"] = sum(bool(group.png_assets) for group in output)
    stats["svg_products"] = sum(bool(group.svg_assets) for group in output)
    stats["svg_only_products"] = sum(bool(group.svg_assets) and not group.png_assets for group in output)
    stats["png_only_products"] = sum(bool(group.png_assets) and not group.svg_assets for group in output)
    stats["svg_png_products"] = sum(bool(group.png_assets) and bool(group.svg_assets) for group in output)
    return output, stats


def _safe_archive_member(value: str | None, extension: str) -> str | None:
    if not value:
        return None
    normalized = value.replace("\\", "/").strip()
    path = PurePosixPath(normalized)
    if (
        path.is_absolute()
        or ".." in path.parts
        or not normalized.casefold().endswith(extension)
        or any(not part or len(part) > 255 for part in path.parts)
    ):
        return None
    return normalized


def _archive_member_stem_key(value: str) -> str:
    path = PurePosixPath(unicodedata.normalize("NFKC", value.replace("\\", "/")))
    stem_path = path.with_suffix("")
    return "/".join(part.casefold() for part in stem_path.parts)


def archive_groups_from_evidence(
    evidence: Sequence[MetadataEvidence],
    inventory_assets: Sequence[DriveAsset],
    source_namespace: str,
) -> tuple[list[AssetGroup], Counter[str]]:
    archives = {asset.file_id: asset for asset in inventory_assets if asset.extension == ".zip"}
    output: dict[str, AssetGroup] = {}
    conflicted: set[str] = set()
    stats: Counter[str] = Counter()
    for item in evidence:
        if not item.archive_drive_id:
            continue
        stats["archive_records_seen"] += 1
        archive = archives.get(item.archive_drive_id or "")
        png_member = _safe_archive_member(item.archive_png_member, ".png")
        svg_member = _safe_archive_member(item.archive_svg_member, ".svg")
        if not archive:
            stats["blocking_archive_source_missing"] += 1
            continue
        if not (png_member or svg_member):
            stats["blocking_archive_member_invalid"] += 1
            continue
        png_key = _archive_member_stem_key(png_member) if png_member else None
        svg_key = _archive_member_stem_key(svg_member) if svg_member else None
        if png_key and svg_key and png_key != svg_key:
            stats["blocking_archive_member_pair_mismatch"] += 1
            continue
        member_key = svg_key or png_key
        assert member_key
        basename = normalize_basename(svg_member or png_member or "")
        identity = f"gdrive:v3:{stable_digest(source_namespace, archive.file_id, member_key)}"
        if identity in conflicted:
            continue
        group = AssetGroup(
            identity=identity,
            root_id=archive.root_id,
            parent_id=archive.file_id,
            cohort=f"{archive.cohort}_archive",
            basename_normalized=basename,
            archive_asset=archive,
            archive_png_member=png_member,
            archive_svg_member=svg_member,
        )
        prior = output.get(identity)
        if prior:
            png_conflict = bool(
                prior.archive_png_member
                and group.archive_png_member
                and prior.archive_png_member != group.archive_png_member
            )
            svg_conflict = bool(
                prior.archive_svg_member
                and group.archive_svg_member
                and prior.archive_svg_member != group.archive_svg_member
            )
            if png_conflict or svg_conflict:
                output.pop(identity, None)
                conflicted.add(identity)
                stats["blocking_archive_identity_conflict"] += 1
                continue
            group = AssetGroup(
                identity=prior.identity,
                root_id=prior.root_id,
                parent_id=prior.parent_id,
                cohort=prior.cohort,
                basename_normalized=prior.basename_normalized,
                archive_asset=prior.archive_asset,
                archive_png_member=prior.archive_png_member or group.archive_png_member,
                archive_svg_member=prior.archive_svg_member or group.archive_svg_member,
            )
        output[identity] = group
    stats["archive_products_discovered"] = len(output)
    return sorted(output.values(), key=lambda item: item.identity), stats


def _metadata_source_priority(item: MetadataEvidence) -> int:
    """Rank reviewed source families; never infer authority from text length."""

    source = normalize_header(PurePosixPath(item.source_label.replace("\\", "/")).name)
    if source == "pinned primary ledger":
        return 700
    if source == "pinned png ledger":
        return 650
    # Exact private filenames never belong in this public runner.  The two
    # authoritative Drive objects are selected and name-checked through
    # secret-backed configuration, then relabelled with the generic labels
    # above before reconciliation.
    if "catalog manifest" in source or source.endswith("jsonl"):
        return 600
    if source.startswith("uploads "):
        return 100
    if "reconciliation" in source:
        return 300
    if "metadata" in source:
        return 250
    if source.endswith((" xlsx", " csv", " md")):
        return 200
    return 150


def _metadata_signature(item: MetadataEvidence) -> tuple[Any, ...]:
    category = item.categories[0] if item.categories else ""
    return (
        item.title.casefold(),
        item.description.casefold(),
        category.casefold(),
        tuple(sorted(tag.casefold() for tag in item.tags)),
    )


def _metadata_for_group(matches: Sequence[MatchedEvidence]) -> ResolvedMetadata:
    candidates = [
        match
        for match in matches
        if any(
            (
                match.evidence.title,
                match.evidence.description,
                match.evidence.categories,
                match.evidence.tags,
            )
        )
    ]
    complete = [
        match
        for match in candidates
        if (
            match.evidence.title
            and match.evidence.description
            and match.evidence.tags
        )
    ]
    pool = complete or candidates
    if not pool:
        return ResolvedMetadata("", "", "", (), {}, {})

    best_rank = max(
        (match.confidence, _metadata_source_priority(match.evidence)) for match in pool
    )
    peers = [
        match
        for match in pool
        if (match.confidence, _metadata_source_priority(match.evidence)) == best_rank
    ]
    signatures = {_metadata_signature(match.evidence) for match in peers}
    selected = min(peers, key=lambda match: match.evidence.source_ref)
    item = selected.evidence

    def variant_count(values: Iterable[str]) -> int:
        return len({value.casefold() for value in values if value})

    provenance = {
        "source_ref": item.source_ref,
        "source_label": item.source_label,
        "section": item.section,
        "row_number": item.row_number,
        "source_priority": best_rank[1],
        "match_confidence": best_rank[0],
        "alternate_source_refs": sorted(
            {
                match.evidence.source_ref
                for match in candidates
                if match.evidence.source_ref != item.source_ref
            }
        ),
    }
    return ResolvedMetadata(
        title=item.title,
        description=item.description,
        category=item.categories[0] if item.categories else "",
        tags=item.tags,
        variants={
            "title_variants": variant_count(match.evidence.title for match in candidates),
            "description_variants": variant_count(
                match.evidence.description for match in candidates
            ),
            "category_variants": variant_count(
                category
                for match in candidates
                for category in match.evidence.categories[:1]
            ),
        },
        provenance=provenance,
        priority_conflict=len(signatures) > 1,
    )


def _metadata_contract_reasons(
    title: str, description: str, category: str, tags: Sequence[str]
) -> list[str]:
    reasons: list[str] = []
    if pipeline.metadata_text_length(title) < 3:
        reasons.append("title_missing")
    elif pipeline.metadata_text_length(title) > MAX_TITLE:
        reasons.append("title_exceeds_pipeline_limit")
    if pipeline.metadata_text_length(description) < 20:
        reasons.append("description_missing")
    elif pipeline.metadata_text_length(description) > MAX_DESCRIPTION:
        reasons.append("description_exceeds_pipeline_limit")
    if category and pipeline.metadata_text_length(category) > MAX_CATEGORY:
        reasons.append("category_exceeds_pipeline_limit")
    if not tags:
        reasons.append("tags_missing")
    elif len(tags) > MAX_TAGS:
        reasons.append("tags_exceed_pipeline_limit")
    if any(pipeline.metadata_text_length(tag) > MAX_TAG for tag in tags):
        reasons.append("tag_exceeds_pipeline_limit")
    return reasons


def _seed_metadata_contract_reasons(
    title: str, description: str, category: str, tags: Sequence[str]
) -> list[str]:
    """Seed-ledger SEO contract; category is useful but not publication-blocking."""

    return _metadata_contract_reasons(title, description, category, tags)


def _record_for_group(
    group: AssetGroup,
    title: str,
    description: str,
    category: str,
    tags: Sequence[str],
    metadata_provenance: Mapping[str, Any],
    matches: Sequence[MatchedEvidence],
    source_namespace: str,
) -> dict[str, Any]:
    dreamstime_ids = sorted(
        {
            image_id
            for match in matches
            for image_id in match.evidence.dreamstime_ids
            if image_id.isdigit()
        },
        key=lambda value: (len(value), value),
    )
    assets: dict[str, Any]
    if group.archive_asset:
        assets = {
            "archive_drive_file_id": group.archive_asset.file_id,
            "archive_png_member": group.archive_png_member,
            "archive_svg_member": group.archive_svg_member,
        }
    else:
        assets = {
            "png_drive_file_id": group.png_assets[0].file_id if group.png_assets else None,
            "svg_drive_file_id": group.svg_assets[0].file_id if group.svg_assets else None,
            "render_needed": bool(group.svg_assets and not group.png_assets),
        }
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "record_type": "heeexy_catalog_product",
        "product_identity": group.identity,
        "metadata": {
            "title": title,
            "description": description,
            "categories": [category] if category else [],
            "keywords": list(tags),
        },
        "metadata_provenance": dict(metadata_provenance),
        "assets": assets,
        "source": {
            "provider": "google_drive",
            "namespace": source_namespace,
            "container_id": group.root_id,
            "folder_id": group.parent_id,
            "basename_normalized": group.basename_normalized,
            "cohort": group.cohort,
        },
        "external_refs": [
            {"provider": "dreamstime", "ref_type": "image_id", "ref_value": value}
            for value in dreamstime_ids
        ],
    }


def _hold_for_group(group: AssetGroup, reasons: Sequence[str]) -> dict[str, Any]:
    return {
        "product_identity": group.identity,
        "cohort": group.cohort,
        "basename_normalized": group.basename_normalized,
        "folder_id": group.parent_id,
        "source_asset_ids": sorted(asset.file_id for asset in group.source_assets),
        "source_asset_names": sorted(asset.name for asset in group.source_assets),
        "reasons": sorted(set(reasons)),
    }


def reconcile_catalog(
    groups: Sequence[AssetGroup],
    evidence: Sequence[MetadataEvidence],
    source_namespace: str,
) -> ReconciliationResult:
    result = ReconciliationResult()
    asset_to_groups: dict[str, set[str]] = defaultdict(set)
    basename_to_groups: dict[str, set[str]] = defaultdict(set)
    archive_to_groups: dict[str, set[str]] = defaultdict(set)
    group_by_identity = {group.identity: group for group in groups}
    for group in groups:
        for asset in group.source_assets:
            asset_to_groups[asset.file_id].add(group.identity)
        if group.archive_asset:
            archive_to_groups[group.archive_asset.file_id].add(group.identity)
        basename_to_groups[group.basename_normalized].add(group.identity)

    matches: dict[str, list[MatchedEvidence]] = defaultdict(list)
    unassigned_title_rows: list[MetadataEvidence] = []
    for item in evidence:
        if item.archive_drive_id:
            archive_candidates = set(archive_to_groups.get(item.archive_drive_id, set()))
            png_member = _safe_archive_member(item.archive_png_member, ".png")
            svg_member = _safe_archive_member(item.archive_svg_member, ".svg")
            if png_member:
                archive_candidates = {
                    identity
                    for identity in archive_candidates
                    if group_by_identity[identity].archive_png_member == png_member
                }
            if svg_member:
                archive_candidates = {
                    identity
                    for identity in archive_candidates
                    if group_by_identity[identity].archive_svg_member == svg_member
                }
            if len(archive_candidates) == 1:
                matches[next(iter(archive_candidates))].append(MatchedEvidence(item, 350))
                continue
            if len(archive_candidates) > 1:
                result.ambiguous_metadata_rows += 1
                continue
            result.unmatched_metadata_rows += 1
            continue
        direct = {
            identity
            for file_id in item.drive_ids + ((item.archive_drive_id,) if item.archive_drive_id else ())
            for identity in asset_to_groups.get(file_id, set())
        }
        if len(direct) == 1:
            matches[next(iter(direct))].append(MatchedEvidence(item, 300))
            continue
        if len(direct) > 1:
            result.ambiguous_metadata_rows += 1
            continue
        basename_matches = {
            identity
            for name in item.source_names
            for identity in basename_to_groups.get(normalize_basename(name), set())
            if normalize_basename(name)
        }
        if len(basename_matches) == 1:
            matches[next(iter(basename_matches))].append(MatchedEvidence(item, 200))
        elif len(basename_matches) > 1:
            result.ambiguous_metadata_rows += 1
        elif item.title:
            unassigned_title_rows.append(item)
        else:
            result.unmatched_metadata_rows += 1

    # Dreamstime's current export contains numeric IDs plus titles but no source
    # file IDs.  Attach those IDs only after a source-backed title has been
    # selected and only when that normalized title identifies exactly one group.
    title_to_groups: dict[str, set[str]] = defaultdict(set)
    for identity, group_matches in matches.items():
        resolved = _metadata_for_group(group_matches)
        if resolved.title:
            title_to_groups[normalize_text(resolved.title).casefold()].add(identity)
    for item in unassigned_title_rows:
        candidates = title_to_groups.get(normalize_text(item.title).casefold(), set())
        if len(candidates) == 1:
            matches[next(iter(candidates))].append(MatchedEvidence(item, 100))
        elif len(candidates) > 1:
            result.ambiguous_metadata_rows += 1
        else:
            result.unmatched_metadata_rows += 1

    for group in sorted(groups, key=lambda item: item.identity):
        reasons: list[str] = []
        if len(group.png_assets) > 1:
            reasons.append("png_identity_ambiguous")
        if len(group.svg_assets) > 1:
            reasons.append("svg_identity_ambiguous")
        group_matches = matches.get(group.identity, [])
        resolved = _metadata_for_group(group_matches)
        if resolved.priority_conflict:
            reasons.append("metadata_priority_conflict")
        reasons.extend(
            _metadata_contract_reasons(
                resolved.title, resolved.description, resolved.category, resolved.tags
            )
        )
        for name, count in resolved.variants.items():
            if count > 1:
                result.warnings[name] += 1
        if reasons:
            result.holds.append(_hold_for_group(group, reasons))
            continue
        record = _record_for_group(
            group,
            resolved.title,
            resolved.description,
            resolved.category,
            resolved.tags,
            resolved.provenance,
            group_matches,
            source_namespace,
        )
        try:
            candidate = pipeline.candidate_from_record(record)
        except pipeline.PipelineError as error:
            result.holds.append(_hold_for_group(group, [f"pipeline_{error.code}"]))
            continue
        if candidate is None or (
            candidate.title != resolved.title
            or candidate.description != resolved.description
            or candidate.category != resolved.category
            or candidate.tags != resolved.tags
        ):
            result.holds.append(_hold_for_group(group, ["pipeline_metadata_not_lossless"]))
            continue
        result.records.append(record)
    return result


def _hold_for_seed(
    seed: ProductSeed, reasons: Sequence[str], resolved_assets: Sequence[DriveAsset]
) -> dict[str, Any]:
    return {
        "product_identity": seed.identity,
        "seed_key_sha256": hashlib.sha256(seed.seed_key.encode("utf-8")).hexdigest(),
        "source_refs": list(seed.source_refs),
        "expected_png_file_ids": list(seed.png_file_ids),
        "expected_svg_file_ids": list(seed.svg_file_ids),
        "resolved_source_asset_ids": sorted(asset.file_id for asset in resolved_assets),
        "resolved_source_asset_names": sorted(asset.name for asset in resolved_assets),
        "reasons": sorted(set(reasons)),
    }


def reconcile_seed_catalog(
    seeds: Sequence[ProductSeed],
    inventory_assets: Sequence[DriveAsset],
    source_namespace: str,
) -> tuple[ReconciliationResult, Counter[str]]:
    """Verify seed IDs against inventory and emit exactly one disposition per seed.

    Inventory is intentionally only an ID/role verification input. Unreferenced
    files are counted, never grouped by basename and never promoted to products.
    """

    result = ReconciliationResult()
    stats: Counter[str] = Counter()
    assets_by_id: dict[str, DriveAsset] = {}
    conflicting_asset_ids: set[str] = set()
    for asset in sorted(inventory_assets, key=lambda item: (item.file_id, item.name.casefold())):
        prior = assets_by_id.get(asset.file_id)
        if prior and prior != asset:
            conflicting_asset_ids.add(asset.file_id)
            continue
        assets_by_id[asset.file_id] = asset

    referenced_ids = {
        source_id for seed in seeds for source_id in seed.source_file_ids
    }
    stats["authoritative_seeds"] = len(seeds)
    stats["inventory_assets"] = len(assets_by_id)
    stats["seed_referenced_asset_ids"] = len(referenced_ids)
    stats["unreferenced_inventory_assets"] = len(set(assets_by_id) - referenced_ids)
    stats["blocking_inventory_file_id_conflicts"] = len(
        conflicting_asset_ids & referenced_ids
    )
    stats["unreferenced_inventory_file_id_conflicts"] = len(
        conflicting_asset_ids - referenced_ids
    )
    for extension in (".png", ".svg", ".zip"):
        stats[f"inventory_{extension.lstrip('.')}_files"] = sum(
            asset.extension == extension for asset in assets_by_id.values()
        )

    for seed in sorted(seeds, key=lambda item: item.identity):
        reasons = list(seed.hold_reasons)
        if set(seed.png_file_ids) & set(seed.svg_file_ids):
            reasons.append("source_id_has_multiple_roles")
        if len(seed.png_file_ids) > 1:
            reasons.append("multiple_png_source_ids")
        if len(seed.svg_file_ids) > 1:
            reasons.append("multiple_svg_source_ids")
        resolved_assets: list[DriveAsset] = []
        for role, source_ids, extension in (
            ("png", seed.png_file_ids, ".png"),
            ("svg", seed.svg_file_ids, ".svg"),
        ):
            for source_id in source_ids:
                if source_id in conflicting_asset_ids:
                    reasons.append("inventory_file_id_conflict")
                    continue
                asset = assets_by_id.get(source_id)
                if not asset:
                    reasons.append(f"{role}_source_missing_from_inventory")
                    continue
                resolved_assets.append(asset)
                if asset.extension != extension:
                    reasons.append(f"{role}_source_role_mismatch")
        matches = [MatchedEvidence(item, 400) for item in seed.metadata_evidence]
        resolved = _metadata_for_group(matches)
        if resolved.priority_conflict:
            reasons.append("metadata_priority_conflict")
        reasons.extend(
            _seed_metadata_contract_reasons(
                resolved.title, resolved.description, resolved.category, resolved.tags
            )
        )
        for name, count in resolved.variants.items():
            if count > 1:
                result.warnings[name] += 1
        if reasons:
            result.holds.append(_hold_for_seed(seed, reasons, resolved_assets))
            continue

        ordered_assets = sorted(resolved_assets, key=lambda item: item.file_id)
        first = ordered_assets[0]
        png_assets = tuple(
            assets_by_id[source_id]
            for source_id in seed.png_file_ids
            if source_id in assets_by_id
        )
        svg_assets = tuple(
            assets_by_id[source_id]
            for source_id in seed.svg_file_ids
            if source_id in assets_by_id
        )
        group = AssetGroup(
            identity=seed.identity,
            root_id=first.root_id,
            parent_id=first.parent_id,
            cohort="authoritative_seed",
            basename_normalized=first.basename_normalized,
            png_assets=png_assets,
            svg_assets=svg_assets,
        )
        record = _record_for_group(
            group,
            resolved.title,
            resolved.description,
            resolved.category,
            resolved.tags,
            {
                **resolved.provenance,
                "authoritative_seed_refs": list(seed.source_refs),
            },
            matches,
            source_namespace,
        )
        record["source"]["seed_key_sha256"] = hashlib.sha256(
            seed.seed_key.encode("utf-8")
        ).hexdigest()
        try:
            candidate = pipeline.candidate_from_record(record)
        except pipeline.PipelineError as error:
            result.holds.append(
                _hold_for_seed(seed, [f"pipeline_{error.code}"], resolved_assets)
            )
            continue
        if candidate is None or (
            candidate.title != resolved.title
            or candidate.description != resolved.description
            or candidate.category != resolved.category
            or candidate.tags != resolved.tags
        ):
            result.holds.append(
                _hold_for_seed(seed, ["pipeline_metadata_not_lossless"], resolved_assets)
            )
            continue
        result.records.append(record)

    if len(result.records) + len(result.holds) != len(seeds):
        raise ManifestBuilderError("authoritative_seed_disposition_incomplete")
    stats["accepted_seed_products"] = len(result.records)
    stats["held_seed_products"] = len(result.holds)
    stats["disposed_seed_products"] = len(result.records) + len(result.holds)
    return result, stats


def render_manifest_jsonl(records: Sequence[Mapping[str, Any]]) -> bytes:
    ordered = sorted(records, key=lambda item: str(item.get("product_identity") or ""))
    return ("".join(pipeline.canonical_json(record) + "\n" for record in ordered)).encode("utf-8")


def manifest_product_identities(payload: bytes) -> frozenset[str]:
    """Validate a prior generated manifest and return its stable identities."""

    identities: set[str] = set()
    for row_number, raw_line in enumerate(_iter_text_lines(payload), start=1):
        if row_number > MAX_METADATA_ROWS:
            raise ManifestBuilderError("previous_manifest_row_limit")
        line = raw_line.strip()
        if not line:
            continue
        if len(line.encode("utf-8")) > 1_000_000:
            raise ManifestBuilderError("previous_manifest_line_limit")
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ManifestBuilderError("previous_manifest_invalid") from error
        if (
            not isinstance(record, Mapping)
            or record.get("schema_version") != MANIFEST_SCHEMA_VERSION
            or record.get("record_type") != "heeexy_catalog_product"
        ):
            raise ManifestBuilderError("previous_manifest_invalid")
        identity = normalize_text(record.get("product_identity"))
        if not MANIFEST_IDENTITY_RE.fullmatch(identity) or identity in identities:
            raise ManifestBuilderError("previous_manifest_invalid")
        identities.add(identity)
    if not identities:
        raise ManifestBuilderError("previous_manifest_empty")
    return frozenset(identities)


def append_only_manifest(
    previous_manifest: bytes, records: Sequence[Mapping[str, Any]]
) -> tuple[bytes, int]:
    """Keep every previously published record and add only new identities.

    Used when the full-catalog gate holds for historical reasons (for example
    unresolved legacy inventory) so newly validated designs can still go live
    without dropping or rewriting anything already published.
    """

    kept: list[Mapping[str, Any]] = []
    identities: set[str] = set()
    for raw_line in _iter_text_lines(previous_manifest):
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ManifestBuilderError("previous_manifest_invalid") from error
        identity = normalize_text(record.get("product_identity")) if isinstance(record, Mapping) else ""
        if (
            not isinstance(record, Mapping)
            or record.get("schema_version") != MANIFEST_SCHEMA_VERSION
            or record.get("record_type") != "heeexy_catalog_product"
            or not MANIFEST_IDENTITY_RE.fullmatch(identity)
            or identity in identities
        ):
            raise ManifestBuilderError("previous_manifest_invalid")
        identities.add(identity)
        kept.append(record)
    if not kept:
        raise ManifestBuilderError("previous_manifest_empty")
    added = [
        record
        for record in records
        if normalize_text(record.get("product_identity")) not in identities
        and MANIFEST_IDENTITY_RE.fullmatch(normalize_text(record.get("product_identity")))
    ]
    return render_manifest_jsonl([*kept, *added]), len(added)


def build_inventory_report(
    *,
    inventory_stats: Mapping[str, int],
    metadata_stats: Mapping[str, int],
    reconciliation: ReconciliationResult,
    manifest_payload: bytes,
    expected_min_products: int,
    expected_min_accepted: int,
    generated_at: str,
) -> dict[str, Any]:
    products_discovered = int(
        inventory_stats.get(
            "authoritative_seeds", inventory_stats.get("products_discovered", 0)
        )
    )
    global_inventory_assets = int(
        inventory_stats.get("global_inventory_assets", products_discovered)
    )
    authoritative_mode = "authoritative_seeds" in inventory_stats
    curated_seed_count = products_discovered
    accepted = len(reconciliation.records)
    inventory_blockers = sum(
        int(value)
        for key, value in inventory_stats.items()
        if key.startswith("blocking_") or "_blocking_" in key
    )
    metadata_blockers = sum(
        int(value)
        for key, value in metadata_stats.items()
        if key.startswith("held_metadata_file_")
    )
    collision_reasons = {
        "source_id_shared_by_primary_groups",
        "supplemental_overlap_ambiguous",
    }
    cross_product_collisions = sum(
        reason in collision_reasons
        for hold in reconciliation.holds
        for reason in hold["reasons"]
    )
    blocking_issue_count = (
        inventory_blockers + metadata_blockers + cross_product_collisions
    )
    reviewed_holds = 0
    unresolved_holds = 0
    for hold in reconciliation.holds:
        reasons = set(hold.get("reasons") or ())
        if reasons and reasons <= REVIEWED_SEED_HOLD_REASONS:
            reviewed_holds += 1
        else:
            unresolved_holds += 1
    complete_seed_disposition = accepted + len(reconciliation.holds) == products_discovered
    full_catalog_complete = (
        products_discovered >= expected_min_products
        and accepted == products_discovered
        and not reconciliation.holds
        and complete_seed_disposition
        and blocking_issue_count == 0
    )
    complete_inventory_coverage = full_catalog_complete
    if authoritative_mode:
        eligible = (
            curated_seed_count >= expected_min_accepted
            and accepted > 0
            and complete_seed_disposition
            and reviewed_holds <= MAX_REVIEWED_SEED_HOLDS
            and unresolved_holds == 0
            and blocking_issue_count == 0
        )
    else:
        eligible = (
            products_discovered >= expected_min_products
            and accepted >= expected_min_accepted
            and accepted > 0
            and complete_inventory_coverage
            and blocking_issue_count == 0
        )
    hold_counts: Counter[str] = Counter(
        reason for hold in reconciliation.holds for reason in hold["reasons"]
    )
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "record_type": "heeexy_catalog_inventory_report",
        "generated_at": generated_at,
        "manifest": {
            "filename": MANIFEST_FILENAME,
            "sha256": hashlib.sha256(manifest_payload).hexdigest(),
            "size_bytes": len(manifest_payload),
            "accepted_products": accepted,
        },
        "inventory": {key: int(value) for key, value in sorted(inventory_stats.items())},
        "metadata": {key: int(value) for key, value in sorted(metadata_stats.items())},
        "reconciliation": {
            "accepted": accepted,
            "held": len(reconciliation.holds),
            "hold_counts": dict(sorted(hold_counts.items())),
            "unmatched_metadata_rows": reconciliation.unmatched_metadata_rows,
            "ambiguous_metadata_rows": reconciliation.ambiguous_metadata_rows,
            "warnings": dict(sorted(reconciliation.warnings.items())),
            "holds": reconciliation.holds,
        },
        "publication_gate": {
            "eligible": eligible,
            "expected_min_products": expected_min_products,
            "expected_min_accepted": expected_min_accepted,
            "global_inventory_target": expected_min_products,
            "global_product_target": expected_min_products,
            "curated_seed_target": expected_min_accepted,
            "global_inventory_assets": global_inventory_assets,
            "authoritative_seeds": products_discovered,
            "curated_seed_count": curated_seed_count,
            "global_product_gap": max(
                0, expected_min_products - curated_seed_count
            ),
            "inventory_shortfall": max(
                0, expected_min_products - products_discovered
            ),
            "curated_seed_shortfall": max(
                0, expected_min_accepted - curated_seed_count
            ),
            "accepted_shortfall": max(0, expected_min_accepted - accepted),
            "complete_inventory_coverage": complete_inventory_coverage,
            "complete_seed_disposition": complete_seed_disposition,
            "full_catalog_complete": full_catalog_complete,
            "pilot_manifest_eligible": eligible,
            "reviewed_holds": reviewed_holds,
            "reviewed_hold_limit": MAX_REVIEWED_SEED_HOLDS,
            "unresolved_holds": unresolved_holds,
            "cross_product_collisions": cross_product_collisions,
            "blocking_issue_count": blocking_issue_count,
            "inventory_blockers": inventory_blockers,
            "metadata_file_blockers": metadata_blockers,
        },
    }


class ManifestDriveClient(pipeline.DriveClient):
    """Drive operations limited to metadata listing/reading and generated output."""

    def list_children_many(
        self, folder_ids: Sequence[str]
    ) -> dict[str, list[dict[str, Any]]]:
        """List direct children of a small, explicitly scoped folder frontier."""

        parents = tuple(folder_ids)
        if (
            not parents
            or len(parents) > MAX_DRIVE_PARENT_BATCH
            or len(set(parents)) != len(parents)
            or any(valid_drive_id(folder_id) != folder_id for folder_id in parents)
        ):
            raise ManifestBuilderError("drive_parent_batch_invalid")
        query = _drive_parent_query(parents)
        if len(query) > MAX_DRIVE_PARENT_QUERY_CHARS:
            raise ManifestBuilderError("drive_parent_query_too_long")

        requested = set(parents)
        output: dict[str, list[dict[str, Any]]] = {
            folder_id: [] for folder_id in parents
        }
        page_token: str | None = None
        try:
            while True:
                response = (
                    self.service.files()
                    .list(
                        q=query,
                        spaces="drive",
                        pageSize=1000,
                        pageToken=page_token,
                        fields=(
                            "nextPageToken,incompleteSearch,"
                            f"files({MANIFEST_DRIVE_LIST_FIELDS})"
                        ),
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    )
                    .execute(num_retries=3)
                )
                if response.get("incompleteSearch"):
                    raise ManifestBuilderError("drive_list_incomplete")
                for item in response.get("files", []):
                    raw_parents = item.get("parents")
                    if (
                        not isinstance(raw_parents, (list, tuple))
                        or len(raw_parents) != 1
                        or raw_parents[0] not in requested
                    ):
                        raise ManifestBuilderError("drive_list_parent_invalid")
                    output[raw_parents[0]].append(item)
                page_token = response.get("nextPageToken")
                if not page_token:
                    return output
        except ManifestBuilderError:
            raise
        except Exception as error:
            raise pipeline.classify_drive_error(
                "drive_list_failed", error, candidate_scoped=False
            ) from error

    def read_generated_bytes(
        self,
        *,
        parent_id: str,
        role: str,
        maximum_bytes: int,
        expected_file_id: str | None = None,
    ) -> bytes | None:
        properties = {"heeexy_role": role, "schema_version": str(MANIFEST_SCHEMA_VERSION)}
        existing = self._find_by_properties(parent_id, properties)
        if len(existing) > 1:
            raise ManifestBuilderError("generated_output_duplicate")
        if not existing:
            if expected_file_id:
                raise ManifestBuilderError("generated_output_expected_missing")
            return None
        file_id = valid_drive_id(existing[0].get("id"))
        if not file_id:
            raise ManifestBuilderError("generated_output_identity_invalid")
        if expected_file_id and file_id != expected_file_id:
            raise ManifestBuilderError("generated_output_identity_mismatch")
        payload, _ = self.download_bytes(file_id, maximum_bytes)
        return payload

    def upsert_generated_bytes(
        self,
        *,
        parent_id: str,
        name: str,
        mime_type: str,
        role: str,
        payload: bytes,
        expected_file_id: str | None = None,
    ) -> str:
        if not payload:
            raise ManifestBuilderError("generated_output_empty")
        properties = {"heeexy_role": role, "schema_version": str(MANIFEST_SCHEMA_VERSION)}
        existing = self._find_by_properties(parent_id, properties)
        if len(existing) > 1:
            raise ManifestBuilderError("generated_output_duplicate")
        if expected_file_id:
            existing_file_id = (
                valid_drive_id(existing[0].get("id")) if existing else None
            )
            if existing_file_id != expected_file_id:
                raise ManifestBuilderError("generated_output_identity_mismatch")
        expected_md5 = hashlib.md5(payload, usedforsecurity=False).hexdigest()
        try:
            from googleapiclient.http import MediaIoBaseUpload

            media = MediaIoBaseUpload(io.BytesIO(payload), mimetype=mime_type, resumable=False)
            if existing:
                response = (
                    self.service.files()
                    .update(
                        fileId=existing[0]["id"],
                        body={"name": name, "appProperties": properties},
                        media_body=media,
                        fields=pipeline.DRIVE_FILE_FIELDS,
                        supportsAllDrives=True,
                    )
                    .execute(num_retries=3)
                )
            else:
                response = (
                    self.service.files()
                    .create(
                        body={
                            "name": name,
                            "parents": [parent_id],
                            "appProperties": properties,
                        },
                        media_body=media,
                        fields=pipeline.DRIVE_FILE_FIELDS,
                        supportsAllDrives=True,
                    )
                    .execute(num_retries=3)
                )
        except ManifestBuilderError:
            raise
        except Exception as error:
            raise pipeline.classify_drive_error(
                "generated_output_upload_failed", error, candidate_scoped=False
            ) from error
        try:
            size = int(response.get("size") or -1)
        except (TypeError, ValueError) as error:
            raise ManifestBuilderError("generated_output_verification_failed") from error
        if (
            size != len(payload)
            or response.get("mimeType") != mime_type
            or str(response.get("md5Checksum") or "").casefold() != expected_md5
        ):
            file_id = valid_drive_id(response.get("id"))
            if not file_id:
                raise ManifestBuilderError("generated_output_verification_failed")
            response = self.get_metadata(file_id)
            try:
                size = int(response.get("size") or -1)
            except (TypeError, ValueError) as error:
                raise ManifestBuilderError("generated_output_verification_failed") from error
            if (
                size != len(payload)
                or response.get("mimeType") != mime_type
                or str(response.get("md5Checksum") or "").casefold() != expected_md5
            ):
                raise ManifestBuilderError("generated_output_verification_failed")
        file_id = valid_drive_id(response.get("id"))
        if not file_id:
            raise ManifestBuilderError("generated_output_verification_failed")
        return file_id


def read_authoritative_product_seeds(
    drive: ManifestDriveClient, config: BuilderConfig
) -> tuple[list[ProductSeed], Counter[str]]:
    """Read both pinned ledgers in memory and return their exact seed union."""

    dream_metadata = drive.get_metadata(config.dreamstime_ledger_zip_file_id)
    if (
        dream_metadata.get("trashed")
        or normalize_text(dream_metadata.get("name")) != config.dreamstime_ledger_zip_name
        or normalize_text(dream_metadata.get("mimeType")) not in ZIP_MIMES
    ):
        raise ManifestBuilderError("dreamstime_ledger_drive_file_invalid")
    try:
        dream_size = int(dream_metadata.get("size") or 0)
    except (TypeError, ValueError) as error:
        raise ManifestBuilderError("dreamstime_ledger_zip_size_invalid") from error
    if dream_size < 1 or dream_size > MAX_LEDGER_ZIP_BYTES:
        raise ManifestBuilderError("dreamstime_ledger_zip_size_invalid")
    dream_zip, _ = drive.download_bytes(
        config.dreamstime_ledger_zip_file_id, MAX_LEDGER_ZIP_BYTES
    )
    dream_jsonl = extract_dreamstime_ledger_member(
        dream_zip,
        expected_zip_sha256=config.dreamstime_ledger_zip_sha256,
        expected_member_sha256=config.dreamstime_ledger_member_sha256,
    )

    ai_metadata = drive.get_metadata(config.ai_png_jsonl_file_id)
    if (
        ai_metadata.get("trashed")
        or normalize_text(ai_metadata.get("name")) != config.ai_png_jsonl_name
    ):
        raise ManifestBuilderError("ai_png_ledger_drive_file_invalid")
    try:
        ai_size = int(ai_metadata.get("size") or 0)
    except (TypeError, ValueError) as error:
        raise ManifestBuilderError("ai_png_ledger_size_invalid") from error
    if ai_size < 1 or ai_size > MAX_LEDGER_MEMBER_BYTES:
        raise ManifestBuilderError("ai_png_ledger_size_invalid")
    ai_jsonl, _ = drive.download_bytes(config.ai_png_jsonl_file_id, MAX_LEDGER_MEMBER_BYTES)
    if len(ai_jsonl) != ai_size:
        raise ManifestBuilderError("ai_png_ledger_size_mismatch")
    if hashlib.sha256(ai_jsonl).hexdigest() != config.ai_png_jsonl_sha256:
        raise ManifestBuilderError("ai_png_ledger_hash_mismatch")

    dream_seeds, dream_stats = parse_dreamstime_product_seeds(
        dream_jsonl, config.source_namespace
    )
    ai_seeds, ai_stats = parse_ai_png_product_seeds(
        ai_jsonl, config.source_namespace, source_label=PNG_LEDGER_SOURCE_LABEL
    )
    seeds, merge_stats = merge_authoritative_product_seeds(dream_seeds, ai_seeds)
    stats: Counter[str] = Counter()
    for key, value in dream_stats.items():
        stats[f"dreamstime_{key}"] += value
    for key, value in ai_stats.items():
        stats[f"ai_png_{key}"] += value
    stats.update(merge_stats)
    stats["dreamstime_zip_bytes"] = len(dream_zip)
    stats["dreamstime_member_bytes"] = len(dream_jsonl)
    stats["ai_png_jsonl_bytes"] = len(ai_jsonl)
    return seeds, stats


def _asset_from_metadata(
    metadata: Mapping[str, Any], parent_id: str, root_id: str, cohort: str
) -> DriveAsset | None:
    file_id = valid_drive_id(metadata.get("id"))
    name = normalize_text(metadata.get("name"))
    extension = PurePosixPath(name.replace("\\", "/")).suffix.casefold()
    if not file_id or not name or extension not in ASSET_EXTENSIONS:
        return None
    if metadata.get("trashed"):
        raise ManifestBuilderError("asset_trashed")
    if "/" in name or "\\" in name:
        raise ManifestBuilderError("asset_name_invalid")
    mime_type = normalize_text(metadata.get("mimeType"))
    permitted_mimes = {
        ".png": {pipeline.PNG_MIME},
        ".svg": set(pipeline.SVG_MIMES),
        ".zip": ZIP_MIMES,
    }[extension]
    if mime_type not in permitted_mimes:
        raise ManifestBuilderError("asset_mime_invalid")
    try:
        size = int(metadata.get("size") or 0)
    except (TypeError, ValueError):
        raise ManifestBuilderError("asset_size_invalid")
    maximum = {
        ".png": pipeline.MAX_SOURCE_BYTES,
        ".svg": pipeline.MAX_SVG_BYTES,
        ".zip": pipeline.MAX_ARCHIVE_BYTES,
    }[extension]
    if size < 1 or size > maximum:
        raise ManifestBuilderError("asset_size_invalid")
    return DriveAsset(
        file_id=file_id,
        name=name,
        mime_type=mime_type,
        size_bytes=size,
        parent_id=parent_id,
        root_id=root_id,
        cohort=cohort,
        md5_checksum=normalize_text(metadata.get("md5Checksum")).casefold(),
        sha256_checksum=normalize_text(metadata.get("sha256Checksum")).casefold(),
        version=normalize_text(metadata.get("version")),
    )


def scan_drive_root(
    drive: ManifestDriveClient,
    root_id: str,
    cohort: str,
    *,
    excluded_folder_ids: frozenset[str] = frozenset(),
) -> tuple[list[DriveAsset], Counter[str]]:
    root = drive.get_metadata(root_id)
    if root.get("trashed") or root.get("mimeType") != FOLDER_MIME:
        raise ManifestBuilderError("source_root_invalid")
    assets: list[DriveAsset] = []
    stats: Counter[str] = Counter()
    frontier: list[tuple[str, int]] = [(root_id, 0)]
    scheduled_folders: set[str] = {root_id}
    seen_folders: set[str] = set()
    seen_files: set[str] = set()
    while frontier:
        current_frontier: list[tuple[str, int]] = []
        for folder_id, depth in frontier:
            if folder_id in seen_folders:
                continue
            if depth > MAX_DRIVE_DEPTH:
                raise ManifestBuilderError("source_tree_depth_limit")
            seen_folders.add(folder_id)
            if len(seen_folders) > MAX_INVENTORY_FOLDERS:
                raise ManifestBuilderError("source_folder_limit")
            current_frontier.append((folder_id, depth))

        next_frontier: list[tuple[str, int]] = []
        for batch in _frontier_batches(current_frontier):
            children_by_parent = drive.list_children_many(
                [folder_id for folder_id, _ in batch]
            )
            for folder_id, depth in batch:
                children = sorted(
                    children_by_parent.get(folder_id, []),
                    key=lambda item: (
                        str(item.get("mimeType") or ""),
                        str(item.get("name") or "").casefold(),
                        str(item.get("id") or ""),
                    ),
                )
                for child in children:
                    child_id = valid_drive_id(child.get("id"))
                    if not child_id:
                        stats["ignored_invalid_drive_id"] += 1
                        continue
                    if child.get("mimeType") == FOLDER_MIME:
                        if child_id in excluded_folder_ids:
                            stats["excluded_nested_roots"] += 1
                        elif child_id not in scheduled_folders:
                            scheduled_folders.add(child_id)
                            next_frontier.append((child_id, depth + 1))
                        continue
                    if child.get("mimeType") == SHORTCUT_MIME:
                        stats["blocking_shortcuts_encountered"] += 1
                        continue
                    if child_id in seen_files:
                        stats["duplicate_file_references"] += 1
                        continue
                    seen_files.add(child_id)
                    if len(seen_files) > MAX_INVENTORY_ITEMS:
                        raise ManifestBuilderError("source_item_limit")
                    try:
                        asset = _asset_from_metadata(child, folder_id, root_id, cohort)
                    except ManifestBuilderError as error:
                        stats[f"blocking_{error.code}"] += 1
                        continue
                    if asset:
                        assets.append(asset)
                        stats[f"{asset.extension.lstrip('.')}_files"] += 1
                    else:
                        stats["ignored_non_product_files"] += 1
        frontier = next_frontier
    stats["folders_scanned"] = len(seen_folders)
    stats["items_seen"] = len(seen_files)
    return assets, stats


def _incremental_sidecar_row(payload: bytes) -> dict[str, str]:
    """Parse one bounded, explicit product-sidecar row without fuzzy aliases."""

    if not payload or len(payload) > MAX_INCREMENTAL_SIDECAR_BYTES:
        raise ManifestBuilderError("incremental_sidecar_size_invalid")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeError as error:
        raise ManifestBuilderError("incremental_sidecar_encoding_invalid") from error
    if "\x00" in text:
        raise ManifestBuilderError("incremental_sidecar_text_invalid")
    try:
        reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
        raw_headers = reader.fieldnames
        if not raw_headers or len(raw_headers) > 128:
            raise ManifestBuilderError("incremental_sidecar_headers_invalid")
        headers = [normalize_text(value).casefold() for value in raw_headers]
        if any(not value for value in headers) or len(set(headers)) != len(headers):
            raise ManifestBuilderError("incremental_sidecar_headers_invalid")
        required_columns = (
            INCREMENTAL_AUTO_APPROVE_REQUIRED_COLUMNS
            if _png_only_auto_approve()
            else INCREMENTAL_SIDECAR_REQUIRED_COLUMNS
        )
        if not required_columns.issubset(headers):
            raise ManifestBuilderError("incremental_sidecar_contract_columns_missing")
        rows: list[dict[str, str]] = []
        for raw_row in reader:
            if None in raw_row:
                raise ManifestBuilderError("incremental_sidecar_columns_ambiguous")
            row: dict[str, str] = {}
            for index, raw_header in enumerate(raw_headers):
                value = unicodedata.normalize(
                    "NFKC", str(raw_row.get(raw_header) or "")
                ).strip()
                if any(
                    (ord(character) < 32 and character not in "\r\n\t")
                    or ord(character) == 127
                    for character in value
                ):
                    raise ManifestBuilderError("incremental_sidecar_text_invalid")
                row[headers[index]] = value
            if not any(row.values()):
                continue
            rows.append(row)
            if len(rows) > 1:
                raise ManifestBuilderError("incremental_sidecar_row_count")
    except csv.Error as error:
        raise ManifestBuilderError("incremental_sidecar_csv_invalid") from error
    if len(rows) != 1:
        raise ManifestBuilderError("incremental_sidecar_row_count")
    return rows[0]


def sanitize_incremental_etsy_description(value: str) -> str:
    """Remove only the reviewed non-product disclosures/promotional suffix."""

    text = unicodedata.normalize("NFKC", str(value or "")).replace("\r\n", "\n").replace(
        "\r", "\n"
    )
    lines = text.split("\n")
    boundary = len(lines)
    for index, line in enumerate(lines):
        normalized = normalize_text(line).casefold()
        if normalized in {"ai disclosure", "etsy purchase protection"} or normalized.startswith(
            "explore our entire shop mega bundle"
        ):
            boundary = index
            break
    core = "\n".join(lines[:boundary])
    core = re.sub(
        r"(?is)(?<!\w)The composition is AI-assisted\b[^.]*\.",
        " ",
        core,
    )
    return normalize_text(core)


def _validate_incremental_sidecar_drive_id(
    row: Mapping[str, str], column: str, expected_id: str | None = None
) -> None:
    raw = normalize_text(row.get(column))
    if raw.casefold() in INCREMENTAL_PENDING_DRIVE_VALUES:
        return
    file_id = valid_drive_id(raw)
    if not file_id:
        raise ManifestBuilderError("incremental_sidecar_drive_id_invalid")
    if expected_id is not None and file_id != expected_id:
        raise ManifestBuilderError("incremental_sidecar_drive_id_mismatch")


def parse_incremental_product_sidecar(
    payload: bytes,
    *,
    folder_id: str,
    sidecar_metadata: Mapping[str, Any],
    png_asset: DriveAsset,
    svg_asset: DriveAsset | None,
) -> MetadataEvidence:
    """Validate the reviewed PNG Only sidecar and preserve its Etsy metadata."""

    if normalize_text(sidecar_metadata.get("name")) != pipeline.METADATA_FILENAME:
        raise ManifestBuilderError("incremental_sidecar_name_invalid")
    if normalize_text(sidecar_metadata.get("mimeType")) not in INCREMENTAL_SIDECAR_MIMES:
        raise ManifestBuilderError("incremental_sidecar_mime_invalid")
    parents = tuple(str(value) for value in sidecar_metadata.get("parents", ()))
    if parents != (folder_id,):
        raise ManifestBuilderError("incremental_sidecar_parent_invalid")

    row = _incremental_sidecar_row(payload)
    for optional in ("ownerapproval", "etsystage", "deliverykind"):
        row.setdefault(optional, "")
    approval = normalize_text(row["ownerapproval"]).casefold()
    # Owner policy (2026-10-14): every design uploaded to PNG Only should go
    # live automatically. With HEEEXY_PNG_ONLY_AUTO_APPROVE=true an empty or
    # pending approval/stage counts as approved; titles, descriptions, tags and
    # exact file checks below still apply.
    auto_approve = _png_only_auto_approve()
    approved = approval in {"approved", "owner_approved"} or bool(
        INCREMENTAL_OWNER_APPROVAL_RE.fullmatch(approval)
    )
    if not approved and not (auto_approve and not INCREMENTAL_REFUSAL_RE.search(approval)):
        raise ManifestBuilderError("incremental_sidecar_owner_approval_invalid")
    delivery_kind = normalize_text(row["deliverykind"]).casefold()
    if delivery_kind != "transparent_png" and not (auto_approve and delivery_kind in {"", "png"}):
        raise ManifestBuilderError("incremental_sidecar_delivery_kind_invalid")
    etsy_stage = normalize_text(row["etsystage"]).casefold()
    if etsy_stage != "active_verified" and not (
        auto_approve and not INCREMENTAL_REFUSAL_RE.search(etsy_stage)
    ):
        raise ManifestBuilderError("incremental_sidecar_etsy_stage_invalid")
    delivery_name = normalize_text(row["deliveryfile"])
    if (
        not delivery_name
        or PurePosixPath(delivery_name.replace("\\", "/")).name != delivery_name
        or delivery_name != png_asset.name
    ):
        raise ManifestBuilderError("incremental_sidecar_delivery_file_not_exact")

    _validate_incremental_sidecar_drive_id(
        row, "driveproductfolderid", folder_id
    )
    _validate_incremental_sidecar_drive_id(row, "drivepngfileid", png_asset.file_id)
    _validate_incremental_sidecar_drive_id(row, "drivedestinationfolderid")

    declared_bytes = row.get("png_bytes", "")
    if declared_bytes:
        try:
            expected_bytes = int(declared_bytes)
        except ValueError as error:
            raise ManifestBuilderError("incremental_sidecar_png_bytes_invalid") from error
        if expected_bytes != png_asset.size_bytes:
            raise ManifestBuilderError("incremental_sidecar_png_bytes_mismatch")
    declared_sha256 = normalize_text(row.get("png_sha256")).casefold()
    if declared_sha256:
        if not valid_sha256(declared_sha256):
            raise ManifestBuilderError("incremental_sidecar_png_sha256_invalid")
        if png_asset.sha256_checksum and declared_sha256 != png_asset.sha256_checksum:
            raise ManifestBuilderError("incremental_sidecar_png_sha256_mismatch")

    dimensions: list[int] = []
    for column in ("width", "height"):
        raw = row.get(column, "")
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError as error:
            raise ManifestBuilderError("incremental_sidecar_dimensions_invalid") from error
        if value < 1 or value > 20_000:
            raise ManifestBuilderError("incremental_sidecar_dimensions_invalid")
        dimensions.append(value)
    if len(dimensions) == 2 and dimensions[0] * dimensions[1] > pipeline.MAX_IMAGE_PIXELS:
        raise ManifestBuilderError("incremental_sidecar_dimensions_invalid")
    alpha_values: list[int] = []
    for column in ("alphamin", "alphamax"):
        raw = row.get(column, "")
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError as error:
            raise ManifestBuilderError("incremental_sidecar_alpha_invalid") from error
        if value < 0 or value > 255:
            raise ManifestBuilderError("incremental_sidecar_alpha_invalid")
        alpha_values.append(value)
    if len(alpha_values) == 2 and alpha_values[0] > alpha_values[1]:
        raise ManifestBuilderError("incremental_sidecar_alpha_invalid")

    title = normalize_text(row["etsytitle"])
    description = sanitize_incremental_etsy_description(row["etsydescription"])
    category = normalize_text(row.get("topic"))
    tags = split_terms(row["etsytags"])
    if _seed_metadata_contract_reasons(title, description, category, tags):
        raise ManifestBuilderError("incremental_sidecar_metadata_contract_invalid")
    sidecar_id = valid_drive_id(sidecar_metadata.get("id"))
    if not sidecar_id:
        raise ManifestBuilderError("incremental_sidecar_id_invalid")
    return MetadataEvidence(
        source_label=pipeline.METADATA_FILENAME,
        section=f"exact-parent-{stable_digest(sidecar_id)[:16]}",
        row_number=2,
        title=title,
        description=description,
        categories=((category,) if category else ()),
        tags=tags,
        drive_ids=(png_asset.file_id,)
        + ((svg_asset.file_id,) if svg_asset else ()),
        source_names=(delivery_name,),
    )


def _incremental_seed_for_folder(
    drive: ManifestDriveClient,
    *,
    folder_id: str,
    direct_assets: Sequence[DriveAsset],
    sidecars: Sequence[Mapping[str, Any]],
    pinned_source_ids: frozenset[str],
    source_namespace: str,
    sidecar_bytes_already_read: int,
) -> tuple[ProductSeed | None, Counter[str], int]:
    """Create at most one exact-parent seed, or one explicit held disposition."""

    stats: Counter[str] = Counter()
    product_assets = tuple(
        sorted(
            (asset for asset in direct_assets if asset.extension in {".png", ".svg"}),
            key=lambda item: (item.extension, item.file_id),
        )
    )
    source_ids = {asset.file_id for asset in product_assets}
    if source_ids and source_ids <= pinned_source_ids:
        stats["historical_pinned_folders_ignored"] += 1
        return None, stats, 0
    if source_ids & pinned_source_ids:
        stats["blocking_mixed_pinned_and_incremental_folder"] += 1
        return None, stats, 0

    pngs = tuple(asset for asset in product_assets if asset.extension == ".png")
    svgs = tuple(asset for asset in product_assets if asset.extension == ".svg")
    if not sidecars and not pngs:
        return None, stats, 0
    stats["product_folders_considered"] += 1
    reasons: list[str] = []
    if not pngs:
        reasons.append("incremental_png_missing")
    elif len(pngs) > 1:
        reasons.append("incremental_png_identity_ambiguous")
    if len(svgs) > 1:
        reasons.append("incremental_svg_identity_ambiguous")
    if len(pngs) == 1 and len(svgs) == 1 and pngs[0].basename_normalized != svgs[0].basename_normalized:
        reasons.append("incremental_svg_png_pair_mismatch")
    if not sidecars:
        reasons.append("incremental_sidecar_missing")
    elif len(sidecars) > 1:
        reasons.append("incremental_sidecar_duplicate")

    evidence: tuple[MetadataEvidence, ...] = ()
    bytes_read = 0
    if len(sidecars) == 1 and len(pngs) == 1 and len(svgs) <= 1:
        sidecar = sidecars[0]
        try:
            declared_size = int(sidecar.get("size") or 0)
        except (TypeError, ValueError):
            declared_size = 0
        if declared_size < 1 or declared_size > MAX_INCREMENTAL_SIDECAR_BYTES:
            reasons.append("incremental_sidecar_size_invalid")
        elif sidecar_bytes_already_read + declared_size > MAX_INCREMENTAL_SIDECAR_TOTAL_BYTES:
            raise ManifestBuilderError("incremental_sidecar_total_size_limit")
        else:
            sidecar_id = valid_drive_id(sidecar.get("id"))
            if not sidecar_id:
                reasons.append("incremental_sidecar_id_invalid")
            else:
                try:
                    payload, refreshed = drive.download_bytes(
                        sidecar_id,
                        MAX_INCREMENTAL_SIDECAR_BYTES,
                        candidate_scoped=True,
                    )
                    bytes_read = len(payload)
                    if (
                        sidecar_bytes_already_read + bytes_read
                        > MAX_INCREMENTAL_SIDECAR_TOTAL_BYTES
                    ):
                        raise ManifestBuilderError(
                            "incremental_sidecar_total_size_limit"
                        )
                    if (
                        valid_drive_id(refreshed.get("id")) != sidecar_id
                        or normalize_text(refreshed.get("name"))
                        != normalize_text(sidecar.get("name"))
                        or tuple(str(value) for value in refreshed.get("parents", ()))
                        != (folder_id,)
                    ):
                        raise ManifestBuilderError("incremental_sidecar_changed_during_read")
                    evidence = (
                        parse_incremental_product_sidecar(
                            payload,
                            folder_id=folder_id,
                            sidecar_metadata=refreshed,
                            png_asset=pngs[0],
                            svg_asset=(svgs[0] if svgs else None),
                        ),
                    )
                except ManifestBuilderError as error:
                    if error.code == "incremental_sidecar_total_size_limit":
                        raise
                    reasons.append(error.code)

    seed_key = (
        f"drive:{pngs[0].file_id}"
        if len(pngs) == 1
        else f"drive-folder:{folder_id}"
    )
    source_refs = tuple(item.source_ref for item in evidence)
    seed = ProductSeed(
        identity=_seed_identity(source_namespace, seed_key),
        seed_key=seed_key,
        png_file_ids=tuple(item.file_id for item in pngs),
        svg_file_ids=tuple(item.file_id for item in svgs),
        metadata_evidence=evidence,
        source_refs=source_refs,
        hold_reasons=tuple(sorted(set(reasons))),
    )
    if seed.hold_reasons:
        stats["held_product_folders"] += 1
        for reason in seed.hold_reasons:
            stats[f"held_{reason}"] += 1
    else:
        stats["accepted_product_folders"] += 1
    return seed, stats, bytes_read


def scan_png_only_root_and_discover_incremental_seeds(
    drive: ManifestDriveClient,
    root_id: str,
    *,
    pinned_source_ids: frozenset[str],
    source_namespace: str,
) -> tuple[list[DriveAsset], list[ProductSeed], Counter[str]]:
    """Inventory PNG Only recursively, but discover products at one safe boundary.

    The recursive walk is a coverage input only.  A folder may become an
    authoritative incremental seed only when it is a direct child of the PNG
    Only root and carries an explicit canonical sidecar.  Historical nested
    layouts, folders without sidecars, and folders with ambiguous product
    assets remain visible as diagnostics without entering the authoritative
    seed set or its unresolved-hold gate.
    """

    root = drive.get_metadata(root_id)
    if root.get("trashed") or root.get("mimeType") != FOLDER_MIME:
        raise ManifestBuilderError("source_root_invalid")
    assets: list[DriveAsset] = []
    seeds: list[ProductSeed] = []
    stats: Counter[str] = Counter()
    frontier: list[tuple[str, int]] = [(root_id, 0)]
    scheduled_folders: set[str] = {root_id}
    seen_folders: set[str] = set()
    seen_files: set[str] = set()
    sidecar_bytes_read = 0
    while frontier:
        current_frontier: list[tuple[str, int]] = []
        for folder_id, depth in frontier:
            if folder_id in seen_folders:
                continue
            if depth > MAX_DRIVE_DEPTH:
                raise ManifestBuilderError("source_tree_depth_limit")
            seen_folders.add(folder_id)
            if len(seen_folders) > MAX_INVENTORY_FOLDERS:
                raise ManifestBuilderError("source_folder_limit")
            current_frontier.append((folder_id, depth))

        next_frontier: list[tuple[str, int]] = []
        for batch in _frontier_batches(current_frontier):
            children_by_parent = drive.list_children_many(
                [folder_id for folder_id, _ in batch]
            )
            for folder_id, depth in batch:
                direct_assets: list[DriveAsset] = []
                sidecars: list[Mapping[str, Any]] = []
                children = sorted(
                    children_by_parent.get(folder_id, []),
                    key=lambda item: (
                        str(item.get("mimeType") or ""),
                        str(item.get("name") or "").casefold(),
                        str(item.get("id") or ""),
                    ),
                )
                for child in children:
                    child_id = valid_drive_id(child.get("id"))
                    if not child_id:
                        stats["ignored_invalid_drive_id"] += 1
                        continue
                    if child.get("mimeType") == FOLDER_MIME:
                        if child_id not in scheduled_folders:
                            scheduled_folders.add(child_id)
                            next_frontier.append((child_id, depth + 1))
                        continue
                    if child.get("mimeType") == SHORTCUT_MIME:
                        stats["blocking_shortcuts_encountered"] += 1
                        continue
                    if child_id in seen_files:
                        stats["duplicate_file_references"] += 1
                        continue
                    seen_files.add(child_id)
                    if len(seen_files) > MAX_INVENTORY_ITEMS:
                        raise ManifestBuilderError("source_item_limit")
                    child_name = normalize_text(child.get("name"))
                    if child_name.casefold() == pipeline.METADATA_FILENAME.casefold():
                        sidecars.append(child)
                        stats["sidecars_discovered"] += 1
                        continue
                    try:
                        item = _asset_from_metadata(
                            child, folder_id, root_id, "png_only"
                        )
                    except ManifestBuilderError as error:
                        stats[f"blocking_{error.code}"] += 1
                        continue
                    if item:
                        assets.append(item)
                        direct_assets.append(item)
                        stats[f"{item.extension.lstrip('.')}_files"] += 1
                    else:
                        stats["ignored_non_product_files"] += 1

                product_assets = tuple(
                    asset
                    for asset in direct_assets
                    if asset.extension in {".png", ".svg"}
                )
                if depth != 1:
                    if product_assets or sidecars:
                        stats["coverage_non_direct_candidate_folders"] += 1
                        stats["coverage_non_direct_product_assets"] += len(
                            product_assets
                        )
                        stats["coverage_non_direct_sidecars"] += len(sidecars)
                        if depth > 1:
                            stats["coverage_nested_candidate_folders"] += 1
                    continue

                stats["direct_child_folders_scanned"] += 1
                if not product_assets and not sidecars:
                    continue
                source_ids = {asset.file_id for asset in product_assets}
                if source_ids and source_ids <= pinned_source_ids:
                    stats["historical_pinned_folders_ignored"] += 1
                    continue

                pngs = tuple(
                    asset for asset in product_assets if asset.extension == ".png"
                )
                svgs = tuple(
                    asset for asset in product_assets if asset.extension == ".svg"
                )
                quarantine_reasons: list[str] = []
                if not sidecars:
                    quarantine_reasons.append("incremental_sidecar_missing")
                if len(pngs) > 1:
                    quarantine_reasons.append("incremental_png_identity_ambiguous")
                if len(svgs) > 1:
                    quarantine_reasons.append("incremental_svg_identity_ambiguous")
                if (
                    len(pngs) == 1
                    and len(svgs) == 1
                    and pngs[0].basename_normalized != svgs[0].basename_normalized
                ):
                    quarantine_reasons.append("incremental_svg_png_pair_mismatch")
                if quarantine_reasons:
                    stats["quarantined_direct_candidate_folders"] += 1
                    for reason in sorted(set(quarantine_reasons)):
                        stats[f"quarantine_{reason}"] += 1
                    continue

                seed, folder_stats, read_count = _incremental_seed_for_folder(
                    drive,
                    folder_id=folder_id,
                    direct_assets=direct_assets,
                    sidecars=sidecars,
                    pinned_source_ids=pinned_source_ids,
                    source_namespace=source_namespace,
                    sidecar_bytes_already_read=sidecar_bytes_read,
                )
                stats.update(folder_stats)
                sidecar_bytes_read += read_count
                if seed:
                    seeds.append(seed)
        frontier = next_frontier

    identities = [seed.identity for seed in seeds]
    if len(set(identities)) != len(identities):
        raise ManifestBuilderError("incremental_seed_identity_conflict")
    source_owners: dict[str, str] = {}
    for seed in seeds:
        for source_id in seed.source_file_ids:
            owner = source_owners.setdefault(source_id, seed.identity)
            if owner != seed.identity:
                raise ManifestBuilderError("incremental_seed_source_conflict")
    stats["folders_scanned"] = len(seen_folders)
    stats["items_seen"] = len(seen_files)
    stats["sidecar_bytes_read"] = sidecar_bytes_read
    stats["incremental_seeds"] = len(seeds)
    return (
        sorted(assets, key=lambda item: item.file_id),
        sorted(seeds, key=lambda item: item.identity),
        stats,
    )


def merge_pinned_and_incremental_product_seeds(
    pinned_seeds: Sequence[ProductSeed], incremental_seeds: Sequence[ProductSeed]
) -> list[ProductSeed]:
    """Union independently reviewed seed families without fuzzy deduplication."""

    pinned_identities = {seed.identity for seed in pinned_seeds}
    pinned_source_ids = {
        source_id for seed in pinned_seeds for source_id in seed.source_file_ids
    }
    for seed in incremental_seeds:
        if seed.identity in pinned_identities:
            raise ManifestBuilderError("incremental_identity_overlaps_pinned_seed")
        if pinned_source_ids.intersection(seed.source_file_ids):
            raise ManifestBuilderError("incremental_source_overlaps_pinned_seed")
    union = sorted([*pinned_seeds, *incremental_seeds], key=lambda item: item.identity)
    if len({seed.identity for seed in union}) != len(union):
        raise ManifestBuilderError("combined_seed_identity_conflict")
    return union


def read_metadata_imports(
    drive: ManifestDriveClient, folder_id: str
) -> tuple[list[MetadataEvidence], Counter[str]]:
    folder = drive.get_metadata(folder_id)
    if folder.get("trashed") or folder.get("mimeType") != FOLDER_MIME:
        raise ManifestBuilderError("metadata_import_folder_invalid")
    frontier: list[tuple[str, int]] = [(folder_id, 0)]
    scheduled_folders: set[str] = {folder_id}
    seen_folders: set[str] = set()
    metadata_files: list[Mapping[str, Any]] = []
    stats: Counter[str] = Counter()
    items_seen = 0
    while frontier:
        current_frontier: list[tuple[str, int]] = []
        for current, depth in frontier:
            if current in seen_folders:
                continue
            if depth > MAX_DRIVE_DEPTH:
                raise ManifestBuilderError("metadata_tree_depth_limit")
            seen_folders.add(current)
            if len(seen_folders) > MAX_INVENTORY_FOLDERS:
                raise ManifestBuilderError("metadata_folder_limit")
            current_frontier.append((current, depth))

        next_frontier: list[tuple[str, int]] = []
        for batch in _frontier_batches(current_frontier):
            children_by_parent = drive.list_children_many(
                [current for current, _ in batch]
            )
            for current, depth in batch:
                for item in children_by_parent.get(current, []):
                    if item.get("mimeType") == FOLDER_MIME:
                        item_id = valid_drive_id(item.get("id"))
                        if item_id and item_id not in scheduled_folders:
                            scheduled_folders.add(item_id)
                            next_frontier.append((item_id, depth + 1))
                        continue
                    items_seen += 1
                    if items_seen > MAX_INVENTORY_ITEMS:
                        raise ManifestBuilderError("metadata_item_limit")
                    if item.get("mimeType") == SHORTCUT_MIME:
                        stats["held_metadata_file_shortcut"] += 1
                        continue
                    name = normalize_text(item.get("name"))
                    if PurePosixPath(name).suffix.casefold() in METADATA_EXTENSIONS:
                        metadata_files.append(item)
        frontier = next_frontier
    metadata_files.sort(key=lambda item: (str(item.get("name") or "").casefold(), str(item.get("id") or "")))
    total_bytes = 0
    evidence: list[MetadataEvidence] = []
    for metadata in metadata_files:
        file_id = valid_drive_id(metadata.get("id"))
        name = normalize_text(metadata.get("name"))
        if not file_id:
            stats["held_invalid_metadata_file_id"] += 1
            continue
        try:
            size = int(metadata.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        if size < 1 or size > MAX_METADATA_FILE_BYTES:
            stats["held_metadata_file_size"] += 1
            continue
        total_bytes += size
        if total_bytes > MAX_METADATA_TOTAL_BYTES:
            raise ManifestBuilderError("metadata_total_size_limit")
        payload, _ = drive.download_bytes(file_id, MAX_METADATA_FILE_BYTES)
        try:
            parsed = parse_metadata_file(name, payload)
        except ManifestBuilderError as error:
            stats[f"held_metadata_file_{error.code}"] += 1
            continue
        evidence.extend(parsed)
        stats["files_read"] += 1
        stats[f"{PurePosixPath(name).suffix.casefold().lstrip('.')}_files_read"] += 1
        stats["rows_parsed"] += len(parsed)
        if len(evidence) > MAX_METADATA_ROWS:
            raise ManifestBuilderError("metadata_row_limit")
    stats["files_discovered"] = len(metadata_files)
    stats["bytes_read"] = total_bytes
    stats["folders_scanned"] = len(seen_folders)
    stats["items_seen"] = items_seen
    return evidence, stats


def _required_drive_id(environment: Mapping[str, str], key: str, code: str) -> str:
    value = valid_drive_id(environment.get(key))
    if not value:
        raise ManifestBuilderError(code)
    return value


def _required_positive_integer(environment: Mapping[str, str], key: str, code: str) -> int:
    try:
        value = int(str(environment.get(key) or ""))
    except ValueError as error:
        raise ManifestBuilderError(code) from error
    if value < 1 or value > MAX_INVENTORY_ITEMS:
        raise ManifestBuilderError(code)
    return value


def _required_sha256(environment: Mapping[str, str], key: str, code: str) -> str:
    value = valid_sha256(environment.get(key))
    if not value:
        raise ManifestBuilderError(code)
    return value


def _required_filename(
    environment: Mapping[str, str], key: str, suffix: str, code: str
) -> str:
    value = normalize_text(environment.get(key))
    path = PurePosixPath(value.replace("\\", "/"))
    if (
        not value
        or len(value) > 255
        or path.name != value
        or path.suffix.casefold() != suffix
    ):
        raise ManifestBuilderError(code)
    return value


def load_builder_config(environment: Mapping[str, str]) -> tuple[BuilderConfig, tuple[str, str, str]]:
    config = BuilderConfig(
        metadata_import_folder_id=_required_drive_id(
            environment, "HEEEXY_METADATA_IMPORT_FOLDER_ID", "metadata_import_folder_id_invalid"
        ),
        catalog_metadata_folder_id=_required_drive_id(
            environment, "HEEEXY_CATALOG_METADATA_FOLDER_ID", "catalog_metadata_folder_id_invalid"
        ),
        catalog_manifest_drive_file_id=_required_drive_id(
            environment,
            "HEEEXY_CATALOG_MANIFEST_DRIVE_FILE_ID",
            "catalog_manifest_drive_file_id_invalid",
        ),
        reports_folder_id=_required_drive_id(
            environment, "HEEEXY_REPORTS_FOLDER_ID", "reports_folder_id_invalid"
        ),
        f16_root_id=_required_drive_id(
            environment, "HEEEXY_F16_ROOT_FOLDER_ID", "f16_root_id_invalid"
        ),
        png_only_root_id=_required_drive_id(
            environment, "HEEEXY_PNG_ONLY_ROOT_FOLDER_ID", "png_only_root_id_invalid"
        ),
        expected_min_products=_required_positive_integer(
            environment, "HEEEXY_EXPECTED_MIN_PRODUCT_COUNT", "expected_min_product_count_invalid"
        ),
        expected_min_accepted=_required_positive_integer(
            environment, "HEEEXY_EXPECTED_MIN_ACCEPTED_COUNT", "expected_min_accepted_count_invalid"
        ),
        source_namespace=normalize_text(environment.get("HEEEXY_CATALOG_SOURCE_NAMESPACE"))
        or "heeexy-store",
        dreamstime_ledger_zip_file_id=_required_drive_id(
            environment,
            "HEEEXY_DREAMSTIME_LEDGER_ZIP_DRIVE_FILE_ID",
            "dreamstime_ledger_zip_file_id_invalid",
        ),
        dreamstime_ledger_zip_name=_required_filename(
            environment,
            "HEEEXY_DREAMSTIME_LEDGER_ZIP_NAME",
            ".zip",
            "dreamstime_ledger_zip_name_invalid",
        ),
        dreamstime_ledger_zip_sha256=_required_sha256(
            environment,
            "HEEEXY_DREAMSTIME_LEDGER_ZIP_SHA256",
            "dreamstime_ledger_zip_sha256_invalid",
        ),
        dreamstime_ledger_member_sha256=_required_sha256(
            environment,
            "HEEEXY_DREAMSTIME_LEDGER_MEMBER_SHA256",
            "dreamstime_ledger_member_sha256_invalid",
        ),
        ai_png_jsonl_file_id=_required_drive_id(
            environment,
            "HEEEXY_AI_PNG_METADATA_JSONL_DRIVE_FILE_ID",
            "ai_png_jsonl_file_id_invalid",
        ),
        ai_png_jsonl_name=_required_filename(
            environment,
            "HEEEXY_AI_PNG_METADATA_JSONL_NAME",
            ".jsonl",
            "ai_png_jsonl_name_invalid",
        ),
        ai_png_jsonl_sha256=_required_sha256(
            environment,
            "HEEEXY_AI_PNG_METADATA_JSONL_SHA256",
            "ai_png_jsonl_sha256_invalid",
        ),
    )
    if config.f16_root_id == config.png_only_root_id:
        raise ManifestBuilderError("source_roots_not_distinct")
    if config.dreamstime_ledger_zip_file_id == config.ai_png_jsonl_file_id:
        raise ManifestBuilderError("authoritative_ledger_files_not_distinct")
    if not re.fullmatch(r"[a-z][a-z0-9._-]{1,63}", config.source_namespace):
        raise ManifestBuilderError("source_namespace_invalid")
    oauth = (
        normalize_text(environment.get("GOOGLE_DRIVE_OAUTH_CLIENT_ID")),
        normalize_text(environment.get("GOOGLE_DRIVE_OAUTH_CLIENT_SECRET")),
        normalize_text(environment.get("GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN")),
    )
    if any(not value for value in oauth):
        raise ManifestBuilderError("drive_oauth_not_configured")
    return config, oauth


def run_builder(config: BuilderConfig, oauth: tuple[str, str, str]) -> tuple[Counter[str], str]:
    drive = ManifestDriveClient(*oauth)
    # Only generated-output folders must be private. The import and artwork
    # folders are read-only inputs and may legitimately inherit a link/domain
    # permission from the user's existing Drive organization.
    drive.assert_private_writable_folder(config.catalog_metadata_folder_id)
    drive.assert_private_writable_folder(config.reports_folder_id)

    pinned_seeds, metadata_stats = read_authoritative_product_seeds(drive, config)
    pinned_source_ids = frozenset(
        source_id
        for seed in pinned_seeds
        for source_id in seed.source_file_ids
    )
    f16_assets, f16_stats = scan_drive_root(
        drive,
        config.f16_root_id,
        "f16",
        excluded_folder_ids=frozenset({config.png_only_root_id}),
    )
    (
        png_only_assets,
        incremental_seeds,
        png_only_stats,
    ) = scan_png_only_root_and_discover_incremental_seeds(
        drive,
        config.png_only_root_id,
        pinned_source_ids=pinned_source_ids,
        source_namespace=config.source_namespace,
    )
    seeds = merge_pinned_and_incremental_product_seeds(
        pinned_seeds, incremental_seeds
    )
    metadata_stats["incremental_sidecar_seeds"] = len(incremental_seeds)
    metadata_stats["incremental_sidecar_accepted"] = sum(
        not seed.hold_reasons for seed in incremental_seeds
    )
    metadata_stats["incremental_sidecar_held"] = sum(
        bool(seed.hold_reasons) for seed in incremental_seeds
    )
    assets = sorted(f16_assets + png_only_assets, key=lambda item: item.file_id)
    reconciliation, inventory_stats = reconcile_seed_catalog(
        seeds, assets, config.source_namespace
    )
    inventory_stats["products_discovered"] = len(seeds)
    inventory_stats["global_inventory_assets"] = len(
        {asset.file_id for asset in assets}
    )
    for key, value in f16_stats.items():
        inventory_stats[f"f16_{key}"] += value
    for key, value in png_only_stats.items():
        inventory_stats[f"png_only_{key}"] += value

    manifest_payload = render_manifest_jsonl(reconciliation.records)
    previous_manifest = drive.read_generated_bytes(
        parent_id=config.catalog_metadata_folder_id,
        role=MANIFEST_ROLE,
        maximum_bytes=pipeline.MAX_MANIFEST_BYTES,
        expected_file_id=config.catalog_manifest_drive_file_id,
    )
    if previous_manifest is None:
        raise ManifestBuilderError("generated_output_expected_missing")
    try:
        previous_identities = manifest_product_identities(previous_manifest)
    except ManifestBuilderError:
        inventory_stats["blocking_previous_manifest_invalid"] += 1
    else:
        current_identities = {
            str(record.get("product_identity") or "")
            for record in reconciliation.records
        }
        inventory_stats["previous_manifest_products"] = len(previous_identities)
        inventory_stats["blocking_previous_manifest_missing_products"] = len(
            previous_identities - current_identities
        )
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    report = build_inventory_report(
        inventory_stats=inventory_stats,
        metadata_stats=metadata_stats,
        reconciliation=reconciliation,
        manifest_payload=manifest_payload,
        expected_min_products=config.global_product_target,
        expected_min_accepted=config.curated_seed_target,
        generated_at=generated_at,
    )
    report_payload = (pipeline.canonical_json(report) + "\n").encode("utf-8")
    drive.upsert_generated_bytes(
        parent_id=config.reports_folder_id,
        name=REPORT_FILENAME,
        mime_type=REPORT_MIME,
        role=REPORT_ROLE,
        payload=report_payload,
    )
    if not report["publication_gate"]["eligible"]:
        append_only = os.environ.get("HEEEXY_APPEND_ONLY_PUBLICATION", "").strip().lower() == "true"
        if not append_only:
            raise ManifestBuilderError("manifest_publication_gate_failed")
        manifest_payload, added = append_only_manifest(previous_manifest, reconciliation.records)
        print(f"manifest_append_only new_products={added}", flush=True)
        if added == 0:
            raise ManifestBuilderError("manifest_publication_gate_failed")
    manifest_file_id = drive.upsert_generated_bytes(
        parent_id=config.catalog_metadata_folder_id,
        name=MANIFEST_FILENAME,
        mime_type=MANIFEST_MIME,
        role=MANIFEST_ROLE,
        payload=manifest_payload,
        expected_file_id=config.catalog_manifest_drive_file_id,
    )
    if manifest_file_id != config.catalog_manifest_drive_file_id:
        raise ManifestBuilderError("generated_output_identity_mismatch")
    return (
        Counter(
            inventory_products=len(seeds),
            metadata_rows=int(metadata_stats.get("dreamstime_rows_seen", 0))
            + int(metadata_stats.get("ai_png_rows_seen", 0))
            + len(incremental_seeds),
            accepted=len(reconciliation.records),
            held=len(reconciliation.holds),
            unmatched_metadata_rows=reconciliation.unmatched_metadata_rows,
            ambiguous_metadata_rows=reconciliation.ambiguous_metadata_rows,
        ),
        manifest_file_id,
    )


def parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        # This is deliberately first: local --help must not access Drive or
        # create temp/cache files on the user's machine.
        pipeline.enforce_cloud_runtime()
        parse_arguments(list(argv if argv is not None else sys.argv[1:]))
        config, oauth = load_builder_config(os.environ)
        _, manifest_file_id = run_builder(config, oauth)
        if manifest_file_id != config.catalog_manifest_drive_file_id:
            raise ManifestBuilderError("generated_output_identity_mismatch")
        print("manifest_builder_status=ok", flush=True)
        return 0
    except (ManifestBuilderError, pipeline.PipelineError) as error:
        print(
            f"manifest_builder_status=failed code={getattr(error, 'code', 'builder_failure')}",
            file=sys.stderr,
            flush=True,
        )
        return 2
    except Exception:
        print(
            "manifest_builder_status=failed code=unexpected_failure",
            file=sys.stderr,
            flush=True,
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
