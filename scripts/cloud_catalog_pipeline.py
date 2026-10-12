#!/usr/bin/env python3
r"""Remote-only HEEEXY catalog packager and publisher.

This module is intentionally unusable as a local asset processor.  ``main``
checks for a GitHub-hosted Actions runner before parsing arguments, opening a
Drive connection, or creating a temporary directory.  Unit tests may import
the pure normalization helpers; they never invoke ``main``.

Asset flow on the remote ephemeral runner:

    private Google Drive -> temporary runner directory -> private Drive ZIP
                                               \-----> site ingest -> R2 WebPs

Only the three opaque WebP previews cross the site-ingest boundary.  The source
PNG, optional SVG, and delivery ZIP remain private in Google Drive.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import logging
import multiprocessing
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Iterable, Iterator, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


PIPELINE_STATE_VERSION = 1
CATALOG_SCHEMA_VERSION = 2
ARTIFACT_RECIPE_VERSION = "catalog-artifacts-v3"
PNG_MIME = "image/png"
SVG_MIMES = {"image/svg+xml", "text/xml", "application/xml", "application/octet-stream"}
FOLDER_MIME = "application/vnd.google-apps.folder"
ZIP_MIME = "application/zip"
METADATA_FILENAME = "design_metadata_and_specs.csv"
WHITE = (255, 255, 255)
DARK_BROWN = (42, 36, 32)
CHECKER_LIGHT = (242, 242, 239)
CHECKER_DARK = (204, 204, 198)
CHECKER_SQUARE = 32
MAX_SOURCE_BYTES = 512 * 1024 * 1024
MAX_SVG_BYTES = 128 * 1024 * 1024
MAX_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
MAX_METADATA_BYTES = 2 * 1024 * 1024
MAX_MANIFEST_BYTES = 256 * 1024 * 1024
MAX_IMAGE_PIXELS = 100_000_000
MAX_DELIVERY_IMAGE_PIXELS = 400_000_000
# Only these diagnosed failures get one automatic retry after a code fix.
# A failed retry records this version and is not retried on every drain.
HOLD_REPAIR_VERSIONS = {
    "svg_namespace_not_allowed": "c2pa-inert-metadata-v1",
    "png_decode_failed": "svg-preview-large-original-v1",
    "png_dimensions_invalid": "svg-preview-large-original-v1",
}
MAX_ARCHIVE_ENTRIES = 50_000
MAX_SVG_ELEMENTS = 250_000
MAX_TITLE_CHARS = 256
MAX_DESCRIPTION_CHARS = 4_000
MAX_CATEGORY_CHARS = 100
MAX_TAGS = 50
MAX_TAG_CHARS = 256
PREVIEW_CONTENT_EDGE = 1104
WEBP_QUALITY = 86
DRIVE_RESUMABLE_RETRY_DELAYS = (1, 2, 4, 8)
DRIVE_RECONCILE_DELAYS = (1, 2, 4, 8)
ZIP_TIMESTAMP = (2026, 1, 1, 0, 0, 0)
STATE_ROLE = "catalog_pipeline_state_v1"
STATE_FILE_NAME = "catalog-pipeline-state.json"
MAX_PRODUCT_WORKERS = 4
PRODUCT_FOLDER_ROLE = "catalog_product_folder_v1"
DELIVERY_ROLE = "delivery_zip_v1"
RENDERED_PNG_ROLE = "rendered_png_v1"
EXTRACTED_PNG_ROLE = "extracted_png_v1"
EXTRACTED_SVG_ROLE = "extracted_svg_v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{20,200}$")
PRODUCT_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9:._-]{7,159}$")
OIDC_JWT_RE = re.compile(
    r"^[A-Za-z0-9_-]{16,4096}\.[A-Za-z0-9_-]{16,12288}\.[A-Za-z0-9_-]{16,2048}$"
)
OIDC_AUDIENCE = "heeexy-catalog-ingest"
OIDC_REFRESH_SKEW_SECONDS = 120
OIDC_REQUEST_HOST_SUFFIX = ".actions.githubusercontent.com"
INGEST_SECRET_RE = re.compile(r"^[A-Za-z0-9._~+/=\-]{43,512}$")
INGEST_SECRET_MIN_DISTINCT_CHARS = 8
SAFE_NAME_RE = re.compile(r"[^a-z0-9]+")
DRIVE_FILE_FIELDS = (
    "id,name,mimeType,size,md5Checksum,sha1Checksum,sha256Checksum,version,"
    "modifiedTime,trashed,parents,createdTime,appProperties"
)
CHECKSUM_PATTERNS = {
    "md5Checksum": re.compile(r"^[0-9a-f]{32}$"),
    "sha1Checksum": re.compile(r"^[0-9a-f]{40}$"),
    "sha256Checksum": SHA256_RE,
}
TRANSIENT_DRIVE_REASONS = {
    "backenderror",
    "downloadquotaexceeded",
    "internalerror",
    "quotaexceeded",
    "ratelimitexceeded",
    "serviceunavailable",
    "sharingratelimitexceeded",
    "userratelimitexceeded",
}
HARD_DRIVE_REASONS = {
    "dailylimitexceeded",
    "storagequotaexceeded",
    "teamdrivefilelimitexceeded",
}
OAUTH_DRIVE_REASONS = {
    "autherror",
    "insufficientpermissions",
    "invalidcredentials",
    "invalidtoken",
    "unauthorized",
}
SAFE_SVG_ELEMENTS = {
    "circle",
    "clippath",
    "defs",
    "desc",
    "ellipse",
    "feblend",
    "fecolormatrix",
    "fecomponenttransfer",
    "fecomposite",
    "feconvolvematrix",
    "fediffuselighting",
    "fedisplacementmap",
    "fedistantlight",
    "fedropshadow",
    "feflood",
    "fefunca",
    "fefuncb",
    "fefuncg",
    "fefuncr",
    "fegaussianblur",
    "feimage",
    "femerge",
    "femergenode",
    "femorphology",
    "feoffset",
    "fepointlight",
    "fespecularlighting",
    "fespotlight",
    "fetile",
    "feturbulence",
    "filter",
    "g",
    "image",
    "line",
    "lineargradient",
    "marker",
    "mask",
    "metadata",
    "path",
    "pattern",
    "polygon",
    "polyline",
    "radialgradient",
    "rect",
    "stop",
    "style",
    "svg",
    "switch",
    "symbol",
    "text",
    "textpath",
    "title",
    "tspan",
    "use",
}
SVG_URL_RE = re.compile(r"url\(\s*(['\"]?)(.*?)\1\s*\)", re.IGNORECASE | re.DOTALL)
SAFE_SVG_DATA_URI_RE = re.compile(
    r"^data:image/(?:png|jpeg|webp);base64,[a-z0-9+/=\s]+$", re.IGNORECASE
)
PUBLIC_AI_CATEGORY = "ai generated images"
PUBLIC_AI_KEYWORDS = frozenset(
    {"ai generated", "generated with ai", "ai generated images"}
)
PUBLIC_AI_DESCRIPTION_SUFFIX_RE = re.compile(
    r"\s*This illustration was generated with AI\.\s*\Z", re.IGNORECASE
)


class PipelineError(RuntimeError):
    """Sanitized failure carrying a stable non-sensitive code."""

    def __init__(self, code: str, *, permanent: bool = False, diagnostic: str = ""):
        super().__init__(code)
        self.code = code
        self.permanent = permanent
        self.diagnostic = diagnostic


def _drive_http_details(error: BaseException) -> tuple[int | None, frozenset[str]]:
    """Extract only status/reason tokens; never surface Drive response text."""

    response = getattr(error, "resp", None)
    try:
        status = int(getattr(response, "status", 0) or 0) or None
    except (TypeError, ValueError):
        status = None
    reasons: set[str] = set()
    content = getattr(error, "content", b"")
    if isinstance(content, bytes):
        text = content[:65_536].decode("utf-8", errors="ignore")
    elif isinstance(content, str):
        text = content[:65_536]
    else:
        text = ""
    if text:
        try:
            payload = json.loads(text)
            detail = payload.get("error") if isinstance(payload, Mapping) else None
            if isinstance(detail, Mapping):
                status_text = detail.get("status")
                if isinstance(status_text, str):
                    reasons.add(status_text.casefold())
                entries = detail.get("errors")
                if isinstance(entries, Sequence) and not isinstance(entries, (str, bytes)):
                    for entry in entries:
                        if isinstance(entry, Mapping) and isinstance(entry.get("reason"), str):
                            reasons.add(str(entry["reason"]).casefold())
        except (ValueError, TypeError):
            pass
    return status, frozenset(reasons)


def classify_drive_error(
    code: str,
    error: BaseException,
    *,
    candidate_scoped: bool,
) -> PipelineError:
    """Separate per-file permanent failures from service/authentication stops."""

    status, reasons = _drive_http_details(error)
    # Public runner logs may contain only bounded status and known reason tokens,
    # never provider messages, URLs, identities, or arbitrary response values.
    safe_status = str(status) if status is not None and 100 <= status <= 599 else "none"
    known_reasons = sorted(reasons.intersection(
        OAUTH_DRIVE_REASONS | TRANSIENT_DRIVE_REASONS | HARD_DRIVE_REASONS
    ))
    diagnostic = f"drive_http={safe_status} drive_reason={','.join(known_reasons) or 'unknown'}"
    if status == 401 or reasons.intersection(OAUTH_DRIVE_REASONS):
        return PipelineError("drive_oauth_failed", diagnostic=diagnostic)
    if (
        status in {408, 425, 429}
        or (status is not None and status >= 500)
        or reasons.intersection(TRANSIENT_DRIVE_REASONS)
    ):
        return PipelineError(code, diagnostic=diagnostic)
    if candidate_scoped and status in {403, 404, 410}:
        return PipelineError(code, permanent=True, diagnostic=diagnostic)
    return PipelineError(code, diagnostic=diagnostic)


def drive_error_is_retryable(error: BaseException) -> bool:
    """Return whether the same Drive operation/session may be retried safely."""

    status, reasons = _drive_http_details(error)
    if status == 401 or reasons.intersection(OAUTH_DRIVE_REASONS | HARD_DRIVE_REASONS):
        return False
    if status is None:
        # Transport failures often have no HTTP response at all. Callers still
        # apply a strict retry bound and must reuse the same resumable session.
        return True
    return bool(
        status in {408, 425, 429}
        or status >= 500
        or reasons.intersection(TRANSIENT_DRIVE_REASONS)
    )


@dataclass(frozen=True)
class Candidate:
    product_key: str
    slug: str
    title: str
    description: str
    category: str
    tags: tuple[str, ...]
    source_png_id: str | None
    source_svg_id: str | None
    source_archive_id: str | None
    archive_png_member: str | None
    archive_svg_member: str | None
    fingerprint: str

    @property
    def product_hash(self) -> str:
        return hashlib.sha256(self.product_key.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AssetInfo:
    file_id: str
    filename: str
    sha256: str
    size_bytes: int
    width: int | None = None
    height: int | None = None


@dataclass(frozen=True)
class PreparedPreview:
    path: Path
    sha256: str
    size_bytes: int
    width: int
    height: int


@dataclass
class GitHubOidcTokenProvider:
    """Keep a valid GitHub OIDC token in memory for a potentially long batch."""

    token_value: str

    def current_token(
        self,
        http_client: Any,
        *,
        environment: Mapping[str, str] | None = None,
        now_epoch: int | None = None,
    ) -> str:
        now = int(time.time()) if now_epoch is None else now_epoch
        self.token_value = validated_ingest_oidc_token(self.token_value)
        if unverified_oidc_expiry(self.token_value) > now + OIDC_REFRESH_SKEW_SECONDS:
            return self.token_value

        env = os.environ if environment is None else environment
        request_url = str(env.get("ACTIONS_ID_TOKEN_REQUEST_URL") or "").strip()
        request_token = str(env.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN") or "").strip()
        parsed = urlsplit(request_url)
        hostname = (parsed.hostname or "").casefold()
        if (
            parsed.scheme != "https"
            or parsed.username
            or parsed.password
            or not hostname.endswith(OIDC_REQUEST_HOST_SUFFIX)
            or parsed.fragment
            or not request_token
            or len(request_token.encode("utf-8")) > 16_384
            or any(ord(character) < 33 or ord(character) == 127 for character in request_token)
        ):
            raise PipelineError("catalog_ingest_oidc_refresh_unavailable")

        query = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key != "audience"
        ]
        query.append(("audience", OIDC_AUDIENCE))
        refresh_url = urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), "")
        )
        try:
            response = http_client.get(
                refresh_url,
                headers={"Authorization": f"bearer {request_token}"},
                timeout=(10, 30),
                allow_redirects=False,
            )
        except Exception as error:
            raise PipelineError("catalog_ingest_oidc_refresh_failed") from error
        if getattr(response, "status_code", None) != 200:
            raise PipelineError("catalog_ingest_oidc_refresh_failed")
        try:
            payload = response.json()
            refreshed = validated_ingest_oidc_token(str(payload.get("value") or ""))
        except (AttributeError, TypeError, ValueError, PipelineError) as error:
            raise PipelineError("catalog_ingest_oidc_refresh_failed") from error
        if unverified_oidc_expiry(refreshed) <= now + OIDC_REFRESH_SKEW_SECONDS:
            raise PipelineError("catalog_ingest_oidc_refresh_failed")
        self.token_value = refreshed
        return refreshed


@dataclass(frozen=True)
class StaticIngestSecretProvider:
    """Return a validated static fallback header without logging its value."""

    secret_value: str

    def current_headers(self, _http_client: Any) -> dict[str, str]:
        return ingest_auth_headers(ingest_secret=self.secret_value)


@dataclass(frozen=True)
class PipelineConfig:
    manifest_file_id: str | None
    png_only_root_id: str | None
    product_zips_folder_id: str
    reports_folder_id: str
    site_base_url: str
    ingest_auth_provider: GitHubOidcTokenProvider | StaticIngestSecretProvider
    batch_size: int
    retry_holds: bool
    runner_temp: Path
    product_workers: int = 1


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def md5_path(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def candidate_source_references(candidate: Candidate) -> tuple[tuple[str, str], ...]:
    if candidate.source_archive_id:
        return (("archive", candidate.source_archive_id),)
    references: list[tuple[str, str]] = []
    if candidate.source_png_id:
        references.append(("png", candidate.source_png_id))
    if candidate.source_svg_id:
        references.append(("svg", candidate.source_svg_id))
    return tuple(references)


def source_revision_descriptor(role: str, metadata: Mapping[str, Any]) -> dict[str, Any]:
    file_id = _valid_drive_id(metadata.get("id"))
    if not file_id or metadata.get("trashed"):
        raise PipelineError("drive_source_unavailable", permanent=True)
    try:
        size = int(metadata.get("size") or 0)
    except (TypeError, ValueError) as error:
        raise PipelineError("drive_source_revision_invalid", permanent=True) from error
    checksums: dict[str, str] = {}
    for field, pattern in CHECKSUM_PATTERNS.items():
        value = str(metadata.get(field) or "").strip().casefold()
        if value:
            if not pattern.fullmatch(value):
                raise PipelineError("drive_source_revision_invalid", permanent=True)
            checksums[field] = value
    version = str(metadata.get("version") or "").strip()
    modified_time = str(metadata.get("modifiedTime") or "").strip()
    if not checksums and not version:
        raise PipelineError("drive_source_revision_missing", permanent=True)
    return {
        "role": role,
        "file_id": file_id,
        "mime_type": str(metadata.get("mimeType") or ""),
        "size": size,
        "version": version,
        "modified_time": modified_time,
        "checksums": checksums,
    }


def artifact_fingerprint(
    candidate: Candidate,
    source_revisions: Sequence[Mapping[str, Any]],
    *,
    recipe_version: str = ARTIFACT_RECIPE_VERSION,
) -> str:
    normalized = {
        "artifact_recipe_version": recipe_version,
        "candidate_fingerprint": candidate.fingerprint,
        "source_revisions": sorted(
            (dict(item) for item in source_revisions),
            key=lambda item: (str(item.get("role") or ""), str(item.get("file_id") or "")),
        ),
    }
    return sha256_bytes(canonical_json(normalized).encode("utf-8"))


def drive_binary_metadata_matches(
    metadata: Mapping[str, Any],
    *,
    expected_size: int,
    expected_mime_type: str,
    expected_md5: str,
) -> bool:
    try:
        size = int(metadata.get("size") or -1)
    except (TypeError, ValueError):
        return False
    return (
        not metadata.get("trashed")
        and size == expected_size
        and str(metadata.get("mimeType") or "") == expected_mime_type
        and str(metadata.get("md5Checksum") or "").casefold() == expected_md5.casefold()
    )


def clean_text(value: Any, maximum: int) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = "".join(" " if ord(char) < 32 or ord(char) == 127 else char for char in text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= maximum:
        return text
    shortened = text[:maximum].rstrip()
    if " " in shortened:
        shortened = shortened.rsplit(" ", 1)[0].rstrip() or shortened
    return shortened


def metadata_text_length(value: str) -> int:
    """Return UTF-16 code units, matching JavaScript/Zod string bounds."""

    return len(value.encode("utf-16-le")) // 2


def clean_metadata_text(value: Any, maximum: int, error_code: str) -> str:
    """Normalize metadata without ever shortening user-visible SEO content."""

    text = unicodedata.normalize("NFKC", str(value or ""))
    text = "".join(" " if ord(char) < 32 or ord(char) == 127 else char for char in text)
    text = re.sub(r"\s+", " ", text).strip()
    if metadata_text_length(text) > maximum:
        raise PipelineError(error_code, permanent=True)
    return text


def slugify(value: str, identity_digest: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii").lower()
    base = SAFE_NAME_RE.sub("-", ascii_value).strip("-") or "design"
    suffix = identity_digest[:12]
    return f"{base[:106].rstrip('-')}-{suffix}"


def normalize_tags(values: Any) -> tuple[str, ...]:
    if isinstance(values, str):
        raw_values: Sequence[Any] = re.split(r"[,;|]", values)
    elif isinstance(values, Sequence) and not isinstance(values, (bytes, bytearray)):
        raw_values = values
    else:
        raw_values = ()
    output: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        tag = clean_metadata_text(raw, MAX_TAG_CHARS, "tag_exceeds_limit")
        key = tag.casefold()
        if not tag or key in seen:
            continue
        output.append(tag)
        seen.add(key)
        if len(output) > MAX_TAGS:
            raise PipelineError("tags_exceed_limit", permanent=True)
    return tuple(output)


def public_site_metadata(candidate: Candidate) -> dict[str, Any]:
    """Return narrowly sanitized metadata for the public site-ingest boundary.

    The authoritative ``Candidate`` remains untouched for private provenance.
    We remove only the known AI disclosure/category terms requested for the
    storefront, then re-apply required-field checks without inventing copy.
    """

    description = PUBLIC_AI_DESCRIPTION_SUFFIX_RE.sub("", candidate.description).rstrip()
    category = (
        ""
        if candidate.category.casefold() == PUBLIC_AI_CATEGORY
        else candidate.category
    )
    tags = tuple(
        tag for tag in candidate.tags if tag.casefold() not in PUBLIC_AI_KEYWORDS
    )
    if metadata_text_length(description) < 20:
        raise PipelineError("description_missing", permanent=True)
    if not tags:
        raise PipelineError("tags_missing", permanent=True)
    return {
        "description": description,
        "category": category,
        "tags": list(tags),
    }


def _valid_drive_id(value: Any) -> str | None:
    text = str(value or "").strip()
    if not DRIVE_ID_RE.fullmatch(text):
        return None
    if text.casefold().startswith("svg-png-render-v1-"):
        return None
    return text


def _first_real_drive_id(values: Any, preferred: Any = None) -> str | None:
    candidates: list[str] = []
    if isinstance(values, str):
        values = [values]
    if isinstance(values, Sequence):
        for value in values:
            valid = _valid_drive_id(value)
            if valid and valid not in candidates:
                candidates.append(valid)
    preferred_id = _valid_drive_id(preferred)
    if preferred_id in candidates:
        return preferred_id
    return sorted(candidates)[0] if candidates else None


def candidate_from_record(record: Mapping[str, Any]) -> Candidate | None:
    """Normalize one existing Dreamstime/canonical JSONL row.

    Quarantine and alias rows are ignored.  A primary SVG row is accepted even
    without a PNG because the cloud runner renders and uploads the missing PNG.
    Metadata is constrained for the receiver but never generated here.
    """

    record_type = str(record.get("record_type") or "")
    if record_type == "whitelist":
        if not record.get("is_product_primary_record"):
            return None
        metadata = record.get("metadata")
        if not isinstance(metadata, Mapping):
            raise PipelineError("metadata_missing", permanent=True)
        png_id = _first_real_drive_id(
            record.get("known_png_drive_file_ids"), record.get("canonical_local_id")
        )
        svg_id = _first_real_drive_id(
            record.get("known_svg_drive_file_ids"), record.get("canonical_local_id")
        )
        archive_id = _valid_drive_id(record.get("archive_zip_drive_file_id"))
        archive_png_member = clean_text(record.get("archive_png_member"), 500) or None
        archive_svg_member = clean_text(record.get("archive_svg_member"), 500) or None
        identity_seed = str(
            record.get("product_group_id")
            or record.get("canonical_local_id")
            or png_id
            or svg_id
            or ""
        )
    elif record_type in {"heeexy_catalog_product", "catalog_product"}:
        metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else record
        assets = record.get("assets") if isinstance(record.get("assets"), Mapping) else {}
        png_id = _valid_drive_id(assets.get("png_drive_file_id") or record.get("png_drive_file_id"))
        svg_id = _valid_drive_id(assets.get("svg_drive_file_id") or record.get("svg_drive_file_id"))
        archive_id = _valid_drive_id(
            assets.get("archive_drive_file_id") or record.get("archive_drive_file_id")
        )
        archive_png_member = clean_text(
            assets.get("archive_png_member") or record.get("archive_png_member"), 500
        ) or None
        archive_svg_member = clean_text(
            assets.get("archive_svg_member") or record.get("archive_svg_member"), 500
        ) or None
        identity_seed = str(
            record.get("product_identity")
            or record.get("product_key")
            or png_id
            or svg_id
            or (f"{archive_id}:{archive_png_member or archive_svg_member}" if archive_id else "")
        )
    else:
        return None

    if archive_id and (png_id or svg_id):
        raise PipelineError("mixed_direct_and_archive_assets", permanent=True)
    if archive_id and not (archive_png_member or archive_svg_member):
        raise PipelineError("archive_member_missing", permanent=True)
    if (archive_png_member or archive_svg_member) and not archive_id:
        raise PipelineError("archive_identity_missing", permanent=True)
    if not identity_seed or not (png_id or svg_id or archive_id):
        raise PipelineError("asset_identity_missing", permanent=True)
    title = clean_metadata_text(metadata.get("title"), MAX_TITLE_CHARS, "title_exceeds_limit")
    description = clean_metadata_text(
        metadata.get("description"), MAX_DESCRIPTION_CHARS, "description_exceeds_limit"
    )
    categories = metadata.get("categories")
    if isinstance(categories, str):
        categories = [item.strip() for item in re.split(r"[,;|]", categories) if item.strip()]
    category_source = (
        categories[0]
        if isinstance(categories, Sequence) and not isinstance(categories, (bytes, bytearray)) and categories
        else metadata.get("category")
    )
    category = clean_metadata_text(
        category_source, MAX_CATEGORY_CHARS, "category_exceeds_limit"
    )
    tags = normalize_tags(metadata.get("keywords") or metadata.get("tags"))
    if metadata_text_length(title) < 3:
        raise PipelineError("title_missing", permanent=True)
    if metadata_text_length(description) < 20:
        raise PipelineError("description_missing", permanent=True)
    if not tags:
        raise PipelineError("tags_missing", permanent=True)

    identity_digest = hashlib.sha256(identity_seed.encode("utf-8")).hexdigest()
    product_key = f"drive:catalog:{identity_digest}"
    if not PRODUCT_KEY_RE.fullmatch(product_key):
        raise PipelineError("product_key_invalid", permanent=True)
    normalized = {
        "product_key": product_key,
        "title": title,
        "description": description,
        "category": category,
        "tags": tags,
        "source_png_id": png_id,
        "source_svg_id": svg_id,
        "source_archive_id": archive_id,
        "archive_png_member": archive_png_member,
        "archive_svg_member": archive_svg_member,
    }
    fingerprint = sha256_bytes(canonical_json(normalized).encode("utf-8"))
    return Candidate(
        product_key=product_key,
        slug=slugify(title, identity_digest),
        title=title,
        description=description,
        category=category,
        tags=tags,
        source_png_id=png_id,
        source_svg_id=svg_id,
        source_archive_id=archive_id,
        archive_png_member=archive_png_member,
        archive_svg_member=archive_svg_member,
        fingerprint=fingerprint,
    )


def parse_manifest_jsonl(stream: Iterable[str]) -> tuple[list[Candidate], Counter[str]]:
    candidates: dict[str, Candidate] = {}
    conflicted: set[str] = set()
    stats: Counter[str] = Counter()
    for line in stream:
        if not line.strip():
            continue
        stats["rows"] += 1
        try:
            raw = json.loads(line)
            if not isinstance(raw, Mapping):
                raise PipelineError("manifest_row_not_object", permanent=True)
            candidate = candidate_from_record(raw)
            if candidate is None:
                stats["ignored"] += 1
                continue
            if candidate.product_key in conflicted:
                stats["held_manifest_identity_conflict"] += 1
                continue
            prior = candidates.get(candidate.product_key)
            if prior and prior.fingerprint != candidate.fingerprint:
                candidates.pop(candidate.product_key, None)
                conflicted.add(candidate.product_key)
                stats["held_manifest_identity_conflict"] += 1
                continue
            candidates[candidate.product_key] = candidate
            stats["accepted"] += 1
        except (json.JSONDecodeError, UnicodeError):
            stats["invalid_json"] += 1
        except PipelineError as error:
            stats[f"held_{error.code}"] += 1
    return sorted(candidates.values(), key=lambda item: item.product_key), stats


def enforce_cloud_runtime(environment: Mapping[str, str] | None = None) -> Path:
    # An explicitly supplied empty mapping must remain empty. Falling back to
    # the process environment would make the security boundary untestable on
    # a real GitHub runner and could hide a caller's missing attestation.
    env = os.environ if environment is None else environment
    checks = {
        "GITHUB_ACTIONS": "true",
        "CI": "true",
        "RUNNER_ENVIRONMENT": "github-hosted",
        "RUNNER_OS": "Linux",
    }
    if any(env.get(key) != expected for key, expected in checks.items()):
        raise PipelineError("cloud_runtime_required")
    if not env.get("GITHUB_RUN_ID") or not env.get("GITHUB_WORKFLOW_SHA"):
        raise PipelineError("cloud_runtime_attestation_missing")
    runner_temp = Path(env.get("RUNNER_TEMP", ""))
    if not runner_temp.is_absolute() or not runner_temp.is_dir():
        raise PipelineError("runner_temp_invalid")
    return runner_temp.resolve()


def _xml_name(value: str) -> tuple[str, str]:
    if value.startswith("{") and "}" in value:
        namespace, local = value[1:].split("}", 1)
        return namespace, local.casefold()
    return "", value.casefold()


def _safe_svg_uri(value: str) -> bool:
    uri = value.strip()
    if not uri or any(ord(char) < 32 or ord(char) == 127 for char in uri):
        return False
    if uri.startswith("#"):
        return len(uri) <= 512 and not any(char.isspace() for char in uri)
    return len(uri) <= MAX_SVG_BYTES and bool(SAFE_SVG_DATA_URI_RE.fullmatch(uri))


def _validate_svg_css(value: str) -> None:
    lowered = value.casefold()
    if "\\" in value or "@import" in lowered or "expression(" in lowered or "behavior:" in lowered:
        raise PipelineError("svg_active_or_external_content", permanent=True)
    matches = list(SVG_URL_RE.finditer(value))
    if "url(" in lowered and not matches:
        raise PipelineError("svg_active_or_external_content", permanent=True)
    for match in matches:
        if not _safe_svg_uri(match.group(2)):
            raise PipelineError("svg_active_or_external_content", permanent=True)


def validate_svg_bytes(payload: bytes) -> None:
    if not payload or len(payload) > MAX_SVG_BYTES:
        raise PipelineError("svg_size_invalid", permanent=True)
    lowered = payload.lower()
    if (
        b"<!doctype" in lowered
        or b"<!entity" in lowered
        or b"<?xml-stylesheet" in lowered
    ):
        raise PipelineError("svg_active_or_external_content", permanent=True)
    try:
        root = ET.fromstring(payload)
    except (ET.ParseError, UnicodeError, ValueError) as error:
        raise PipelineError("svg_xml_invalid", permanent=True) from error
    svg_namespace = "http://www.w3.org/2000/svg"
    inert_namespaces = {
        "http://c2pa.org/manifest",
        "http://creativecommons.org/ns#",
        "http://purl.org/dc/elements/1.1/",
        "http://www.inkscape.org/namespaces/inkscape",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "http://sodipodi.sourceforge.net/DTD/sodipodi-0.dtd",
    }
    root_namespace, root_name = _xml_name(root.tag)
    if root_name != "svg" or root_namespace not in {"", svg_namespace}:
        raise PipelineError("svg_root_missing", permanent=True)
    metadata_nodes: set[int] = set()
    for element in root.iter():
        namespace, local_name = _xml_name(element.tag)
        if namespace in {"", svg_namespace} and local_name == "metadata":
            metadata_nodes.update(id(descendant) for descendant in element.iter())
    element_count = 0
    for element in root.iter():
        element_count += 1
        if element_count > MAX_SVG_ELEMENTS:
            raise PipelineError("svg_element_limit_exceeded", permanent=True)
        namespace, local_name = _xml_name(element.tag)
        if namespace not in {"", svg_namespace}:
            # Foreign vocabularies are inert metadata only.  Allowing a known
            # metadata namespace in the render tree would turn the namespace
            # list into an element allowlist bypass.
            if namespace not in inert_namespaces or id(element) not in metadata_nodes:
                raise PipelineError("svg_namespace_not_allowed", permanent=True)
        if namespace in {"", svg_namespace} and local_name not in SAFE_SVG_ELEMENTS:
            raise PipelineError("svg_element_not_allowed", permanent=True)
        for raw_name, raw_value in element.attrib.items():
            attribute_namespace, attribute_name = _xml_name(raw_name)
            value = str(raw_value)
            if attribute_name.startswith("on") or attribute_name == "base":
                raise PipelineError("svg_active_or_external_content", permanent=True)
            if attribute_name in {"href", "src"} and not _safe_svg_uri(value):
                raise PipelineError("svg_active_or_external_content", permanent=True)
            if "url(" in value.casefold() or attribute_name == "style":
                _validate_svg_css(value)
            if attribute_namespace == "http://www.w3.org/XML/1998/namespace" and attribute_name == "base":
                raise PipelineError("svg_active_or_external_content", permanent=True)
        if local_name == "style" and element.text:
            _validate_svg_css(element.text)


def normalized_archive_member(value: str) -> str:
    if not value or "\\" in value or "\x00" in value:
        raise PipelineError("archive_member_invalid", permanent=True)
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise PipelineError("archive_member_invalid", permanent=True)
    return path.as_posix()


def enforce_archive_entry_count(count: int) -> None:
    if count < 1 or count > MAX_ARCHIVE_ENTRIES:
        raise PipelineError("archive_entry_count_invalid", permanent=True)


def extract_archive_members(
    archive_path: Path,
    *,
    png_member: str | None,
    svg_member: str | None,
    png_destination: Path,
    svg_destination: Path,
) -> tuple[bool, bool]:
    requested: dict[str, tuple[Path, int]] = {}
    if png_member:
        requested[normalized_archive_member(png_member)] = (png_destination, MAX_SOURCE_BYTES)
    if svg_member:
        normalized = normalized_archive_member(svg_member)
        if normalized in requested:
            raise PipelineError("archive_member_collision", permanent=True)
        requested[normalized] = (svg_destination, MAX_SVG_BYTES)
    if not requested:
        raise PipelineError("archive_member_missing", permanent=True)
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            enforce_archive_entry_count(len(archive.infolist()))
            indexed: dict[str, list[zipfile.ZipInfo]] = {}
            for info in archive.infolist():
                try:
                    normalized = normalized_archive_member(info.filename)
                except PipelineError:
                    continue
                if normalized in requested:
                    indexed.setdefault(normalized, []).append(info)
            if any(len(indexed.get(name, [])) != 1 for name in requested):
                raise PipelineError("archive_member_not_unique", permanent=True)
            for name, (destination, maximum) in requested.items():
                info = indexed[name][0]
                unix_mode = (info.external_attr >> 16) & 0o170000
                if info.is_dir() or unix_mode == 0o120000 or (info.flag_bits & 0x1):
                    raise PipelineError("archive_member_unsafe", permanent=True)
                if info.file_size < 1 or info.file_size > maximum:
                    raise PipelineError("archive_member_size_invalid", permanent=True)
                if info.compress_size == 0 or info.file_size / info.compress_size > 1000:
                    raise PipelineError("archive_member_ratio_invalid", permanent=True)
                written = 0
                with archive.open(info, "r") as source, destination.open("wb") as output:
                    while chunk := source.read(1024 * 1024):
                        written += len(chunk)
                        if written > maximum:
                            raise PipelineError("archive_member_exceeded_limit", permanent=True)
                        output.write(chunk)
                if written != info.file_size:
                    raise PipelineError("archive_member_size_mismatch", permanent=True)
    except PipelineError:
        raise
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        raise PipelineError("archive_read_failed", permanent=True) from error
    return bool(png_member), bool(svg_member)


def safe_drive_name(value: str, extension: str, fallback: str) -> str:
    name = clean_text(value, 220).replace("/", " ").replace("\\", " ").strip(" .")
    if not name.casefold().endswith(extension):
        name = f"{fallback}{extension}"
    return name[:255]


def _query_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


class DriveClient:
    """Small private-Drive client; it never creates sharing permissions."""

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        try:
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build
        except ImportError as error:  # pragma: no cover - dependency preflight
            raise PipelineError("drive_dependencies_missing") from error
        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        try:
            self.service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        except Exception as error:
            raise PipelineError("drive_client_initialization_failed") from error

    def get_metadata(self, file_id: str, *, candidate_scoped: bool = False) -> dict[str, Any]:
        if not DRIVE_ID_RE.fullmatch(file_id):
            raise PipelineError("drive_id_invalid", permanent=True)
        try:
            return (
                self.service.files()
                .get(
                    fileId=file_id,
                    fields=DRIVE_FILE_FIELDS,
                    supportsAllDrives=True,
                )
                .execute(num_retries=3)
            )
        except Exception as error:
            raise classify_drive_error(
                "drive_metadata_failed", error, candidate_scoped=candidate_scoped
            ) from error

    def list_children(
        self, folder_id: str, *, candidate_scoped: bool = False
    ) -> list[dict[str, Any]]:
        query = f"'{_query_escape(folder_id)}' in parents and trashed = false"
        return self._list_query(query, candidate_scoped=candidate_scoped)

    def list_named_files(self, name: str) -> list[dict[str, Any]]:
        query = f"name = '{_query_escape(name)}' and trashed = false"
        return self._list_query(query, candidate_scoped=False)

    def _list_query(
        self, query: str, *, candidate_scoped: bool = False
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
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
                        fields=f"nextPageToken,files({DRIVE_FILE_FIELDS})",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    )
                    .execute(num_retries=3)
                )
                output.extend(response.get("files", []))
                page_token = response.get("nextPageToken")
                if not page_token:
                    return output
        except Exception as error:
            raise classify_drive_error(
                "drive_list_failed", error, candidate_scoped=candidate_scoped
            ) from error

    def assert_private_writable_folder(self, folder_id: str) -> None:
        metadata = self.get_metadata(folder_id)
        if metadata.get("trashed") or metadata.get("mimeType") != FOLDER_MIME:
            raise PipelineError("drive_output_folder_invalid")
        try:
            response = (
                self.service.permissions()
                .list(
                    fileId=folder_id,
                    fields="permissions(type,role)",
                    supportsAllDrives=True,
                )
                .execute(num_retries=3)
            )
        except Exception as error:
            raise classify_drive_error(
                "drive_permission_check_failed", error, candidate_scoped=False
            ) from error
        if any(item.get("type") in {"anyone", "domain"} for item in response.get("permissions", [])):
            raise PipelineError("drive_output_not_private")

    def download_to(
        self,
        file_id: str,
        destination: Path,
        maximum_bytes: int,
        *,
        candidate_scoped: bool = False,
        expected_metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = dict(expected_metadata) if expected_metadata is not None else self.get_metadata(
            file_id, candidate_scoped=candidate_scoped
        )
        if metadata.get("id") != file_id:
            raise PipelineError("drive_source_identity_mismatch", permanent=candidate_scoped)
        if metadata.get("trashed"):
            raise PipelineError("drive_source_trashed", permanent=True)
        try:
            declared_size = int(metadata.get("size") or 0)
        except (TypeError, ValueError) as error:
            raise PipelineError("drive_source_size_invalid", permanent=True) from error
        if declared_size < 1 or declared_size > maximum_bytes:
            raise PipelineError("drive_source_size_invalid", permanent=True)
        try:
            from googleapiclient.http import MediaIoBaseDownload

            request = self.service.files().get_media(fileId=file_id, supportsAllDrives=True)
            with destination.open("wb") as stream:
                downloader = MediaIoBaseDownload(stream, request, chunksize=8 * 1024 * 1024)
                done = False
                while not done:
                    _, done = downloader.next_chunk(num_retries=3)
                    if stream.tell() > maximum_bytes:
                        raise PipelineError("drive_download_exceeded_limit", permanent=True)
        except PipelineError:
            raise
        except Exception as error:
            raise classify_drive_error(
                "drive_download_failed", error, candidate_scoped=candidate_scoped
            ) from error
        if destination.stat().st_size != declared_size:
            raise PipelineError("drive_download_size_mismatch")
        expected_md5 = str(metadata.get("md5Checksum") or "").casefold()
        if expected_md5 and md5_path(destination) != expected_md5:
            raise PipelineError("drive_source_changed_during_download")
        return metadata

    def download_bytes(
        self,
        file_id: str,
        maximum_bytes: int,
        *,
        candidate_scoped: bool = False,
    ) -> tuple[bytes, dict[str, Any]]:
        metadata = self.get_metadata(file_id, candidate_scoped=candidate_scoped)
        try:
            size = int(metadata.get("size") or 0)
        except (TypeError, ValueError) as error:
            raise PipelineError("drive_source_size_invalid", permanent=True) from error
        if size < 1 or size > maximum_bytes:
            raise PipelineError("drive_source_size_invalid", permanent=True)
        try:
            from googleapiclient.http import MediaIoBaseDownload

            output = io.BytesIO()
            downloader = MediaIoBaseDownload(
                output,
                self.service.files().get_media(fileId=file_id, supportsAllDrives=True),
                chunksize=4 * 1024 * 1024,
            )
            done = False
            while not done:
                _, done = downloader.next_chunk(num_retries=3)
                if output.tell() > maximum_bytes:
                    raise PipelineError("drive_download_exceeded_limit", permanent=True)
            payload = output.getvalue()
        except PipelineError:
            raise
        except Exception as error:
            raise classify_drive_error(
                "drive_download_failed", error, candidate_scoped=candidate_scoped
            ) from error
        if len(payload) != size:
            raise PipelineError("drive_download_size_mismatch")
        expected_md5 = str(metadata.get("md5Checksum") or "").casefold()
        if expected_md5 and hashlib.md5(payload, usedforsecurity=False).hexdigest() != expected_md5:
            raise PipelineError("drive_source_changed_during_download")
        return payload, metadata

    def _find_by_properties(self, parent_id: str, properties: Mapping[str, str]) -> list[dict[str, Any]]:
        clauses = [f"'{_query_escape(parent_id)}' in parents", "trashed = false"]
        for key, value in properties.items():
            clauses.append(
                "appProperties has { key='"
                + _query_escape(key)
                + "' and value='"
                + _query_escape(value)
                + "' }"
            )
        return self._list_query(" and ".join(clauses), candidate_scoped=False)

    def ensure_product_folder(self, parent_id: str, product_hash: str, slug: str) -> str:
        properties = {"heeexy_role": PRODUCT_FOLDER_ROLE, "heeexy_product": product_hash}
        found = [item for item in self._find_by_properties(parent_id, properties) if item.get("mimeType") == FOLDER_MIME]
        if found:
            return sorted(found, key=lambda item: str(item.get("createdTime") or ""))[0]["id"]
        body = {
            "name": f"{slug}-{product_hash[:8]}",
            "mimeType": FOLDER_MIME,
            "parents": [parent_id],
            "appProperties": properties,
        }
        try:
            result = (
                self.service.files()
                .create(body=body, fields="id", supportsAllDrives=True)
                .execute(num_retries=3)
            )
            return result["id"]
        except Exception as error:
            raise classify_drive_error(
                "drive_folder_create_failed", error, candidate_scoped=False
            ) from error

    def upload_content_addressed(
        self,
        path: Path,
        *,
        parent_id: str,
        name: str,
        mime_type: str,
        role: str,
        product_hash: str,
        content_sha256: str,
    ) -> dict[str, Any]:
        expected_size = path.stat().st_size
        expected_md5 = md5_path(path)
        properties = {
            "heeexy_role": role,
            "heeexy_product": product_hash,
            "sha256": content_sha256,
        }
        found = self._find_by_properties(parent_id, properties)
        if found:
            valid = [
                item
                for item in found
                if drive_binary_metadata_matches(
                    item,
                    expected_size=expected_size,
                    expected_mime_type=mime_type,
                    expected_md5=expected_md5,
                )
            ]
            if not valid:
                raise PipelineError("drive_content_address_mismatch")
            return sorted(valid, key=lambda item: str(item.get("createdTime") or ""))[0]
        try:
            from googleapiclient.http import MediaFileUpload

            media = MediaFileUpload(
                str(path), mimetype=mime_type, resumable=True, chunksize=8 * 1024 * 1024
            )
            request = self.service.files().create(
                body={
                    "name": name,
                    "parents": [parent_id],
                    "appProperties": properties,
                },
                media_body=media,
                fields=DRIVE_FILE_FIELDS,
                supportsAllDrives=True,
            )
            result: dict[str, Any] | None = None
            retry_index = 0
            while result is None:
                try:
                    _, result = request.next_chunk(num_retries=3)
                    retry_index = 0
                except Exception as error:
                    classified = classify_drive_error(
                        "drive_upload_failed", error, candidate_scoped=False
                    )
                    if (
                        classified.code != "drive_upload_failed"
                        or not drive_error_is_retryable(error)
                        or retry_index == len(DRIVE_RESUMABLE_RETRY_DELAYS)
                    ):
                        raise
                    time.sleep(DRIVE_RESUMABLE_RETRY_DELAYS[retry_index])
                    retry_index += 1
            if not drive_binary_metadata_matches(
                result,
                expected_size=expected_size,
                expected_mime_type=mime_type,
                expected_md5=expected_md5,
            ):
                result_id = _valid_drive_id(result.get("id"))
                if not result_id:
                    raise PipelineError("drive_upload_verification_failed")
                result = self.get_metadata(result_id)
            if not drive_binary_metadata_matches(
                result,
                expected_size=expected_size,
                expected_mime_type=mime_type,
                expected_md5=expected_md5,
            ):
                raise PipelineError("drive_upload_verification_failed")
            return result
        except PipelineError:
            raise
        except Exception as error:
            classified = classify_drive_error(
                "drive_upload_failed", error, candidate_scoped=False
            )
            if (
                classified.code == "drive_oauth_failed"
                or not drive_error_is_retryable(error)
            ):
                raise classified from error

            # A final transport error can occur after Drive commits the file
            # but before the response reaches the runner. Poll only for the
            # immutable content-addressed object; never start a second upload
            # session inside this invocation.
            saw_mismatch = False
            for delay in DRIVE_RECONCILE_DELAYS:
                time.sleep(delay)
                try:
                    found = self._find_by_properties(parent_id, properties)
                except PipelineError as reconciliation_error:
                    if reconciliation_error.code == "drive_oauth_failed":
                        raise
                    continue
                if not found:
                    continue
                valid = [
                    item
                    for item in found
                    if drive_binary_metadata_matches(
                        item,
                        expected_size=expected_size,
                        expected_mime_type=mime_type,
                        expected_md5=expected_md5,
                    )
                ]
                if valid:
                    return sorted(
                        valid,
                        key=lambda item: str(item.get("createdTime") or ""),
                    )[0]
                saw_mismatch = True
            if saw_mismatch:
                raise PipelineError("drive_content_address_mismatch") from error
            raise classified from error

    def load_state(self, reports_folder_id: str) -> tuple[dict[str, Any], str | None]:
        role, _ = state_identity()
        found = self._find_by_properties(
            reports_folder_id, {"heeexy_role": role}
        )
        if not found:
            return empty_state(), None
        state_file = sorted(found, key=lambda item: str(item.get("createdTime") or ""))[0]
        payload, _ = self.download_bytes(state_file["id"], 16 * 1024 * 1024)
        try:
            state = json.loads(payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise PipelineError("pipeline_state_invalid") from error
        validate_state(state)
        return state, state_file["id"]

    def save_state(self, reports_folder_id: str, state_file_id: str | None, state: dict[str, Any]) -> str:
        role, name = state_identity()
        payload = (canonical_json(state) + "\n").encode("utf-8")
        try:
            from googleapiclient.http import MediaIoBaseUpload

            media = MediaIoBaseUpload(io.BytesIO(payload), mimetype="application/json", resumable=False)
            if state_file_id:
                result = (
                    self.service.files()
                    .update(
                        fileId=state_file_id,
                        media_body=media,
                        fields="id",
                        supportsAllDrives=True,
                    )
                    .execute(num_retries=3)
                )
            else:
                result = (
                    self.service.files()
                    .create(
                        body={
                            "name": name,
                            "parents": [reports_folder_id],
                            "appProperties": {"heeexy_role": role},
                        },
                        media_body=media,
                        fields="id",
                        supportsAllDrives=True,
                    )
                    .execute(num_retries=3)
                )
            return result["id"]
        except Exception as error:
            raise classify_drive_error(
                "pipeline_state_write_failed", error, candidate_scoped=False
            ) from error


def resolve_candidate_artifact_fingerprint(
    drive: DriveClient,
    candidate: Candidate,
    metadata_cache: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, dict[str, Any]]]:
    revisions: list[dict[str, Any]] = []
    selected_metadata: dict[str, dict[str, Any]] = {}
    for role, file_id in candidate_source_references(candidate):
        metadata = metadata_cache.get(file_id)
        if metadata is None:
            metadata = drive.get_metadata(file_id, candidate_scoped=True)
            metadata_cache[file_id] = metadata
        revisions.append(source_revision_descriptor(role, metadata))
        selected_metadata[file_id] = metadata
    if not revisions:
        raise PipelineError("asset_identity_missing", permanent=True)
    return artifact_fingerprint(candidate, revisions), selected_metadata


def state_identity(site_base_url: str | None = None) -> tuple[str, str]:
    """Each publishing target owns its checkpoint; the first site keeps the original.

    A checkpoint records what one site has received. Reusing it for another
    site would mark every product complete there without publishing anything.
    """
    value = os.environ.get("HEEEXY_SITE_BASE_URL", "") if site_base_url is None else site_base_url
    host = (urlsplit(value.strip()).hostname or "").casefold()
    if not host or host.endswith(".chatgpt.site"):
        return STATE_ROLE, STATE_FILE_NAME
    target = hashlib.sha256(host.encode("utf-8")).hexdigest()[:16]
    return f"{STATE_ROLE}_{target}", f"catalog-pipeline-state-{target}.json"


def empty_state() -> dict[str, Any]:
    return {
        "schema_version": PIPELINE_STATE_VERSION,
        "completed": {},
        "holds": {},
        "updated_at_epoch": 0,
    }


def validate_state(state: Any) -> None:
    if not isinstance(state, dict) or state.get("schema_version") != PIPELINE_STATE_VERSION:
        raise PipelineError("pipeline_state_invalid")
    if not isinstance(state.get("completed"), dict) or not isinstance(state.get("holds"), dict):
        raise PipelineError("pipeline_state_invalid")


def prioritize_unseen_candidates(
    candidates: Sequence[Candidate],
    completed: Mapping[str, Any],
    holds: Mapping[str, Any],
    *,
    retry_holds: bool,
) -> list[Candidate]:
    """Put never-processed work first without skipping the verification pass.

    A catalog drain can contain thousands of already-completed records before the
    first unseen record in manifest order. Resolving every completed source
    revision before selecting a bounded batch makes the live catalog appear
    stalled. This stable partition lets new work publish first. Completed and
    unchanged held records remain in the drain's later verification pass, so
    source changes and recipe changes are still detected before the drain stops.
    """

    def priority(candidate: Candidate) -> int:
        if candidate.product_key in completed:
            return 1
        if not retry_holds and candidate.product_key in holds:
            return 1
        return 0

    return sorted(candidates, key=priority)


def hold_has_untried_repair(hold: Mapping[str, Any]) -> bool:
    version = HOLD_REPAIR_VERSIONS.get(str(hold.get("reason") or ""))
    return bool(version and hold.get("repair_version") != version)


def is_descendant_folder(
    drive: DriveClient,
    folder_id: str,
    root_id: str,
    parent_cache: dict[str, tuple[str, ...]],
) -> bool:
    pending = [folder_id]
    seen: set[str] = set()
    while pending and len(seen) < 64:
        current = pending.pop()
        if current == root_id:
            return True
        if current in seen:
            continue
        seen.add(current)
        if current not in parent_cache:
            metadata = drive.get_metadata(current, candidate_scoped=True)
            if metadata.get("trashed") or metadata.get("mimeType") != FOLDER_MIME:
                parent_cache[current] = ()
            else:
                parent_cache[current] = tuple(str(item) for item in metadata.get("parents", []))
        pending.extend(parent_cache[current])
    return False


def _row_value(row: Mapping[str, str], aliases: Sequence[str]) -> str:
    indexed = {str(key).strip().casefold(): str(value or "").strip() for key, value in row.items()}
    for alias in aliases:
        value = indexed.get(alias.casefold())
        if value:
            return value
    return ""


def candidate_from_sidecar(
    payload: bytes,
    folder_id: str,
    children: Sequence[Mapping[str, Any]],
) -> Candidate:
    try:
        text = payload.decode("utf-8-sig")
        rows = [row for row in csv.DictReader(io.StringIO(text, newline="")) if any(row.values())]
    except (UnicodeError, csv.Error) as error:
        raise PipelineError("sidecar_invalid", permanent=True) from error
    if len(rows) != 1:
        raise PipelineError("sidecar_row_count", permanent=True)
    row = {str(key or "").strip(): str(value or "").strip() for key, value in rows[0].items()}
    delivery_name = _row_value(row, ("DeliveryFile", "Filename", "PNGFile"))
    pngs = [item for item in children if item.get("mimeType") == PNG_MIME]
    if delivery_name:
        exact = [item for item in pngs if str(item.get("name") or "") == delivery_name]
        if len(exact) != 1:
            raise PipelineError("sidecar_delivery_file_not_exact", permanent=True)
        pngs = exact
    if len(pngs) != 1:
        raise PipelineError("sidecar_png_identity_ambiguous", permanent=True)
    png = pngs[0]
    png_id = _valid_drive_id(png.get("id"))
    if not png_id:
        raise PipelineError("sidecar_png_identity_invalid", permanent=True)
    png_stem = Path(str(png.get("name") or "")).stem.casefold()
    svg_candidates = [
        item
        for item in children
        if item.get("mimeType") in SVG_MIMES
        and str(item.get("name") or "").casefold().endswith(".svg")
        and Path(str(item.get("name") or "")).stem.casefold() == png_stem
    ]
    if len(svg_candidates) > 1:
        raise PipelineError("sidecar_svg_identity_ambiguous", permanent=True)
    svg_id = _valid_drive_id(svg_candidates[0].get("id")) if svg_candidates else None
    metadata = {
        "title": _row_value(row, ("Title", "ProductTitle", "DreamstimeTitle", "Headline")),
        "description": _row_value(row, ("Description", "ProductDescription", "Caption")),
        "category": _row_value(row, ("Category", "Categories", "DreamstimeCategory")),
        "keywords": _row_value(row, ("Keywords", "Tags", "SearchTerms")),
    }
    return candidate_from_record(
        {
            "record_type": "heeexy_catalog_product",
            "product_identity": f"drive:auto:{folder_id}",
            "metadata": metadata,
            "assets": {
                "png_drive_file_id": png_id,
                "svg_drive_file_id": svg_id,
            },
        }
    )  # type: ignore[return-value]


def discover_png_only_candidates(
    drive: DriveClient, root_id: str
) -> tuple[list[Candidate], Counter[str]]:
    stats: Counter[str] = Counter()
    candidates: dict[str, Candidate] = {}
    seen_sidecar_folders: set[str] = set()
    parent_cache: dict[str, tuple[str, ...]] = {}
    for sidecar in drive.list_named_files(METADATA_FILENAME):
        try:
            parents = [str(item) for item in sidecar.get("parents", [])]
            if len(parents) != 1 or not is_descendant_folder(
                drive, parents[0], root_id, parent_cache
            ):
                continue
            stats["sidecars_in_scope"] += 1
            folder_id = parents[0]
            if folder_id in seen_sidecar_folders:
                identity_digest = hashlib.sha256(
                    f"drive:auto:{folder_id}".encode("utf-8")
                ).hexdigest()
                candidates.pop(f"drive:catalog:{identity_digest}", None)
                stats["held_sidecar_duplicate"] += 1
                continue
            seen_sidecar_folders.add(folder_id)
            payload, _ = drive.download_bytes(
                str(sidecar["id"]), MAX_METADATA_BYTES, candidate_scoped=True
            )
            candidate = candidate_from_sidecar(
                payload,
                parents[0],
                drive.list_children(parents[0], candidate_scoped=True),
            )
            candidates[candidate.product_key] = candidate
        except PipelineError as error:
            if not error.permanent:
                raise
            stats[f"held_{error.code}"] += 1
    stats["accepted"] = len(candidates)
    return sorted(candidates.values(), key=lambda item: item.product_key), stats


def verify_png(path: Path, *, allow_large_delivery: bool = False) -> tuple[int, int]:
    """Check PNG format/chunk integrity without allocating decoded pixels.

    Larger originals are permitted only with a validated SVG twin. They are
    delivered unchanged; their pixels are never passed to the preview decoder.
    """
    try:
        from PIL import Image

        maximum_pixels = MAX_DELIVERY_IMAGE_PIXELS if allow_large_delivery else MAX_IMAGE_PIXELS
        Image.MAX_IMAGE_PIXELS = maximum_pixels
        with Image.open(path) as image:
            if image.format != "PNG":
                raise PipelineError("png_format_invalid", permanent=True)
            width, height = image.size
            if not png_dimensions_allowed(width, height, maximum_pixels):
                raise PipelineError("png_dimensions_invalid", permanent=True)
            image.verify()
    except PipelineError:
        raise
    except Exception as error:
        raise PipelineError("png_decode_failed", permanent=True) from error
    finally:
        # The normal raster decoder always retains the tighter guard.
        try:
            Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS
        except UnboundLocalError:
            pass
    return width, height


def png_dimensions_allowed(width: int, height: int, maximum_pixels: int = MAX_IMAGE_PIXELS) -> bool:
    return 0 < width <= 20_000 and 0 < height <= 20_000 and width * height <= maximum_pixels


def bounded_preview_source(png_path: Path, svg_path: Path | None, dimensions: tuple[int, int], work_dir: Path) -> Path:
    if dimensions[0] * dimensions[1] <= MAX_IMAGE_PIXELS:
        return png_path
    if svg_path is None:
        raise PipelineError("large_png_requires_svg_preview", permanent=True)
    preview_source = work_dir / "bounded-preview-source.png"
    render_svg(svg_path, preview_source, work_dir)
    return preview_source


def render_svg(svg_path: Path, png_path: Path, work_dir: Path) -> None:
    payload = svg_path.read_bytes()
    validate_svg_bytes(payload)
    environment = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "HOME": str(work_dir),
        "XDG_CACHE_HOME": str(work_dir / "cache"),
    }
    try:
        result = subprocess.run(
            [
                "rsvg-convert",
                "--width=4096",
                "--height=4096",
                "--keep-aspect-ratio",
                "--output",
                str(png_path),
                str(svg_path),
            ],
            cwd=work_dir,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PipelineError("svg_render_failed", permanent=True) from error
    if result.returncode != 0 or not png_path.is_file():
        raise PipelineError("svg_render_failed", permanent=True)
    verify_png(png_path)


def visible_content_bounds(image: Any) -> tuple[int, int, int, int]:
    """Return the complete non-transparent artwork bounds without clipping an edge pixel."""

    content_bounds = image.getchannel("A").getbbox()
    if content_bounds is None:
        raise PipelineError("png_content_empty", permanent=True)
    return content_bounds


def preview_padding(size: tuple[int, int]) -> int:
    """Reserve a visible 8% safety area around artwork of every aspect ratio."""

    return max(48, min(128, round(max(size) * 0.08)))


def create_previews(
    png_path: Path, work_dir: Path
) -> tuple[PreparedPreview, PreparedPreview, PreparedPreview]:
    try:
        from PIL import Image, ImageDraw, ImageOps

        Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS
        with Image.open(png_path) as source:
            source = ImageOps.exif_transpose(source).convert("RGBA")
            # Fit the visible artwork rather than the source canvas. This keeps
            # tiny artwork and unusual aspect ratios legible while preserving
            # every non-transparent edge pixel.
            source = source.crop(visible_content_bounds(source))
            source.thumbnail((PREVIEW_CONTENT_EDGE, PREVIEW_CONTENT_EDGE), Image.Resampling.LANCZOS)
            padding = preview_padding(source.size)
            canvas_size = (source.width + padding * 2, source.height + padding * 2)
            checkerboard = Image.new("RGBA", canvas_size, (*CHECKER_LIGHT, 255))
            checker_draw = ImageDraw.Draw(checkerboard)
            for top in range(0, canvas_size[1], CHECKER_SQUARE):
                for left in range(0, canvas_size[0], CHECKER_SQUARE):
                    if (left // CHECKER_SQUARE + top // CHECKER_SQUARE) % 2:
                        checker_draw.rectangle(
                            (
                                left,
                                top,
                                min(canvas_size[0] - 1, left + CHECKER_SQUARE - 1),
                                min(canvas_size[1] - 1, top + CHECKER_SQUARE - 1),
                            ),
                            fill=(*CHECKER_DARK, 255),
                        )
            outputs: list[PreparedPreview] = []
            backgrounds = (
                ("white", Image.new("RGBA", canvas_size, (*WHITE, 255))),
                ("dark", Image.new("RGBA", canvas_size, (*DARK_BROWN, 255))),
                ("transparency", checkerboard),
            )
            for label, canvas in backgrounds:
                canvas.alpha_composite(source, (padding, padding))
                path = work_dir / f"preview-{label}.webp"
                canvas.convert("RGB").save(
                    path,
                    format="WEBP",
                    quality=WEBP_QUALITY,
                    method=6,
                    exact=True,
                )
                outputs.append(
                    PreparedPreview(
                        path=path,
                        sha256=sha256_path(path),
                        size_bytes=path.stat().st_size,
                        width=canvas_size[0],
                        height=canvas_size[1],
                    )
                )
    except PipelineError:
        raise
    except Exception as error:
        raise PipelineError("preview_generation_failed", permanent=True) from error
    if len({preview.sha256 for preview in outputs}) != len(outputs):
        raise PipelineError("preview_variants_identical", permanent=True)
    return outputs[0], outputs[1], outputs[2]


def _deterministic_zip_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=ZIP_TIMESTAMP)
    info.create_system = 3
    info.compress_type = zipfile.ZIP_DEFLATED
    info._compresslevel = 6
    info.external_attr = 0o100644 << 16
    return info


def write_deterministic_zip_member(
    archive: zipfile.ZipFile,
    name: str,
    source: BinaryIO,
) -> int:
    written = 0
    with archive.open(_deterministic_zip_info(name), mode="w", force_zip64=True) as output:
        while chunk := source.read(1024 * 1024):
            written += len(chunk)
            output.write(chunk)
    return written


def validate_delivery_zip_archive(
    archive: zipfile.ZipFile,
    expected_members: Mapping[str, int],
) -> None:
    infos = archive.infolist()
    if [info.filename for info in infos] != list(expected_members):
        raise PipelineError("delivery_zip_members_invalid")
    for info in infos:
        unix_mode = (info.external_attr >> 16) & 0o170000
        if (
            info.is_dir()
            or unix_mode == 0o120000
            or (info.flag_bits & 0x1)
            or info.file_size != expected_members[info.filename]
        ):
            raise PipelineError("delivery_zip_members_invalid")
    if archive.testzip() is not None:
        raise PipelineError("delivery_zip_crc_invalid")


def create_delivery_zip(
    destination: Path,
    *,
    png_path: Path,
    svg_path: Path | None,
    slug: str,
) -> None:
    expected_members: dict[str, int] = {}
    try:
        with zipfile.ZipFile(
            destination,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
            strict_timestamps=True,
        ) as archive:
            with png_path.open("rb") as stream:
                expected_members[f"{slug}.png"] = write_deterministic_zip_member(
                    archive, f"{slug}.png", stream
                )
            if svg_path:
                with svg_path.open("rb") as stream:
                    expected_members[f"{slug}.svg"] = write_deterministic_zip_member(
                        archive, f"{slug}.svg", stream
                    )
        with zipfile.ZipFile(destination, mode="r") as archive:
            validate_delivery_zip_archive(archive, expected_members)
    except PipelineError:
        raise
    except (OSError, zipfile.BadZipFile, RuntimeError, zipfile.LargeZipFile) as error:
        raise PipelineError("delivery_zip_failed") from error
    if destination.stat().st_size < 1:
        raise PipelineError("delivery_zip_failed")


def validate_ingest_response_payload(payload: Any, expected_slug: str) -> None:
    if not isinstance(payload, Mapping) or payload.get("ok") is not True:
        raise PipelineError("catalog_ingest_response_invalid")
    product = payload.get("product")
    if (
        not isinstance(product, Mapping)
        or product.get("slug") != expected_slug
        or product.get("status") != "published"
        or not isinstance(product.get("id"), str)
        or not product.get("id")
    ):
        raise PipelineError("catalog_ingest_response_invalid")


def post_ingest(
    config: PipelineConfig,
    manifest: Mapping[str, Any],
    white: PreparedPreview,
    dark: PreparedPreview,
    transparency: PreparedPreview,
) -> None:
    try:
        import requests
    except ImportError as error:  # pragma: no cover
        raise PipelineError("http_dependency_missing") from error
    endpoint = f"{config.site_base_url}/api/admin/catalog/ingest"
    manifest_bytes = canonical_json(manifest).encode("utf-8")
    for attempt in range(5):
        try:
            with (
                white.path.open("rb") as white_stream,
                dark.path.open("rb") as dark_stream,
                transparency.path.open("rb") as transparency_stream,
            ):
                response = requests.post(
                    endpoint,
                    headers=(
                        ingest_auth_headers(
                            oidc_token=config.ingest_auth_provider.current_token(requests)
                        )
                        if isinstance(config.ingest_auth_provider, GitHubOidcTokenProvider)
                        else config.ingest_auth_provider.current_headers(requests)
                    ),
                    files={
                        "manifest": ("manifest.json", manifest_bytes, "application/json"),
                        "preview_white": ("preview-white.webp", white_stream, "image/webp"),
                        "preview_dark": ("preview-dark.webp", dark_stream, "image/webp"),
                        "preview_transparency": (
                            "preview-transparency.webp",
                            transparency_stream,
                            "image/webp",
                        ),
                    },
                    timeout=(15, 180),
                    allow_redirects=False,
                )
        except requests.RequestException as error:
            if attempt == 4:
                raise PipelineError("catalog_ingest_network_failed") from error
            time.sleep(2**attempt)
            continue
        if response.status_code in {200, 201}:
            try:
                payload = response.json()
            except (TypeError, ValueError) as error:
                raise PipelineError("catalog_ingest_response_invalid") from error
            validate_ingest_response_payload(payload, str(manifest.get("slug") or ""))
            return
        if response.status_code in {408, 425, 429, 500, 502, 503, 504} and attempt < 4:
            time.sleep(2**attempt)
            continue
        if response.status_code in {400, 409, 413, 415, 422}:
            raise PipelineError("catalog_ingest_rejected", permanent=True)
        if response.status_code in {401, 403}:
            raise PipelineError("catalog_ingest_unauthorized")
        raise PipelineError("catalog_ingest_failed")
    raise PipelineError("catalog_ingest_failed")


def prepare_and_publish_candidate(
    drive: DriveClient,
    config: PipelineConfig,
    candidate: Candidate,
    run_root: Path,
    source_metadata: Mapping[str, Mapping[str, Any]],
) -> None:
    public_metadata = public_site_metadata(candidate)
    with tempfile.TemporaryDirectory(prefix="product-", dir=run_root) as temp_name:
        work_dir = Path(temp_name)
        png_path = work_dir / "source.png"
        svg_path = work_dir / "source.svg"
        png_metadata: Mapping[str, Any] | None = None
        svg_metadata: Mapping[str, Any] | None = None
        archive_png = False
        archive_svg = False

        if candidate.source_archive_id:
            # Range extraction on the hosted runner avoids fetching the same
            # whole bundle separately for every product. Existing ZIP safety
            # guards still validate exact paths, sizes, encryption and CRCs.
            try:
                from scripts.cloud_catalog_archive_recovery import extract_selected
            except ImportError:
                from cloud_catalog_archive_recovery import extract_selected
            archive_metadata = source_metadata.get(candidate.source_archive_id) or drive.get_metadata(
                candidate.source_archive_id, candidate_scoped=True)
            if archive_metadata.get("id") != candidate.source_archive_id or archive_metadata.get("trashed"):
                raise PipelineError("archive_source_identity_invalid", permanent=True)
            if archive_metadata.get("mimeType") not in {
                ZIP_MIME,
                "application/x-zip-compressed",
                "application/octet-stream",
            }:
                raise PipelineError("archive_mime_invalid", permanent=True)
            archive_png, archive_svg = extract_selected(
                drive, archive_metadata,
                png_member=candidate.archive_png_member,
                svg_member=candidate.archive_svg_member,
                png_destination=png_path,
                svg_destination=svg_path,
            )
        else:
            if candidate.source_svg_id:
                svg_metadata = drive.download_to(
                    candidate.source_svg_id,
                    svg_path,
                    MAX_SVG_BYTES,
                    candidate_scoped=True,
                    expected_metadata=source_metadata.get(candidate.source_svg_id),
                )
                if svg_metadata.get("mimeType") not in SVG_MIMES:
                    raise PipelineError("svg_mime_invalid", permanent=True)
            if candidate.source_png_id:
                png_metadata = drive.download_to(
                    candidate.source_png_id,
                    png_path,
                    MAX_SOURCE_BYTES,
                    candidate_scoped=True,
                    expected_metadata=source_metadata.get(candidate.source_png_id),
                )
                if png_metadata.get("mimeType") != PNG_MIME:
                    raise PipelineError("png_mime_invalid", permanent=True)

        has_svg = bool(candidate.source_svg_id or archive_svg)
        has_png = bool(candidate.source_png_id or archive_png)
        if has_svg:
            validate_svg_bytes(svg_path.read_bytes())
        if not has_png and has_svg:
            render_svg(svg_path, png_path, work_dir)
        elif not has_png:
            raise PipelineError("source_asset_missing", permanent=True)

        width, height = verify_png(png_path, allow_large_delivery=has_svg)
        png_hash = sha256_path(png_path)
        svg_hash = sha256_path(svg_path) if has_svg else None
        preview_source = bounded_preview_source(png_path, svg_path if has_svg else None, (width, height), work_dir)
        white, dark, transparency = create_previews(preview_source, work_dir)
        product_folder_id = drive.ensure_product_folder(
            config.product_zips_folder_id, candidate.product_hash, candidate.slug
        )

        if candidate.source_png_id:
            source_png_id = candidate.source_png_id
            source_png_name = safe_drive_name(
                str((png_metadata or {}).get("name") or ""), ".png", candidate.slug
            )
        else:
            uploaded_png = drive.upload_content_addressed(
                png_path,
                parent_id=product_folder_id,
                name=f"{candidate.slug}-{png_hash[:12]}.png",
                mime_type=PNG_MIME,
                role=EXTRACTED_PNG_ROLE if archive_png else RENDERED_PNG_ROLE,
                product_hash=candidate.product_hash,
                content_sha256=png_hash,
            )
            source_png_id = str(uploaded_png["id"])
            source_png_name = f"{candidate.slug}.png"

        source_svg_id: str | None = None
        source_svg_name: str | None = None
        if candidate.source_svg_id:
            source_svg_id = candidate.source_svg_id
            source_svg_name = safe_drive_name(
                str((svg_metadata or {}).get("name") or ""), ".svg", candidate.slug
            )
        elif archive_svg:
            assert svg_hash is not None
            uploaded_svg = drive.upload_content_addressed(
                svg_path,
                parent_id=product_folder_id,
                name=f"{candidate.slug}-{svg_hash[:12]}.svg",
                mime_type="image/svg+xml",
                role=EXTRACTED_SVG_ROLE,
                product_hash=candidate.product_hash,
                content_sha256=svg_hash,
            )
            source_svg_id = str(uploaded_svg["id"])
            source_svg_name = f"{candidate.slug}.svg"

        delivery_path = work_dir / "delivery.zip"
        create_delivery_zip(
            delivery_path,
            png_path=png_path,
            svg_path=svg_path if has_svg else None,
            slug=candidate.slug,
        )
        delivery_hash = sha256_path(delivery_path)
        uploaded_zip = drive.upload_content_addressed(
            delivery_path,
            parent_id=product_folder_id,
            name=f"{candidate.slug}-{delivery_hash[:12]}.zip",
            mime_type=ZIP_MIME,
            role=DELIVERY_ROLE,
            product_hash=candidate.product_hash,
            content_sha256=delivery_hash,
        )

        manifest: dict[str, Any] = {
            "schema_version": CATALOG_SCHEMA_VERSION,
            "product_key": candidate.product_key,
            "slug": candidate.slug,
            "title": candidate.title,
            "description": public_metadata["description"],
            "category": public_metadata["category"],
            "formats": ["PNG", *(["SVG"] if has_svg else [])],
            "tags": public_metadata["tags"],
            "source_png": {
                "drive_file_id": source_png_id,
                "filename": source_png_name,
                "sha256": png_hash,
                "size_bytes": png_path.stat().st_size,
                "width": width,
                "height": height,
            },
            "delivery_zip": {
                "drive_file_id": str(uploaded_zip["id"]),
                "filename": f"{candidate.slug}.zip",
                "sha256": delivery_hash,
                "size_bytes": delivery_path.stat().st_size,
            },
            "previews": {
                "white": {
                    "background": "white",
                    "sha256": white.sha256,
                    "size_bytes": white.size_bytes,
                    "width": white.width,
                    "height": white.height,
                },
                "dark": {
                    "background": "dark_brown",
                    "sha256": dark.sha256,
                    "size_bytes": dark.size_bytes,
                    "width": dark.width,
                    "height": dark.height,
                },
                "transparency": {
                    "background": "checkerboard",
                    "sha256": transparency.sha256,
                    "size_bytes": transparency.size_bytes,
                    "width": transparency.width,
                    "height": transparency.height,
                },
            },
        }
        if source_svg_id:
            manifest["source_svg"] = {
                "drive_file_id": source_svg_id,
                "filename": source_svg_name,
                "sha256": svg_hash,
                "size_bytes": svg_path.stat().st_size,
            }
        post_ingest(config, manifest, white, dark, transparency)


def read_manifest_from_drive(
    drive: DriveClient, file_id: str, runner_temp: Path
) -> tuple[list[Candidate], Counter[str]]:
    with tempfile.TemporaryDirectory(prefix="manifest-", dir=runner_temp) as temp_name:
        manifest_path = Path(temp_name) / "catalog.jsonl"
        # Recovery may append while a batch loads its private snapshot. Retry
        # only revision/size races, always fetching fresh source metadata.
        for attempt in range(3):
            try:
                drive.download_to(file_id, manifest_path, MAX_MANIFEST_BYTES)
                break
            except PipelineError as error:
                if error.code not in {"drive_source_changed_during_download", "drive_download_size_mismatch"} or attempt == 2:
                    raise
                time.sleep(attempt + 1)
        try:
            with manifest_path.open("r", encoding="utf-8-sig", newline="") as stream:
                return parse_manifest_jsonl(stream)
        except UnicodeError as error:
            raise PipelineError("manifest_encoding_invalid") from error


def merge_candidates(groups: Sequence[Sequence[Candidate]]) -> tuple[list[Candidate], int]:
    merged: dict[str, Candidate] = {}
    conflicts = 0
    for group in groups:
        for candidate in group:
            prior = merged.get(candidate.product_key)
            if prior and prior.fingerprint != candidate.fingerprint:
                conflicts += 1
                continue
            merged[candidate.product_key] = candidate
    return sorted(merged.values(), key=lambda item: item.product_key), conflicts


def normalized_site_base_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise PipelineError("site_base_url_invalid")
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise PipelineError("site_base_url_invalid")
    return f"https://{parsed.netloc}"


def validated_ingest_oidc_token(value: str) -> str:
    token = value.strip()
    if len(token) > 16_384 or not OIDC_JWT_RE.fullmatch(token):
        raise PipelineError("catalog_ingest_oidc_token_invalid")
    return token


def unverified_oidc_expiry(token: str) -> int:
    validated = validated_ingest_oidc_token(token)
    encoded_payload = validated.split(".", maxsplit=2)[1]
    padded_payload = encoded_payload + "=" * (-len(encoded_payload) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded_payload).decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise PipelineError("catalog_ingest_oidc_token_invalid") from error
    expiry = payload.get("exp") if isinstance(payload, Mapping) else None
    if isinstance(expiry, bool) or not isinstance(expiry, int) or expiry < 1:
        raise PipelineError("catalog_ingest_oidc_token_invalid")
    return expiry


def validated_ingest_secret(value: str) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise PipelineError("catalog_ingest_secret_invalid")
    if (
        not INGEST_SECRET_RE.fullmatch(value)
        or len(set(value)) < INGEST_SECRET_MIN_DISTINCT_CHARS
    ):
        raise PipelineError("catalog_ingest_secret_invalid")
    return value


def ingest_auth_headers(
    *, oidc_token: str = "", ingest_secret: str = ""
) -> dict[str, str]:
    if ingest_secret:
        return {"Authorization": f"Bearer {validated_ingest_secret(ingest_secret)}"}
    return {"X-GitHub-OIDC-Token": validated_ingest_oidc_token(oidc_token)}


def build_ingest_auth_provider(
    environment: Mapping[str, str],
) -> GitHubOidcTokenProvider | StaticIngestSecretProvider:
    ingest_secret = environment.get("CATALOG_INGEST_SECRET", "")
    if ingest_secret:
        return StaticIngestSecretProvider(validated_ingest_secret(ingest_secret))
    ingest_oidc_token = validated_ingest_oidc_token(
        environment.get("GITHUB_ACTIONS_OIDC_TOKEN", "")
    )
    unverified_oidc_expiry(ingest_oidc_token)
    return GitHubOidcTokenProvider(ingest_oidc_token)


def require_drive_id(value: str, code: str) -> str:
    if not DRIVE_ID_RE.fullmatch(value):
        raise PipelineError(code)
    return value


def load_config(arguments: argparse.Namespace, runner_temp: Path) -> tuple[PipelineConfig, tuple[str, str, str]]:
    manifest_file_id = os.environ.get("HEEEXY_CATALOG_MANIFEST_DRIVE_FILE_ID", "").strip() or None
    png_only_root_id = os.environ.get("HEEEXY_PNG_ONLY_ROOT_FOLDER_ID", "").strip() or None
    if manifest_file_id:
        require_drive_id(manifest_file_id, "manifest_drive_id_invalid")
    if png_only_root_id:
        require_drive_id(png_only_root_id, "png_only_root_id_invalid")
    if not manifest_file_id and not png_only_root_id:
        raise PipelineError("catalog_source_not_configured")
    zips_id = require_drive_id(
        os.environ.get("HEEEXY_PRODUCT_ZIPS_FOLDER_ID", "").strip(),
        "product_zips_folder_id_invalid",
    )
    reports_id = require_drive_id(
        os.environ.get("HEEEXY_REPORTS_FOLDER_ID", "").strip(),
        "reports_folder_id_invalid",
    )
    ingest_auth_provider = build_ingest_auth_provider(os.environ)
    oauth = (
        os.environ.get("GOOGLE_DRIVE_OAUTH_CLIENT_ID", "").strip(),
        os.environ.get("GOOGLE_DRIVE_OAUTH_CLIENT_SECRET", "").strip(),
        os.environ.get("GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN", "").strip(),
    )
    if any(not value for value in oauth):
        raise PipelineError("drive_oauth_not_configured")
    return (
        PipelineConfig(
            manifest_file_id=manifest_file_id,
            png_only_root_id=png_only_root_id,
            product_zips_folder_id=zips_id,
            reports_folder_id=reports_id,
            site_base_url=normalized_site_base_url(
                os.environ.get("HEEEXY_SITE_BASE_URL", "")
            ),
            ingest_auth_provider=ingest_auth_provider,
            batch_size=arguments.batch_size,
            retry_holds=arguments.retry_holds,
            runner_temp=runner_temp,
            product_workers=arguments.workers,
        ),
        oauth,
    )


_PRODUCT_WORKER_DRIVE = None
_PRODUCT_WORKER_CONFIG = None


def publish_in_isolated_worker(config, oauth, candidate, run_root, source_metadata):
    """Two bounded cloud processes: separate Pillow globals, Drive and auth.

    Workers never write shared checkpoint state or leak exception payloads.
    A process-local client/token provider is reused across product jobs.
    """
    global _PRODUCT_WORKER_DRIVE, _PRODUCT_WORKER_CONFIG
    try:
        enforce_cloud_runtime()
        if _PRODUCT_WORKER_DRIVE is None:
            _PRODUCT_WORKER_DRIVE = DriveClient(*oauth)
            _PRODUCT_WORKER_CONFIG = config
        prepare_and_publish_candidate(
            _PRODUCT_WORKER_DRIVE, _PRODUCT_WORKER_CONFIG, candidate, run_root, source_metadata
        )
        return None
    except PipelineError as error:
        return (error.code, error.permanent, error.diagnostic)
    except Exception:
        return ("product_worker_failed", False, "")


def publish_outcomes(config, oauth, pending, run_root, drive):
    """Yield every started result; stop scheduling on the first transient error.

    Only one product per worker is ever in flight, not the whole 500-product batch.
    Already running successes are acknowledged before the parent saves state.
    """
    if config.product_workers == 1:
        for index, (candidate, _, source_metadata) in enumerate(pending):
            error = None
            try:
                prepare_and_publish_candidate(drive, config, candidate, run_root, source_metadata)
            except PipelineError as failure:
                error = failure
            except Exception:
                error = PipelineError("product_worker_failed")
            yield index, error
            if error and not error.permanent:
                return
        return
    workers = config.product_workers
    if workers < 2 or workers > MAX_PRODUCT_WORKERS:
        raise PipelineError("product_worker_count_invalid")
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as executor:
        in_flight = {}
        cursor = 0
        stopped = False
        while in_flight or (cursor < len(pending) and not stopped):
            while not stopped and cursor < len(pending) and len(in_flight) < workers:
                candidate, _, source_metadata = pending[cursor]
                try:
                    future = executor.submit(publish_in_isolated_worker, config, oauth, candidate, run_root, source_metadata)
                except Exception:
                    stopped = True
                    yield cursor, PipelineError("product_worker_failed")
                    break
                in_flight[future] = cursor
                cursor += 1
            if not in_flight:
                break
            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            outcomes = []
            for future in sorted(done, key=lambda item: in_flight[item]):
                index = in_flight.pop(future)
                try:
                    result = future.result()
                    error = PipelineError(result[0], permanent=result[1], diagnostic=result[2]) if result else None
                except Exception:
                    error = PipelineError("product_worker_failed")
                if error and not error.permanent:
                    stopped = True
                outcomes.append((index, error))
            yield from outcomes


def warm_candidate_metadata(client_factory, candidates, metadata_cache) -> None:
    """Bounded metadata-only lookahead; isolated HTTP clients, no artwork I/O.

    Failed prefetches deliberately remain uncached. The authoritative serial
    selector retries and classifies that candidate using its existing guards.
    """
    source_ids = sorted({source_id for candidate in candidates
                         for _, source_id in candidate_source_references(candidate)
                         if source_id not in metadata_cache})
    local = threading.local()
    def fetch(source_id):
        try:
            if not hasattr(local, "drive"):
                local.drive = client_factory()
            return source_id, local.drive.get_metadata(source_id, candidate_scoped=True)
        except PipelineError:
            return source_id, None
    with ThreadPoolExecutor(max_workers=4) as executor:
        for source_id, metadata in executor.map(fetch, source_ids):
            if metadata is not None:
                metadata_cache[source_id] = metadata


def checkpoint_coverage(candidates: Sequence[Candidate], state: Mapping[str, Any]) -> dict[str, Any]:
    """Account for this manifest only, never claim all of Drive is published."""
    keys = {candidate.product_key for candidate in candidates}
    completed = keys.intersection(state["completed"])
    held = keys.intersection(state["holds"]) - completed
    pending = keys - completed - held
    return {"manifest_candidates": len(keys), "checkpoint_completed": len(completed),
            "checkpoint_held": len(held), "checkpoint_unseen": len(pending),
            "hold_reasons": dict(Counter(state["holds"][key]["reason"] for key in held)),
            "status": "more_work" if pending else "manifest_has_holds" if held else "manifest_snapshot_settled",
            "full_library_complete": False}


def safe_progress(phase: str, processed: int, total: int) -> None:
    if phase not in {"select", "publish", "resume"}:
        raise ValueError("progress_phase_invalid")
    print(f"catalog_progress phase={phase} processed={int(processed)} total={int(total)}", file=sys.stderr, flush=True)


def batch_error_can_resume(error: PipelineError) -> bool:
    if error.permanent:
        return False
    if error.code == "catalog_ingest_network_failed":
        return True
    if error.code not in {"drive_metadata_failed", "drive_list_failed", "drive_download_failed",
                          "drive_upload_failed", "pipeline_state_write_failed"}:
        return False
    match = re.fullmatch(r"drive_http=(\d{3}|none) drive_reason=([a-z_,]+)", error.diagnostic)
    if not match:
        return False
    status, reasons = match.groups()
    reason_set = set(reasons.split(","))
    if reason_set & (OAUTH_DRIVE_REASONS | HARD_DRIVE_REASONS):
        return False
    return status in {"408", "425", "429"} or (status.isdigit() and int(status) >= 500) or bool(reason_set & TRANSIENT_DRIVE_REASONS)


def run_with_bounded_resume(config: PipelineConfig, oauth: tuple[str, str, str]) -> Counter[str]:
    for attempt in range(2):
        try:
            return run_pipeline(config, oauth)
        except PipelineError as error:
            if attempt or not batch_error_can_resume(error):
                raise
            safe_progress("resume", 1, 1)
            time.sleep(30)
    raise AssertionError("unreachable")


def run_pipeline(config: PipelineConfig, oauth: tuple[str, str, str]) -> Counter[str]:
    stats: Counter[str] = Counter()
    drive = DriveClient(*oauth)
    drive.assert_private_writable_folder(config.product_zips_folder_id)
    drive.assert_private_writable_folder(config.reports_folder_id)

    candidate_groups: list[list[Candidate]] = []
    if config.manifest_file_id:
        candidates, manifest_stats = read_manifest_from_drive(
            drive, config.manifest_file_id, config.runner_temp
        )
        candidate_groups.append(candidates)
        for key, value in manifest_stats.items():
            stats[f"manifest_{key}"] += value
    if config.png_only_root_id:
        discovered, discovery_stats = discover_png_only_candidates(
            drive, config.png_only_root_id
        )
        candidate_groups.append(discovered)
        for key, value in discovery_stats.items():
            stats[f"discovery_{key}"] += value
    candidates, conflicts = merge_candidates(candidate_groups)
    stats["candidate_conflicts"] = conflicts
    if conflicts:
        raise PipelineError("candidate_identity_conflict")
    state, state_file_id = drive.load_state(config.reports_folder_id)
    completed: dict[str, Any] = state["completed"]
    holds: dict[str, Any] = state["holds"]
    new_work_keys = {candidate.product_key for candidate in candidates
                     if candidate.product_key not in completed
                     and (candidate.product_key not in holds or config.retry_holds
                          or hold_has_untried_repair(holds[candidate.product_key]))}
    candidates = prioritize_unseen_candidates(
        candidates,
        completed,
        {key: value for key, value in holds.items() if not hold_has_untried_repair(value)},
        retry_holds=config.retry_holds,
    )
    pending: list[tuple[Candidate, str, dict[str, dict[str, Any]]]] = []
    source_metadata_cache: dict[str, dict[str, Any]] = {}
    transient_error: PipelineError | None = None
    safe_progress("select", 0, len(candidates))
    for candidate_index, candidate in enumerate(candidates):
        if pending and new_work_keys and candidate.product_key not in new_work_keys:
            # Publish a short new-work batch now. An explicit continuation flag
            # guarantees the old-source revision audit runs on a fresh batch.
            stats["verification_deferred"] = 1
            break
        if candidate_index % 128 == 0:
            warm_candidate_metadata(lambda: DriveClient(*oauth),
                                    candidates[candidate_index:candidate_index + 128], source_metadata_cache)
        if candidate_index and candidate_index % 250 == 0:
            safe_progress("select", candidate_index, len(candidates))
        try:
            fingerprint, source_metadata = resolve_candidate_artifact_fingerprint(
                drive, candidate, source_metadata_cache
            )
        except PipelineError as error:
            stats[f"failed_{error.code}"] += 1
            if error.permanent:
                holds[candidate.product_key] = {
                    "fingerprint": candidate.fingerprint,
                    "artifact_recipe_version": ARTIFACT_RECIPE_VERSION,
                    "reason": error.code,
                    "repair_version": HOLD_REPAIR_VERSIONS.get(error.code),
                    "held_at_epoch": int(time.time()),
                }
                completed.pop(candidate.product_key, None)
                stats["held"] += 1
                continue
            transient_error = error
            break
        if completed.get(candidate.product_key, {}).get("fingerprint") == fingerprint:
            stats["already_completed"] += 1
            continue
        if (
            not config.retry_holds
            and holds.get(candidate.product_key, {}).get("fingerprint") == fingerprint
            and not hold_has_untried_repair(holds.get(candidate.product_key, {}))
        ):
            stats["unchanged_hold"] += 1
            continue
        pending.append((candidate, fingerprint, source_metadata))
        if len(pending) == config.batch_size:
            break

    stats["selected"] = len(pending)
    if transient_error:
        state["updated_at_epoch"] = int(time.time())
        drive.save_state(config.reports_folder_id, state_file_id, state)
        raise transient_error
    safe_progress("publish", 0, len(pending))
    with tempfile.TemporaryDirectory(prefix="heeexy-run-", dir=config.runner_temp) as run_name:
        run_root = Path(run_name)
        processed = 0
        for index, error in publish_outcomes(config, oauth, pending, run_root, drive):
            candidate, fingerprint, _ = pending[index]
            if error is None:
                completed[candidate.product_key] = {
                    "fingerprint": fingerprint,
                    "artifact_recipe_version": ARTIFACT_RECIPE_VERSION,
                    "completed_at_epoch": int(time.time()),
                }
                holds.pop(candidate.product_key, None)
                stats["published"] += 1
            else:
                stats[f"failed_{error.code}"] += 1
                if error.permanent:
                    holds[candidate.product_key] = {
                        "fingerprint": fingerprint,
                        "artifact_recipe_version": ARTIFACT_RECIPE_VERSION,
                        "reason": error.code,
                        "repair_version": HOLD_REPAIR_VERSIONS.get(error.code),
                        "held_at_epoch": int(time.time()),
                    }
                    stats["held"] += 1
                else:
                    transient_error = transient_error or error
            processed += 1
            if processed % 10 == 0:
                safe_progress("publish", processed, len(pending))
                state["updated_at_epoch"] = int(time.time())
                state["coverage"] = checkpoint_coverage(candidates, state)
                state_file_id = drive.save_state(
                    config.reports_folder_id, state_file_id, state
                )
    state["updated_at_epoch"] = int(time.time())
    state["coverage"] = checkpoint_coverage(candidates, state)
    drive.save_state(config.reports_folder_id, state_file_id, state)
    safe_progress("publish", processed, len(pending))
    if transient_error:
        raise transient_error
    return stats


def parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--retry-holds", action="store_true")
    parser.add_argument("--workers", type=int, choices=tuple(range(1, MAX_PRODUCT_WORKERS + 1)), default=1)
    arguments = parser.parse_args(argv)
    if arguments.batch_size < 1 or arguments.batch_size > 500:
        raise PipelineError("batch_size_invalid")
    return arguments


def safe_summary(stats: Mapping[str, int]) -> str:
    allowed = {
        "selected",
        "published",
        "held",
        "already_completed",
        "unchanged_hold",
        "candidate_conflicts",
        "manifest_rows",
        "manifest_accepted",
        "manifest_ignored",
        "discovery_sidecars_in_scope",
        "discovery_accepted",
        "verification_deferred",
    }
    fields = [f"{key}={int(stats.get(key, 0))}" for key in sorted(allowed)]
    failed = sum(value for key, value in stats.items() if key.startswith("failed_"))
    fields.append(f"failed={failed}")
    return " ".join(fields)


def main(argv: Sequence[str] | None = None) -> int:
    # This guard deliberately precedes argument parsing and all I/O.  Even a
    # local --help invocation is refused by the asset-capable entry point.
    try:
        logging.getLogger("googleapiclient.http").setLevel(logging.CRITICAL)
        runner_temp = enforce_cloud_runtime()
        arguments = parse_arguments(list(argv if argv is not None else sys.argv[1:]))
        config, oauth = load_config(arguments, runner_temp)
        stats = run_with_bounded_resume(config, oauth)
        print(f"pipeline_status=ok {safe_summary(stats)}", flush=True)
        return 0
    except PipelineError as error:
        detail = f" {error.diagnostic}" if error.diagnostic else ""
        print(f"pipeline_status=failed code={error.code}{detail}", file=sys.stderr, flush=True)
        return 2
    except Exception:
        # Never print exception strings: Drive/HTTP libraries can embed URLs,
        # IDs, or source filenames in them.
        print("pipeline_status=failed code=unexpected_failure", file=sys.stderr, flush=True)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
