#!/usr/bin/env python3
"""Print a small, allowlisted summary of the private catalog report.

The report remains in Google Drive.  This entry point may run only on a
GitHub-hosted runner, reads the generated report into memory with a hard size
limit, and emits aggregate counters only.  Product hold rows, file identities,
hashes, and OAuth values are never included in its output.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from typing import Any

try:  # Imported as ``scripts.cloud_catalog_report_summary`` by unit tests.
    from scripts import cloud_catalog_manifest as manifest
    from scripts import cloud_catalog_pipeline as pipeline
except ImportError:  # Executed directly from the scripts directory.
    import cloud_catalog_manifest as manifest  # type: ignore[no-redef]
    import cloud_catalog_pipeline as pipeline  # type: ignore[no-redef]


MAX_REPORT_BYTES = 64 * 1024 * 1024
COUNTER_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
PUBLICATION_INTEGER_FIELDS = (
    "expected_min_products",
    "expected_min_accepted",
    "inventory_shortfall",
    "accepted_shortfall",
    "blocking_issue_count",
    "inventory_blockers",
    "metadata_file_blockers",
)


class ReportSummaryError(RuntimeError):
    """Sanitized failure with a stable code safe for CI logs."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReportSummaryError(code)
    return value


def _nonnegative_integer(value: Any, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReportSummaryError(code)
    return value


def _numeric_counters(value: Any, code: str) -> dict[str, int]:
    source = _mapping(value, code)
    counters: dict[str, int] = {}
    for key, raw_value in source.items():
        if not isinstance(key, str) or not COUNTER_NAME_RE.fullmatch(key):
            raise ReportSummaryError(code)
        counters[key] = _nonnegative_integer(raw_value, code)
    return dict(sorted(counters.items()))


def parse_report_summary(payload: bytes) -> dict[str, Any]:
    """Parse one report and return only the approved aggregate fields."""

    if not payload or len(payload) > MAX_REPORT_BYTES:
        raise ReportSummaryError("report_size_invalid")
    try:
        report = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ReportSummaryError("report_json_invalid") from error
    root = _mapping(report, "report_shape_invalid")

    generated_at = root.get("generated_at")
    if (
        not isinstance(generated_at, str)
        or not generated_at
        or len(generated_at) > 64
        or any(ord(character) < 32 or ord(character) == 127 for character in generated_at)
    ):
        raise ReportSummaryError("report_generated_at_invalid")

    manifest_section = _mapping(root.get("manifest"), "report_manifest_invalid")
    reconciliation = _mapping(
        root.get("reconciliation"), "report_reconciliation_invalid"
    )
    publication = _mapping(
        root.get("publication_gate"), "report_publication_gate_invalid"
    )

    eligible = publication.get("eligible")
    complete_coverage = publication.get("complete_inventory_coverage")
    complete_seed_disposition = publication.get(
        "complete_seed_disposition", complete_coverage
    )
    full_catalog_complete = publication.get(
        "full_catalog_complete", complete_coverage
    )
    pilot_manifest_eligible = publication.get("pilot_manifest_eligible", eligible)
    if (
        not isinstance(eligible, bool)
        or not isinstance(complete_coverage, bool)
        or not isinstance(complete_seed_disposition, bool)
        or not isinstance(full_catalog_complete, bool)
        or not isinstance(pilot_manifest_eligible, bool)
    ):
        raise ReportSummaryError("report_publication_gate_invalid")

    publication_summary: dict[str, Any] = {
        "eligible": eligible,
        "complete_inventory_coverage": complete_coverage,
        "complete_seed_disposition": complete_seed_disposition,
        "full_catalog_complete": full_catalog_complete,
        "pilot_manifest_eligible": pilot_manifest_eligible,
    }
    for field in PUBLICATION_INTEGER_FIELDS:
        publication_summary[field] = _nonnegative_integer(
            publication.get(field), "report_publication_gate_invalid"
        )
    inventory_section = _mapping(root.get("inventory"), "report_inventory_invalid")
    legacy_products = inventory_section.get("products_discovered", 0)
    optional_integer_defaults = {
        "global_inventory_target": publication_summary["expected_min_products"],
        "curated_seed_target": publication_summary["expected_min_accepted"],
        "global_inventory_assets": inventory_section.get(
            "global_inventory_assets", legacy_products
        ),
        "authoritative_seeds": inventory_section.get(
            "authoritative_seeds", legacy_products
        ),
        "curated_seed_shortfall": publication_summary["accepted_shortfall"],
        "global_product_target": publication.get(
            "global_inventory_target", publication_summary["expected_min_products"]
        ),
        "curated_seed_count": publication.get(
            "authoritative_seeds", legacy_products
        ),
        "global_product_gap": publication_summary["inventory_shortfall"],
        # Legacy reports predate the reviewed/unresolved classification.  Be
        # fail-closed: an old hold is unresolved until a new builder report
        # explicitly proves otherwise.
        "reviewed_holds": 0,
        "reviewed_hold_limit": 0,
        "unresolved_holds": reconciliation.get("held", 0),
        "cross_product_collisions": 0,
    }
    for field, default in optional_integer_defaults.items():
        publication_summary[field] = _nonnegative_integer(
            publication.get(field, default), "report_publication_gate_invalid"
        )

    return {
        "generated_at": generated_at,
        "manifest": {
            "accepted_products": _nonnegative_integer(
                manifest_section.get("accepted_products"), "report_manifest_invalid"
            ),
            "size_bytes": _nonnegative_integer(
                manifest_section.get("size_bytes"), "report_manifest_invalid"
            ),
        },
        "inventory": _numeric_counters(
            root.get("inventory"), "report_inventory_invalid"
        ),
        "metadata": _numeric_counters(root.get("metadata"), "report_metadata_invalid"),
        "reconciliation": {
            "accepted": _nonnegative_integer(
                reconciliation.get("accepted"), "report_reconciliation_invalid"
            ),
            "held": _nonnegative_integer(
                reconciliation.get("held"), "report_reconciliation_invalid"
            ),
            "hold_counts": _numeric_counters(
                reconciliation.get("hold_counts"), "report_reconciliation_invalid"
            ),
            "unmatched_metadata_rows": _nonnegative_integer(
                reconciliation.get("unmatched_metadata_rows"),
                "report_reconciliation_invalid",
            ),
            "ambiguous_metadata_rows": _nonnegative_integer(
                reconciliation.get("ambiguous_metadata_rows"),
                "report_reconciliation_invalid",
            ),
            "warnings": _numeric_counters(
                reconciliation.get("warnings"), "report_reconciliation_invalid"
            ),
        },
        "publication_gate": publication_summary,
    }


def load_summary_config(
    environment: Mapping[str, str],
) -> tuple[str, tuple[str, str, str]]:
    reports_folder_id = manifest.valid_drive_id(
        environment.get("HEEEXY_REPORTS_FOLDER_ID")
    )
    if not reports_folder_id:
        raise ReportSummaryError("reports_folder_id_invalid")
    oauth = (
        manifest.normalize_text(environment.get("GOOGLE_DRIVE_OAUTH_CLIENT_ID")),
        manifest.normalize_text(environment.get("GOOGLE_DRIVE_OAUTH_CLIENT_SECRET")),
        manifest.normalize_text(environment.get("GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN")),
    )
    if any(not value for value in oauth):
        raise ReportSummaryError("drive_oauth_not_configured")
    return reports_folder_id, oauth


def read_report_summary(
    reports_folder_id: str,
    oauth: tuple[str, str, str],
    *,
    drive_factory: Callable[..., manifest.ManifestDriveClient] = manifest.ManifestDriveClient,
) -> dict[str, Any]:
    drive = drive_factory(*oauth)
    payload = drive.read_generated_bytes(
        parent_id=reports_folder_id,
        role=manifest.REPORT_ROLE,
        maximum_bytes=MAX_REPORT_BYTES,
    )
    if payload is None:
        raise ReportSummaryError("generated_report_not_found")
    return parse_report_summary(payload)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        # Keep this first so local invocation cannot authenticate or access Drive.
        pipeline.enforce_cloud_runtime()
        if list(argv if argv is not None else sys.argv[1:]):
            raise ReportSummaryError("arguments_not_supported")
        reports_folder_id, oauth = load_summary_config(os.environ)
        summary = read_report_summary(reports_folder_id, oauth)
        publication_gate = _mapping(
            summary.get("publication_gate"), "report_publication_gate_invalid"
        )
        eligible = publication_gate.get("eligible")
        if not isinstance(eligible, bool):
            raise ReportSummaryError("report_publication_gate_invalid")
        print(
            "manifest_report_summary_status=ok "
            f"publication_eligible={'true' if eligible else 'false'}",
            flush=True,
        )
        return 0
    except (ReportSummaryError, manifest.ManifestBuilderError, pipeline.PipelineError) as error:
        print(
            "manifest_report_summary_status=failed "
            f"code={getattr(error, 'code', 'report_summary_failure')}",
            file=sys.stderr,
            flush=True,
        )
        return 2
    except Exception:
        print(
            "manifest_report_summary_status=failed code=unexpected_failure",
            file=sys.stderr,
            flush=True,
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
