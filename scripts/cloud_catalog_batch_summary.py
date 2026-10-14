#!/usr/bin/env python3
"""Validate the pipeline's allowlisted one-line summary for workflow chaining."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys
from typing import Sequence


MAX_SUMMARY_BYTES = 2_048
SUMMARY_FIELDS = (
    "already_completed",
    "candidate_conflicts",
    "discovery_accepted",
    "discovery_sidecars_in_scope",
    "held",
    "manifest_accepted",
    "manifest_ignored",
    "manifest_rows",
    "published",
    "selected",
    "unchanged_hold",
    "verification_deferred",
    "failed",
)
SUMMARY_PATTERN = re.compile(
    r"^pipeline_status=ok "
    + " ".join(rf"{field}=(0|[1-9][0-9]*)" for field in SUMMARY_FIELDS)
    + r"(?:\r?\n)?$"
)


class BatchSummaryError(Exception):
    """A stable, non-sensitive validation error."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def parse_batch_size(value: str) -> int:
    if not re.fullmatch(r"[1-9][0-9]{0,2}", value):
        raise BatchSummaryError("batch_size_invalid")
    parsed = int(value)
    if parsed > 500:
        raise BatchSummaryError("batch_size_invalid")
    return parsed


def parse_summary(payload: bytes, batch_size: int) -> tuple[str, int, bool]:
    if not payload or len(payload) > MAX_SUMMARY_BYTES:
        raise BatchSummaryError("pipeline_summary_size_invalid")
    try:
        line = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise BatchSummaryError("pipeline_summary_encoding_invalid") from error
    match = SUMMARY_PATTERN.fullmatch(line)
    if match is None:
        raise BatchSummaryError("pipeline_summary_shape_invalid")
    values = {field: int(value) for field, value in zip(SUMMARY_FIELDS, match.groups())}
    selected = values["selected"]
    if selected > batch_size:
        raise BatchSummaryError("pipeline_selected_invalid")
    if (
        values["candidate_conflicts"] != 0
        or values["verification_deferred"] not in {0, 1}
        or (values["verification_deferred"] and selected == 0)
        or values["failed"] != values["held"]
        or values["published"] > selected
        # Permanent fingerprint failures are held before batch selection, so
        # total holds may legitimately exceed the selected-batch remainder.
        # Every selected item must still finish as either published or held.
        or values["published"] + values["held"] < selected
        or values["manifest_accepted"] + values["manifest_ignored"]
        != values["manifest_rows"]
        or selected
        > values["manifest_accepted"] + values["discovery_accepted"]
    ):
        raise BatchSummaryError("pipeline_summary_semantics_invalid")
    # Keep the historical workflow output name for compatibility. It now means
    # continuation required: a full batch OR a deferred source-revision audit.
    return line.rstrip("\r\n"), selected, selected == batch_size or bool(values["verification_deferred"])


def write_github_outputs(path: Path, selected: int, batch_size: int, batch_full: bool) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as output:
        output.write(f"selected={selected}\n")
        output.write(f"batch_size={batch_size}\n")
        # "yes"/"no", not "true": GitHub drops job outputs equal to a secret value.
        output.write(f"batch_full={'yes' if batch_full else 'no'}\n")


def parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--summary-file", type=Path, required=True)
    parser.add_argument("--github-output", type=Path, required=True)
    parser.add_argument("--batch-size", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        arguments = parse_arguments(list(argv if argv is not None else sys.argv[1:]))
        batch_size = parse_batch_size(arguments.batch_size)
        payload = arguments.summary_file.read_bytes()
        _, selected, batch_full = parse_summary(payload, batch_size)
        write_github_outputs(arguments.github_output, selected, batch_size, batch_full)
        print(
            "catalog_batch_status=ok "
            f"selected={selected} batch_full={'true' if batch_full else 'false'}",
            flush=True,
        )
        return 0
    except BatchSummaryError as error:
        print(
            f"pipeline_summary_status=failed code={error.code}",
            file=sys.stderr,
            flush=True,
        )
        return 2
    except Exception:
        print(
            "pipeline_summary_status=failed code=unexpected_failure",
            file=sys.stderr,
            flush=True,
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
