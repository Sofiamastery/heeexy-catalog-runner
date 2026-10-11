# Catalog recovery and bounded resume

## October 11 recovery changes

- `cloud_catalog_svg_recovery.py` adds **loose SVGs only** from a SHA-256-pinned private metadata JSONL ledger. Configure `SVG_RECOVERY_LEDGER_ID` and `SVG_RECOVERY_LEDGER_SHA256` as production environment secrets. They are optional together, never public variables.
- Each new row must have an exact source ID, complete existing title/description/tags/category, and the recognized previously screened human-original cohort evidence. `needs_screen`, missing evidence, and explicit rights blocks remain held. Archive paths are not guessed. No metadata is generated.
- Recovery retrieves Drive **metadata** for each eligible source, validates its SVG role, and uses the existing manifest contract. The asset-capable hosted batch validates/renders/packages it later. A rendered PNG accompanies a standalone SVG.
- The manifest update is append-only and rechecks the original manifest bytes before publication. Existing product identities and metadata stay intact. The incomplete full-rebuild SVG workbook changes are a separate, unpublished workstream.
- The private recovery report includes SVG row dispositions and hold reasons. It explicitly keeps `full_library_complete=false`: completing an input ledger is not proof that all source folders or unresolved archive identities are covered.
- The pipeline emits counts-only selection progress every 250 candidates and publication progress every 10 completed outcomes on stderr. The strict stdout batch summary remains unchanged.
- Selection prefetches metadata for at most 128 candidates at a time with four isolated HTTP clients. It does not download artwork or bypass source fingerprint validation. Failed lookups remain uncached for the original per-candidate failure handling; repeated source IDs are fetched once per cache.
- Private checkpoint coverage distinguishes `more_work`, `manifest_has_holds`, and `manifest_snapshot_settled`, with counts for this manifest snapshot only. It is not a live revision audit or a whole-Drive completion claim.
- A batch resumes once from its checkpoint after a verified transient Drive status (408/425/429/5xx or known transient reason) or an ingest network failure. It waits 30 seconds. Permission/authentication, validation and unknown failures are not blindly retried. Provider-level retries remain bounded. No paid runners or local asset processing are introduced.

## Verification and operations

Run the pure recovery, resume, summary and safety tests locally with `python -B`. Artwork-generating integration tests run on the hosted runner only. Recovery remains serialized with other manifest writers. The published batch manifest is a snapshot; a concurrent append is picked up by the next bounded drain.

Dispatch `catalog-recovery.yml` with `publish=true`, `diagnose_holds=true`, `start_drain=true` after reviewing the pinned ledger and confirming no competing recovery is active. Confirm the private report and actual ingest job, not just a green workflow or a site counter. Scheduled discovery remains every six hours. Monitor an active drain separately, without canceling work or dispatching duplicates.

Still required: exact archive-member reconciliation, unresolved metadata/screening dispositions, investigation of remaining invalid PNG sources, and a zero-loss reconciliation against the historical library target. Never lower that target to make the current manifest appear complete.
