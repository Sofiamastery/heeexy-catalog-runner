# Catalog recovery and bounded resume

## October 11 recovery changes

- `cloud_catalog_svg_recovery.py` adds **loose SVGs only** from a SHA-256-pinned private metadata JSONL ledger. Configure `SVG_RECOVERY_LEDGER_ID` and `SVG_RECOVERY_LEDGER_SHA256` as production environment secrets. They are optional together, never public variables.
- Each new row must have an exact source ID, complete existing title/description/tags/category, and the recognized previously screened human-original cohort evidence. `needs_screen`, missing evidence, and explicit rights blocks remain held. Archive paths are not guessed. No metadata is generated.
- Recovery retrieves Drive **metadata** for each eligible source, validates its SVG role, and uses the existing manifest contract. The asset-capable hosted batch validates/renders/packages it later. A rendered PNG accompanies a standalone SVG.
- The manifest update is append-only and rechecks the original manifest bytes before publication. Existing product identities and metadata stay intact. The incomplete full-rebuild SVG workbook changes are a separate, unpublished workstream.
- The private recovery report includes SVG row dispositions and hold reasons. It explicitly keeps `full_library_complete=false`: completing an input ledger is not proof that all source folders or unresolved archive identities are covered.
- The pipeline emits counts-only selection progress every 250 candidates and publication progress every 10 completed outcomes on stderr. The strict stdout batch summary remains unchanged.
- Selection prefetches metadata for at most 128 candidates at a time with four isolated HTTP clients. It does not download artwork or bypass source fingerprint validation. Failed lookups remain uncached for the original per-candidate failure handling; repeated source IDs are fetched once per cache.
- A short new-work batch publishes before scanning all old source revisions. `verification_deferred=1` requests the mandatory follow-up audit. For compatibility the workflow output is still named `batch_full`, but its boolean now means continuation required. With no new work, the full revision pass runs normally and ends without an endless empty loop. New and old code/summary versions must be deployed together.
- Private checkpoint coverage distinguishes `more_work`, `manifest_has_holds`, and `manifest_snapshot_settled`, with counts for this manifest snapshot only. It is not a live revision audit or a whole-Drive completion claim.
- A batch resumes once from its checkpoint after a verified transient Drive status (408/425/429/5xx or known transient reason) or an ingest network failure. It waits 30 seconds. Permission/authentication, validation and unknown failures are not blindly retried. Provider-level retries remain bounded. No paid runners or local asset processing are introduced.

## Verification and operations

Run the pure recovery, resume, summary and safety tests locally with `python -B`. Artwork-generating integration tests run on the hosted runner only. Recovery remains serialized with other manifest writers. The published batch manifest is a snapshot; a concurrent append is picked up by the next bounded drain.

Dispatch `catalog-recovery.yml` with `publish=true`, `diagnose_holds=true`, `start_drain=true` after reviewing the pinned ledger and confirming no competing recovery is active. Confirm the private report and actual ingest job, not just a green workflow or a site counter. Scheduled discovery remains every six hours. Monitor an active drain separately, without canceling work or dispatching duplicates.

## Exact archive recovery

- `cloud_catalog_archive_recovery.py` resolves the existing full canonical path against the actual ZIP central directory. It never guesses a basename, appends an extension to an abbreviated key, or treats a similarly named member as the source.
- The distinct historical `archive_screen_terminal; local=clear` cohort records a completed strict render/OCR IP screen, as documented by the original archive-screening implementation. It is not a new visual-model approval. Only the exact known evidence string, human-original provenance and complete original metadata are accepted. `needs_screen`, absent evidence, rights blocks and partial metadata remain held.
- The pinned ledger and private report retain original provenance. Exact archive/member claims prevent duplicate products on repeated discovery. Existing manifest records remain unchanged.
- HTTP Range reads require a valid 206 response and exact Content-Range; byte budgets, entry limits, safe paths, encrypted/symlink rejection, member sizes and compression-ratio guards remain enforced. Archive metadata is compared again after reading. Whole archives are not downloaded per product. The cloud asset pipeline extracts the exact SVG with ZIP CRC validation, validates XML and renders its PNG; it does not guess an unverified PNG twin.
- All archive I/O is guarded by the hosted-runner restriction. Pure tests use memory-only text fixtures. No artwork is read or rendered on the owner's laptop.
- Recovery still starts one automatically continuing drain at batch size 500. This is a single requested queue, with internal restart checkpoints, not manual batches of 25. It is deliberately bounded by the hosted runner's execution limit.

Still required: unresolved archive paths, incomplete metadata/screening dispositions, investigation of remaining invalid PNG sources, and a zero-loss reconciliation against the historical library target. Never lower that target to make the current manifest appear complete.
