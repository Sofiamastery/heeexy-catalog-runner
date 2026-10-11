# Append-only catalog recovery

`catalog-recovery.yml` is a separate recovery workflow, manually dispatchable
and scheduled every six hours after owner-approved automatic discovery.
It uses the same repository authorization, protected production environment,
one-day retention as the batch importer. Metadata writers share a separate
concurrency lock, allowing discovery to overlap the current upload batch.
Ingest checkpoints still have a single serialized writer. A batch consumes a
validated snapshot and retries a manifest revision race at most three times.
No assets,
private metadata or credentials belong in this repository.

Run an audit with `publish=false` before reviewing unfamiliar metadata formats.
The owner-approved publish mode preserves every existing manifest object and
appends only uniquely identified, contract-valid products. It does not mark
the historical catalog complete and does not clear pipeline holds.

Discovery walks every descendant folder of the configured PNG-only root using
four metadata readers. It supports the established exact-parent CSV sidecar
contract and a JSON sidecar with an explicit Etsy filename and original
metadata. JSON matching requires an exact same-parent PNG filename and a
matching canonical SHA-256 from Drive. A bare folder title is never metadata.
Explicit rights blocks, ambiguous identities and missing metadata stay held.

Historical CSV adapters recognize dated explicit owner approvals and a
completed final-visual-QA schema, with exact filename, Drive ID, byte size and
SHA-256 matching. They preserve original copy and do not fabricate marketplace
publication status. Unknown or unfinished variants remain held.

The private report records the previous manifest's modification timestamp,
files created after that baseline, new valid candidates, unresolved PNG file
identities, checkpoint counts, and detailed sidecar holds. Raw PNG file count
is not a unique product count: historical folders contain renditions/previews.

`diagnose_holds=true` inspects at most three source files for each known error
inside the hosted runner's temporary directory. Its diagnostic report stays
in the private Drive reports folder. No SVG security checks or PNG validation
are disabled. Product files are never downloaded to an owner's workstation.

With `publish=true` and `start_drain=true`, a successful recovery starts the
existing batch workflow with size 500, no full manifest rebuild, and no blind
retry of held records. It safely resumes existing completed checkpoints.
Two isolated product processes overlap rendering and network I/O on the free
hosted runner. Each has its own Pillow state, Drive client and authentication
provider. At most two products are in flight; only the parent writes the
checkpoint, every ten outcomes and at batch end. On a transient error it stops
scheduling, acknowledges already-running successes, saves, then fails safely.
The CLI default remains one process; `--workers 1` is the rollback switch.
Scheduled runs use the same append-only publication path and trigger a drain
only when verified additions or unprocessed/repaired candidates remain. The
schedule is not an exact-time service-level guarantee; GitHub may delay it.
Disable this workflow in Actions to pause automatic discovery. Do not enable
the old `SCHEDULE_ENABLED` full-builder gate as a substitute.

Diagnosed held SVGs permit C2PA provenance only inside inert SVG metadata;
external references, scripts and foreign render-tree nodes remain forbidden.
Very large originals with a validated SVG twin may be delivered up to 400 MP
and 20,000 pixels/side, unchanged. Their public previews use a bounded SVG
render, never a full decoded original canvas. CRC/header verification remains
enabled, and PNG-only raster inputs retain the tighter 100 MP limit. The site
must have the matching ingest contract deployed before enabling this producer.

Do not enable an older full-rebuild schedule without reconciling the added
identities into its input ledgers: its no-loss check must reject any proposal
that would remove previously published identities.

Code-only verification:

```sh
python -B -m unittest scripts/cloud_catalog_recovery_test.py
```
