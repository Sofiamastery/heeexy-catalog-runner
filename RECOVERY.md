# Append-only catalog recovery

`catalog-recovery.yml` is a separate, manually dispatched recovery workflow.
It uses the same repository authorization, protected production environment,
one-day retention and concurrency group as the batch importer. No assets,
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

The private report records the previous manifest's modification timestamp,
files created after that baseline, new valid candidates, unresolved PNG file
identities, checkpoint counts, and detailed sidecar holds. Raw PNG file count
is not a unique product count: historical folders contain renditions/previews.

`diagnose_holds=true` inspects at most three source files for each known error
inside the hosted runner's temporary directory. Its diagnostic report stays
in the private Drive reports folder. No SVG security checks or PNG validation
are disabled. Product files are never downloaded to an owner's workstation.

With `publish=true` and `start_drain=true`, a successful recovery starts the
existing batch workflow with size 100, no full manifest rebuild, and no blind
retry of held records. It safely resumes existing completed checkpoints.

Do not enable an older full-rebuild schedule without reconciling the added
identities into its input ledgers: its no-loss check must reject any proposal
that would remove previously published identities.

Code-only verification:

```sh
python -B -m unittest scripts/cloud_catalog_recovery_test.py
```
