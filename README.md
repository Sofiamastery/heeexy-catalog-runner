# Public catalog runner

This repository contains only the reviewed GitHub Actions runner for a private,
cloud-to-cloud catalog pipeline. Product assets and private catalog data are not
stored here. On schedule, a metadata-only job scans the configured F16 and PNG
Only Drive trees, reads the private metadata imports, and refreshes the private
manifest only after its no-loss publication gate succeeds. A separate ingest
job then reads one bounded manifest-index batch, creates the delivery ZIP and
three WebP previews in its temporary filesystem, uploads the private ZIP to
Drive, publishes the previews through the site's authenticated ingest endpoint,
then is destroyed.

The metadata job lists product files but never downloads PNG, SVG, or delivery
ZIP bytes. Only the small private metadata ledgers/imports and generated
manifest/report are read or written there. The ingest job is the only job that
temporarily handles product bytes, and those bytes remain on a fresh hosted
runner rather than the workstation.

The workflow deliberately has no `pull_request` or `pull_request_target` trigger.
Forks receive no environment secrets, and the first step also requires the
`EXPECTED_REPOSITORY` secret to match `github.repository` exactly. Manual runs
are accepted only from `main`.

## Why the repository must stay sanitized

GitHub-hosted standard runners can be used for a public-repository workflow
without putting the private product library in the repository. Keep this
repository code-only. Never commit product images, ZIP files, CSV/XLSX/JSONL
metadata, Drive IDs, OAuth material, ingest credentials, account identifiers,
or a private site URL.

The `.gitignore` blocks common asset and metadata formats as a second line of
defense. The repository-safety unit test also rejects forbidden workflow
triggers, unpinned actions, repository variables, and committed catalog-data
extensions.

## Required repository settings

Before the first run:

1. Create a **public** GitHub repository from this code-only directory.
2. Under **Settings → Actions → General**, set workflow permissions to
   read-only by default and set artifact/log retention to **1 day**.
3. Protect `main`: require review before merge, block force-pushes and deletion,
   and restrict who can push.
4. Keep Actions restricted to trusted/pinned actions. The workflow pins both
   `actions/checkout` and `actions/setup-python` to full commit SHAs.
5. Create a GitHub Actions environment named exactly `production-catalog`,
   restrict it to the protected `main` branch, and add every value below as an
   **environment secret**. Do not use repository variables or repository-level
   secrets for private IDs, URLs, hashes, or account configuration.
6. Leave `SCHEDULE_ENABLED` set to `false` until one manual batch succeeds.

Required `production-catalog` environment secrets:

| Secret | Purpose |
| --- | --- |
| `EXPECTED_REPOSITORY` | Exact `OWNER/REPOSITORY` authorization value |
| `ACTIONS_RETENTION_DAYS` | Must be exactly `1`, matching repository settings |
| `SCHEDULE_ENABLED` | `true` only after the scheduled runner is approved |
| `DEFAULT_BATCH_SIZE` | Scheduled batch size: `25`, `50`, `100`, `250`, or `500` |
| `CATALOG_MANIFEST_DRIVE_FILE_ID` | Existing private manifest-index Drive file ID; refresh must update this exact file |
| `METADATA_IMPORT_FOLDER_ID` | Private metadata-import folder scope |
| `CATALOG_METADATA_FOLDER_ID` | Private folder containing the generated manifest |
| `PRODUCT_ZIPS_FOLDER_ID` | Private destination folder for delivery ZIPs |
| `REPORTS_FOLDER_ID` | Private folder containing resumable pipeline state |
| `F16_ROOT_FOLDER_ID` | F16 Drive inventory root |
| `PNG_ONLY_ROOT_FOLDER_ID` | PNG Only Drive inventory root |
| `EXPECTED_MIN_PRODUCT_COUNT` | Reviewed minimum inventory gate |
| `EXPECTED_MIN_ACCEPTED_COUNT` | Reviewed minimum accepted-product gate |
| `CATALOG_SOURCE_NAMESPACE` | Stable private identity namespace |
| `DREAMSTIME_LEDGER_ZIP_DRIVE_FILE_ID` | Pinned primary metadata-ledger file ID |
| `DREAMSTIME_LEDGER_ZIP_NAME` | Exact private primary-ledger filename |
| `DREAMSTIME_LEDGER_ZIP_SHA256` | Reviewed SHA-256 of the primary ledger ZIP |
| `DREAMSTIME_LEDGER_MEMBER_SHA256` | Reviewed SHA-256 of the ledger member |
| `AI_PNG_METADATA_JSONL_DRIVE_FILE_ID` | Pinned PNG Only metadata-ledger file ID |
| `AI_PNG_METADATA_JSONL_NAME` | Exact private supplemental-ledger filename |
| `AI_PNG_METADATA_JSONL_SHA256` | Reviewed SHA-256 of the PNG Only metadata ledger |
| `SITE_BASE_URL` | HTTPS origin of the ingest service |
| `GOOGLE_DRIVE_OAUTH_CLIENT_ID` | Google OAuth client ID |
| `GOOGLE_DRIVE_OAUTH_CLIENT_SECRET` | Google OAuth client secret |
| `GOOGLE_DRIVE_OAUTH_REFRESH_TOKEN` | Google OAuth refresh token |

The ingest request uses a short-lived GitHub Actions OIDC token with the fixed
audience `heeexy-catalog-ingest`; there is no long-lived ingest secret in this
repository. The token is requested directly from GitHub immediately before the
batch, kept only in shell/process memory, masked defensively, and sent in the
`X-GitHub-OIDC-Token` header. It is never written to a file or log.
For a long batch, the runner refreshes the token in memory shortly before its
expiry through GitHub's built-in OIDC endpoint; the audience never changes.

GitHub masking is not a substitute for the allowlisted log design in this
runner. The asset-capable script emits only a small aggregate success line or a
stable error code. It never prints filenames, Drive IDs, response bodies,
exception text, tokens, or signed URLs.

## Running the catalog

Use **Actions → Private catalog batch → Run workflow** on `main`.

- Start with 25 products, keep `drain_catalog` off, and enable
  `rebuild_manifest` for the first pilot.
- `rebuild_manifest` performs a metadata-only cloud scan before ingest. The
  scheduled run always rebuilds. A new PNG Only product is discovered only when
  one folder contains exactly one PNG, at most one same-name SVG, and one exact
  `design_metadata_and_specs.csv` sidecar in that same folder. The sidecar must
  explicitly approve the release, bind the exact PNG filename, and provide the
  existing Etsy title, description, and tags. Missing, duplicate, ambiguous, or
  malformed sidecars are held; the builder never invents SEO from filenames or
  matches metadata across folders.
- After verifying the site, run another batch or enable `drain_catalog`.
- Drain mode dispatches another fresh, bounded run only when the validated
  summary says the prior batch was full. A partial batch stops the chain.
- Scheduled runs intentionally process only one bounded batch every six hours;
  they never opt into drain mode. Each scheduled run refreshes the private
  manifest first.
- Set `retry_holds` only when a previously reviewed hold should be retried.

The manifest refresh is fail-closed. It pins the exact existing manifest Drive
ID, validates the previous manifest, and refuses publication if any prior
product identity would disappear. IDs, hashes, metadata rows, product titles,
filenames, and report counts are never printed. The public log receives only a
generic builder success line and the publication-eligible boolean.

The metadata scan downloads only bounded CSV/ledger bytes. It lists PNG/SVG
metadata but never downloads product asset bytes. For incremental sidecars it
preserves the reviewed title and tags and deterministically removes only the
known AI-disclosure / Etsy-protection / mega-bundle promotional suffix from the
description; it does not regenerate copy.

The resumable state lives in the private Drive reports folder. A stopped or
failed run does not require starting over, and already-completed fingerprints
are skipped.

## Local verification (code only)

Unit tests import pure helpers and do not call the guarded asset entry point:

```bash
python -m pip install -r requirements.txt
python -B -m unittest \
  scripts/cloud_catalog_pipeline_test.py \
  scripts/cloud_catalog_batch_summary_test.py \
  scripts/cloud_catalog_manifest_test.py \
  scripts/cloud_catalog_report_summary_test.py \
  scripts/repository_safety_test.py
```

Do not run `scripts/cloud_catalog_pipeline.py` on a workstation. Its runtime
attestation rejects non-GitHub-hosted environments before argument parsing or
asset I/O.

## Retention and incident cleanup

This workflow does not use caches or upload artifacts. GitHub Actions logs must
still be configured for one-day retention. If a secret is ever exposed, disable
the schedule, cancel active runs, rotate the affected long-lived credential at
its source, replace the environment secret, and only then resume processing. The
short-lived OIDC token does not require rotation.

See [SECURITY.md](SECURITY.md) for the public-repository threat model.
