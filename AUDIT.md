# Public-runner audit

This directory is a sanitized, code-only GitHub Actions runner. The audit is
designed to remain safe in a public repository and therefore contains no
account values, Drive IDs, filenames from the private catalog, URLs, tokens,
hashes, product metadata, or asset samples.

## Verified controls

- Workflow triggers are limited to `workflow_dispatch` and `schedule`; no pull
  request event can access the pipeline.
- Production values are read only from the branch-restricted
  `production-catalog` environment. Repository variables are not used.
- The metadata job has `contents: read` only. The ingest job alone receives
  `id-token: write`; the continuation job alone receives `actions: write` and
  consumes no environment secret.
- Third-party Actions are pinned to full commit SHAs. No Actions cache or
  artifact upload/download step exists.
- Metadata refresh lists F16 and PNG Only file metadata and reads only bounded
  ledger/CSV bytes. It never downloads PNG, SVG, or ZIP product bytes.
- Automatic PNG Only discovery accepts one exact-parent PNG, at most one
  same-name SVG, and one strict `design_metadata_and_specs.csv` sidecar.
  Missing, duplicate, malformed, unapproved, ambiguous, or cross-folder input
  produces a hold or blocker; filenames never supply title, description, or
  tags.
- Existing Etsy title/tags are preserved. Description processing removes only
  the reviewed disclosure/protection/mega-bundle suffix and the exact
  AI-assisted sentence; it does not generate replacement copy.
- Publication updates one pinned private manifest file and is rejected when a
  previously published product identity would disappear.
- Ingest authentication is a short-lived GitHub OIDC token with the fixed
  `heeexy-catalog-ingest` audience. No static ingest secret is present.
- The transparency preview is rendered on one canvas-wide 32-pixel checker
  grid. Tests sample corners, outer edges, and transparent pixels behind the
  artwork to prevent an inner/outer grid seam.
- Runtime output is allowlisted to aggregate status and stable error codes.

## Verification result

The complete local code-only suite passes 102 synthetic unit tests. Tests use
in-memory fixtures and temporary generated images only; they do not call Drive,
the site, Cloudflare, or GitHub and do not read any real catalog asset.

No repository was created, no code was pushed, no workflow was dispatched, no
deployment was performed, and neither production entry point was executed as
part of this audit.
