# Security policy

## Supported code

Only the current `main` branch is supported. This repository is intentionally a
code-only public runner; it is not a catalog, backup, or credential store.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting feature for this repository. Do
not open a public issue containing credentials, private URLs, Drive IDs,
filenames, customer data, screenshots of repository settings, or workflow logs
that may contain sensitive context.

## Threat model and controls

- There are no pull-request workflow triggers.
- Manual processing is restricted to `main`.
- Environment secrets are unavailable to forks, and the workflow also requires
  an exact secret-backed repository identity match before checkout. Sensitive
  values are stored only as secrets of the branch-restricted
  `production-catalog` environment.
- Third-party Actions are pinned to immutable commit SHAs. Production
  environment secrets are scoped only to the authorization step or the final
  first-party processing step.
- The default token is read-only. Only the isolated, secret-free continuation
  job receives `actions: write`, solely to dispatch the next reviewed workflow
  on `main`.
- The metadata-refresh job has `contents: read` and no OIDC permission. It lists
  scoped Drive inventory metadata and reads only bounded metadata sources; it
  never downloads product PNG, SVG, or ZIP bytes.
- Incremental PNG Only discovery is exact-parent and fail-closed: one bounded
  allowlisted CSV sidecar may bind only the one PNG in its own folder. Missing,
  duplicate, malformed, cross-folder, unapproved, or ambiguous evidence cannot
  publish a product.
- Product bytes exist only in Google Drive, the authenticated ingest service,
  and the fresh runner's temporary storage. No cache or artifact is uploaded.
- Ingest authentication uses a short-lived GitHub OIDC token with a fixed
  audience. The token exists only in memory and is never persisted.
- Pipeline output is allowlisted. Unexpected exception content, HTTP bodies,
  URLs, IDs, and filenames are never printed.
- Manifest publication is pinned to one private Drive file and fails closed if
  the prior manifest is missing, invalid, resolves to another file, or contains
  any product identity absent from the proposed replacement.
- Actions log/artifact retention is required to be one day.

## Secret rotation

If compromise is suspected, set `SCHEDULE_ENABLED` to `false`, cancel active
runs, rotate Google OAuth credentials at their provider, replace the GitHub
environment secrets, and perform a 25-product manual pilot before restoring
scheduled processing.
