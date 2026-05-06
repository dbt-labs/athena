# Releasing

How releases work for this driver, and the conventions to follow.

## Overview

The Athena ADBC driver ships through a two-stage pipeline:

1. **Upstream (this repo).** A pushed `v*` tag triggers
   [`.github/workflows/release.yml`](.github/workflows/release.yml), which
   builds the CGo shared library on native runners for four platforms,
   packages the binaries, and publishes a GitHub Release containing the
   tarballs and a `manifest.yaml`.
2. **Downstream (`dbt-labs/fs`).** The `Release ADBC` workflow's
   `build-athena` job calls the reusable `redist-adbc-driver-gh.yml`, which
   reads the latest release from this repo, validates the license against an
   allowlist, repackages each binary with zstd compression, and uploads them
   to the public dbt CDN.

End artifact name on the CDN:

```
adbc_driver_athena-<version>-<target>.<ext>.zst
```

e.g. `adbc_driver_athena-0.1.0-aarch64-apple-darwin.dylib.zst`.

## Versioning

We follow [Semantic Versioning 2.0.0](https://semver.org/). The version
itself is numeric only — no `v` prefix:

| Bump | When |
|------|------|
| MAJOR | Incompatible ABI/API change (e.g. removing/renaming an exported C entrypoint, breaking the option key contract). |
| MINOR | Backwards-compatible feature additions (new option keys, new type-mapping coverage). |
| PATCH | Backwards-compatible bug fixes. |

Pre-release suffixes are allowed and follow semver: `0.2.0-rc.1`,
`1.0.0-beta.3`. Anything matching `v*` triggers the release workflow, so the
suffix is what gates downstream consumers — there is no separate "stable"
channel.

The version must be **monotonically increasing**. The downstream CDN's S3
duplicate-file check rejects a release that would overwrite an existing
version.

## Tag convention

Tags are `v<version>`:

- ✅ `v0.1.0`
- ✅ `v1.2.3-rc.1`
- ❌ `0.1.0` — won't match the workflow's `v*` filter
- ❌ `release/v0.1.0` — same; the `/` also breaks the artifact filename
- ❌ `src/v0.1.0` — that prefix is reserved for `adbc-drivers/*` repos

The release workflow's `determine-version` job parses `github.ref`
(`refs/tags/v0.1.0`) and strips the leading `v` to derive the **version**
used in archive filenames. So tag `v0.1.0` produces upstream archives like
`athena_linux_amd64_v0.1.0.tar.gz` and downstream CDN files like
`adbc_driver_athena-0.1.0-...zst`.

Do not retag. If you need to fix a bad release, ship a new patch version.

## How to cut a release

1. Make sure `main` has the changes you want to ship and CI is green.
2. From a clean checkout of `main`:
   ```bash
   git pull --ff-only
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```
3. Watch the [`Release`
   workflow](https://github.com/dbt-labs/athena/actions/workflows/release.yml)
   run. It builds four platform binaries in parallel, then publishes a
   GitHub Release.
4. Verify the [Releases page](https://github.com/dbt-labs/athena/releases)
   contains:
   - `athena_linux_amd64_v<version>.tar.gz`
   - `athena_linux_arm64_v<version>.tar.gz`
   - `athena_macos_arm64_v<version>.tar.gz`
   - `athena_windows_amd64_v<version>.tar.gz`
   - `manifest.yaml`
5. In `dbt-labs/fs`, run `Release ADBC` from the Actions tab with
   `dry_run: true` and `build_athena: true` (everything else off) to verify
   the staging CDN upload. Once green, run again with `dry_run: false` to
   publish to the production CDN.

## Dry-running the upstream workflow

You can run `release.yml` from the Actions tab via `workflow_dispatch` (no
tag required). It will:

- Build all four platform tarballs.
- **Skip** `gh release create` — the publish step is gated on
  `is_release == 'true'`, which is only true on a `refs/tags/v*` push.
- Use a synthetic version `0.0.0-dryrun.<run_id>` so archive filenames are
  valid and don't collide with real releases.

Download the artifacts from the run page to inspect their contents.

## Rolling back

GitHub Releases can be deleted, but that does not revoke already-distributed
CDN artifacts. To recover from a bad release:

- Ship a new patch version with the fix (e.g. `v0.1.1`).
- If the CDN copy itself is corrupt, coordinate with the FS team — the
  `latest.json` and individual `.zst` files can be overwritten with a
  manually-triggered re-run of the FS workflow against a corrected upstream
  release.
