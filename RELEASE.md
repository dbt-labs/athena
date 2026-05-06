# Releasing

How releases work for this driver, and the conventions to follow.

## Overview

A pushed `v*` tag triggers
[`.github/workflows/release.yml`](.github/workflows/release.yml), which
builds the CGo shared library on native runners for four platforms,
packages each binary into a tarball, and publishes a GitHub Release
containing the four tarballs and a `manifest.yaml`.

Platforms produced by every release:

| Archive | Binary |
|---|---|
| `athena_linux_amd64_v<version>.tar.gz` | `libadbc_driver_athena.so` |
| `athena_linux_arm64_v<version>.tar.gz` | `libadbc_driver_athena.so` |
| `athena_macos_arm64_v<version>.tar.gz` | `libadbc_driver_athena.dylib` |
| `athena_windows_amd64_v<version>.tar.gz` | `libadbc_driver_athena.dll` |

`manifest.yaml` declares the driver path and license:

```yaml
drivers:
  - path: athena
    license: Apache-2.0
```

## Versioning

We follow [Semantic Versioning 2.0.0](https://semver.org/). The version
itself is numeric only — no `v` prefix:

| Bump | When |
|------|------|
| MAJOR | Incompatible ABI/API change (e.g. removing/renaming an exported C entrypoint, breaking the option key contract). |
| MINOR | Backwards-compatible feature additions (new option keys, new type-mapping coverage). |
| PATCH | Backwards-compatible bug fixes. |

Pre-release suffixes are allowed and follow semver: `0.2.0-rc.1`,
`1.0.0-beta.3`. Anything matching `v*` triggers the release workflow, so
the suffix is what gates consumers — there is no separate "stable"
channel.

The version must be **monotonically increasing**. Do not retag; if you
need to fix a bad release, ship a new patch version.

## Tag convention

Tags are `v<version>`:

- ✅ `v0.1.0`
- ✅ `v1.2.3-rc.1`
- ❌ `0.1.0` — won't match the workflow's `v*` filter
- ❌ `release/v0.1.0` — same; the `/` also breaks the artifact filename

The release workflow's `determine-version` job parses `github.ref`
(`refs/tags/v0.1.0`) and strips the leading `v` to derive the **version**
used in archive filenames. So tag `v0.1.0` produces archives like
`athena_linux_amd64_v0.1.0.tar.gz`.

## How to cut a release

1. Make sure `main` has the changes you want to ship and CI is green.
2. From a clean checkout of `main`:
   ```bash
   git pull --ff-only
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```
3. Watch the `Release` workflow run from the Actions tab. It builds the
   four platform binaries in parallel, then publishes the GitHub Release.
4. Verify the Releases page contains all four tarballs and
   `manifest.yaml`.

## Dry-running

You can run `release.yml` from the Actions tab via `workflow_dispatch` (no
tag required). It will:

- Build all four platform tarballs (downloadable from the run page).
- **Skip** `gh release create` — the publish step is gated on
  `is_release == 'true'`, which is only true on a `refs/tags/v*` push.
- Use a synthetic version `0.0.0-dryrun.<run_id>` so archive filenames
  are valid and don't collide with real releases.

## Rolling back

GitHub Releases can be deleted, but artifacts already pulled by consumers
are not revoked by deleting the release. To recover from a bad release,
ship a new patch version with the fix (e.g. `v0.1.1`). Never retag.
