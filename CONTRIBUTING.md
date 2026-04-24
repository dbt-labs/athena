# Contributing

Thanks for your interest in contributing to the Athena ADBC driver.

## Ground rules

- By submitting a contribution, you agree it is licensed under the repository's
  [Apache 2.0 license](LICENSE).
- Please do not include secrets, customer data, or internal-only information in
  PRs, issues, or discussions.
- Security issues: see [SECURITY.md](SECURITY.md) — report privately, not via
  public issues.

## Development setup

Requirements: Go 1.26+, a C toolchain (for the CGo FFI shim), and — for
integration tests — AWS credentials and an S3 staging bucket.

```sh
# Unit + functional tests (no AWS credentials required)
make test-unit

# Integration tests against real Athena
cp .env.example .env   # fill in values
make test-integration

# Vet
make vet
```

See [README.md](README.md) for a full walkthrough and [CLAUDE.md](CLAUDE.md)
for the code layout and architecture notes.

## Pull requests

1. Fork the repo and create a feature branch.
2. Keep changes focused — one logical change per PR.
3. Add or update tests. New Athena type mappings must have a case in
   `TestBuildRecordBatch_AllTypes`.
4. Run `make test-unit` and `make vet` before pushing.
5. Open a PR against `main` with a clear description of what changed and why.

CI runs unit tests, the CGo shim build, and (on pushes to `main` only)
integration tests. External fork PRs will have workflow runs gated on
maintainer approval.

## Reporting bugs / requesting features

Open a GitHub issue with a minimal reproduction (SQL, driver options, expected
vs. actual behavior, driver version / commit). For questions about usage,
GitHub Discussions is preferred over issues.
