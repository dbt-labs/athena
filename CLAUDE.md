# CLAUDE.md

Guidelines and context for working in this repository.

## What this project is

A Go [ADBC](https://arrow.apache.org/adbc/) driver for [AWS Athena](https://aws.amazon.com/athena/). It executes SQL via the Athena API, polls for completion, pages through results, and returns them as Apache Arrow record batches through the standard ADBC `array.RecordReader` interface.

## Key commands

```sh
make test-unit          # unit tests — no AWS credentials needed
make test-integration   # integration tests against real Athena (needs .env)
make vet                # go vet
```

Copy `.env.example` → `.env` and fill in your values before running integration tests.

## Code layout (`go/`)

| File | Role |
|------|------|
| `driver.go` | Entry point — implements `adbc.Driver`, exports `NewDriver` |
| `athena_database.go` | `databaseImpl`: option storage, AWS config construction, `Open()` |
| `connection.go` | `connectionImpl`: catalog/schema metadata queries |
| `statement.go` | `statementImpl`: `ExecuteQuery` / `ExecuteUpdate`, polling loop, pagination |
| `record_reader.go` | Schema derivation and string→Arrow conversion |
| `client.go` | `athenaClientAPI` interface — allows mock injection in tests |

Tests are in the same package (`package athena`) except `driver_test.go` which uses the public API (`package athena_test`) and contains the integration tests.

## Architecture

**Execution flow:**
1. `StartQueryExecution` — submits the query
2. `GetQueryExecution` — polled every 500 ms until SUCCEEDED / FAILED / CANCELLED
3. `GetQueryResultsPaginator` — fetches results one page at a time; one Arrow record batch per page
4. Returns an `array.RecordReader` over all accumulated batches

**Memory:** All pages are fetched before the RecordReader is returned, so peak memory scales with total result size.

## Type mapping

Athena returns all values as strings (`VarCharValue`). The conversion is two-step:

1. **`buildSchema(colInfo)`** — maps Athena type strings → Arrow `DataType` via `athenaTypeStringToArrow`. This is the single source of truth for type mapping.
2. **`appendValue(bldr, dt, val, isNull)`** — switches on `dt.ID()` (the Arrow type ID from the schema), never on the Athena type string directly.

This pattern mirrors `record_builder.go` in [dbt-labs/arrow-adbc#9](https://github.com/dbt-labs/arrow-adbc/pull/9). Keep type logic in `athenaTypeStringToArrow`; keep `appendValue` working off Arrow type IDs.

**Complex types** (`array`, `map`, `row`, `decimal`, `json`) are stringified — Athena's `GetQueryResults` API has no structured encoding for them.

**Athena SDK limitation:** There is no native Arrow streaming API in `github.com/aws/aws-sdk-go-v2/service/athena` (checked up to v1.57.4). `GetQueryResults` is the only results endpoint and it returns string data.

## Testing approach

- **Unit tests** (`record_reader_test.go`, `functional_test.go`): use `mockAthenaClient` (function-field struct) injected via `databaseImpl.testClient`. No AWS credentials needed.
- **Integration tests** (`driver_test.go`, `TestIntegration*`): skipped unless `ADBC_ATHENA_TESTS=1` is set. Use `integrationConn()` helper to build a real connection from env vars.
- `TestBuildRecordBatch_AllTypes` exercises `appendValue` end-to-end for every Athena type — add a case here whenever a new type is added.

## Option constants

All option keys are `"athena.<name>"` strings, exported as `Option*` constants from `driver.go`. Auth types are `"iam"` (default credential chain), `"access_key"` (static), `"profile"` (named profile).

## Conventions

- `nilIfEmpty(s string) *string` — used when Athena API fields require nil rather than empty string
- Connection copies `catalog`/`schema` from the database at `Open()` time for per-connection isolation
- `StopQueryExecution` is called best-effort on context cancellation (5 s timeout) to avoid unnecessary Athena cost

---

## CGo FFI shim (`go/pkg/`)

The shim wraps the pure-Go driver in C-callable ADBC functions and is compiled with
`-buildmode=c-shared` to produce `libadbc_driver_athena.so` (`.dylib` on macOS,
`.dll` on Windows). dbt-fusion loads this via `dlopen`.

```
go/pkg/
  Makefile
  athena/          package main, //go:build driverlib — shim source + unit tests
  shimtest/        standalone CGo integration-test program
```

### Build

```bash
cd go/pkg
make                  # produces libadbc_driver_athena.so (deletes the generated .h)
make test             # build .so → run unit tests → run shimtest
make clean
```

### Build tags

- `go/pkg/athena/*.go` (shim source + tests): `//go:build driverlib`. A `doc.go` stub
  with no build tag keeps the package visible to `go test ./...` without pulling in CGo.
- `go/pkg/shimtest/main.go`: `//go:build ignore`. Requires the pre-built `.so`; excluded
  from normal `go build`/`go test` to prevent linker errors. Use `make test` to run it.
- C files (`utils.c`, `utils.h`): `// clang-format off/on` wraps `//go:build driverlib`.

### Exported symbols

The library exports a single driver-init entrypoint:
- **`AdbcDriverAthenaInit`** — the Athena-specific entrypoint

`utils.c` has a `#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)` block that exports generic
wrappers (e.g. `AdbcErrorGetDetailCount`), but deliberately does **not** include a
generic `AdbcDriverInit` alias — see the comment at the bottom of that block.

### Testing

Go 1.26 disallows CGo in `_test.go` files entirely. Shim tests are split into two layers:

1. **Unit tests** (`go/pkg/athena/shim_test.go`, `//go:build driverlib`): pure-Go tests
   for helpers extracted from the shim (currently `fillStringOption`).
   Run via: `cd go && go test -tags driverlib -v ./pkg/athena/`

2. **Integration tests** (`go/pkg/shimtest/main.go`): standalone CGo program that links
   against the built `.so` and calls `AdbcDriverAthenaInit` directly.
   Run via `make test` (sets `LD_LIBRARY_PATH=.` on Linux, `DYLD_LIBRARY_PATH=.` on macOS).

### Key shim invariants

- **`AdbcDriverAthenaInit`** is the sole exported entrypoint.
- **`driver.release`** must be set (points to `AthenaDriverRelease`, a no-op C function).
  Driver managers crash on unload if it is nil.
- **`fillStringOption(val string, buf []byte) int`** is the pure-Go core of
  `exportStringOption`. It handles nil and too-small buffers safely; always returns the
  required length (`len(val)+1`).
- **`setErrWithDetails`** releases any prior error before writing a new one.

### CI

`.github/workflows/ci.yml` runs `test` and `shim` on every push/PR, plus `integration` on pushes to `main`:

| Job           | Trigger         | What it runs                                         |
|---------------|-----------------|------------------------------------------------------|
| `test`        | all branches    | `go test -race -count=1 ./...`                       |
| `shim`        | all branches    | `make test` in `go/pkg/` (builds `.so` + shimtest)   |
| `integration` | push to `main`  | `go test -race -run TestIntegration` with AWS creds  |
