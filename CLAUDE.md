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
