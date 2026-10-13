# athena

An [ADBC](https://arrow.apache.org/adbc/) driver for [AWS Athena](https://aws.amazon.com/athena/), implemented in Go. Returns query results as Apache Arrow record batches.

## Prerequisites

- Go 1.26+
- AWS credentials configured (via environment variables, `~/.aws/credentials`, or IAM role)

## Installation

```go
import athena "github.com/dbt-labs/athena/go"
```

```sh
go get github.com/dbt-labs/athena/go
```

## Usage

```go
import (
    "context"

    athena "github.com/dbt-labs/athena/go"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

driver := athena.NewDriver(memory.DefaultAllocator)

db, err := driver.NewDatabase(map[string]string{
    athena.OptionRegion:         "us-east-1",
    athena.OptionOutputLocation: "s3://your-bucket/athena-results/",
    athena.OptionCatalog:        "AwsDataCatalog",  // optional, default: AwsDataCatalog
    athena.OptionSchema:         "default",          // optional
})

conn, err := db.Open(context.Background())
stmt, err := conn.NewStatement()
err = stmt.SetSqlQuery("SELECT * FROM my_table LIMIT 10")

reader, rowCount, err := stmt.ExecuteQuery(context.Background())
defer reader.Release()

for reader.Next() {
    rec := reader.Record()
    // process Arrow record batch
}
```

### Authentication

The driver supports three auth modes, set via `athena.OptionAuthType`:

| Value         | Constant              | Description                                         |
|---------------|-----------------------|-----------------------------------------------------|
| `"iam"`       | `AuthTypeDefault`     | Default AWS credential chain (env vars, `~/.aws/credentials`, instance profile) |
| `"access_key"`| `AuthTypeAccessKey`   | Static credentials via `OptionAccessKeyID` / `OptionSecretKey` / `OptionSessionToken` |
| `"profile"`   | `AuthTypeProfile`     | Named profile via `OptionProfileName`               |

### Connection options

Besides the options in the example above:

| option | value |
|---|---|
| `athena.aws.role_arn` | IAM role assumed on top of the auth type's credentials, for every AWS call |
| `athena.aws.role_external_id`, `athena.aws.role_session_name` | AssumeRole external ID and session name |
| `athena.aws.role_duration` | lifetime of the assumed credentials (`"1h"`); refreshed before they expire |
| `athena.aws.max_api_retries` | retries of a failed AWS API request, the first attempt not included (SDK default 2); a query that ran and failed is not rerun |
| `athena.aws.athena_endpoint_url`, `athena.aws.sts_endpoint_url` | endpoint of the Athena API and of the STS API (used for `athena.aws.role_arn`), e.g. VPC interface endpoints without private DNS |
| `athena.poll_interval` | interval between query status checks (default `"500ms"`) |

The AWS SDK's own service-specific endpoint settings (`AWS_ENDPOINT_URL_ATHENA`, `AWS_ENDPOINT_URL_STS`, or a `services` section of the shared config file) apply too; the options above take precedence.

## Development

### Build

```sh
cd go
go build ./...
```

### Test

Unit tests (no AWS credentials required):

```sh
make test-unit
# or: cd go && go test ./... -count=1
```

Integration tests against real Athena:

```sh
# 1. Copy the example env file and fill in your values
cp .env.example .env

# 2. Run integration tests
make test-integration
# or: cd go && ADBC_ATHENA_TESTS=1 go test ./... -run TestIntegration -v -count=1
```

Required environment variables for integration tests:

| Variable                  | Description                                      |
|---------------------------|--------------------------------------------------|
| `AWS_DEFAULT_REGION`      | AWS region (e.g. `us-east-1`)                    |
| `ATHENA_OUTPUT_LOCATION`  | S3 output location (e.g. `s3://bucket/results/`) |
| `AWS_ACCESS_KEY_ID`       | Optional — omit to use IAM role or `~/.aws/credentials` |
| `AWS_SECRET_ACCESS_KEY`   | Optional                                         |
| `AWS_SESSION_TOKEN`       | Optional                                         |

### Vet

```sh
make vet
```

## Support

This project is provided **as-is**, without SLAs or guarantees of support.
Maintenance is best-effort.

- **Bugs / feature requests:** open a GitHub issue with a minimal reproduction.
- **Questions / discussion:** use GitHub Discussions.
- **Security vulnerabilities:** see [SECURITY.md](SECURITY.md) — report
  privately, not via public issues.
- **Contributing:** see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
