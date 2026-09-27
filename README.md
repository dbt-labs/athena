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

## Catalog and storage operations

Athena tables are Glue catalog entries over S3 prefixes, and Athena SQL manages
neither: dropping a Hive table leaves its files behind, Glue keeps every table
version, and a seed has to be staged as a CSV object before Athena can read it.
dbt-athena performs that work through the AWS APIs, runs Python models as
calculations in the Spark sessions of a Spark-enabled work group, and manages
Lake Formation tags and data cell filters. The driver exposes the same
calls behind two statement options, so a client keeps one set of credentials:

```go
stmt.SetOption("athena.operation", "glue.get_table")
stmt.SetOption("athena.operation.payload", `{"DatabaseName": "analytics", "Name": "orders"}`)
rdr, _, err := stmt.ExecuteQuery(ctx) // one row, one utf8 column `result`: {"Table": {...}}
```

The SQL text is ignored. The payload is the JSON form of the AWS API input and the
result the JSON form of the API output, except for the three S3 operations, which
take and return the simplified shapes in the table; paginated calls return every page,
batch calls are chunked to the API limits. `ExecuteUpdate` runs the operation
and returns 0. Unknown operations and malformed payloads fail with
`InvalidArgument`; AWS errors with `IO`.

| operation | input | result |
|---|---|---|
| `sts.get_caller_identity` | `{}` | `{"Account": ...}` |
| `athena.get_data_catalog` | `{"Name"}` | `{"DataCatalog": ...}` |
| `athena.get_work_group` | `{"WorkGroup"}` | `{"WorkGroup": ...}` |
| `athena.start_session` | `{"WorkGroup", "EngineConfiguration", ...}` | `{"SessionId", "State"}` |
| `athena.get_session_status` | `{"SessionId"}` | `{"Status": {"State", ...}}` |
| `athena.start_calculation_execution` | `{"SessionId", "CodeBlock"}` | `{"CalculationExecutionId", "State"}` |
| `athena.get_calculation_execution` | `{"CalculationExecutionId"}` | `{"Status": ..., "Result": ...}` |
| `athena.stop_calculation_execution` | `{"CalculationExecutionId"}` | `{"State"}` |
| `athena.terminate_session` | `{"SessionId"}` | `{"State"}` |
| `lakeformation.add_lf_tags_to_resource` / `remove_lf_tags_from_resource` | `{"Resource", "LFTags"}` | `{"Failures"}` |
| `lakeformation.get_resource_lf_tags` | `{"Resource"}` | `{"LFTagOnDatabase", "LFTagsOnTable", "LFTagsOnColumns"}` |
| `lakeformation.list_data_cells_filter` | `{"Table"}` | `{"DataCellsFilters"}`, all pages |
| `lakeformation.create_data_cells_filter` / `update_data_cells_filter` | `{"TableData"}` | `{}` |
| `lakeformation.delete_data_cells_filter` | `{"TableCatalogId", "DatabaseName", "TableName", "Name"}` | `{}` |
| `lakeformation.list_permissions` | `{"Resource", ...}` | `{"PrincipalResourcePermissions"}`, all pages |
| `lakeformation.batch_grant_permissions` / `batch_revoke_permissions` | `{"CatalogId"?, "Entries"}` | `{"Failures"}` |
| `glue.get_table` | `{"CatalogId"?, "DatabaseName", "Name"}` | `{"Table": ...}`; `{"Table": null}` when missing |
| `glue.get_tables` | `{"CatalogId"?, "DatabaseName", "Expression"?}` | `{"TableList": [...]}`, all pages; empty when the database is missing |
| `glue.get_database` | `{"CatalogId"?, "Name"}` | `{"Database": ...}`; `{"Database": null}` when missing |
| `glue.get_databases` | `{"CatalogId"?}` | `{"DatabaseList": [...]}`, all pages |
| `glue.delete_table` | `{"CatalogId"?, "DatabaseName", "Name"}` | `{"Deleted": bool}`; a missing table is not an error |
| `glue.delete_database` | `{"CatalogId"?, "Name"}` | `{}` |
| `glue.get_table_versions` | `{"CatalogId"?, "DatabaseName", "TableName"}` | `{"TableVersions": [...]}` |
| `glue.delete_table_version` | `{"CatalogId"?, "DatabaseName", "TableName", "VersionId"}` | `{}` |
| `glue.get_partitions` | `{"CatalogId"?, "DatabaseName", "TableName", "Expression"?, "ExcludeColumnSchema"?}` | `{"Partitions": [...]}` |
| `glue.batch_delete_partition` | `{"CatalogId"?, "DatabaseName", "TableName", "PartitionsToDelete": [{"Values": [...]}]}` | `{"Errors": [...]}`, 25 per request |
| `glue.batch_create_partition` | `{"CatalogId"?, "DatabaseName", "TableName", "PartitionInputList": [...]}` | `{"Errors": [...]}`, 100 per request |
| `glue.update_table` | `{"CatalogId"?, "DatabaseName", "TableInput": {...}, "SkipArchive"?}` | `{}` |
| `s3.list_objects` | `{"Bucket", "Prefix"}` | `{"Keys": [...]}` |
| `s3.delete_objects` | `{"Bucket", "Keys": [...]}` | `{"Errors": [{"Key", "Code", "Message"}]}`, 1000 per request |
| `s3.put_object` | `{"Bucket", "Key", "Body" (base64), "ServerSideEncryption"?, "SSEKMSKeyId"?, "ACL"?, "StorageClass"?, "ContentType"?, "BucketKeyEnabled"?}` | `{}` |

The credentials need the matching IAM permissions (`glue:GetTable`, `s3:PutObject`, ...)
in addition to the Athena ones.

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
