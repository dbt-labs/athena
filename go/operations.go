// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package athena

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	glueSDK "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	lakeformationSDK "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lftypes "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	s3SDK "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	stsSDK "github.com/aws/aws-sdk-go-v2/service/sts"
)

// Catalog and storage operations.
//
// Athena tables are Glue catalog entries over S3 prefixes, and Athena SQL cannot
// manage either: dropping a Hive table leaves its files, table versions accumulate
// in Glue, seeds have to be staged as CSV objects. dbt-athena does that work
// through the AWS APIs. A client sets these two statement options and executes;
// the SQL text is ignored, the driver performs the named API call with the
// connection's credentials and returns the API response as JSON:
//
//	statement.SetOption("athena.operation", "glue.get_table")
//	statement.SetOption("athena.operation.payload", `{"DatabaseName":"db","Name":"t"}`)
//	rdr, _, err := statement.ExecuteQuery(ctx)
//
// The payload is the JSON form of the AWS API input, except for the S3
// operations, whose simplified inputs are documented on each constant; the
// result is one record with a single utf8 column `result` holding JSON.
// Paginated calls return every page in one response; batch calls are chunked to
// the API limits. Only the operations below are accepted.
const (
	// OptionOperation names the catalog or storage operation to run instead of
	// the SQL text. See OperationGlueGetTable and siblings.
	OptionOperation = "athena.operation"
	// OptionOperationPayload is the operation's JSON input.
	OptionOperationPayload = "athena.operation.payload"

	// OperationStsGetCallerIdentity: `{}` -> `{"Account": "..."}`.
	OperationStsGetCallerIdentity = "sts.get_caller_identity"
	// OperationAthenaGetDataCatalog: `{"Name"}` -> `{"DataCatalog": {...}}`.
	OperationAthenaGetDataCatalog = "athena.get_data_catalog"
	// OperationAthenaGetWorkGroup: `{"WorkGroup"}` -> `{"WorkGroup": {...}}`.
	OperationAthenaGetWorkGroup = "athena.get_work_group"
	// OperationAthenaStartSession: `{"WorkGroup", "EngineConfiguration", ...}` ->
	// `{"SessionId", "State"}`. The Spark sessions and calculations below run the
	// Python models of a Spark-enabled work group.
	OperationAthenaStartSession = "athena.start_session"
	// OperationAthenaGetSessionStatus: `{"SessionId"}` -> `{"Status": {"State", ...}}`.
	OperationAthenaGetSessionStatus = "athena.get_session_status"
	// OperationAthenaStartCalculationExecution: `{"SessionId", "CodeBlock"}` ->
	// `{"CalculationExecutionId", "State"}`.
	OperationAthenaStartCalculationExecution = "athena.start_calculation_execution"
	// OperationAthenaGetCalculationExecution: `{"CalculationExecutionId"}` ->
	// `{"Status": {"State", ...}, "Result": {...}}`.
	OperationAthenaGetCalculationExecution = "athena.get_calculation_execution"
	// OperationAthenaStopCalculationExecution: `{"CalculationExecutionId"}` -> `{"State"}`.
	OperationAthenaStopCalculationExecution = "athena.stop_calculation_execution"
	// OperationAthenaTerminateSession: `{"SessionId"}` -> `{"State"}`. Stopping a
	// calculation is best effort; terminating its session is not.
	OperationAthenaTerminateSession = "athena.terminate_session"
	// OperationGlueGetTable: `{"CatalogId"?, "DatabaseName", "Name"}` ->
	// `{"Table": {...}}`, or `{"Table": null}` when the table does not exist.
	OperationGlueGetTable = "glue.get_table"
	// OperationGlueGetTables: `{"CatalogId"?, "DatabaseName", "Expression"?}` ->
	// `{"TableList": [...]}`, all pages; empty when the database does not exist.
	OperationGlueGetTables = "glue.get_tables"
	// OperationGlueGetDatabase: `{"CatalogId"?, "Name"}` -> `{"Database": {...}}`, or
	// `{"Database": null}` when the database does not exist.
	OperationGlueGetDatabase = "glue.get_database"
	// OperationGlueGetDatabases: `{"CatalogId"?}` -> `{"DatabaseList": [...]}`, all pages.
	OperationGlueGetDatabases = "glue.get_databases"
	// OperationGlueDeleteTable: `{"CatalogId"?, "DatabaseName", "Name"}` ->
	// `{"Deleted": bool}`; a missing table is not an error.
	OperationGlueDeleteTable = "glue.delete_table"
	// OperationGlueDeleteDatabase: `{"CatalogId"?, "Name"}` -> `{}`.
	OperationGlueDeleteDatabase = "glue.delete_database"
	// OperationGlueGetTableVersions: `{"CatalogId"?, "DatabaseName", "TableName"}`
	// -> `{"TableVersions": [...]}`, all pages.
	OperationGlueGetTableVersions = "glue.get_table_versions"
	// OperationGlueDeleteTableVersion: `{"CatalogId"?, "DatabaseName", "TableName",
	// "VersionId"}` -> `{}`.
	OperationGlueDeleteTableVersion = "glue.delete_table_version"
	// OperationGlueGetPartitions: `{"CatalogId"?, "DatabaseName", "TableName",
	// "Expression"?, "ExcludeColumnSchema"?}` -> `{"Partitions": [...]}`, all pages.
	OperationGlueGetPartitions = "glue.get_partitions"
	// OperationGlueBatchDeletePartition: `{"CatalogId"?, "DatabaseName", "TableName",
	// "PartitionsToDelete": [{"Values": [...]}]}` -> `{"Errors": [...]}`; chunked
	// to 25 partitions per request.
	OperationGlueBatchDeletePartition = "glue.batch_delete_partition"
	// OperationGlueBatchCreatePartition: `{"CatalogId"?, "DatabaseName", "TableName",
	// "PartitionInputList": [...]}` -> `{"Errors": [...]}`; chunked to 100 per request.
	OperationGlueBatchCreatePartition = "glue.batch_create_partition"
	// OperationGlueUpdateTable: `{"CatalogId"?, "DatabaseName", "TableInput": {...},
	// "SkipArchive"?}` -> `{}`.
	OperationGlueUpdateTable = "glue.update_table"
	// OperationS3ListObjects: `{"Bucket", "Prefix"}` -> `{"Keys": [...]}`, all pages.
	OperationS3ListObjects = "s3.list_objects"
	// OperationS3DeleteObjects: `{"Bucket", "Keys": [...]}` -> `{"Errors": [{"Key",
	// "Code", "Message"}]}`; chunked to 1000 keys per request.
	OperationS3DeleteObjects = "s3.delete_objects"
	// OperationS3PutObject: `{"Bucket", "Key", "Body" (base64), "ServerSideEncryption"?,
	// "SSEKMSKeyId"?, "ACL"?, "StorageClass"?, "ContentType"?, "BucketKeyEnabled"?}` -> `{}`.
	OperationS3PutObject = "s3.put_object"

	// The Lake Formation operations below take and return the API shapes; the two
	// list calls return every page. dbt-athena manages LF-Tags and data cell
	// filters with them.

	// OperationLakeFormationAddLFTagsToResource: `{"Resource", "LFTags"}` -> `{"Failures"}`.
	OperationLakeFormationAddLFTagsToResource = "lakeformation.add_lf_tags_to_resource"
	// OperationLakeFormationRemoveLFTagsFromResource: `{"Resource", "LFTags"}` -> `{"Failures"}`.
	OperationLakeFormationRemoveLFTagsFromResource = "lakeformation.remove_lf_tags_from_resource"
	// OperationLakeFormationGetResourceLFTags: `{"Resource"}` ->
	// `{"LFTagOnDatabase", "LFTagsOnTable", "LFTagsOnColumns"}`.
	OperationLakeFormationGetResourceLFTags = "lakeformation.get_resource_lf_tags"
	// OperationLakeFormationListDataCellsFilter: `{"Table"}` -> `{"DataCellsFilters"}`, all pages.
	OperationLakeFormationListDataCellsFilter = "lakeformation.list_data_cells_filter"
	// OperationLakeFormationCreateDataCellsFilter: `{"TableData"}` -> `{}`.
	OperationLakeFormationCreateDataCellsFilter = "lakeformation.create_data_cells_filter"
	// OperationLakeFormationUpdateDataCellsFilter: `{"TableData"}` -> `{}`.
	OperationLakeFormationUpdateDataCellsFilter = "lakeformation.update_data_cells_filter"
	// OperationLakeFormationDeleteDataCellsFilter: `{"TableCatalogId", "DatabaseName",
	// "TableName", "Name"}` -> `{}`.
	OperationLakeFormationDeleteDataCellsFilter = "lakeformation.delete_data_cells_filter"
	// OperationLakeFormationListPermissions: `{"Resource", ...}` ->
	// `{"PrincipalResourcePermissions"}`, all pages.
	OperationLakeFormationListPermissions = "lakeformation.list_permissions"
	// OperationLakeFormationBatchGrantPermissions: `{"CatalogId"?, "Entries"}` -> `{"Failures"}`.
	OperationLakeFormationBatchGrantPermissions = "lakeformation.batch_grant_permissions"
	// OperationLakeFormationBatchRevokePermissions: `{"CatalogId"?, "Entries"}` -> `{"Failures"}`.
	OperationLakeFormationBatchRevokePermissions = "lakeformation.batch_revoke_permissions"

	// cleanupTimeout bounds the cleanup operations, which ignore the statement's cancellation.
	cleanupTimeout = 10 * time.Second

	glueBatchDeletePartitionLimit = 25
	glueBatchCreatePartitionLimit = 100
	s3DeleteObjectsLimit          = 1000
)

// knownOperations is checked before any AWS client is built, so an unknown
// name fails with InvalidArgument rather than with a credentials error.
var knownOperations = map[string]bool{
	OperationStsGetCallerIdentity:            true,
	OperationAthenaGetDataCatalog:            true,
	OperationAthenaGetWorkGroup:              true,
	OperationAthenaStartSession:              true,
	OperationAthenaGetSessionStatus:          true,
	OperationAthenaStartCalculationExecution: true,
	OperationAthenaGetCalculationExecution:   true,
	OperationAthenaStopCalculationExecution:  true,
	OperationAthenaTerminateSession:          true,
	OperationGlueGetTable:                    true,
	OperationGlueGetTables:                   true,
	OperationGlueGetDatabase:                 true,
	OperationGlueGetDatabases:                true,
	OperationGlueDeleteTable:                 true,
	OperationGlueDeleteDatabase:              true,
	OperationGlueGetTableVersions:            true,
	OperationGlueDeleteTableVersion:          true,
	OperationGlueGetPartitions:               true,
	OperationGlueBatchDeletePartition:        true,
	OperationGlueBatchCreatePartition:        true,
	OperationGlueUpdateTable:                 true,
	OperationS3ListObjects:                   true,
	OperationS3DeleteObjects:                 true,
	OperationS3PutObject:                     true,

	OperationLakeFormationAddLFTagsToResource:      true,
	OperationLakeFormationRemoveLFTagsFromResource: true,
	OperationLakeFormationGetResourceLFTags:        true,
	OperationLakeFormationListDataCellsFilter:      true,
	OperationLakeFormationCreateDataCellsFilter:    true,
	OperationLakeFormationUpdateDataCellsFilter:    true,
	OperationLakeFormationDeleteDataCellsFilter:    true,
	OperationLakeFormationListPermissions:          true,
	OperationLakeFormationBatchGrantPermissions:    true,
	OperationLakeFormationBatchRevokePermissions:   true,
}

// cleanupOperations are how a client ends work it has given up on, often because it
// is being cancelled itself (dbt cancels every open statement on Ctrl-C): they run to
// completion, within cleanupTimeout, even when the statement is cancelled.
var cleanupOperations = map[string]bool{
	OperationAthenaStopCalculationExecution: true,
	OperationAthenaTerminateSession:         true,
}

// operationResultSchema is the schema of every operation result: one utf8
// column holding the JSON response.
var operationResultSchema = arrow.NewSchema([]arrow.Field{
	{Name: "result", Type: arrow.BinaryTypes.String, Nullable: false},
}, nil)

// glueClientAPI is the subset of the Glue SDK client the operations use.
// *glueSDK.Client satisfies it; the paginator constructors accept it too.
type glueClientAPI interface {
	GetTable(ctx context.Context, params *glueSDK.GetTableInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetTableOutput, error)
	GetTables(ctx context.Context, params *glueSDK.GetTablesInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetTablesOutput, error)
	GetDatabase(ctx context.Context, params *glueSDK.GetDatabaseInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetDatabaseOutput, error)
	GetDatabases(ctx context.Context, params *glueSDK.GetDatabasesInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetDatabasesOutput, error)
	DeleteTable(ctx context.Context, params *glueSDK.DeleteTableInput, optFns ...func(*glueSDK.Options)) (*glueSDK.DeleteTableOutput, error)
	DeleteDatabase(ctx context.Context, params *glueSDK.DeleteDatabaseInput, optFns ...func(*glueSDK.Options)) (*glueSDK.DeleteDatabaseOutput, error)
	GetTableVersions(ctx context.Context, params *glueSDK.GetTableVersionsInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetTableVersionsOutput, error)
	DeleteTableVersion(ctx context.Context, params *glueSDK.DeleteTableVersionInput, optFns ...func(*glueSDK.Options)) (*glueSDK.DeleteTableVersionOutput, error)
	GetPartitions(ctx context.Context, params *glueSDK.GetPartitionsInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetPartitionsOutput, error)
	BatchDeletePartition(ctx context.Context, params *glueSDK.BatchDeletePartitionInput, optFns ...func(*glueSDK.Options)) (*glueSDK.BatchDeletePartitionOutput, error)
	BatchCreatePartition(ctx context.Context, params *glueSDK.BatchCreatePartitionInput, optFns ...func(*glueSDK.Options)) (*glueSDK.BatchCreatePartitionOutput, error)
	UpdateTable(ctx context.Context, params *glueSDK.UpdateTableInput, optFns ...func(*glueSDK.Options)) (*glueSDK.UpdateTableOutput, error)
}

// s3ClientAPI is the subset of the S3 SDK client the operations use.
type s3ClientAPI interface {
	ListObjectsV2(ctx context.Context, params *s3SDK.ListObjectsV2Input, optFns ...func(*s3SDK.Options)) (*s3SDK.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, params *s3SDK.DeleteObjectsInput, optFns ...func(*s3SDK.Options)) (*s3SDK.DeleteObjectsOutput, error)
	PutObject(ctx context.Context, params *s3SDK.PutObjectInput, optFns ...func(*s3SDK.Options)) (*s3SDK.PutObjectOutput, error)
}

// stsClientAPI is the subset of the STS SDK client the operations use.
type stsClientAPI interface {
	GetCallerIdentity(ctx context.Context, params *stsSDK.GetCallerIdentityInput, optFns ...func(*stsSDK.Options)) (*stsSDK.GetCallerIdentityOutput, error)
}

// lakeformationClientAPI is the subset of the Lake Formation SDK client the
// operations use; the paginator constructors accept it too.
type lakeformationClientAPI interface {
	AddLFTagsToResource(ctx context.Context, params *lakeformationSDK.AddLFTagsToResourceInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.AddLFTagsToResourceOutput, error)
	RemoveLFTagsFromResource(ctx context.Context, params *lakeformationSDK.RemoveLFTagsFromResourceInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.RemoveLFTagsFromResourceOutput, error)
	GetResourceLFTags(ctx context.Context, params *lakeformationSDK.GetResourceLFTagsInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.GetResourceLFTagsOutput, error)
	ListDataCellsFilter(ctx context.Context, params *lakeformationSDK.ListDataCellsFilterInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.ListDataCellsFilterOutput, error)
	CreateDataCellsFilter(ctx context.Context, params *lakeformationSDK.CreateDataCellsFilterInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.CreateDataCellsFilterOutput, error)
	UpdateDataCellsFilter(ctx context.Context, params *lakeformationSDK.UpdateDataCellsFilterInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.UpdateDataCellsFilterOutput, error)
	DeleteDataCellsFilter(ctx context.Context, params *lakeformationSDK.DeleteDataCellsFilterInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.DeleteDataCellsFilterOutput, error)
	ListPermissions(ctx context.Context, params *lakeformationSDK.ListPermissionsInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.ListPermissionsOutput, error)
	BatchGrantPermissions(ctx context.Context, params *lakeformationSDK.BatchGrantPermissionsInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.BatchGrantPermissionsOutput, error)
	BatchRevokePermissions(ctx context.Context, params *lakeformationSDK.BatchRevokePermissionsInput, optFns ...func(*lakeformationSDK.Options)) (*lakeformationSDK.BatchRevokePermissionsOutput, error)
}

// awsClients holds the non-Athena SDK clients of a connection, built from the
// database's AWS config on first use.
type awsClients struct {
	glue          glueClientAPI
	s3            s3ClientAPI
	sts           stsClientAPI
	lakeformation lakeformationClientAPI
}

func (c *connectionImpl) clients(ctx context.Context) (*awsClients, error) {
	if c.aws != nil {
		return c.aws, nil
	}
	cfg, err := c.db.buildAWSConfig(ctx)
	if err != nil {
		return nil, err
	}
	c.aws = &awsClients{
		glue: glueSDK.NewFromConfig(cfg),
		s3:   s3SDK.NewFromConfig(cfg),
		sts:  stsSDK.NewFromConfig(cfg),

		lakeformation: lakeformationSDK.NewFromConfig(cfg),
	}
	return c.aws, nil
}

func operationError(op string, err error) error {
	return adbc.Error{
		Code: adbc.StatusIO,
		Msg:  fmt.Sprintf("[athena] %s failed: %v", op, err),
	}
}

func decodePayload(op string, payload string, into any) error {
	if payload == "" {
		payload = "{}"
	}
	if err := json.Unmarshal([]byte(payload), into); err != nil {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[athena] %s: invalid %s: %v", op, OptionOperationPayload, err),
		}
	}
	return nil
}

// callAPI decodes the payload into the API input, makes the call and returns its
// output: the whole operation, for the calls that need no paging or chunking.
func callAPI[In, Out, Opt any](ctx context.Context, op, payload string, call func(context.Context, *In, ...func(*Opt)) (*Out, error)) (any, error) {
	var in In
	if err := decodePayload(op, payload, &in); err != nil {
		return nil, err
	}
	out, err := call(ctx, &in)
	if err != nil {
		return nil, operationError(op, err)
	}
	return out, nil
}

func isGlueNotFound(err error) bool {
	var notFound *gluetypes.EntityNotFoundException
	return errors.As(err, &notFound)
}

// runOperation performs the operation and returns the JSON-encoded response.
func (s *statementImpl) runOperation(ctx context.Context) ([]byte, error) {
	op := s.operation
	if !knownOperations[op] {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[athena] unknown %s '%s'", OptionOperation, op),
		}
	}
	if cleanupOperations[op] {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
	}
	clients, err := s.conn.clients(ctx)
	if err != nil {
		return nil, err
	}
	var out any
	switch op {
	case OperationStsGetCallerIdentity:
		var in stsSDK.GetCallerIdentityInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		identity, err := clients.sts.GetCallerIdentity(ctx, &in)
		if err != nil {
			return nil, operationError(op, err)
		}
		out = identity

	case OperationAthenaGetDataCatalog:
		var in athenaSDK.GetDataCatalogInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		catalog, err := s.conn.athenaClient.GetDataCatalog(ctx, &in)
		if err != nil {
			return nil, operationError(op, err)
		}
		out = catalog

	case OperationAthenaGetWorkGroup:
		var in athenaSDK.GetWorkGroupInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		workGroup, err := s.conn.athenaClient.GetWorkGroup(ctx, &in)
		if err != nil {
			return nil, operationError(op, err)
		}
		out = workGroup

	case OperationAthenaStartSession:
		out, err = callAPI(ctx, op, s.operationPayload, s.conn.athenaClient.StartSession)
	case OperationAthenaGetSessionStatus:
		out, err = callAPI(ctx, op, s.operationPayload, s.conn.athenaClient.GetSessionStatus)
	case OperationAthenaStartCalculationExecution:
		out, err = callAPI(ctx, op, s.operationPayload, s.conn.athenaClient.StartCalculationExecution)
	case OperationAthenaGetCalculationExecution:
		out, err = callAPI(ctx, op, s.operationPayload, s.conn.athenaClient.GetCalculationExecution)
	case OperationAthenaStopCalculationExecution:
		out, err = callAPI(ctx, op, s.operationPayload, s.conn.athenaClient.StopCalculationExecution)
	case OperationAthenaTerminateSession:
		out, err = callAPI(ctx, op, s.operationPayload, s.conn.athenaClient.TerminateSession)

	case OperationLakeFormationAddLFTagsToResource:
		out, err = callAPI(ctx, op, s.operationPayload, clients.lakeformation.AddLFTagsToResource)
	case OperationLakeFormationRemoveLFTagsFromResource:
		out, err = callAPI(ctx, op, s.operationPayload, clients.lakeformation.RemoveLFTagsFromResource)
	case OperationLakeFormationGetResourceLFTags:
		out, err = callAPI(ctx, op, s.operationPayload, clients.lakeformation.GetResourceLFTags)
	case OperationLakeFormationCreateDataCellsFilter:
		out, err = callAPI(ctx, op, s.operationPayload, clients.lakeformation.CreateDataCellsFilter)
	case OperationLakeFormationUpdateDataCellsFilter:
		out, err = callAPI(ctx, op, s.operationPayload, clients.lakeformation.UpdateDataCellsFilter)
	case OperationLakeFormationDeleteDataCellsFilter:
		out, err = callAPI(ctx, op, s.operationPayload, clients.lakeformation.DeleteDataCellsFilter)
	case OperationLakeFormationBatchGrantPermissions:
		out, err = callAPI(ctx, op, s.operationPayload, clients.lakeformation.BatchGrantPermissions)
	case OperationLakeFormationBatchRevokePermissions:
		out, err = callAPI(ctx, op, s.operationPayload, clients.lakeformation.BatchRevokePermissions)

	case OperationLakeFormationListDataCellsFilter:
		var in lakeformationSDK.ListDataCellsFilterInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		filters := []lftypes.DataCellsFilter{}
		paginator := lakeformationSDK.NewListDataCellsFilterPaginator(clients.lakeformation, &in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, operationError(op, err)
			}
			filters = append(filters, page.DataCellsFilters...)
		}
		out = map[string]any{"DataCellsFilters": filters}

	case OperationLakeFormationListPermissions:
		var in lakeformationSDK.ListPermissionsInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		permissions := []lftypes.PrincipalResourcePermissions{}
		paginator := lakeformationSDK.NewListPermissionsPaginator(clients.lakeformation, &in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, operationError(op, err)
			}
			permissions = append(permissions, page.PrincipalResourcePermissions...)
		}
		out = map[string]any{"PrincipalResourcePermissions": permissions}

	case OperationGlueGetTable:
		var in glueSDK.GetTableInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		table, err := clients.glue.GetTable(ctx, &in)
		switch {
		case err == nil:
			out = table
		case isGlueNotFound(err):
			out = &glueSDK.GetTableOutput{}
		default:
			return nil, operationError(op, err)
		}

	case OperationGlueGetTables:
		var in glueSDK.GetTablesInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		tables := []gluetypes.Table{}
		paginator := glueSDK.NewGetTablesPaginator(clients.glue, &in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if isGlueNotFound(err) {
				break
			}
			if err != nil {
				return nil, operationError(op, err)
			}
			tables = append(tables, page.TableList...)
		}
		out = map[string]any{"TableList": tables}

	case OperationGlueGetDatabase:
		var in glueSDK.GetDatabaseInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		database, err := clients.glue.GetDatabase(ctx, &in)
		switch {
		case err == nil:
			out = database
		case isGlueNotFound(err):
			out = &glueSDK.GetDatabaseOutput{}
		default:
			return nil, operationError(op, err)
		}

	case OperationGlueGetDatabases:
		var in glueSDK.GetDatabasesInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		databases := []gluetypes.Database{}
		paginator := glueSDK.NewGetDatabasesPaginator(clients.glue, &in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, operationError(op, err)
			}
			databases = append(databases, page.DatabaseList...)
		}
		out = map[string]any{"DatabaseList": databases}

	case OperationGlueDeleteTable:
		var in glueSDK.DeleteTableInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		_, err := clients.glue.DeleteTable(ctx, &in)
		switch {
		case err == nil:
			out = map[string]bool{"Deleted": true}
		case isGlueNotFound(err):
			out = map[string]bool{"Deleted": false}
		default:
			return nil, operationError(op, err)
		}

	case OperationGlueDeleteDatabase:
		var in glueSDK.DeleteDatabaseInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		if _, err := clients.glue.DeleteDatabase(ctx, &in); err != nil {
			return nil, operationError(op, err)
		}
		out = struct{}{}

	case OperationGlueGetTableVersions:
		var in glueSDK.GetTableVersionsInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		versions := []gluetypes.TableVersion{}
		paginator := glueSDK.NewGetTableVersionsPaginator(clients.glue, &in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, operationError(op, err)
			}
			versions = append(versions, page.TableVersions...)
		}
		out = map[string]any{"TableVersions": versions}

	case OperationGlueDeleteTableVersion:
		var in glueSDK.DeleteTableVersionInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		if _, err := clients.glue.DeleteTableVersion(ctx, &in); err != nil {
			return nil, operationError(op, err)
		}
		out = struct{}{}

	case OperationGlueGetPartitions:
		var in glueSDK.GetPartitionsInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		partitions := []gluetypes.Partition{}
		paginator := glueSDK.NewGetPartitionsPaginator(clients.glue, &in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, operationError(op, err)
			}
			partitions = append(partitions, page.Partitions...)
		}
		out = map[string]any{"Partitions": partitions}

	case OperationGlueBatchDeletePartition:
		var in glueSDK.BatchDeletePartitionInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		all := in.PartitionsToDelete
		partitionErrors := []gluetypes.PartitionError{}
		for start := 0; start < len(all); start += glueBatchDeletePartitionLimit {
			end := min(start+glueBatchDeletePartitionLimit, len(all))
			chunk := in
			chunk.PartitionsToDelete = all[start:end]
			resp, err := clients.glue.BatchDeletePartition(ctx, &chunk)
			if err != nil {
				return nil, operationError(op, err)
			}
			partitionErrors = append(partitionErrors, resp.Errors...)
		}
		out = map[string]any{"Errors": partitionErrors}

	case OperationGlueBatchCreatePartition:
		var in glueSDK.BatchCreatePartitionInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		all := in.PartitionInputList
		partitionErrors := []gluetypes.PartitionError{}
		for start := 0; start < len(all); start += glueBatchCreatePartitionLimit {
			end := min(start+glueBatchCreatePartitionLimit, len(all))
			chunk := in
			chunk.PartitionInputList = all[start:end]
			resp, err := clients.glue.BatchCreatePartition(ctx, &chunk)
			if err != nil {
				return nil, operationError(op, err)
			}
			partitionErrors = append(partitionErrors, resp.Errors...)
		}
		out = map[string]any{"Errors": partitionErrors}

	case OperationGlueUpdateTable:
		var in glueSDK.UpdateTableInput
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		if _, err := clients.glue.UpdateTable(ctx, &in); err != nil {
			return nil, operationError(op, err)
		}
		out = struct{}{}

	case OperationS3ListObjects:
		var in s3SDK.ListObjectsV2Input
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		keys := []string{}
		paginator := s3SDK.NewListObjectsV2Paginator(clients.s3, &in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, operationError(op, err)
			}
			for _, object := range page.Contents {
				if object.Key != nil {
					keys = append(keys, *object.Key)
				}
			}
		}
		out = map[string]any{"Keys": keys}

	case OperationS3DeleteObjects:
		var in struct {
			Bucket string
			Keys   []string
		}
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		deleteErrors := []s3types.Error{}
		for start := 0; start < len(in.Keys); start += s3DeleteObjectsLimit {
			end := min(start+s3DeleteObjectsLimit, len(in.Keys))
			objects := make([]s3types.ObjectIdentifier, 0, end-start)
			for _, key := range in.Keys[start:end] {
				objects = append(objects, s3types.ObjectIdentifier{Key: &key})
			}
			resp, err := clients.s3.DeleteObjects(ctx, &s3SDK.DeleteObjectsInput{
				Bucket: &in.Bucket,
				Delete: &s3types.Delete{Objects: objects},
			})
			if err != nil {
				return nil, operationError(op, err)
			}
			deleteErrors = append(deleteErrors, resp.Errors...)
		}
		out = map[string]any{"Errors": deleteErrors}

	case OperationS3PutObject:
		var in struct {
			Bucket               string
			Key                  string
			Body                 string
			ServerSideEncryption string
			SSEKMSKeyId          string
			ACL                  string
			StorageClass         string
			ContentType          string
			BucketKeyEnabled     *bool
		}
		if err := decodePayload(op, s.operationPayload, &in); err != nil {
			return nil, err
		}
		body, err := base64.StdEncoding.DecodeString(in.Body)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[athena] %s: Body is not base64: %v", op, err),
			}
		}
		put := &s3SDK.PutObjectInput{
			Bucket:           &in.Bucket,
			Key:              &in.Key,
			Body:             bytes.NewReader(body),
			ContentType:      nilIfEmpty(in.ContentType),
			SSEKMSKeyId:      nilIfEmpty(in.SSEKMSKeyId),
			BucketKeyEnabled: in.BucketKeyEnabled,
		}
		if in.ServerSideEncryption != "" {
			put.ServerSideEncryption = s3types.ServerSideEncryption(in.ServerSideEncryption)
		}
		if in.ACL != "" {
			put.ACL = s3types.ObjectCannedACL(in.ACL)
		}
		if in.StorageClass != "" {
			put.StorageClass = s3types.StorageClass(in.StorageClass)
		}
		if _, err := clients.s3.PutObject(ctx, put); err != nil {
			return nil, operationError(op, err)
		}
		out = struct{}{}

	default:
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[athena] unknown %s '%s'", OptionOperation, op),
		}
	}
	if err != nil {
		return nil, err
	}

	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[athena] %s: cannot encode the response: %v", op, err),
		}
	}
	return encoded, nil
}

// operationRecordReader wraps a JSON response in the one-row result batch.
func operationRecordReader(alloc memory.Allocator, response []byte) (array.RecordReader, error) {
	builder := array.NewRecordBuilder(alloc, operationResultSchema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).Append(string(response))
	batch := builder.NewRecordBatch()
	defer batch.Release()
	return array.NewRecordReader(operationResultSchema, []arrow.Record{batch})
}
