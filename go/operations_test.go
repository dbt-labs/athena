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
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	glueSDK "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	lakeformationSDK "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lftypes "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	s3SDK "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	stsSDK "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockGlueClient struct {
	getTableFn             func(*glueSDK.GetTableInput) (*glueSDK.GetTableOutput, error)
	getTablesFn            func(*glueSDK.GetTablesInput) (*glueSDK.GetTablesOutput, error)
	getDatabaseFn          func(*glueSDK.GetDatabaseInput) (*glueSDK.GetDatabaseOutput, error)
	getDatabasesFn         func(*glueSDK.GetDatabasesInput) (*glueSDK.GetDatabasesOutput, error)
	deleteTableFn          func(*glueSDK.DeleteTableInput) (*glueSDK.DeleteTableOutput, error)
	deleteDatabaseFn       func(*glueSDK.DeleteDatabaseInput) (*glueSDK.DeleteDatabaseOutput, error)
	getTableVersionsFn     func(*glueSDK.GetTableVersionsInput) (*glueSDK.GetTableVersionsOutput, error)
	deleteTableVersionFn   func(*glueSDK.DeleteTableVersionInput) (*glueSDK.DeleteTableVersionOutput, error)
	getPartitionsFn        func(*glueSDK.GetPartitionsInput) (*glueSDK.GetPartitionsOutput, error)
	batchDeletePartitionFn func(*glueSDK.BatchDeletePartitionInput) (*glueSDK.BatchDeletePartitionOutput, error)
	batchCreatePartitionFn func(*glueSDK.BatchCreatePartitionInput) (*glueSDK.BatchCreatePartitionOutput, error)
	updateTableFn          func(*glueSDK.UpdateTableInput) (*glueSDK.UpdateTableOutput, error)
}

func (m *mockGlueClient) GetTable(_ context.Context, in *glueSDK.GetTableInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetTableOutput, error) {
	return m.getTableFn(in)
}
func (m *mockGlueClient) GetTables(_ context.Context, in *glueSDK.GetTablesInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetTablesOutput, error) {
	return m.getTablesFn(in)
}
func (m *mockGlueClient) GetDatabase(_ context.Context, in *glueSDK.GetDatabaseInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetDatabaseOutput, error) {
	return m.getDatabaseFn(in)
}
func (m *mockGlueClient) GetDatabases(_ context.Context, in *glueSDK.GetDatabasesInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetDatabasesOutput, error) {
	return m.getDatabasesFn(in)
}
func (m *mockGlueClient) DeleteTable(_ context.Context, in *glueSDK.DeleteTableInput, _ ...func(*glueSDK.Options)) (*glueSDK.DeleteTableOutput, error) {
	return m.deleteTableFn(in)
}
func (m *mockGlueClient) DeleteDatabase(_ context.Context, in *glueSDK.DeleteDatabaseInput, _ ...func(*glueSDK.Options)) (*glueSDK.DeleteDatabaseOutput, error) {
	return m.deleteDatabaseFn(in)
}
func (m *mockGlueClient) GetTableVersions(_ context.Context, in *glueSDK.GetTableVersionsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetTableVersionsOutput, error) {
	return m.getTableVersionsFn(in)
}
func (m *mockGlueClient) DeleteTableVersion(_ context.Context, in *glueSDK.DeleteTableVersionInput, _ ...func(*glueSDK.Options)) (*glueSDK.DeleteTableVersionOutput, error) {
	return m.deleteTableVersionFn(in)
}
func (m *mockGlueClient) GetPartitions(_ context.Context, in *glueSDK.GetPartitionsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetPartitionsOutput, error) {
	return m.getPartitionsFn(in)
}
func (m *mockGlueClient) BatchDeletePartition(_ context.Context, in *glueSDK.BatchDeletePartitionInput, _ ...func(*glueSDK.Options)) (*glueSDK.BatchDeletePartitionOutput, error) {
	return m.batchDeletePartitionFn(in)
}
func (m *mockGlueClient) BatchCreatePartition(_ context.Context, in *glueSDK.BatchCreatePartitionInput, _ ...func(*glueSDK.Options)) (*glueSDK.BatchCreatePartitionOutput, error) {
	return m.batchCreatePartitionFn(in)
}
func (m *mockGlueClient) UpdateTable(_ context.Context, in *glueSDK.UpdateTableInput, _ ...func(*glueSDK.Options)) (*glueSDK.UpdateTableOutput, error) {
	return m.updateTableFn(in)
}

type mockS3Client struct {
	listObjectsV2Fn func(*s3SDK.ListObjectsV2Input) (*s3SDK.ListObjectsV2Output, error)
	deleteObjectsFn func(*s3SDK.DeleteObjectsInput) (*s3SDK.DeleteObjectsOutput, error)
	putObjectFn     func(*s3SDK.PutObjectInput) (*s3SDK.PutObjectOutput, error)
}

func (m *mockS3Client) ListObjectsV2(_ context.Context, in *s3SDK.ListObjectsV2Input, _ ...func(*s3SDK.Options)) (*s3SDK.ListObjectsV2Output, error) {
	return m.listObjectsV2Fn(in)
}
func (m *mockS3Client) DeleteObjects(_ context.Context, in *s3SDK.DeleteObjectsInput, _ ...func(*s3SDK.Options)) (*s3SDK.DeleteObjectsOutput, error) {
	return m.deleteObjectsFn(in)
}
func (m *mockS3Client) PutObject(_ context.Context, in *s3SDK.PutObjectInput, _ ...func(*s3SDK.Options)) (*s3SDK.PutObjectOutput, error) {
	return m.putObjectFn(in)
}

type mockStsClient struct {
	getCallerIdentityFn func(*stsSDK.GetCallerIdentityInput) (*stsSDK.GetCallerIdentityOutput, error)
}

func (m *mockStsClient) GetCallerIdentity(_ context.Context, in *stsSDK.GetCallerIdentityInput, _ ...func(*stsSDK.Options)) (*stsSDK.GetCallerIdentityOutput, error) {
	return m.getCallerIdentityFn(in)
}

// mockLakeFormationClient records the calls it serves; the list calls answer from
// the pages given, one page per call.
type mockLakeFormationClient struct {
	calls           []string
	lfTags          *lakeformationSDK.GetResourceLFTagsOutput
	filterPages     [][]lftypes.DataCellsFilter
	permissionPages [][]lftypes.PrincipalResourcePermissions
	added           *lakeformationSDK.AddLFTagsToResourceInput
	granted         *lakeformationSDK.BatchGrantPermissionsInput
}

func (m *mockLakeFormationClient) AddLFTagsToResource(_ context.Context, in *lakeformationSDK.AddLFTagsToResourceInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.AddLFTagsToResourceOutput, error) {
	m.calls, m.added = append(m.calls, "add"), in
	return &lakeformationSDK.AddLFTagsToResourceOutput{}, nil
}
func (m *mockLakeFormationClient) RemoveLFTagsFromResource(_ context.Context, _ *lakeformationSDK.RemoveLFTagsFromResourceInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.RemoveLFTagsFromResourceOutput, error) {
	m.calls = append(m.calls, "remove")
	return &lakeformationSDK.RemoveLFTagsFromResourceOutput{}, nil
}
func (m *mockLakeFormationClient) GetResourceLFTags(_ context.Context, _ *lakeformationSDK.GetResourceLFTagsInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.GetResourceLFTagsOutput, error) {
	m.calls = append(m.calls, "get")
	return m.lfTags, nil
}
func (m *mockLakeFormationClient) ListDataCellsFilter(_ context.Context, in *lakeformationSDK.ListDataCellsFilterInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.ListDataCellsFilterOutput, error) {
	page, next := pageAt(in.NextToken, len(m.filterPages))
	return &lakeformationSDK.ListDataCellsFilterOutput{DataCellsFilters: m.filterPages[page], NextToken: next}, nil
}
func (m *mockLakeFormationClient) CreateDataCellsFilter(_ context.Context, _ *lakeformationSDK.CreateDataCellsFilterInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.CreateDataCellsFilterOutput, error) {
	m.calls = append(m.calls, "create_filter")
	return &lakeformationSDK.CreateDataCellsFilterOutput{}, nil
}
func (m *mockLakeFormationClient) UpdateDataCellsFilter(_ context.Context, _ *lakeformationSDK.UpdateDataCellsFilterInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.UpdateDataCellsFilterOutput, error) {
	m.calls = append(m.calls, "update_filter")
	return &lakeformationSDK.UpdateDataCellsFilterOutput{}, nil
}
func (m *mockLakeFormationClient) DeleteDataCellsFilter(_ context.Context, _ *lakeformationSDK.DeleteDataCellsFilterInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.DeleteDataCellsFilterOutput, error) {
	m.calls = append(m.calls, "delete_filter")
	return &lakeformationSDK.DeleteDataCellsFilterOutput{}, nil
}
func (m *mockLakeFormationClient) ListPermissions(_ context.Context, in *lakeformationSDK.ListPermissionsInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.ListPermissionsOutput, error) {
	page, next := pageAt(in.NextToken, len(m.permissionPages))
	return &lakeformationSDK.ListPermissionsOutput{PrincipalResourcePermissions: m.permissionPages[page], NextToken: next}, nil
}
func (m *mockLakeFormationClient) BatchGrantPermissions(_ context.Context, in *lakeformationSDK.BatchGrantPermissionsInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.BatchGrantPermissionsOutput, error) {
	m.calls, m.granted = append(m.calls, "grant"), in
	return &lakeformationSDK.BatchGrantPermissionsOutput{}, nil
}
func (m *mockLakeFormationClient) BatchRevokePermissions(_ context.Context, _ *lakeformationSDK.BatchRevokePermissionsInput, _ ...func(*lakeformationSDK.Options)) (*lakeformationSDK.BatchRevokePermissionsOutput, error) {
	m.calls = append(m.calls, "revoke")
	return &lakeformationSDK.BatchRevokePermissionsOutput{}, nil
}

// pageAt maps a NextToken ("1", "2", ...) to a page index and the next token.
func pageAt(token *string, pages int) (int, *string) {
	page := 0
	if token != nil {
		page, _ = strconv.Atoi(*token)
	}
	if page+1 < pages {
		next := strconv.Itoa(page + 1)
		return page, &next
	}
	return page, nil
}

// newOperationStmt builds a statement whose connection carries the given mocks
// and whose options select `op` with `payload`.
func newOperationStmt(t testing.TB, athena athenaClientAPI, aws *awsClients, op, payload string) *statementImpl {
	t.Helper()
	if athena == nil {
		athena = &mockAthenaClient{}
	}
	stmt := newTestStmt(t, athena)
	stmt.conn.aws = aws
	require.NoError(t, stmt.SetOption(OptionOperation, op))
	require.NoError(t, stmt.SetOption(OptionOperationPayload, payload))
	return stmt
}

// runOperationJSON executes the statement as a query and decodes the single
// `result` cell into `into`.
func runOperationJSON(t testing.TB, stmt *statementImpl, into any) {
	t.Helper()
	rdr, rows, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	defer rdr.Release()
	assert.Equal(t, int64(-1), rows)
	assert.True(t, operationResultSchema.Equal(rdr.Schema()))
	require.True(t, rdr.Next())
	rec := rdr.RecordBatch()
	require.Equal(t, int64(1), rec.NumRows())
	cell := rec.Column(0).(*array.String).Value(0)
	require.NoError(t, json.Unmarshal([]byte(cell), into), cell)
	assert.False(t, rdr.Next())
}

func requireStatus(t testing.TB, err error, status adbc.Status) {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, status, adbcErr.Code, adbcErr.Msg)
}

func TestOperation_OptionsAreStatementLocal(t *testing.T) {
	stmt := newTestStmt(t, &mockAthenaClient{})
	require.NoError(t, stmt.SetOption(OptionOperation, OperationGlueGetTable))
	require.NoError(t, stmt.SetOption(OptionOperationPayload, `{"DatabaseName":"db","Name":"t"}`))
	assert.Equal(t, OperationGlueGetTable, stmt.operation)
	assert.Equal(t, `{"DatabaseName":"db","Name":"t"}`, stmt.operationPayload)

	// other unknown statement options still go to driverbase
	err := stmt.SetOption("athena.no_such_option", "x")
	requireStatus(t, err, adbc.StatusNotImplemented)
}

func TestOperation_UnknownOperationAndBadPayload(t *testing.T) {
	stmt := newOperationStmt(t, nil, &awsClients{}, "glue.no_such_operation", "{}")
	_, _, err := stmt.ExecuteQuery(context.Background())
	requireStatus(t, err, adbc.StatusInvalidArgument)

	stmt = newOperationStmt(t, nil, &awsClients{glue: &mockGlueClient{}}, OperationGlueGetTable, `{"DatabaseName":`)
	_, _, err = stmt.ExecuteQuery(context.Background())
	requireStatus(t, err, adbc.StatusInvalidArgument)
}

func TestOperation_UnknownOperationFailsBeforeBuildingClients(t *testing.T) {
	stmt := newOperationStmt(t, nil, nil, "glue.no_such_operation", "{}")
	_, err := stmt.ExecuteUpdate(context.Background())
	requireStatus(t, err, adbc.StatusInvalidArgument)
	assert.Nil(t, stmt.conn.aws, "no AWS client is built for an unknown operation")
}

func TestOperation_StsRejectsAMalformedPayload(t *testing.T) {
	sts := &mockStsClient{getCallerIdentityFn: func(*stsSDK.GetCallerIdentityInput) (*stsSDK.GetCallerIdentityOutput, error) {
		t.Fatal("GetCallerIdentity must not be called with a malformed payload")
		return nil, nil
	}}
	stmt := newOperationStmt(t, nil, &awsClients{sts: sts}, OperationStsGetCallerIdentity, "{")
	_, _, err := stmt.ExecuteQuery(context.Background())
	requireStatus(t, err, adbc.StatusInvalidArgument)
}

func TestOperation_ExecuteUpdateRunsTheOperation(t *testing.T) {
	deleted := 0
	glue := &mockGlueClient{deleteTableVersionFn: func(in *glueSDK.DeleteTableVersionInput) (*glueSDK.DeleteTableVersionOutput, error) {
		deleted++
		assert.Equal(t, "7", *in.VersionId)
		assert.Equal(t, "123", *in.CatalogId)
		return &glueSDK.DeleteTableVersionOutput{}, nil
	}}
	stmt := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueDeleteTableVersion,
		`{"CatalogId":"123","DatabaseName":"db","TableName":"t","VersionId":"7"}`)
	rows, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
	assert.Equal(t, 1, deleted)
}

func TestOperation_StsGetCallerIdentity(t *testing.T) {
	sts := &mockStsClient{getCallerIdentityFn: func(*stsSDK.GetCallerIdentityInput) (*stsSDK.GetCallerIdentityOutput, error) {
		return &stsSDK.GetCallerIdentityOutput{Account: strp("123456789012")}, nil
	}}
	stmt := newOperationStmt(t, nil, &awsClients{sts: sts}, OperationStsGetCallerIdentity, "")
	var out struct{ Account string }
	runOperationJSON(t, stmt, &out)
	assert.Equal(t, "123456789012", out.Account)
}

func TestOperation_AthenaGetWorkGroupAndDataCatalog(t *testing.T) {
	athena := &mockAthenaClient{
		getWorkGroupFn: func(_ context.Context, in *athenaSDK.GetWorkGroupInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetWorkGroupOutput, error) {
			assert.Equal(t, "wg", *in.WorkGroup)
			return &athenaSDK.GetWorkGroupOutput{WorkGroup: &athenatypes.WorkGroup{
				Name: strp("wg"),
				Configuration: &athenatypes.WorkGroupConfiguration{
					EnforceWorkGroupConfiguration: boolp(true),
					ResultConfiguration:           &athenatypes.ResultConfiguration{OutputLocation: strp("s3://out/")},
				},
			}}, nil
		},
		getDataCatalogFn: func(_ context.Context, in *athenaSDK.GetDataCatalogInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetDataCatalogOutput, error) {
			assert.Equal(t, "cat", *in.Name)
			return &athenaSDK.GetDataCatalogOutput{DataCatalog: &athenatypes.DataCatalog{
				Name: strp("cat"), Type: athenatypes.DataCatalogTypeGlue, Parameters: map[string]string{"catalog-id": "123"},
			}}, nil
		},
	}
	stmt := newOperationStmt(t, athena, &awsClients{}, OperationAthenaGetWorkGroup, `{"WorkGroup":"wg"}`)
	var wg struct {
		WorkGroup struct {
			Configuration struct {
				EnforceWorkGroupConfiguration bool
				ResultConfiguration           struct{ OutputLocation string }
			}
		}
	}
	runOperationJSON(t, stmt, &wg)
	assert.True(t, wg.WorkGroup.Configuration.EnforceWorkGroupConfiguration)
	assert.Equal(t, "s3://out/", wg.WorkGroup.Configuration.ResultConfiguration.OutputLocation)

	stmt = newOperationStmt(t, athena, &awsClients{}, OperationAthenaGetDataCatalog, `{"Name":"cat"}`)
	var cat struct {
		DataCatalog struct {
			Type       string
			Parameters map[string]string
		}
	}
	runOperationJSON(t, stmt, &cat)
	assert.Equal(t, "GLUE", cat.DataCatalog.Type)
	assert.Equal(t, "123", cat.DataCatalog.Parameters["catalog-id"])
}

func TestOperation_AthenaSparkSessionAndCalculation(t *testing.T) {
	athena := &mockAthenaClient{
		startSessionFn: func(_ context.Context, in *athenaSDK.StartSessionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StartSessionOutput, error) {
			assert.Equal(t, "spark-wg", *in.WorkGroup)
			assert.Equal(t, int32(2), *in.EngineConfiguration.MaxConcurrentDpus)
			return &athenaSDK.StartSessionOutput{SessionId: strp("s-1"), State: athenatypes.SessionStateCreating}, nil
		},
		getSessionStatusFn: func(_ context.Context, in *athenaSDK.GetSessionStatusInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetSessionStatusOutput, error) {
			assert.Equal(t, "s-1", *in.SessionId)
			return &athenaSDK.GetSessionStatusOutput{SessionId: in.SessionId, Status: &athenatypes.SessionStatus{State: athenatypes.SessionStateIdle}}, nil
		},
		startCalculationFn: func(_ context.Context, in *athenaSDK.StartCalculationExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StartCalculationExecutionOutput, error) {
			assert.Equal(t, "print(1)", *in.CodeBlock)
			return &athenaSDK.StartCalculationExecutionOutput{CalculationExecutionId: strp("c-1")}, nil
		},
		getCalculationFn: func(_ context.Context, in *athenaSDK.GetCalculationExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetCalculationExecutionOutput, error) {
			return &athenaSDK.GetCalculationExecutionOutput{
				CalculationExecutionId: in.CalculationExecutionId,
				Status:                 &athenatypes.CalculationStatus{State: athenatypes.CalculationExecutionStateCompleted},
				Result:                 &athenatypes.CalculationResult{StdErrorS3Uri: strp("s3://out/stderr")},
			}, nil
		},
		stopCalculationFn: func(_ context.Context, in *athenaSDK.StopCalculationExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StopCalculationExecutionOutput, error) {
			return &athenaSDK.StopCalculationExecutionOutput{State: athenatypes.CalculationExecutionStateCanceling}, nil
		},
		terminateSessionFn: func(_ context.Context, in *athenaSDK.TerminateSessionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.TerminateSessionOutput, error) {
			assert.Equal(t, "s-1", *in.SessionId)
			return &athenaSDK.TerminateSessionOutput{State: athenatypes.SessionStateTerminating}, nil
		},
	}

	var session struct{ SessionId, State string }
	runOperationJSON(t, newOperationStmt(t, athena, &awsClients{}, OperationAthenaStartSession,
		`{"WorkGroup":"spark-wg","EngineConfiguration":{"CoordinatorDpuSize":1,"MaxConcurrentDpus":2,"DefaultExecutorDpuSize":1}}`), &session)
	assert.Equal(t, "s-1", session.SessionId)
	assert.Equal(t, "CREATING", session.State)

	var status struct{ Status struct{ State string } }
	runOperationJSON(t, newOperationStmt(t, athena, &awsClients{}, OperationAthenaGetSessionStatus, `{"SessionId":"s-1"}`), &status)
	assert.Equal(t, "IDLE", status.Status.State)

	var started struct{ CalculationExecutionId string }
	runOperationJSON(t, newOperationStmt(t, athena, &awsClients{}, OperationAthenaStartCalculationExecution,
		`{"SessionId":"s-1","CodeBlock":"print(1)"}`), &started)
	assert.Equal(t, "c-1", started.CalculationExecutionId)

	var calculation struct {
		Status struct{ State string }
		Result struct{ StdErrorS3Uri string }
	}
	runOperationJSON(t, newOperationStmt(t, athena, &awsClients{}, OperationAthenaGetCalculationExecution,
		`{"CalculationExecutionId":"c-1"}`), &calculation)
	assert.Equal(t, "COMPLETED", calculation.Status.State)
	assert.Equal(t, "s3://out/stderr", calculation.Result.StdErrorS3Uri)

	var stopped struct{ State string }
	runOperationJSON(t, newOperationStmt(t, athena, &awsClients{}, OperationAthenaStopCalculationExecution,
		`{"CalculationExecutionId":"c-1"}`), &stopped)
	assert.Equal(t, "CANCELING", stopped.State)

	var terminated struct{ State string }
	runOperationJSON(t, newOperationStmt(t, athena, &awsClients{}, OperationAthenaTerminateSession, `{"SessionId":"s-1"}`), &terminated)
	assert.Equal(t, "TERMINATING", terminated.State)
}

func TestOperation_CleanupOperationsIgnoreTheStatementsCancellation(t *testing.T) {
	var calls []string
	athena := &mockAthenaClient{
		stopCalculationFn: func(ctx context.Context, _ *athenaSDK.StopCalculationExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StopCalculationExecutionOutput, error) {
			calls = append(calls, "stop")
			require.NoError(t, ctx.Err())
			return &athenaSDK.StopCalculationExecutionOutput{State: athenatypes.CalculationExecutionStateCanceling}, nil
		},
		terminateSessionFn: func(ctx context.Context, _ *athenaSDK.TerminateSessionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.TerminateSessionOutput, error) {
			calls = append(calls, "terminate")
			require.NoError(t, ctx.Err())
			_, hasDeadline := ctx.Deadline()
			assert.True(t, hasDeadline)
			return &athenaSDK.TerminateSessionOutput{State: athenatypes.SessionStateTerminating}, nil
		},
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for op, payload := range map[string]string{
		OperationAthenaTerminateSession:         `{"SessionId":"s-1"}`,
		OperationAthenaStopCalculationExecution: `{"CalculationExecutionId":"c-1"}`,
	} {
		rdr, _, err := newOperationStmt(t, athena, &awsClients{}, op, payload).ExecuteQuery(cancelled)
		require.NoError(t, err, op)
		rdr.Release()
	}
	assert.ElementsMatch(t, []string{"stop", "terminate"}, calls)
}

func TestOperation_LakeFormation(t *testing.T) {
	lf := &mockLakeFormationClient{
		lfTags: &lakeformationSDK.GetResourceLFTagsOutput{
			LFTagsOnTable: []lftypes.LFTagPair{{TagKey: strp("tier"), TagValues: []string{"lab"}}},
		},
		filterPages: [][]lftypes.DataCellsFilter{
			{{Name: strp("f1")}},
			{{Name: strp("f2")}},
		},
		permissionPages: [][]lftypes.PrincipalResourcePermissions{
			{{Principal: &lftypes.DataLakePrincipal{DataLakePrincipalIdentifier: strp("arn:aws:iam::1:role/a")}}},
			{{Principal: &lftypes.DataLakePrincipal{DataLakePrincipalIdentifier: strp("arn:aws:iam::1:role/b")}}},
		},
	}
	aws := &awsClients{lakeformation: lf}
	table := `{"Table":{"DatabaseName":"db","Name":"t"}}`

	var failures struct{ Failures []any }
	runOperationJSON(t, newOperationStmt(t, nil, aws, OperationLakeFormationAddLFTagsToResource,
		`{"Resource":{"Table":{"DatabaseName":"db","Name":"t"}},"LFTags":[{"TagKey":"tier","TagValues":["lab"]}]}`), &failures)
	assert.Equal(t, "t", *lf.added.Resource.Table.Name)
	assert.Equal(t, []string{"lab"}, lf.added.LFTags[0].TagValues)

	var tags struct{ LFTagsOnTable []struct{ TagKey string } }
	runOperationJSON(t, newOperationStmt(t, nil, aws, OperationLakeFormationGetResourceLFTags, `{"Resource":`+table+`}`), &tags)
	assert.Equal(t, "tier", tags.LFTagsOnTable[0].TagKey)

	var filters struct{ DataCellsFilters []struct{ Name string } }
	runOperationJSON(t, newOperationStmt(t, nil, aws, OperationLakeFormationListDataCellsFilter,
		`{"Table":{"DatabaseName":"db","Name":"t"}}`), &filters)
	assert.Len(t, filters.DataCellsFilters, 2, "every page")

	var permissions struct {
		PrincipalResourcePermissions []struct {
			Principal struct{ DataLakePrincipalIdentifier string }
		}
	}
	runOperationJSON(t, newOperationStmt(t, nil, aws, OperationLakeFormationListPermissions,
		`{"Resource":{"DataCellsFilter":{"DatabaseName":"db","TableName":"t","Name":"f1"}}}`), &permissions)
	assert.Len(t, permissions.PrincipalResourcePermissions, 2, "every page")

	runOperationJSON(t, newOperationStmt(t, nil, aws, OperationLakeFormationBatchGrantPermissions,
		`{"Entries":[{"Id":"0","Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::1:role/a"},"Permissions":["SELECT"]}]}`), &failures)
	assert.Equal(t, []lftypes.Permission{lftypes.PermissionSelect}, lf.granted.Entries[0].Permissions)

	for _, op := range []string{
		OperationLakeFormationRemoveLFTagsFromResource,
		OperationLakeFormationCreateDataCellsFilter,
		OperationLakeFormationUpdateDataCellsFilter,
		OperationLakeFormationDeleteDataCellsFilter,
		OperationLakeFormationBatchRevokePermissions,
	} {
		var empty map[string]any
		runOperationJSON(t, newOperationStmt(t, nil, aws, op, `{}`), &empty)
	}
	assert.Equal(t, []string{"add", "get", "grant", "remove", "create_filter", "update_filter", "delete_filter", "revoke"}, lf.calls)
}

func TestOperation_GlueGetTable(t *testing.T) {
	glue := &mockGlueClient{getTableFn: func(in *glueSDK.GetTableInput) (*glueSDK.GetTableOutput, error) {
		assert.Equal(t, "db", *in.DatabaseName)
		switch *in.Name {
		case "t":
			return &glueSDK.GetTableOutput{Table: &gluetypes.Table{
				Name:      strp("t"),
				TableType: strp("EXTERNAL_TABLE"),
				Parameters: map[string]string{
					"table_type": "ICEBERG",
				},
				StorageDescriptor: &gluetypes.StorageDescriptor{
					Location: strp("s3://b/tables/t/"),
					Columns:  []gluetypes.Column{{Name: strp("id"), Type: strp("int"), Comment: strp("pk")}},
				},
			}}, nil
		default:
			return nil, &gluetypes.EntityNotFoundException{Message: strp("Entity Not Found")}
		}
	}}
	stmt := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetTable, `{"DatabaseName":"db","Name":"t"}`)
	var out struct {
		Table *struct {
			TableType         string
			Parameters        map[string]string
			StorageDescriptor struct {
				Location string
				Columns  []struct{ Name, Type, Comment string }
			}
		}
	}
	runOperationJSON(t, stmt, &out)
	require.NotNil(t, out.Table)
	assert.Equal(t, "EXTERNAL_TABLE", out.Table.TableType)
	assert.Equal(t, "ICEBERG", out.Table.Parameters["table_type"])
	assert.Equal(t, "s3://b/tables/t/", out.Table.StorageDescriptor.Location)
	assert.Equal(t, "pk", out.Table.StorageDescriptor.Columns[0].Comment)

	stmt = newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetTable, `{"DatabaseName":"db","Name":"missing"}`)
	out.Table = nil
	runOperationJSON(t, stmt, &out)
	assert.Nil(t, out.Table)
}

func TestOperation_GlueGetTablesCollectsAllPagesAndTreatsAMissingDatabaseAsEmpty(t *testing.T) {
	calls := 0
	glue := &mockGlueClient{getTablesFn: func(in *glueSDK.GetTablesInput) (*glueSDK.GetTablesOutput, error) {
		calls++
		if *in.DatabaseName == "missing" {
			return nil, &gluetypes.EntityNotFoundException{Message: strp("Database missing not found")}
		}
		assert.Equal(t, "123:s3tablescatalog/b", *in.CatalogId)
		if in.NextToken == nil {
			return &glueSDK.GetTablesOutput{
				TableList: []gluetypes.Table{{Name: strp("a"), TableType: strp("EXTERNAL_TABLE")}},
				NextToken: strp("more"),
			}, nil
		}
		return &glueSDK.GetTablesOutput{TableList: []gluetypes.Table{{Name: strp("v"), TableType: strp("VIRTUAL_VIEW")}}}, nil
	}}
	stmt := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetTables, `{"CatalogId":"123:s3tablescatalog/b","DatabaseName":"db"}`)
	var out struct {
		TableList []struct{ Name, TableType string }
	}
	runOperationJSON(t, stmt, &out)
	assert.Equal(t, 2, calls)
	require.Len(t, out.TableList, 2)
	assert.Equal(t, "VIRTUAL_VIEW", out.TableList[1].TableType)

	stmt = newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetTables, `{"DatabaseName":"missing"}`)
	runOperationJSON(t, stmt, &out)
	assert.Empty(t, out.TableList)
}

func TestOperation_GlueGetDatabaseAndDatabases(t *testing.T) {
	glue := &mockGlueClient{
		getDatabaseFn: func(in *glueSDK.GetDatabaseInput) (*glueSDK.GetDatabaseOutput, error) {
			if *in.Name == "db" {
				return &glueSDK.GetDatabaseOutput{Database: &gluetypes.Database{Name: strp("db")}}, nil
			}
			return nil, &gluetypes.EntityNotFoundException{Message: strp("no")}
		},
		getDatabasesFn: func(in *glueSDK.GetDatabasesInput) (*glueSDK.GetDatabasesOutput, error) {
			if in.NextToken == nil {
				return &glueSDK.GetDatabasesOutput{DatabaseList: []gluetypes.Database{{Name: strp("a")}}, NextToken: strp("n")}, nil
			}
			return &glueSDK.GetDatabasesOutput{DatabaseList: []gluetypes.Database{{Name: strp("b")}}}, nil
		},
	}
	var one struct{ Database *struct{ Name string } }
	runOperationJSON(t, newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetDatabase, `{"Name":"db"}`), &one)
	require.NotNil(t, one.Database)
	assert.Equal(t, "db", one.Database.Name)
	one.Database = nil
	runOperationJSON(t, newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetDatabase, `{"Name":"missing"}`), &one)
	assert.Nil(t, one.Database)

	var all struct{ DatabaseList []struct{ Name string } }
	runOperationJSON(t, newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetDatabases, `{}`), &all)
	require.Len(t, all.DatabaseList, 2)
	assert.Equal(t, "b", all.DatabaseList[1].Name)
}

func TestOperation_GlueDeleteTableTreatsNotFoundAsDeletedFalse(t *testing.T) {
	glue := &mockGlueClient{deleteTableFn: func(in *glueSDK.DeleteTableInput) (*glueSDK.DeleteTableOutput, error) {
		if *in.Name == "missing" {
			return nil, &gluetypes.EntityNotFoundException{Message: strp("no")}
		}
		if *in.Name == "denied" {
			return nil, errors.New("AccessDeniedException")
		}
		return &glueSDK.DeleteTableOutput{}, nil
	}}
	var out struct{ Deleted bool }
	runOperationJSON(t, newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueDeleteTable, `{"DatabaseName":"db","Name":"t"}`), &out)
	assert.True(t, out.Deleted)
	runOperationJSON(t, newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueDeleteTable, `{"DatabaseName":"db","Name":"missing"}`), &out)
	assert.False(t, out.Deleted)
	_, _, err := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueDeleteTable, `{"DatabaseName":"db","Name":"denied"}`).ExecuteQuery(context.Background())
	requireStatus(t, err, adbc.StatusIO)
}

func TestOperation_GlueGetTableVersionsCollectsAllPages(t *testing.T) {
	calls := 0
	glue := &mockGlueClient{getTableVersionsFn: func(in *glueSDK.GetTableVersionsInput) (*glueSDK.GetTableVersionsOutput, error) {
		calls++
		if in.NextToken == nil {
			return &glueSDK.GetTableVersionsOutput{
				TableVersions: []gluetypes.TableVersion{{VersionId: strp("1")}, {VersionId: strp("2")}},
				NextToken:     strp("more"),
			}, nil
		}
		return &glueSDK.GetTableVersionsOutput{TableVersions: []gluetypes.TableVersion{{VersionId: strp("3")}}}, nil
	}}
	stmt := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetTableVersions, `{"DatabaseName":"db","TableName":"t"}`)
	var out struct{ TableVersions []struct{ VersionId string } }
	runOperationJSON(t, stmt, &out)
	assert.Equal(t, 2, calls)
	assert.Equal(t, []string{"1", "2", "3"}, []string{out.TableVersions[0].VersionId, out.TableVersions[1].VersionId, out.TableVersions[2].VersionId})
}

func TestOperation_GlueGetPartitionsPassesExpressionAndPaginates(t *testing.T) {
	var expressions []string
	glue := &mockGlueClient{getPartitionsFn: func(in *glueSDK.GetPartitionsInput) (*glueSDK.GetPartitionsOutput, error) {
		expressions = append(expressions, *in.Expression)
		assert.True(t, *in.ExcludeColumnSchema)
		if in.NextToken == nil {
			return &glueSDK.GetPartitionsOutput{Partitions: []gluetypes.Partition{{Values: []string{"a"}}}, NextToken: strp("n")}, nil
		}
		return &glueSDK.GetPartitionsOutput{Partitions: []gluetypes.Partition{{Values: []string{"b"}}}}, nil
	}}
	stmt := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueGetPartitions,
		`{"DatabaseName":"db","TableName":"t","Expression":"(status='a') or (status='b')","ExcludeColumnSchema":true}`)
	var out struct{ Partitions []struct{ Values []string } }
	runOperationJSON(t, stmt, &out)
	assert.Equal(t, []string{"(status='a') or (status='b')", "(status='a') or (status='b')"}, expressions)
	require.Len(t, out.Partitions, 2)
	assert.Equal(t, []string{"b"}, out.Partitions[1].Values)
}

func TestOperation_GlueBatchDeletePartitionChunksTo25(t *testing.T) {
	var sizes []int
	glue := &mockGlueClient{batchDeletePartitionFn: func(in *glueSDK.BatchDeletePartitionInput) (*glueSDK.BatchDeletePartitionOutput, error) {
		sizes = append(sizes, len(in.PartitionsToDelete))
		assert.Equal(t, "db", *in.DatabaseName)
		if len(sizes) == 1 {
			return &glueSDK.BatchDeletePartitionOutput{Errors: []gluetypes.PartitionError{{
				PartitionValues: in.PartitionsToDelete[0].Values,
				ErrorDetail:     &gluetypes.ErrorDetail{ErrorCode: strp("EntityNotFoundException")},
			}}}, nil
		}
		return &glueSDK.BatchDeletePartitionOutput{}, nil
	}}
	partitions := make([]map[string][]string, 0, 60)
	for i := 0; i < 60; i++ {
		partitions = append(partitions, map[string][]string{"Values": {strconv.Itoa(i)}})
	}
	payload, err := json.Marshal(map[string]any{"DatabaseName": "db", "TableName": "t", "PartitionsToDelete": partitions})
	require.NoError(t, err)
	stmt := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueBatchDeletePartition, string(payload))
	var out struct {
		Errors []struct {
			PartitionValues []string
			ErrorDetail     struct{ ErrorCode string }
		}
	}
	runOperationJSON(t, stmt, &out)
	assert.Equal(t, []int{25, 25, 10}, sizes)
	require.Len(t, out.Errors, 1)
	assert.Equal(t, []string{"0"}, out.Errors[0].PartitionValues)
	assert.Equal(t, "EntityNotFoundException", out.Errors[0].ErrorDetail.ErrorCode)
}

func TestOperation_GlueBatchCreatePartitionChunksTo100(t *testing.T) {
	var sizes []int
	glue := &mockGlueClient{batchCreatePartitionFn: func(in *glueSDK.BatchCreatePartitionInput) (*glueSDK.BatchCreatePartitionOutput, error) {
		sizes = append(sizes, len(in.PartitionInputList))
		assert.Equal(t, "s3://b/p/", *in.PartitionInputList[0].StorageDescriptor.Location)
		return &glueSDK.BatchCreatePartitionOutput{}, nil
	}}
	inputs := make([]map[string]any, 0, 150)
	for i := 0; i < 150; i++ {
		inputs = append(inputs, map[string]any{"Values": []string{strconv.Itoa(i)}, "StorageDescriptor": map[string]any{"Location": "s3://b/p/"}})
	}
	payload, err := json.Marshal(map[string]any{"DatabaseName": "db", "TableName": "t", "PartitionInputList": inputs})
	require.NoError(t, err)
	stmt := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueBatchCreatePartition, string(payload))
	var out struct{ Errors []any }
	runOperationJSON(t, stmt, &out)
	assert.Equal(t, []int{100, 50}, sizes)
	assert.Empty(t, out.Errors)
}

func TestOperation_GlueUpdateTableTakesTheTableInputJSON(t *testing.T) {
	var got *glueSDK.UpdateTableInput
	glue := &mockGlueClient{updateTableFn: func(in *glueSDK.UpdateTableInput) (*glueSDK.UpdateTableOutput, error) {
		got = in
		return &glueSDK.UpdateTableOutput{}, nil
	}}
	stmt := newOperationStmt(t, nil, &awsClients{glue: glue}, OperationGlueUpdateTable, `{
		"CatalogId": "123", "DatabaseName": "db", "SkipArchive": true,
		"TableInput": {"Name": "t", "Description": "docs", "TableType": "EXTERNAL_TABLE",
		               "Parameters": {"comment": "docs"},
		               "StorageDescriptor": {"Location": "s3://b/t/", "Columns": [{"Name": "id", "Type": "int", "Comment": "pk"}]},
		               "PartitionKeys": [{"Name": "dt", "Type": "date"}]}}`)
	var out struct{}
	runOperationJSON(t, stmt, &out)
	require.NotNil(t, got)
	assert.Equal(t, "123", *got.CatalogId)
	assert.True(t, *got.SkipArchive)
	assert.Equal(t, "docs", *got.TableInput.Description)
	assert.Equal(t, "pk", *got.TableInput.StorageDescriptor.Columns[0].Comment)
	assert.Equal(t, "dt", *got.TableInput.PartitionKeys[0].Name)
}

func TestOperation_S3ListAndDeleteObjects(t *testing.T) {
	var deleteSizes []int
	s3 := &mockS3Client{
		listObjectsV2Fn: func(in *s3SDK.ListObjectsV2Input) (*s3SDK.ListObjectsV2Output, error) {
			assert.Equal(t, "b", *in.Bucket)
			assert.Equal(t, "tables/t/", *in.Prefix)
			if in.ContinuationToken == nil {
				return &s3SDK.ListObjectsV2Output{
					Contents:              []s3types.Object{{Key: strp("tables/t/a.parquet")}},
					IsTruncated:           boolp(true),
					NextContinuationToken: strp("c"),
				}, nil
			}
			return &s3SDK.ListObjectsV2Output{Contents: []s3types.Object{{Key: strp("tables/t/b.parquet")}}}, nil
		},
		deleteObjectsFn: func(in *s3SDK.DeleteObjectsInput) (*s3SDK.DeleteObjectsOutput, error) {
			deleteSizes = append(deleteSizes, len(in.Delete.Objects))
			if len(deleteSizes) == 3 {
				return &s3SDK.DeleteObjectsOutput{Errors: []s3types.Error{{Key: in.Delete.Objects[0].Key, Code: strp("AccessDenied"), Message: strp("no")}}}, nil
			}
			return &s3SDK.DeleteObjectsOutput{}, nil
		},
	}
	stmt := newOperationStmt(t, nil, &awsClients{s3: s3}, OperationS3ListObjects, `{"Bucket":"b","Prefix":"tables/t/"}`)
	var listed struct{ Keys []string }
	runOperationJSON(t, stmt, &listed)
	assert.Equal(t, []string{"tables/t/a.parquet", "tables/t/b.parquet"}, listed.Keys)

	keys := make([]string, 0, 2500)
	for i := 0; i < 2500; i++ {
		keys = append(keys, fmt.Sprintf("tables/t/%d", i))
	}
	payload, err := json.Marshal(map[string]any{"Bucket": "b", "Keys": keys})
	require.NoError(t, err)
	stmt = newOperationStmt(t, nil, &awsClients{s3: s3}, OperationS3DeleteObjects, string(payload))
	var deleted struct {
		Errors []struct{ Key, Code, Message string }
	}
	runOperationJSON(t, stmt, &deleted)
	assert.Equal(t, []int{1000, 1000, 500}, deleteSizes)
	require.Len(t, deleted.Errors, 1)
	assert.Equal(t, "tables/t/2000", deleted.Errors[0].Key)
	assert.Equal(t, "AccessDenied", deleted.Errors[0].Code)
}

func TestOperation_S3PutObjectDecodesBodyAndMapsUploadArgs(t *testing.T) {
	var got *s3SDK.PutObjectInput
	var body []byte
	s3 := &mockS3Client{putObjectFn: func(in *s3SDK.PutObjectInput) (*s3SDK.PutObjectOutput, error) {
		got = in
		var err error
		body, err = io.ReadAll(in.Body)
		require.NoError(t, err)
		return &s3SDK.PutObjectOutput{}, nil
	}}
	csv := "id,name\n1,\"a\"\n"
	payload := fmt.Sprintf(`{"Bucket":"b","Key":"tables/t/t.csv","Body":"%s","ServerSideEncryption":"aws:kms","SSEKMSKeyId":"k","ACL":"bucket-owner-full-control","StorageClass":"STANDARD_IA","ContentType":"text/csv","BucketKeyEnabled":true}`,
		base64.StdEncoding.EncodeToString([]byte(csv)))
	stmt := newOperationStmt(t, nil, &awsClients{s3: s3}, OperationS3PutObject, payload)
	var out struct{}
	runOperationJSON(t, stmt, &out)
	require.NotNil(t, got)
	assert.Equal(t, csv, string(body))
	assert.Equal(t, "tables/t/t.csv", *got.Key)
	assert.Equal(t, s3types.ServerSideEncryptionAwsKms, got.ServerSideEncryption)
	assert.Equal(t, "k", *got.SSEKMSKeyId)
	assert.Equal(t, s3types.ObjectCannedACLBucketOwnerFullControl, got.ACL)
	assert.Equal(t, s3types.StorageClassStandardIa, got.StorageClass)
	assert.Equal(t, "text/csv", *got.ContentType)
	assert.True(t, *got.BucketKeyEnabled)

	stmt = newOperationStmt(t, nil, &awsClients{s3: s3}, OperationS3PutObject, `{"Bucket":"b","Key":"k","Body":"%%%"}`)
	_, _, err := stmt.ExecuteQuery(context.Background())
	requireStatus(t, err, adbc.StatusInvalidArgument)
}

func TestOperation_PlainSQLStillRunsWhenNoOperationIsSet(t *testing.T) {
	// A statement without the option behaves as before: the query text is required.
	stmt := newTestStmt(t, &mockAthenaClient{})
	_, _, err := stmt.ExecuteQuery(context.Background())
	requireStatus(t, err, adbc.StatusInvalidState)
}

func boolp(b bool) *bool { return &b }
