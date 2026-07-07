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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	athenaTypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	glueSDK "github.com/aws/aws-sdk-go-v2/service/glue"
	glueTypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock client
// ---------------------------------------------------------------------------

// mockAthenaClient implements athenaClientAPI using per-method function fields.
// Any field left nil will panic if that method is called, surfacing unexpected
// calls immediately — except StopQueryExecution which is best-effort and
// returns a no-op if the function is not set.
type mockAthenaClient struct {
	startQueryExecutionFn func(ctx context.Context, params *athenaSDK.StartQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error)
	stopQueryExecutionFn  func(ctx context.Context, params *athenaSDK.StopQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.StopQueryExecutionOutput, error)
	getDatabaseFn         func(ctx context.Context, params *athenaSDK.GetDatabaseInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetDatabaseOutput, error)
	getDataCatalogFn      func(ctx context.Context, params *athenaSDK.GetDataCatalogInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetDataCatalogOutput, error)
	getQueryExecutionFn   func(ctx context.Context, params *athenaSDK.GetQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error)
	getQueryResultsFn     func(ctx context.Context, params *athenaSDK.GetQueryResultsInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryResultsOutput, error)
	getTableMetadataFn    func(ctx context.Context, params *athenaSDK.GetTableMetadataInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error)
	getWorkGroupFn        func(ctx context.Context, params *athenaSDK.GetWorkGroupInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetWorkGroupOutput, error)
	listDataCatalogsFn    func(ctx context.Context, params *athenaSDK.ListDataCatalogsInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error)
	listDatabasesFn       func(ctx context.Context, params *athenaSDK.ListDatabasesInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error)
	listTableMetadataFn   func(ctx context.Context, params *athenaSDK.ListTableMetadataInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error)
}

func (m *mockAthenaClient) StartQueryExecution(ctx context.Context, params *athenaSDK.StartQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error) {
	return m.startQueryExecutionFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) StopQueryExecution(ctx context.Context, params *athenaSDK.StopQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.StopQueryExecutionOutput, error) {
	if m.stopQueryExecutionFn != nil {
		return m.stopQueryExecutionFn(ctx, params, optFns...)
	}
	return &athenaSDK.StopQueryExecutionOutput{}, nil
}
func (m *mockAthenaClient) GetDatabase(ctx context.Context, params *athenaSDK.GetDatabaseInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetDatabaseOutput, error) {
	return m.getDatabaseFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) GetDataCatalog(ctx context.Context, params *athenaSDK.GetDataCatalogInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetDataCatalogOutput, error) {
	return m.getDataCatalogFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) GetQueryExecution(ctx context.Context, params *athenaSDK.GetQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error) {
	return m.getQueryExecutionFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) GetQueryResults(ctx context.Context, params *athenaSDK.GetQueryResultsInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryResultsOutput, error) {
	return m.getQueryResultsFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) GetTableMetadata(ctx context.Context, params *athenaSDK.GetTableMetadataInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error) {
	return m.getTableMetadataFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) GetWorkGroup(ctx context.Context, params *athenaSDK.GetWorkGroupInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetWorkGroupOutput, error) {
	return m.getWorkGroupFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) ListDataCatalogs(ctx context.Context, params *athenaSDK.ListDataCatalogsInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error) {
	return m.listDataCatalogsFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) ListDatabases(ctx context.Context, params *athenaSDK.ListDatabasesInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error) {
	return m.listDatabasesFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) ListTableMetadata(ctx context.Context, params *athenaSDK.ListTableMetadataInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
	return m.listTableMetadataFn(ctx, params, optFns...)
}

// mockGlueClient implements glueClientAPI using per-method function fields.
type mockGlueClient struct {
	getCatalogFn  func(ctx context.Context, params *glueSDK.GetCatalogInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetCatalogOutput, error)
	getCatalogsFn func(ctx context.Context, params *glueSDK.GetCatalogsInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error)
}

func (m *mockGlueClient) GetCatalog(ctx context.Context, params *glueSDK.GetCatalogInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetCatalogOutput, error) {
	return m.getCatalogFn(ctx, params, optFns...)
}
func (m *mockGlueClient) GetCatalogs(ctx context.Context, params *glueSDK.GetCatalogsInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
	return m.getCatalogsFn(ctx, params, optFns...)
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// strp returns a pointer to the given string. Named differently from the
// strPtr helper in record_reader_test.go to avoid a duplicate declaration.
func strp(s string) *string { return &s }

// newTestDB builds a databaseImpl with the given mock client and sensible
// defaults. No AWS credentials are needed.
func newTestDB(t testing.TB, mock athenaClientAPI) *databaseImpl {
	t.Helper()
	info := driverbase.DefaultDriverInfo("Athena")
	driverBase := driverbase.NewDriverImplBase(info, memory.DefaultAllocator)
	dbBase, err := driverbase.NewDatabaseImplBase(context.Background(), &driverBase)
	require.NoError(t, err)
	return &databaseImpl{
		DatabaseImplBase: dbBase,
		catalog:          "AwsDataCatalog",
		schema:           "default",
		outputLocation:   "s3://test-bucket/results/",
		authType:         AuthTypeDefault,
		testAthenaClient: mock,
	}
}

// newTestConn builds a connectionImpl directly with the mock client, bypassing
// driverbase.Open and AWS credential resolution entirely.
func newTestConn(t testing.TB, athenaMock athenaClientAPI, glueMock glueClientAPI) *connectionImpl {
	return newTestConnWithWorkGroup(t, athenaMock, glueMock, nil)
}

func newTestConnWithWorkGroup(t testing.TB, athenaMock athenaClientAPI, glueMock glueClientAPI, workGroup *string) *connectionImpl {
	t.Helper()
	db := newTestDB(t, athenaMock)
	if workGroup != nil {
		db.workGroup = *workGroup
	}
	return &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&db.DatabaseImplBase),
		athenaClient:       athenaMock,
		glueClient:         glueMock,
		db:                 db,
		catalog:            db.catalog,
		schema:             db.schema,
	}
}

// newTestStmt creates a statementImpl attached to a test connection.
func newTestStmt(t testing.TB, athenaMock athenaClientAPI) *statementImpl {
	conn := newTestConn(t, athenaMock, nil)
	return &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&conn.ConnectionImplBase, conn.ErrorHelper),
		conn:              conn,
	}
}

// succeedAfterN returns a GetQueryExecution function that returns RUNNING for
// the first n calls, then SUCCEEDED.
func succeedAfterN(n int) func(context.Context, *athenaSDK.GetQueryExecutionInput, ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error) {
	var calls int32
	return func(_ context.Context, _ *athenaSDK.GetQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error) {
		c := int(atomic.AddInt32(&calls, 1))
		state := types.QueryExecutionStateRunning
		if c > n {
			state = types.QueryExecutionStateSucceeded
		}
		return &athenaSDK.GetQueryExecutionOutput{
			QueryExecution: &types.QueryExecution{
				Status: &types.QueryExecutionStatus{State: state},
			},
		}, nil
	}
}

// singlePageResults returns a GetQueryResults function that produces one page
// with a header row followed by the given data rows, then errors if called again.
func singlePageResults(colName string, values []string) func(context.Context, *athenaSDK.GetQueryResultsInput, ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryResultsOutput, error) {
	called := false
	return func(_ context.Context, _ *athenaSDK.GetQueryResultsInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryResultsOutput, error) {
		if called {
			return nil, fmt.Errorf("unexpected second call to GetQueryResults")
		}
		called = true

		rows := make([]types.Row, 0, len(values)+1)
		rows = append(rows, types.Row{Data: []types.Datum{{VarCharValue: strp(colName)}}}) // header
		for _, v := range values {
			rows = append(rows, types.Row{Data: []types.Datum{{VarCharValue: strp(v)}}})
		}
		return &athenaSDK.GetQueryResultsOutput{
			ResultSet: &types.ResultSet{
				ResultSetMetadata: &types.ResultSetMetadata{
					ColumnInfo: []types.ColumnInfo{
						{Name: strp(colName), Type: strp("varchar")},
					},
				},
				Rows: rows,
			},
		}, nil
	}
}

// ---------------------------------------------------------------------------
// Functional tests
// ---------------------------------------------------------------------------

// TestFunctional_SimpleSelectQuery exercises the full execution path:
// StartQueryExecution → poll RUNNING twice → SUCCEEDED → GetQueryResults → RecordReader.
func TestFunctional_SimpleSelectQuery(t *testing.T) {
	const execID = "exec-001"
	mock := &mockAthenaClient{
		startQueryExecutionFn: func(_ context.Context, _ *athenaSDK.StartQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error) {
			return &athenaSDK.StartQueryExecutionOutput{QueryExecutionId: strp(execID)}, nil
		},
		getQueryExecutionFn: succeedAfterN(2),
		getQueryResultsFn:   singlePageResults("n", []string{"42"}),
	}

	stmt := newTestStmt(t, mock)
	require.NoError(t, stmt.SetSqlQuery(context.Background(),"SELECT 42 AS n"))

	rdr, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.NotNil(t, rdr)
	defer rdr.Release()

	assert.EqualValues(t, -1, rowCount) // Athena never knows row count upfront

	require.True(t, rdr.Next())
	rec := rdr.Record()
	assert.EqualValues(t, 1, rec.NumCols())
	assert.EqualValues(t, 1, rec.NumRows())
	assert.Equal(t, "n", rec.Schema().Field(0).Name)
	assert.False(t, rdr.Next())
}

// TestFunctional_QueryFailure verifies that a FAILED query state is surfaced
// as an ADBC IO error containing the failure reason.
func TestFunctional_QueryFailure(t *testing.T) {
	const reason = "HIVE_METASTORE_ERROR: table not found"
	mock := &mockAthenaClient{
		startQueryExecutionFn: func(_ context.Context, _ *athenaSDK.StartQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error) {
			return &athenaSDK.StartQueryExecutionOutput{QueryExecutionId: strp("exec-fail")}, nil
		},
		getQueryExecutionFn: func(_ context.Context, _ *athenaSDK.GetQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error) {
			return &athenaSDK.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{
						State:             types.QueryExecutionStateFailed,
						StateChangeReason: strp(reason),
					},
				},
			}, nil
		},
	}

	stmt := newTestStmt(t, mock)
	require.NoError(t, stmt.SetSqlQuery(context.Background(),"SELECT * FROM nonexistent_table"))

	_, _, err := stmt.ExecuteQuery(context.Background())
	require.Error(t, err)

	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusIO, adbcErr.Code)
	assert.Contains(t, adbcErr.Msg, reason)
}

// TestFunctional_QueryCancelled verifies that CANCELLED state is surfaced as
// an ADBC Cancelled error.
func TestFunctional_QueryCancelled(t *testing.T) {
	mock := &mockAthenaClient{
		startQueryExecutionFn: func(_ context.Context, _ *athenaSDK.StartQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error) {
			return &athenaSDK.StartQueryExecutionOutput{QueryExecutionId: strp("exec-cancel")}, nil
		},
		getQueryExecutionFn: func(_ context.Context, _ *athenaSDK.GetQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error) {
			return &athenaSDK.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{
						State: types.QueryExecutionStateCancelled,
					},
				},
			}, nil
		},
	}

	stmt := newTestStmt(t, mock)
	require.NoError(t, stmt.SetSqlQuery(context.Background(),"SELECT 1"))

	_, _, err := stmt.ExecuteQuery(context.Background())
	require.Error(t, err)

	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusCancelled, adbcErr.Code)
}

// TestFunctional_ContextCancellationMidPoll verifies that a cancelled context
// breaks out of the polling loop in waitForQuery.
func TestFunctional_ContextCancellationMidPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var pollCount int32
	mock := &mockAthenaClient{
		startQueryExecutionFn: func(_ context.Context, _ *athenaSDK.StartQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error) {
			return &athenaSDK.StartQueryExecutionOutput{QueryExecutionId: strp("exec-ctx")}, nil
		},
		getQueryExecutionFn: func(_ context.Context, _ *athenaSDK.GetQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error) {
			// Cancel before returning so ctx.Done() is already closed when
			// waitForQuery reaches the select statement.
			if atomic.AddInt32(&pollCount, 1) == 1 {
				cancel()
			}
			return &athenaSDK.GetQueryExecutionOutput{
				QueryExecution: &types.QueryExecution{
					Status: &types.QueryExecutionStatus{State: types.QueryExecutionStateRunning},
				},
			}, nil
		},
	}

	stmt := newTestStmt(t, mock)
	require.NoError(t, stmt.SetSqlQuery(context.Background(),"SELECT sleep(60)"))

	_, _, err := stmt.ExecuteQuery(ctx)
	require.Error(t, err)

	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusCancelled, adbcErr.Code)
}

// TestFunctional_MultiPageResults verifies that multi-page pagination is
// read correctly across multiple result batches and returns all rows.
func TestFunctional_MultiPageResults(t *testing.T) {
	const execID = "exec-multi"
	var callCount int32

	mock := &mockAthenaClient{
		startQueryExecutionFn: func(_ context.Context, _ *athenaSDK.StartQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error) {
			return &athenaSDK.StartQueryExecutionOutput{QueryExecutionId: strp(execID)}, nil
		},
		getQueryExecutionFn: succeedAfterN(0),
		getQueryResultsFn: func(_ context.Context, params *athenaSDK.GetQueryResultsInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryResultsOutput, error) {
			n := int(atomic.AddInt32(&callCount, 1))
			colInfo := []types.ColumnInfo{{Name: strp("id"), Type: strp("bigint")}}

			switch n {
			case 1:
				// First page — header row + 2 data rows, NextToken signals more pages.
				return &athenaSDK.GetQueryResultsOutput{
					NextToken: strp("token-2"),
					ResultSet: &types.ResultSet{
						ResultSetMetadata: &types.ResultSetMetadata{ColumnInfo: colInfo},
						Rows: []types.Row{
							{Data: []types.Datum{{VarCharValue: strp("id")}}}, // header
							{Data: []types.Datum{{VarCharValue: strp("1")}}},
							{Data: []types.Datum{{VarCharValue: strp("2")}}},
						},
					},
				}, nil
			case 2:
				// Second page — no header, no NextToken → paginator stops.
				return &athenaSDK.GetQueryResultsOutput{
					ResultSet: &types.ResultSet{
						Rows: []types.Row{
							{Data: []types.Datum{{VarCharValue: strp("3")}}},
							{Data: []types.Datum{{VarCharValue: strp("4")}}},
						},
					},
				}, nil
			default:
				return nil, fmt.Errorf("unexpected GetQueryResults call %d", n)
			}
		},
	}

	stmt := newTestStmt(t, mock)
	require.NoError(t, stmt.SetSqlQuery(context.Background(),"SELECT id FROM t"))

	rdr, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	defer rdr.Release()

	var totalRows int64
	var batchCount int
	for rdr.Next() {
		rec := rdr.Record()
		totalRows += rec.NumRows()
		assert.EqualValues(t, 1, rec.NumCols())
		batchCount++
	}

	assert.Greater(t, batchCount, 0)
	assert.EqualValues(t, 4, totalRows)
}

// TestFunctional_GetTableSchema verifies GetTableSchema calls GetTableMetadata
// and converts the result to a correct Arrow schema.
func TestFunctional_GetTableSchema(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getTableMetadataFn: func(_ context.Context, params *athenaSDK.GetTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error) {
			assert.Equal(t, "my_catalog", *params.CatalogName)
			assert.Equal(t, "my_db", *params.DatabaseName)
			assert.Equal(t, "my_table", *params.TableName)
			return &athenaSDK.GetTableMetadataOutput{
				TableMetadata: &types.TableMetadata{
					Name: strp("my_table"),
					Columns: []types.Column{
						{Name: strp("id"), Type: strp("bigint")},
						{Name: strp("name"), Type: strp("varchar")},
						{Name: strp("score"), Type: strp("double")},
					},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	catalogName := "my_catalog"
	schemaName := "my_db"
	schema, err := conn.GetTableSchema(context.Background(), &catalogName, &schemaName, "my_table")
	require.NoError(t, err)
	require.NotNil(t, schema)

	assert.Equal(t, 3, schema.NumFields())
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "name", schema.Field(1).Name)
	assert.Equal(t, "score", schema.Field(2).Name)
}

// TestFunctional_ListCatalogs verifies the ListDataCatalogs pagination path.
func TestFunctional_ListCatalogs(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDataCatalogsFn: func(_ context.Context, _ *athenaSDK.ListDataCatalogsInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error) {
			return &athenaSDK.ListDataCatalogsOutput{
				DataCatalogsSummary: []types.DataCatalogSummary{
					{CatalogName: strp("AwsDataCatalog")},
					{CatalogName: strp("MyGlueCatalog")},
				},
			}, nil
		},
	}
	glueMock := &mockGlueClient{
		getCatalogsFn: func(_ context.Context, _ *glueSDK.GetCatalogsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
			return &glueSDK.GetCatalogsOutput{}, nil
		},
	}

	conn := newTestConn(t, athenaMock, glueMock)
	catalogs, err := conn.GetCatalogs(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"AwsDataCatalog", "MyGlueCatalog"}, catalogs)
}

// TestFunctional_ListCatalogs_RecursivelyListsGlueCatalogs verifies Glue GetCatalogs is called with the recursive option
func TestFunctional_ListCatalogs_RecursivelyListsGlueCatalogs(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDataCatalogsFn: func(_ context.Context, _ *athenaSDK.ListDataCatalogsInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error) {
			return &athenaSDK.ListDataCatalogsOutput{
				DataCatalogsSummary: []types.DataCatalogSummary{
					{CatalogName: strp("AwsDataCatalog")},
					{CatalogName: strp("MyGlueCatalog")},
				},
			}, nil
		},
	}
	glueMock := &mockGlueClient{
		getCatalogsFn: func(_ context.Context, params *glueSDK.GetCatalogsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
			assert.True(t, params.Recursive, "Glue GetCatalogs should be called with the recursive option")
			return &glueSDK.GetCatalogsOutput{
				CatalogList: []glueTypes.Catalog{
					{CatalogId: strp("111111111111:my_glue_catalog")},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, glueMock)
	catalogs, err := conn.GetCatalogs(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"AwsDataCatalog", "MyGlueCatalog", "my_glue_catalog"}, catalogs)
}

// TestFunctional_ListCatalogs_WithEmptyCatalogName verifies the GetCatalogs short circuits when given an empty string
func TestFunctional_ListCatalogs_WithEmptyCatalogName(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDataCatalogsFn: func(_ context.Context, _ *athenaSDK.ListDataCatalogsInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error) {
			t.Fatal("ListDataCatalogs should not be called")
			return nil, nil
		},
	}
	glueMock := &mockGlueClient{
		getCatalogsFn: func(_ context.Context, _ *glueSDK.GetCatalogsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
			t.Fatal("GetCatalogs should not be called")
			return nil, nil
		},
	}

	conn := newTestConn(t, athenaMock, glueMock)
	emptyString := ""
	catalogs, err := conn.GetCatalogs(context.Background(), &emptyString)
	require.NoError(t, err)
	assert.Equal(t, []string{}, catalogs)
}

// TestFunctional_ListCatalogs_WithFilter verifies the catalogs are filtered by the specified pattern
func TestFunctional_ListCatalogs_WithFilter(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDataCatalogsFn: func(_ context.Context, _ *athenaSDK.ListDataCatalogsInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error) {
			return &athenaSDK.ListDataCatalogsOutput{
				DataCatalogsSummary: []types.DataCatalogSummary{
					{CatalogName: strp("AwsDataCatalog")},
					{CatalogName: strp("MyGlueCatalog")},
				},
			}, nil
		},
	}
	glueMock := &mockGlueClient{
		getCatalogsFn: func(_ context.Context, params *glueSDK.GetCatalogsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
			assert.True(t, params.Recursive, "Glue GetCatalogs should be called with the recursive option")
			return &glueSDK.GetCatalogsOutput{
				CatalogList: []glueTypes.Catalog{
					{CatalogId: strp("111111111111:my_glue_catalog")},
					{CatalogId: strp("111111111111:my-glue_catalog")},
					{CatalogId: strp("222222222222:another_glue_catalog")},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, glueMock)
	catalogs, err := conn.GetCatalogs(context.Background(), strp("my%"))
	require.NoError(t, err)
	assert.Equal(t, []string{"MyGlueCatalog", "my_glue_catalog", "my-glue_catalog"}, catalogs)
	catalogs, err = conn.GetCatalogs(context.Background(), strp("my\\_glue%"))
	require.NoError(t, err)
	assert.Equal(t, []string{"my_glue_catalog"}, catalogs)
}

func TestFunctional_ListCatalogs_WithNonWildcardName(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getDataCatalogFn: func(_ context.Context, params *athenaSDK.GetDataCatalogInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetDataCatalogOutput, error) {
			if *params.Name == "SomeDataCatalog" {
				return &athenaSDK.GetDataCatalogOutput{
					DataCatalog: &athenaTypes.DataCatalog{
						Name: strp("SomeDataCatalog"),
					},
				}, nil
			} else {
				return nil, &types.InvalidRequestException{Message: strp("Not found")}
			}
		},
	}
	glueMock := &mockGlueClient{
		getCatalogFn: func(_ context.Context, params *glueSDK.GetCatalogInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogOutput, error) {
			if *params.CatalogId == "my_glue_catalog" {
				return &glueSDK.GetCatalogOutput{
					Catalog: &glueTypes.Catalog{
						CatalogId: strp("111111111111:my_glue_catalog"),
						Name:      strp("my_glue_catalog"),
					},
				}, nil
			} else {
				return nil, &glueTypes.EntityNotFoundException{Message: strp("Not found")}
			}
		},
	}

	conn := newTestConn(t, athenaMock, glueMock)
	catalogs, err := conn.GetCatalogs(context.Background(), strp("SomeDataCatalog"))
	require.NoError(t, err)
	assert.Equal(t, []string{"SomeDataCatalog"}, catalogs)
	catalogs, err = conn.GetCatalogs(context.Background(), strp("my\\_glue\\_catalog"))
	require.NoError(t, err)
	assert.Equal(t, []string{"my_glue_catalog"}, catalogs)
}

func TestFunctional_CheckCatalog_ReturnsFullNameFromGlueCatalogId(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getDataCatalogFn: func(_ context.Context, _ *athenaSDK.GetDataCatalogInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetDataCatalogOutput, error) {
			return nil, &types.InvalidRequestException{Message: strp("Not found")}
		},
	}
	glueMock := &mockGlueClient{
		getCatalogFn: func(_ context.Context, params *glueSDK.GetCatalogInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogOutput, error) {
			assert.Equal(t, "s3tablescatalog/my-table-bucket", *params.CatalogId)
			return &glueSDK.GetCatalogOutput{
				Catalog: &glueTypes.Catalog{
					CatalogId: strp("111111111111:s3tablescatalog/my-table-bucket"),
					Name:      strp("my-table-bucket"),
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, glueMock)
	catalogs, err := conn.GetCatalogs(context.Background(), strp("s3tablescatalog/my-table-bucket"))
	require.NoError(t, err)
	assert.Equal(t, []string{"s3tablescatalog/my-table-bucket"}, catalogs)
}

func TestFunctional_ListCatalogs_WithAwsDataCatalog(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getDataCatalogFn: func(_ context.Context, params *athenaSDK.GetDataCatalogInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetDataCatalogOutput, error) {
			t.Fatal("GetDataCatalog should not be called")
			return nil, nil
		},
	}
	glueMock := &mockGlueClient{
		getCatalogFn: func(_ context.Context, params *glueSDK.GetCatalogInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogOutput, error) {
			t.Fatal("GetCatalog should not be called")
			return nil, nil
		},
	}

	conn := newTestConn(t, athenaMock, glueMock)
	catalogs, err := conn.GetCatalogs(context.Background(), strp("AwsDataCatalog"))
	require.NoError(t, err)
	assert.Equal(t, []string{"AwsDataCatalog"}, catalogs)
}

// TestFunctional_ListSchemas verifies the ListDatabases pagination path.
func TestFunctional_ListSchemas(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDatabasesFn: func(_ context.Context, params *athenaSDK.ListDatabasesInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error) {
			assert.Equal(t, "AwsDataCatalog", *params.CatalogName)
			return &athenaSDK.ListDatabasesOutput{
				DatabaseList: []types.Database{
					{Name: strp("default")},
					{Name: strp("analytics")},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	schemas, err := conn.GetDBSchemasForCatalog(context.Background(), "AwsDataCatalog", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"default", "analytics"}, schemas)
}

// TestFunctional_ListSchemas_WithEmptyCatalogName verifies GetDBSchemasForCatalog short circuits when the catalog name is empty
func TestFunctional_ListSchemas_WithEmptyCatalogName(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDatabasesFn: func(_ context.Context, params *athenaSDK.ListDatabasesInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error) {
			t.Fatal("ListDatabases should not be called")
			return nil, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	schemas, err := conn.GetDBSchemasForCatalog(context.Background(), "", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{}, schemas)
}

// TestFunctional_ListSchemas_WithEmptySchemaName verifies GetDBSchemasForCatalog short circuits when the schema name is empty
func TestFunctional_ListSchemas_WithEmptySchemaName(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDatabasesFn: func(_ context.Context, params *athenaSDK.ListDatabasesInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error) {
			t.Fatal("ListDatabases should not be called")
			return nil, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	emptyString := ""
	schemas, err := conn.GetDBSchemasForCatalog(context.Background(), "AwsDataCatalog", &emptyString)
	require.NoError(t, err)
	assert.Equal(t, []string{}, schemas)
}

// TestFunctional_ListSchemas_WithFilter verifies the schemas are filtered by the specified pattern
func TestFunctional_ListSchemas_WithFilter(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDatabasesFn: func(_ context.Context, params *athenaSDK.ListDatabasesInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error) {
			assert.Equal(t, "AwsDataCatalog", *params.CatalogName)
			return &athenaSDK.ListDatabasesOutput{
				DatabaseList: []types.Database{
					{Name: strp("default")},
					{Name: strp("analytics")},
					{Name: strp("another_schema")},
					{Name: strp("another-schema")},
					{Name: strp("schema_four")},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	schemas, err := conn.GetDBSchemasForCatalog(context.Background(), "AwsDataCatalog", strp("%schema%"))
	require.NoError(t, err)
	assert.Equal(t, []string{"another_schema", "another-schema", "schema_four"}, schemas)
	schemas, err = conn.GetDBSchemasForCatalog(context.Background(), "AwsDataCatalog", strp("another\\_sch%"))
	require.NoError(t, err)
	assert.Equal(t, []string{"another_schema"}, schemas)
}

func TestFunctional_ListSchemas_WithNonWildcardName(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getDatabaseFn: func(_ context.Context, params *athenaSDK.GetDatabaseInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetDatabaseOutput, error) {
			assert.Equal(t, "AwsDataCatalog", *params.CatalogName)
			assert.Equal(t, "another_schema", *params.DatabaseName)
			return &athenaSDK.GetDatabaseOutput{
				Database: &types.Database{
					Name: strp("another_schema"),
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	schemas, err := conn.GetDBSchemasForCatalog(context.Background(), "AwsDataCatalog", strp("another\\_schema"))
	require.NoError(t, err)
	assert.Equal(t, []string{"another_schema"}, schemas)
}

// TestFunctional_ListSchemas_SkipsMetadataException verifies that
// GetDBSchemasForCatalog returns an empty list (not an error) when
// ListDatabases returns a MetadataException.
func TestFunctional_ListSchemas_SkipsMetadataException(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDatabasesFn: func(_ context.Context, _ *athenaSDK.ListDatabasesInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error) {
			return nil, &types.MetadataException{Message: strp("The specified bucket does not exist")}
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	schemas, err := conn.GetDBSchemasForCatalog(context.Background(), "some_glue_catalog", nil)
	require.NoError(t, err)
	assert.Empty(t, schemas)
}

// TestFunctional_GetTablesForDBSchema verifies the happy path with a wildcard
// filter: parameters are forwarded correctly and returned tables are mapped to TableInfo.
func TestFunctional_GetTablesForDBSchema(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, params *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			assert.Equal(t, "cat", *params.CatalogName)
			assert.Equal(t, "db", *params.DatabaseName)
			assert.Equal(t, "(?i)^tbl.*$", *params.Expression)
			return &athenaSDK.ListTableMetadataOutput{
				TableMetadataList: []types.TableMetadata{
					{
						Name:      strp("tbl"),
						TableType: strp("EXTERNAL_TABLE"),
					},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tableFilter := "tbl%"
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", &tableFilter, nil, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)

	assert.Equal(t, "tbl", tables[0].TableName)
	assert.Equal(t, "EXTERNAL_TABLE", tables[0].TableType)
	assert.Empty(t, tables[0].TableColumns)
}

// TestFunctional_GetTablesForDBSchema_WithColumns verifies that column metadata
// is populated when includeColumns is true.
func TestFunctional_GetTablesForDBSchema_WithColumns(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, _ *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			return &athenaSDK.ListTableMetadataOutput{
				TableMetadataList: []types.TableMetadata{
					{
						Name:      strp("events"),
						TableType: strp("EXTERNAL_TABLE"),
						Columns: []types.Column{
							{Name: strp("event_id"), Type: strp("bigint")},
							{Name: strp("event_name"), Type: strp("varchar")},
							{Name: strp("created_at"), Type: strp("timestamp")},
						},
					},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", nil, nil, true)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Len(t, tables[0].TableColumns, 3)

	col0 := tables[0].TableColumns[0]
	assert.Equal(t, "event_id", col0.ColumnName)
	assert.Equal(t, int32(1), *col0.OrdinalPosition)
	assert.Equal(t, "bigint", *col0.XdbcTypeName)

	col2 := tables[0].TableColumns[2]
	assert.Equal(t, "created_at", col2.ColumnName)
	assert.Equal(t, int32(3), *col2.OrdinalPosition)
	assert.Equal(t, "timestamp", *col2.XdbcTypeName)
}

func TestFunctional_GetTablesForDBSchema_WithColumnFilter(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getTableMetadataFn: func(_ context.Context, params *athenaSDK.GetTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error) {
			assert.Equal(t, "events", *params.TableName)
			return &athenaSDK.GetTableMetadataOutput{
				TableMetadata: &types.TableMetadata{
					Name:      strp("events"),
					TableType: strp("EXTERNAL_TABLE"),
					Columns: []types.Column{
						{Name: strp("event_id"), Type: strp("bigint")},
						{Name: strp("event_name"), Type: strp("varchar")},
						{Name: strp("created_at"), Type: strp("timestamp")},
					},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", strp("events"), strp("event%"), true)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Len(t, tables[0].TableColumns, 2)

	col0 := tables[0].TableColumns[0]
	assert.Equal(t, "event_id", col0.ColumnName)
	assert.Equal(t, int32(1), *col0.OrdinalPosition)
	assert.Equal(t, "bigint", *col0.XdbcTypeName)

	col2 := tables[0].TableColumns[1]
	assert.Equal(t, "event_name", col2.ColumnName)
	assert.Equal(t, int32(2), *col2.OrdinalPosition)
	assert.Equal(t, "varchar", *col2.XdbcTypeName)
}

func TestFunctional_GetTablesForDBSchema_WithEmptyColumnFilter(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getTableMetadataFn: func(_ context.Context, _ *athenaSDK.GetTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error) {
			return &athenaSDK.GetTableMetadataOutput{
				TableMetadata: &types.TableMetadata{
					Name:      strp("events"),
					TableType: strp("EXTERNAL_TABLE"),
					Columns: []types.Column{
						{Name: strp("event_id"), Type: strp("bigint")},
						{Name: strp("event_name"), Type: strp("varchar")},
						{Name: strp("created_at"), Type: strp("timestamp")},
					},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", strp("events"), strp(""), true)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Len(t, tables[0].TableColumns, 0)
}

// TestFunctional_GetTablesForDBSchema_NilTableFilter verifies that Expression
// is not set when tableFilter is nil.
func TestFunctional_GetTablesForDBSchema_NilTableFilter(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, params *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			assert.Nil(t, params.Expression)
			return &athenaSDK.ListTableMetadataOutput{}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", nil, nil, false)
	require.NoError(t, err)
	assert.Empty(t, tables)
}

// TestFunctional_GetTablesForDBSchema_Patterns verifies that underscores and percent in the table are translated into regular expression syntax
func TestFunctional_GetTablesForDBSchema_Patterns(t *testing.T) {
	examples := map[string]string{
		"my_table":         "(?i)^my.table$",
		"my\\\\_table":     "(?i)^my\\\\.table$",
		"my\\\\\\\\_table": "(?i)^my\\\\\\\\.table$",
		"_t":               "(?i)^.t$",
		"table_":           "(?i)^table.$",
		"my______table":    "(?i)^my......table$",
		"__table":          "(?i)^..table$",
		"table____":        "(?i)^table....$",
		"table%":           "(?i)^table.*$",
		"%table%":          "(?i)^.*table.*$",
		"%table":           "(?i)^.*table$",
		"my%table":         "(?i)^my.*table$",
		"my%%table":        "(?i)^my.*table$",
		"my\\\\%table":     "(?i)^my\\\\.*table$",
		"my\\\\\\\\%table": "(?i)^my\\\\\\\\.*table$",
		"table%%%%":        "(?i)^table.*$",
		"%%%%table":        "(?i)^.*table$",
		"+abl%":            "(?i)^\\+abl.*$",
	}

	for tableFilter, expectedExpression := range examples {
		athenaMock := &mockAthenaClient{
			listTableMetadataFn: func(_ context.Context, params *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
				assert.Equal(t, expectedExpression, *params.Expression)
				return &athenaSDK.ListTableMetadataOutput{}, nil
			},
		}

		conn := newTestConn(t, athenaMock, nil)
		_, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", &tableFilter, nil, false)
		require.NoError(t, err)
	}
}

// TestFunctional_GetTablesForDBSchema_NilTableFilter verifies that malformed table filters are not accepted
func TestFunctional_GetTablesForDBSchema_MalformedTableFilter(t *testing.T) {
	conn := newTestConn(t, nil, nil)
	_, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", strp("tbl\\"), nil, false)
	assert.ErrorContains(t, err, "pattern cannot end with an escape")
}

// TestFunctional_GetTablesForDBSchema_EmptyTableFilter verifies that GetTablesForDBSchema
// short circuits when the table name filter is an empty string
func TestFunctional_GetTablesForDBSchema_EmptyTableFilter(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, params *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			t.Fatal("ListTableMetadata should not be called")
			return &athenaSDK.ListTableMetadataOutput{}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	empty := ""
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", &empty, nil, false)
	require.NoError(t, err)
	assert.Empty(t, tables)
}

// TestFunctional_GetTablesForDBSchema_SkipsNilName verifies that table entries
// with a nil Name are skipped.
func TestFunctional_GetTablesForDBSchema_SkipsNilName(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, _ *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			return &athenaSDK.ListTableMetadataOutput{
				TableMetadataList: []types.TableMetadata{
					{Name: nil, TableType: strp("EXTERNAL_TABLE")},
					{Name: strp("valid_table"), TableType: strp("EXTERNAL_TABLE")},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", nil, nil, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Equal(t, "valid_table", tables[0].TableName)
}

// TestFunctional_GetTablesForDBSchema_DefaultsTableType verifies that tables
// with nil TableType default to "EXTERNAL_TABLE".
func TestFunctional_GetTablesForDBSchema_DefaultsTableType(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, _ *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			return &athenaSDK.ListTableMetadataOutput{
				TableMetadataList: []types.TableMetadata{
					{Name: strp("my_table"), TableType: nil},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", nil, nil, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Equal(t, "EXTERNAL_TABLE", tables[0].TableType)
}

// TestFunctional_GetTablesForDBSchema_NormalizesTableType verifies that
// unknown table types are normalized to known types from ListTableTypes.
func TestFunctional_GetTablesForDBSchema_NormalizesTableType(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, _ *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			return &athenaSDK.ListTableMetadataOutput{
				TableMetadataList: []types.TableMetadata{
					{Name: strp("t1"), TableType: strp("EXTERNAL_TABLE")},
					{Name: strp("t2"), TableType: strp("MANAGED_TABLE")},
					{Name: strp("t3"), TableType: strp("VIRTUAL_VIEW")},
					{Name: strp("t4"), TableType: strp("EXTERNAL")},
					{Name: strp("t5"), TableType: strp("VIEW")},
					{Name: strp("t6"), TableType: strp("TABLE")},
					{Name: strp("t7"), TableType: strp("")},
					{Name: strp("t8"), TableType: nil},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", nil, nil, false)
	require.NoError(t, err)
	require.Len(t, tables, 8)
	assert.Equal(t, "EXTERNAL_TABLE", tables[0].TableType)
	assert.Equal(t, "EXTERNAL_TABLE", tables[1].TableType)
	assert.Equal(t, "VIRTUAL_VIEW", tables[2].TableType)
	assert.Equal(t, "EXTERNAL_TABLE", tables[3].TableType)
	assert.Equal(t, "EXTERNAL_TABLE", tables[4].TableType)
	assert.Equal(t, "EXTERNAL_TABLE", tables[5].TableType)
	assert.Equal(t, "EXTERNAL_TABLE", tables[6].TableType)
	assert.Equal(t, "EXTERNAL_TABLE", tables[7].TableType)
}

// TestFunctional_GetTablesForDBSchema_APIError verifies that a ListTableMetadata
// error is wrapped as an adbc.Error with StatusIO.
func TestFunctional_GetTablesForDBSchema_APIError(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, _ *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			return nil, fmt.Errorf("throttled")
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	_, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", nil, nil, false)
	require.Error(t, err)
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusIO, adbcErr.Code)
	assert.Contains(t, adbcErr.Msg, "ListTableMetadata failed")
}

func TestFunctional_GetTablesForDBSchema_SkipsMetadataException(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, _ *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			return nil, &types.MetadataException{Message: strp("not possible!")}
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", nil, nil, true)
	require.NoError(t, err)
	assert.Empty(t, tables)
}

// TestFunctional_GetTablesForDBSchema_WithNonWildcardName verifies that
// GetTableMetadata is used instead of ListTableMetadata when the table filter
// has no wildcards.
func TestFunctional_GetTablesForDBSchema_WithNonWildcardName(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getTableMetadataFn: func(_ context.Context, params *athenaSDK.GetTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error) {
			assert.Equal(t, "my_catalog", *params.CatalogName)
			assert.Equal(t, "my_db", *params.DatabaseName)
			assert.Equal(t, "my_table", *params.TableName)
			return &athenaSDK.GetTableMetadataOutput{
				TableMetadata: &types.TableMetadata{
					Name:      strp("my_table"),
					TableType: strp("EXTERNAL_TABLE"),
					Columns: []types.Column{
						{Name: strp("id"), Type: strp("bigint")},
						{Name: strp("name"), Type: strp("varchar")},
					},
				},
			}, nil
		},
		listTableMetadataFn: func(_ context.Context, _ *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			t.Fatal("ListTableMetadata should not be called")
			return nil, nil
		},
	}

	conn := newTestConn(t, athenaMock, nil)
	filter := "my\\_table"
	tables, err := conn.GetTablesForDBSchema(context.Background(), "my_catalog", "my_db", &filter, nil, true)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Equal(t, "my_table", tables[0].TableName)
	assert.Equal(t, "EXTERNAL_TABLE", tables[0].TableType)
	require.Len(t, tables[0].TableColumns, 2)
	assert.Equal(t, "id", tables[0].TableColumns[0].ColumnName)
	assert.Equal(t, "name", tables[0].TableColumns[1].ColumnName)
}

func TestFunctional_WorkGroup_GetDataCatalog(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getDataCatalogFn: func(_ context.Context, params *athenaSDK.GetDataCatalogInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetDataCatalogOutput, error) {
			require.NotNil(t, params.WorkGroup)
			assert.Equal(t, "my-workgroup", *params.WorkGroup)
			return &athenaSDK.GetDataCatalogOutput{
				DataCatalog: &athenaTypes.DataCatalog{
					Name: params.Name,
				},
			}, nil
		},
	}
	glueMock := &mockGlueClient{
		getCatalogFn: func(_ context.Context, _ *glueSDK.GetCatalogInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogOutput, error) {
			return nil, &glueTypes.EntityNotFoundException{Message: strp("not found")}
		},
	}

	conn := newTestConnWithWorkGroup(t, athenaMock, glueMock, strp("my-workgroup"))
	catalogs, err := conn.GetCatalogs(context.Background(), strp("MyCatalog"))
	require.NoError(t, err)
	assert.Equal(t, []string{"MyCatalog"}, catalogs)
}

func TestFunctional_WorkGroup_ListDataCatalogs(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDataCatalogsFn: func(_ context.Context, params *athenaSDK.ListDataCatalogsInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error) {
			require.NotNil(t, params.WorkGroup)
			assert.Equal(t, "my-workgroup", *params.WorkGroup)
			return &athenaSDK.ListDataCatalogsOutput{
				DataCatalogsSummary: []types.DataCatalogSummary{
					{CatalogName: strp("AwsDataCatalog")},
				},
			}, nil
		},
	}
	glueMock := &mockGlueClient{
		getCatalogsFn: func(_ context.Context, _ *glueSDK.GetCatalogsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
			return &glueSDK.GetCatalogsOutput{}, nil
		},
	}

	conn := newTestConnWithWorkGroup(t, athenaMock, glueMock, strp("my-workgroup"))
	catalogs, err := conn.GetCatalogs(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"AwsDataCatalog"}, catalogs)
}

func TestFunctional_WorkGroup_GetDatabase(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getDatabaseFn: func(_ context.Context, params *athenaSDK.GetDatabaseInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetDatabaseOutput, error) {
			require.NotNil(t, params.WorkGroup)
			assert.Equal(t, "my-workgroup", *params.WorkGroup)
			return &athenaSDK.GetDatabaseOutput{
				Database: &types.Database{Name: strp("my_db")},
			}, nil
		},
	}

	conn := newTestConnWithWorkGroup(t, athenaMock, nil, strp("my-workgroup"))
	schemas, err := conn.GetDBSchemasForCatalog(context.Background(), "AwsDataCatalog", strp("my\\_db"))
	require.NoError(t, err)
	assert.Equal(t, []string{"my_db"}, schemas)
}

func TestFunctional_WorkGroup_ListDatabases(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDatabasesFn: func(_ context.Context, params *athenaSDK.ListDatabasesInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error) {
			require.NotNil(t, params.WorkGroup)
			assert.Equal(t, "my-workgroup", *params.WorkGroup)
			return &athenaSDK.ListDatabasesOutput{
				DatabaseList: []types.Database{{Name: strp("default")}},
			}, nil
		},
	}

	conn := newTestConnWithWorkGroup(t, athenaMock, nil, strp("my-workgroup"))
	schemas, err := conn.GetDBSchemasForCatalog(context.Background(), "AwsDataCatalog", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"default"}, schemas)
}

func TestFunctional_WorkGroup_GetTableMetadata(t *testing.T) {
	athenaMock := &mockAthenaClient{
		getTableMetadataFn: func(_ context.Context, params *athenaSDK.GetTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error) {
			require.NotNil(t, params.WorkGroup)
			assert.Equal(t, "my-workgroup", *params.WorkGroup)
			return &athenaSDK.GetTableMetadataOutput{
				TableMetadata: &types.TableMetadata{
					Name:      strp("my_table"),
					TableType: strp("EXTERNAL_TABLE"),
				},
			}, nil
		},
	}

	conn := newTestConnWithWorkGroup(t, athenaMock, nil, strp("my-workgroup"))
	filter := "my\\_table"
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", &filter, nil, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Equal(t, "my_table", tables[0].TableName)
}

func TestFunctional_WorkGroup_ListTableMetadata(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listTableMetadataFn: func(_ context.Context, params *athenaSDK.ListTableMetadataInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error) {
			require.NotNil(t, params.WorkGroup)
			assert.Equal(t, "my-workgroup", *params.WorkGroup)
			return &athenaSDK.ListTableMetadataOutput{
				TableMetadataList: []types.TableMetadata{
					{Name: strp("tbl1"), TableType: strp("EXTERNAL_TABLE")},
				},
			}, nil
		},
	}

	conn := newTestConnWithWorkGroup(t, athenaMock, nil, strp("my-workgroup"))
	tables, err := conn.GetTablesForDBSchema(context.Background(), "cat", "db", nil, nil, false)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Equal(t, "tbl1", tables[0].TableName)
}

// ---------------------------------------------------------------------------
// GetInfo tests
// ---------------------------------------------------------------------------

// newTestWrappedConn opens a driverbase-wrapped connection with a mock Athena
// client, using the same DriverInfo registrations as production. This allows
// testing methods like GetInfo that live on the wrapper.
func newTestWrappedConn(t testing.TB) adbc.ConnectionWithContext {
	t.Helper()
	return newTestWrappedConnWithMock(t, &mockAthenaClient{
		getWorkGroupFn: func(_ context.Context, _ *athenaSDK.GetWorkGroupInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetWorkGroupOutput, error) {
			return &athenaSDK.GetWorkGroupOutput{
				WorkGroup: &types.WorkGroup{
					Name: strp("primary"),
					Configuration: &types.WorkGroupConfiguration{
						EngineVersion: &types.EngineVersion{
							EffectiveEngineVersion: strp("Athena engine version 3"),
						},
					},
				},
			}, nil
		},
	})
}

func newTestWrappedConnWithMock(t testing.TB, mock athenaClientAPI) adbc.ConnectionWithContext {
	t.Helper()
	info := driverbase.DefaultDriverInfo("Athena")
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoVendorName:         "Amazon Athena",
		adbc.InfoVendorArrowVersion: infoDriverArrowVersion,
		adbc.InfoVendorSql:          true,
		adbc.InfoVendorSubstrait:    false,
		adbc.InfoDriverName:         "Amazon Athena ADBC driver",
		adbc.InfoDriverVersion:      driverVersion,
		adbc.InfoDriverArrowVersion: infoDriverArrowVersion,
	})
	driverBase := driverbase.NewDriverImplBase(info, memory.DefaultAllocator)
	dbBase, err := driverbase.NewDatabaseImplBase(context.Background(), &driverBase)
	require.NoError(t, err)
	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		catalog:          "AwsDataCatalog",
		schema:           "default",
		outputLocation:   "s3://test-bucket/results/",
		authType:         AuthTypeDefault,
		testAthenaClient: mock,
	}
	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// getInfoString is a helper that calls GetInfo for a single code and returns the string value.
func getInfoString(t *testing.T, conn adbc.ConnectionWithContext, code adbc.InfoCode) string {
	t.Helper()
	rdr, err := conn.GetInfo(context.Background(), []adbc.InfoCode{code})
	require.NoError(t, err)
	defer rdr.Release()

	require.True(t, rdr.Next())
	rec := rdr.RecordBatch()
	require.EqualValues(t, 1, rec.NumRows())

	nameCol := rec.Column(0).(*array.Uint32)
	require.EqualValues(t, code, nameCol.Value(0))

	valueCol := rec.Column(1).(*array.DenseUnion)
	return valueCol.Field(valueCol.ChildID(0)).(*array.String).Value(int(valueCol.ValueOffset(0)))
}

func TestFunctional_GetInfo_VendorName(t *testing.T) {
	conn := newTestWrappedConn(t)
	assert.Equal(t, "Amazon Athena", getInfoString(t, conn, adbc.InfoVendorName))
}

func TestFunctional_GetInfo_DriverName(t *testing.T) {
	conn := newTestWrappedConn(t)
	assert.Equal(t, "Amazon Athena ADBC driver", getInfoString(t, conn, adbc.InfoDriverName))
}

func TestFunctional_GetInfo_VendorSql(t *testing.T) {
	conn := newTestWrappedConn(t)
	rdr, err := conn.GetInfo(context.Background(), []adbc.InfoCode{adbc.InfoVendorSql})
	require.NoError(t, err)
	defer rdr.Release()

	require.True(t, rdr.Next())
	rec := rdr.RecordBatch()
	require.EqualValues(t, 1, rec.NumRows())

	valueCol := rec.Column(1).(*array.DenseUnion)
	boolArr := valueCol.Field(valueCol.ChildID(0)).(*array.Boolean)
	assert.True(t, boolArr.Value(int(valueCol.ValueOffset(0))))
}

func TestFunctional_GetInfo_VendorSubstrait(t *testing.T) {
	conn := newTestWrappedConn(t)
	rdr, err := conn.GetInfo(context.Background(), []adbc.InfoCode{adbc.InfoVendorSubstrait})
	require.NoError(t, err)
	defer rdr.Release()

	require.True(t, rdr.Next())
	rec := rdr.RecordBatch()
	require.EqualValues(t, 1, rec.NumRows())

	valueCol := rec.Column(1).(*array.DenseUnion)
	boolArr := valueCol.Field(valueCol.ChildID(0)).(*array.Boolean)
	assert.False(t, boolArr.Value(int(valueCol.ValueOffset(0))))
}

func TestFunctional_GetInfo_DriverVersion(t *testing.T) {
	conn := newTestWrappedConn(t)
	version := getInfoString(t, conn, adbc.InfoDriverVersion)
	assert.Equal(t, driverVersion, version)
}

func TestFunctional_GetInfo_DriverArrowVersion(t *testing.T) {
	conn := newTestWrappedConn(t)
	version := getInfoString(t, conn, adbc.InfoDriverArrowVersion)
	// In test binaries, debug.ReadBuildInfo() doesn't populate Deps, so the
	// Arrow version may be empty. In built binaries it will be e.g. "v18.5.2".
	assert.Equal(t, infoDriverArrowVersion, version)
}

func TestFunctional_GetInfo_DriverADBCVersion(t *testing.T) {
	conn := newTestWrappedConn(t)
	rdr, err := conn.GetInfo(context.Background(), []adbc.InfoCode{adbc.InfoDriverADBCVersion})
	require.NoError(t, err)
	defer rdr.Release()

	require.True(t, rdr.Next())
	rec := rdr.RecordBatch()
	require.EqualValues(t, 1, rec.NumRows())

	valueCol := rec.Column(1).(*array.DenseUnion)
	int64Arr := valueCol.Field(valueCol.ChildID(0)).(*array.Int64)
	assert.EqualValues(t, adbc.AdbcVersion1_1_0, int64Arr.Value(int(valueCol.ValueOffset(0))))
}

func TestFunctional_GetInfo_VendorVersion(t *testing.T) {
	mock := &mockAthenaClient{
		getWorkGroupFn: func(_ context.Context, params *athenaSDK.GetWorkGroupInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetWorkGroupOutput, error) {
			assert.Equal(t, "primary", *params.WorkGroup)
			return &athenaSDK.GetWorkGroupOutput{
				WorkGroup: &types.WorkGroup{
					Name: strp("primary"),
					Configuration: &types.WorkGroupConfiguration{
						EngineVersion: &types.EngineVersion{
							EffectiveEngineVersion: strp("Athena engine version 3"),
						},
					},
				},
			}, nil
		},
	}
	conn := newTestWrappedConnWithMock(t, mock)
	assert.Equal(t, "Athena engine version 3", getInfoString(t, conn, adbc.InfoVendorVersion))
}

func TestFunctional_GetInfo_VendorVersion_CustomWorkGroup(t *testing.T) {
	mock := &mockAthenaClient{
		getWorkGroupFn: func(_ context.Context, params *athenaSDK.GetWorkGroupInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.GetWorkGroupOutput, error) {
			assert.Equal(t, "my-workgroup", *params.WorkGroup)
			return &athenaSDK.GetWorkGroupOutput{
				WorkGroup: &types.WorkGroup{
					Name: strp("my-workgroup"),
					Configuration: &types.WorkGroupConfiguration{
						EngineVersion: &types.EngineVersion{
							EffectiveEngineVersion: strp("Athena engine version 2"),
						},
					},
				},
			}, nil
		},
	}

	info := driverbase.DefaultDriverInfo("Athena")
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoVendorName:         "Amazon Athena",
		adbc.InfoVendorArrowVersion: infoDriverArrowVersion,
		adbc.InfoVendorSql:          true,
		adbc.InfoVendorSubstrait:    false,
		adbc.InfoDriverName:         "Amazon Athena ADBC driver",
		adbc.InfoDriverVersion:      driverVersion,
		adbc.InfoDriverArrowVersion: infoDriverArrowVersion,
	})
	driverBase := driverbase.NewDriverImplBase(info, memory.DefaultAllocator)
	dbBase, err := driverbase.NewDatabaseImplBase(context.Background(), &driverBase)
	require.NoError(t, err)
	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		catalog:          "AwsDataCatalog",
		schema:           "default",
		outputLocation:   "s3://test-bucket/results/",
		workGroup:        "my-workgroup",
		authType:         AuthTypeDefault,
		testAthenaClient: mock,
	}
	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer conn.Close(context.Background())

	assert.Equal(t, "Athena engine version 2", getInfoString(t, conn, adbc.InfoVendorVersion))
}

func TestFunctional_GetInfo_VendorArrowVersion(t *testing.T) {
	conn := newTestWrappedConn(t)
	version := getInfoString(t, conn, adbc.InfoVendorArrowVersion)
	assert.Equal(t, infoDriverArrowVersion, version)
}

func TestFunctional_GetInfo_AllCodes(t *testing.T) {
	conn := newTestWrappedConn(t)
	rdr, err := conn.GetInfo(context.Background(), nil)
	require.NoError(t, err)
	defer rdr.Release()

	codes := make(map[adbc.InfoCode]bool)
	for rdr.Next() {
		rec := rdr.RecordBatch()
		nameCol := rec.Column(0).(*array.Uint32)
		for i := 0; i < nameCol.Len(); i++ {
			codes[adbc.InfoCode(nameCol.Value(i))] = true
		}
	}
	require.NoError(t, rdr.Err())

	assert.True(t, codes[adbc.InfoVendorName], "missing InfoVendorName")
	assert.True(t, codes[adbc.InfoVendorVersion], "missing InfoVendorVersion")
	assert.True(t, codes[adbc.InfoVendorArrowVersion], "missing InfoVendorArrowVersion")
	assert.True(t, codes[adbc.InfoVendorSql], "missing InfoVendorSql")
	assert.True(t, codes[adbc.InfoVendorSubstrait], "missing InfoVendorSubstrait")
	assert.True(t, codes[adbc.InfoDriverName], "missing InfoDriverName")
	assert.True(t, codes[adbc.InfoDriverVersion], "missing InfoDriverVersion")
	assert.True(t, codes[adbc.InfoDriverArrowVersion], "missing InfoDriverArrowVersion")
	assert.True(t, codes[adbc.InfoDriverADBCVersion], "missing InfoDriverADBCVersion")
}

// ---------------------------------------------------------------------------
// Connection GetSetOptions tests
// ---------------------------------------------------------------------------

func TestFunctional_Connection_GetOption_CurrentCatalog(t *testing.T) {
	conn := newTestWrappedConn(t)
	gso := conn.(adbc.GetSetOptionsWithContext)

	val, err := gso.GetOption(context.Background(), adbc.OptionKeyCurrentCatalog)
	require.NoError(t, err)
	assert.Equal(t, "AwsDataCatalog", val)
}

func TestFunctional_Connection_GetOption_CurrentDbSchema(t *testing.T) {
	conn := newTestWrappedConn(t)
	gso := conn.(adbc.GetSetOptionsWithContext)

	val, err := gso.GetOption(context.Background(), adbc.OptionKeyCurrentDbSchema)
	require.NoError(t, err)
	assert.Equal(t, "default", val)
}

func TestFunctional_Connection_SetOption_CurrentCatalog(t *testing.T) {
	conn := newTestWrappedConn(t)
	gso := conn.(adbc.GetSetOptionsWithContext)
	ctx := context.Background()

	require.NoError(t, gso.SetOption(ctx, adbc.OptionKeyCurrentCatalog, "NewCatalog"))

	val, err := gso.GetOption(ctx, adbc.OptionKeyCurrentCatalog)
	require.NoError(t, err)
	assert.Equal(t, "NewCatalog", val)
}

func TestFunctional_Connection_SetOption_CurrentDbSchema(t *testing.T) {
	conn := newTestWrappedConn(t)
	gso := conn.(adbc.GetSetOptionsWithContext)
	ctx := context.Background()

	require.NoError(t, gso.SetOption(ctx, adbc.OptionKeyCurrentDbSchema, "my_schema"))

	val, err := gso.GetOption(ctx, adbc.OptionKeyCurrentDbSchema)
	require.NoError(t, err)
	assert.Equal(t, "my_schema", val)
}

func TestFunctional_Connection_GetOption_Autocommit(t *testing.T) {
	conn := newTestWrappedConn(t)
	gso := conn.(adbc.GetSetOptionsWithContext)

	val, err := gso.GetOption(context.Background(), adbc.OptionKeyAutoCommit)
	require.NoError(t, err)
	assert.Equal(t, adbc.OptionValueEnabled, val)
}

func TestFunctional_Connection_SetOption_Autocommit_True(t *testing.T) {
	conn := newTestWrappedConn(t)
	gso := conn.(adbc.GetSetOptionsWithContext)

	err := gso.SetOption(context.Background(), adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled)
	require.NoError(t, err)
}

func TestFunctional_Connection_SetOption_Autocommit_False(t *testing.T) {
	conn := newTestWrappedConn(t)
	gso := conn.(adbc.GetSetOptionsWithContext)

	err := gso.SetOption(context.Background(), adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support multi-statement transactions")
}
