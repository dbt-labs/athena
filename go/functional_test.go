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
	"github.com/apache/arrow-go/v18/arrow/memory"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
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
	getQueryExecutionFn   func(ctx context.Context, params *athenaSDK.GetQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error)
	getQueryResultsFn     func(ctx context.Context, params *athenaSDK.GetQueryResultsInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryResultsOutput, error)
	getTableMetadataFn    func(ctx context.Context, params *athenaSDK.GetTableMetadataInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error)
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
func (m *mockAthenaClient) GetQueryExecution(ctx context.Context, params *athenaSDK.GetQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error) {
	return m.getQueryExecutionFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) GetQueryResults(ctx context.Context, params *athenaSDK.GetQueryResultsInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryResultsOutput, error) {
	return m.getQueryResultsFn(ctx, params, optFns...)
}
func (m *mockAthenaClient) GetTableMetadata(ctx context.Context, params *athenaSDK.GetTableMetadataInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error) {
	return m.getTableMetadataFn(ctx, params, optFns...)
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
	getCatalogsFn func(ctx context.Context, params *glueSDK.GetCatalogsInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error)
}

func (m *mockGlueClient) GetCatalogs(ctx context.Context, params *glueSDK.GetCatalogsInput, optFns ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
	return m.getCatalogsFn(ctx, params, optFns...)
}

func emptyGlueCatalogs(_ context.Context, _ *glueSDK.GetCatalogsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
	return &glueSDK.GetCatalogsOutput{}, nil
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
	db := newTestDB(t, athenaMock)
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
	require.NoError(t, stmt.SetSqlQuery("SELECT 42 AS n"))

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
	require.NoError(t, stmt.SetSqlQuery("SELECT * FROM nonexistent_table"))

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
	require.NoError(t, stmt.SetSqlQuery("SELECT 1"))

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
	require.NoError(t, stmt.SetSqlQuery("SELECT sleep(60)"))

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
	require.NoError(t, stmt.SetSqlQuery("SELECT id FROM t"))

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
			assert.Equal(t, "AwsDataCatalog", *params.CatalogName)
			assert.Equal(t, "mydb", *params.DatabaseName)
			assert.Equal(t, "mytable", *params.TableName)
			return &athenaSDK.GetTableMetadataOutput{
				TableMetadata: &types.TableMetadata{
					Name: strp("mytable"),
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
	dbSchema := "mydb"
	schema, err := conn.GetTableSchema(context.Background(), nil, &dbSchema, "mytable")
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

	glueMock := &mockGlueClient{getCatalogsFn: emptyGlueCatalogs}

	conn := newTestConn(t, athenaMock, glueMock)
	conn.catalog = "" // force the paginator path rather than the shortcut

	catalogs, err := conn.GetCatalogs(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"AwsDataCatalog", "MyGlueCatalog"}, catalogs)
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

// TestFunctional_ListCatalogs_IncludesGlue verifies that GetCatalogs merges
// results from both Athena ListDataCatalogs and Glue GetCatalogs.
func TestFunctional_ListCatalogs_IncludesGlue(t *testing.T) {
	athenaMock := &mockAthenaClient{
		listDataCatalogsFn: func(_ context.Context, _ *athenaSDK.ListDataCatalogsInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error) {
			return &athenaSDK.ListDataCatalogsOutput{
				DataCatalogsSummary: []types.DataCatalogSummary{
					{CatalogName: strp("AwsDataCatalog")},
				},
			}, nil
		},
	}

	glueMock := &mockGlueClient{
		getCatalogsFn: func(_ context.Context, params *glueSDK.GetCatalogsInput, _ ...func(*glueSDK.Options)) (*glueSDK.GetCatalogsOutput, error) {
			assert.True(t, params.Recursive, "GetCatalogs should be called with Recursive=true")
			return &glueSDK.GetCatalogsOutput{
				CatalogList: []glueTypes.Catalog{
					{CatalogId: strp("111111111111:my_glue_catalog")},
				},
			}, nil
		},
	}

	conn := newTestConn(t, athenaMock, glueMock)
	conn.catalog = ""

	catalogs, err := conn.GetCatalogs(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"AwsDataCatalog", "my_glue_catalog"}, catalogs)
}
