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

package athena_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	glueSDK "github.com/aws/aws-sdk-go-v2/service/glue"
	glueTypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	athena "github.com/dbt-labs/athena/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string { return &s }

// getSetOptions is a helper to cast adbc.DatabaseWithContext to adbc.GetSetOptionsWithContext.
func getSetOptions(t *testing.T, db adbc.DatabaseWithContext) adbc.GetSetOptionsWithContext {
	t.Helper()
	gso, ok := db.(adbc.GetSetOptionsWithContext)
	require.True(t, ok, "database does not implement adbc.GetSetOptionsWithContext")
	return gso
}

func TestNewDriver(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)
	assert.NotNil(t, drv)
}

func TestNewDatabase_NoOptions(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	// Should succeed even with no options (validation deferred to Open)
	db, err := drv.NewDatabaseWithContext(context.Background(),map[string]string{})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close(context.Background())
}

func TestNewDatabase_WithOptions(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabaseWithContext(context.Background(),map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionCatalog:        "AwsDataCatalog",
		athena.OptionSchema:         "default",
		athena.OptionOutputLocation: "s3://my-bucket/athena-results/",
		athena.OptionWorkGroup:      "primary",
		athena.OptionAuthType:       athena.AuthTypeDefault,
	})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close(context.Background())
}

func TestNewDatabase_InvalidAuthType(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	_, err := drv.NewDatabaseWithContext(context.Background(),map[string]string{
		athena.OptionAuthType: "invalid_auth_type",
	})
	require.Error(t, err)
}

func TestGetSetOption(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabaseWithContext(context.Background(),map[string]string{
		athena.OptionRegion:  "us-west-2",
		athena.OptionCatalog: "MyCatalog",
	})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close(context.Background())

	gso := getSetOptions(t, db)

	ctx := context.Background()

	region, err := gso.GetOption(ctx, athena.OptionRegion)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", region)

	catalog, err := gso.GetOption(ctx, athena.OptionCatalog)
	require.NoError(t, err)
	assert.Equal(t, "MyCatalog", catalog)

	// Update an option
	err = gso.SetOption(ctx, athena.OptionRegion, "eu-west-1")
	require.NoError(t, err)

	region, err = gso.GetOption(ctx, athena.OptionRegion)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", region)
}

func TestAuthTypeAccessKey_MissingKey(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabaseWithContext(context.Background(),map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionOutputLocation: "s3://bucket/prefix/",
		athena.OptionAuthType:       athena.AuthTypeAccessKey,
		// Missing access key ID and secret key
	})
	require.NoError(t, err)
	defer db.Close(context.Background())

	// Open should fail because credentials are incomplete
	_, err = db.Open(context.Background())
	require.Error(t, err)
}

func TestAuthTypeProfile_MissingProfileName(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabaseWithContext(context.Background(),map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionOutputLocation: "s3://bucket/prefix/",
		athena.OptionAuthType:       athena.AuthTypeProfile,
		// Missing profile name
	})
	require.NoError(t, err)
	defer db.Close(context.Background())

	_, err = db.Open(context.Background())
	require.Error(t, err)
}

func TestAllAuthTypeConstants(t *testing.T) {
	assert.Equal(t, "iam", athena.AuthTypeDefault)
	assert.Equal(t, "access_key", athena.AuthTypeAccessKey)
	assert.Equal(t, "profile", athena.AuthTypeProfile)
}

func TestAllOptionConstants(t *testing.T) {
	assert.Equal(t, "athena.region", athena.OptionRegion)
	assert.Equal(t, "athena.catalog", athena.OptionCatalog)
	assert.Equal(t, "athena.schema", athena.OptionSchema)
	assert.Equal(t, "athena.output_location", athena.OptionOutputLocation)
	assert.Equal(t, "athena.work_group", athena.OptionWorkGroup)
	assert.Equal(t, "athena.auth_type", athena.OptionAuthType)
	assert.Equal(t, "athena.aws.access_key_id", athena.OptionAccessKeyID)
	assert.Equal(t, "athena.aws.secret_access_key", athena.OptionSecretKey)
	assert.Equal(t, "athena.aws.session_token", athena.OptionSessionToken)
	assert.Equal(t, "athena.aws.profile", athena.OptionProfileName)
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

var testRegion string
var testCatalogName string
var testSchemaName string

func skipIntegrationTests() bool {
	return os.Getenv("ADBC_ATHENA_TESTS") == ""
}

func TestMain(m *testing.M) {
	if skipIntegrationTests() {
		os.Exit(m.Run())
	}

	testRegion = os.Getenv("AWS_DEFAULT_REGION")
	if testRegion == "" {
		testRegion = "us-east-1"
	}

	testCatalogName = os.Getenv("ATHENA_CATALOG")
	if testCatalogName == "" {
		testCatalogName = "AwsDataCatalog"
	}

	testSchemaName = os.Getenv("ATHENA_SCHEMA")
	createSchema := false
	if testSchemaName == "" {
		testSchemaName = fmt.Sprintf("athena_adbc_test_%06d", rand.Intn(1_000_000))
		createSchema = true
	}

	glueClient := createGlueClient(testRegion)
	setupTestCatalog(glueClient, testSchemaName, createSchema)

	code := m.Run()

	teardownTestCatalog(glueClient, testSchemaName, createSchema)
	os.Exit(code)
}

func createGlueClient(region string) *glueSDK.Client {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(region))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load AWS config: %v\n", err)
		os.Exit(1)
	}
	return glueSDK.NewFromConfig(cfg)
}

func setupTestCatalog(glueClient *glueSDK.Client, schemaName string, createSchema bool) {
	ctx := context.Background()

	if createSchema {
		_, err := glueClient.CreateDatabase(ctx, &glueSDK.CreateDatabaseInput{
			DatabaseInput: &glueTypes.DatabaseInput{
				Name: &schemaName,
			},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create database %s: %v\n", schemaName, err)
			os.Exit(1)
		}
	}

	tables := []struct {
		name      string
		tableType string
		columns   []glueTypes.Column
	}{
		{
			name:      "test_table_1",
			tableType: "EXTERNAL_TABLE",
			columns: []glueTypes.Column{
				{Name: strPtr("id"), Type: strPtr("bigint")},
				{Name: strPtr("name"), Type: strPtr("string")},
				{Name: strPtr("created_at"), Type: strPtr("timestamp")},
			},
		},
		{
			name:      "test_table_2",
			tableType: "EXTERNAL_TABLE",
			columns: []glueTypes.Column{
				{Name: strPtr("user_id"), Type: strPtr("bigint")},
				{Name: strPtr("score"), Type: strPtr("double")},
				{Name: strPtr("active"), Type: strPtr("boolean")},
				{Name: strPtr("updated_at"), Type: strPtr("timestamp")},
			},
		},
		{
			name:      "another_table",
			tableType: "EXTERNAL_TABLE",
			columns: []glueTypes.Column{
				{Name: strPtr("key"), Type: strPtr("string")},
				{Name: strPtr("value"), Type: strPtr("string")},
			},
		},
		{
			name:      "a_view",
			tableType: "VIRTUAL_VIEW",
			columns: []glueTypes.Column{
				{Name: strPtr("key"), Type: strPtr("string")},
				{Name: strPtr("value"), Type: strPtr("string")},
			},
		},
	}

	for _, tbl := range tables {
		_, err := glueClient.CreateTable(ctx, &glueSDK.CreateTableInput{
			DatabaseName: &schemaName,
			TableInput: &glueTypes.TableInput{
				Name:      strPtr(tbl.name),
				TableType: strPtr(tbl.tableType),
				StorageDescriptor: &glueTypes.StorageDescriptor{
					Columns: tbl.columns,
				},
			},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create table %s: %v\n", tbl.name, err)
			os.Exit(1)
		}
	}
}

func teardownTestCatalog(glueClient *glueSDK.Client, schemaName string, deleteSchema bool) {
	ctx := context.Background()
	tableNames := []string{"test_table_1", "test_table_2", "another_table"}

	for _, name := range tableNames {
		glueClient.DeleteTable(ctx, &glueSDK.DeleteTableInput{
			DatabaseName: &schemaName,
			Name:         strPtr(name),
		})
	}
	if deleteSchema {
		glueClient.DeleteDatabase(ctx, &glueSDK.DeleteDatabaseInput{
			Name: &schemaName,
		})
	}
}

// integrationConn opens a real Athena connection and registers cleanup. Calls
// setupCatalog to ensure test tables exist, then uses catalogSchema as the
// default database. Skips the test if ADBC_ATHENA_TESTS is unset.
func integrationConn(t *testing.T) adbc.ConnectionWithContext {
	t.Helper()
	if skipIntegrationTests() {
		t.Skip("set ADBC_ATHENA_TESTS=1 to run integration tests")
	}

	outputLocation := os.Getenv("ATHENA_OUTPUT_LOCATION")

	opts := map[string]string{
		athena.OptionRegion:         testRegion,
		athena.OptionOutputLocation: outputLocation,
		athena.OptionCatalog:        testCatalogName,
		athena.OptionSchema:         testSchemaName,
		athena.OptionAuthType:       athena.AuthTypeDefault,
	}
	if profile := os.Getenv("AWS_PROFILE"); profile != "" {
		opts[athena.OptionAuthType] = athena.AuthTypeProfile
		opts[athena.OptionProfileName] = profile
	}

	drv := athena.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabaseWithContext(context.Background(),opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close(context.Background()) })

	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })

	return conn
}

// runQuery is a helper that executes sql and returns the first record.
func runQuery(t *testing.T, conn adbc.ConnectionWithContext, sql string) arrow.Record {
	t.Helper()
	ctx := context.Background()
	stmt, err := conn.NewStatement(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close(ctx) })

	require.NoError(t, stmt.SetSqlQuery(ctx, sql))

	rdr, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { rdr.Release() })

	require.True(t, rdr.Next(), "expected at least one record")
	return rdr.Record()
}

func TestIntegration(t *testing.T) {
	conn := integrationConn(t)
	rec := runQuery(t, conn, "SELECT 1 AS n")
	assert.EqualValues(t, 1, rec.NumCols())
}

func TestIntegration_DataTypes(t *testing.T) {
	conn := integrationConn(t)

	// Covers scalar types and two nested types (array, map).
	// Nested types are returned by Athena as opaque strings and stored as
	// Arrow utf8 columns by this driver.
	const query = `
SELECT
  CAST('hello'           AS VARCHAR)   AS str_col,
  CAST(42                AS BIGINT)    AS bigint_col,
  CAST(7                 AS INTEGER)   AS int_col,
  CAST(3.14              AS DOUBLE)    AS double_col,
  true                                 AS bool_col,
  DATE        '2024-01-15'             AS date_col,
  TIMESTAMP   '2024-01-15 12:30:00.123' AS ts_col,
  ARRAY[1, 2, 3]                       AS array_col,
  MAP(ARRAY['k'], ARRAY['v'])          AS map_col
`
	rec := runQuery(t, conn, query)

	require.EqualValues(t, 9, rec.NumCols(), "expected 9 columns")
	require.EqualValues(t, 1, rec.NumRows(), "expected 1 row")

	schema := rec.Schema()

	// Scalar types map to their native Arrow types.
	assert.Equal(t, arrow.BinaryTypes.String,         schema.Field(0).Type, "str_col")
	assert.Equal(t, arrow.PrimitiveTypes.Int64,        schema.Field(1).Type, "bigint_col")
	assert.Equal(t, arrow.PrimitiveTypes.Int32,        schema.Field(2).Type, "int_col")
	assert.Equal(t, arrow.PrimitiveTypes.Float64,      schema.Field(3).Type, "double_col")
	assert.Equal(t, arrow.FixedWidthTypes.Boolean,     schema.Field(4).Type, "bool_col")
	assert.Equal(t, arrow.FixedWidthTypes.Date32,      schema.Field(5).Type, "date_col")
	assert.Equal(t, arrow.FixedWidthTypes.Timestamp_us, schema.Field(6).Type, "ts_col")
	// Nested types are stringified.
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(7).Type, "array_col")
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(8).Type, "map_col")

	// Spot-check scalar values.
	assert.Equal(t, "hello", rec.Column(0).(*array.String).Value(0))
	assert.EqualValues(t, 42, rec.Column(1).(*array.Int64).Value(0))
	assert.EqualValues(t, 7, rec.Column(2).(*array.Int32).Value(0))
	assert.InDelta(t, 3.14, rec.Column(3).(*array.Float64).Value(0), 1e-9)
	assert.True(t, rec.Column(4).(*array.Boolean).Value(0))
	assert.EqualValues(t, arrow.Date32(19737), rec.Column(5).(*array.Date32).Value(0), "date_col: days since epoch")
	assert.EqualValues(t, arrow.Timestamp(1705321800123), rec.Column(6).(*array.Timestamp).Value(0), "ts_col: millis since epoch")

	// Nested columns must be non-empty strings.
	assert.NotEmpty(t, rec.Column(7).(*array.String).Value(0), "array_col should be non-empty")
	assert.NotEmpty(t, rec.Column(8).(*array.String).Value(0), "map_col should be non-empty")
}

func listCatalogs(t *testing.T, conn adbc.ConnectionWithContext, catalogFilter *string) []string {
	rdr, err := conn.GetObjects(
		context.Background(),
		adbc.ObjectDepthCatalogs,
		catalogFilter, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	defer rdr.Release()

	var catalogNames []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		col := rec.Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			catalogNames = append(catalogNames, col.Value(i))
		}
	}
	require.NoError(t, rdr.Err())

	return catalogNames
}

func TestIntegration_ListCatalogs(t *testing.T) {
	conn := integrationConn(t)
	catalogNames := listCatalogs(t, conn, nil)
	assert.Contains(t, catalogNames, "AwsDataCatalog")
}

func TestIntegration_ListCatalogs_WithWildcard(t *testing.T) {
	conn := integrationConn(t)
	catalogNames := listCatalogs(t, conn, strPtr("Aws%Catalog"))
	assert.Equal(t, []string{"AwsDataCatalog"}, catalogNames)
	catalogNames = listCatalogs(t, conn, strPtr("_wsDataCatalo_"))
	assert.Equal(t, []string{"AwsDataCatalog"}, catalogNames)
}

func listSchemas(t *testing.T, conn adbc.ConnectionWithContext, catalogFilter *string, schemaFilter *string) []string {
	rdr, err := conn.GetObjects(
		context.Background(),
		adbc.ObjectDepthDBSchemas,
		catalogFilter, schemaFilter, nil, nil, nil,
	)
	require.NoError(t, err)
	defer rdr.Release()

	var schemaNames []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		dbSchemasList := rec.Column(1).(*array.List)
		dbSchemasStruct := dbSchemasList.ListValues().(*array.Struct)
		nameCol := dbSchemasStruct.Field(0).(*array.String)
		for i := 0; i < nameCol.Len(); i++ {
			schemaNames = append(schemaNames, nameCol.Value(i))
		}
	}
	require.NoError(t, rdr.Err())

	return schemaNames
}

func TestIntegration_ListSchemas(t *testing.T) {
	conn := integrationConn(t)
	schemaNames := listSchemas(t, conn, strPtr("AwsDataCatalog"), nil)
	assert.Contains(t, schemaNames, testSchemaName)
}

func TestIntegration_ListSchemas_WithWildcards(t *testing.T) {
	conn := integrationConn(t)
	schemaFilter := testSchemaName[:len(testSchemaName)-3] + "%"
	schemaNames := listSchemas(t, conn, strPtr("AwsDat_Catalog"), &schemaFilter)
	assert.Contains(t, schemaNames, testSchemaName)
	schemaNames = listSchemas(t, conn, strPtr("AwsDataCatalog"), strPtr("no\\_such\\_schema"))
	assert.NotContains(t, schemaNames, testSchemaName)
}

func listTables(t *testing.T, conn adbc.ConnectionWithContext, catalogName *string, schemaName *string, tableName *string, tableTypes []string) []string {
	rdr, err := conn.GetObjects(
		context.Background(),
		adbc.ObjectDepthTables,
		catalogName, schemaName, tableName, nil, tableTypes,
	)
	require.NoError(t, err)
	defer rdr.Release()

	var tableNames []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		dbSchemasList := rec.Column(1).(*array.List)
		dbSchemasStruct := dbSchemasList.ListValues().(*array.Struct)
		tablesList := dbSchemasStruct.Field(1).(*array.List)
		tablesStruct := tablesList.ListValues().(*array.Struct)
		nameCol := tablesStruct.Field(0).(*array.String)
		for i := 0; i < nameCol.Len(); i++ {
			tableNames = append(tableNames, nameCol.Value(i))
		}
	}
	require.NoError(t, rdr.Err())

	return tableNames
}

func TestIntegration_ListTables(t *testing.T) {
	conn := integrationConn(t)
	tableNames := listTables(t, conn, strPtr("AwsDataCatalog"), &testSchemaName, nil, nil)
	assert.Equal(t, []string{"a_view", "another_table", "test_table_1", "test_table_2"}, tableNames)
}

func TestIntegration_ListTables_WithWildcards(t *testing.T) {
	conn := integrationConn(t)
	schemaFilter := testSchemaName[:len(testSchemaName)-3] + "%"
	tableNames := listTables(t, conn, strPtr("AwsDataCatalog"), &schemaFilter, strPtr("test%"), nil)
	assert.Equal(t, []string{"test_table_1", "test_table_2"}, tableNames)
}

func TestIntegration_ListTables_FilterByTableType(t *testing.T) {
	conn := integrationConn(t)

	// Only views
	views := listTables(t, conn, strPtr("AwsDataCatalog"), &testSchemaName, nil, []string{"VIRTUAL_VIEW"})
	assert.Equal(t, []string{"a_view"}, views)

	// Only tables
	tables := listTables(t, conn, strPtr("AwsDataCatalog"), &testSchemaName, nil, []string{"EXTERNAL_TABLE"})
	assert.Equal(t, []string{"another_table", "test_table_1", "test_table_2"}, tables)

	// Both types returns all
	all := listTables(t, conn, strPtr("AwsDataCatalog"), &testSchemaName, nil, []string{"EXTERNAL_TABLE", "VIRTUAL_VIEW"})
	assert.Equal(t, []string{"a_view", "another_table", "test_table_1", "test_table_2"}, all)
}

type columnInfo struct {
	table    string
	name     string
	dataType string
}

func listColumns(t *testing.T, conn adbc.ConnectionWithContext, catalogName *string, schemaName *string, tableName *string, columnFilter *string) []columnInfo {
	rdr, err := conn.GetObjects(
		context.Background(),
		adbc.ObjectDepthColumns,
		catalogName, schemaName, tableName, columnFilter, nil,
	)
	require.NoError(t, err)
	defer rdr.Release()

	var columns []columnInfo
	for rdr.Next() {
		rec := rdr.RecordBatch()
		dbSchemasList := rec.Column(1).(*array.List)
		dbSchemasStruct := dbSchemasList.ListValues().(*array.Struct)
		tablesList := dbSchemasStruct.Field(1).(*array.List)
		tablesStruct := tablesList.ListValues().(*array.Struct)
		tableNameCol := tablesStruct.Field(0).(*array.String)
		columnsList := tablesStruct.Field(2).(*array.List)
		columnsStruct := columnsList.ListValues().(*array.Struct)
		colNameCol := columnsStruct.Field(0).(*array.String)
		colTypeCol := columnsStruct.Field(4).(*array.String)
		for ti := 0; ti < tablesStruct.Len(); ti++ {
			tblName := tableNameCol.Value(ti)
			colStart := int(columnsList.Offsets()[ti])
			colEnd := int(columnsList.Offsets()[ti+1])
			for ci := colStart; ci < colEnd; ci++ {
				columns = append(columns, columnInfo{tblName, colNameCol.Value(ci), colTypeCol.Value(ci)})
			}
		}
	}
	require.NoError(t, rdr.Err())

	return columns
}

func TestIntegration_ListColumns(t *testing.T) {
	conn := integrationConn(t)
	columns := listColumns(t, conn, strPtr("AwsDataCatalog"), &testSchemaName, strPtr("test\\_table_2"), nil)
	assert.Equal(t, columnInfo{"test_table_2", "user_id", "bigint"}, columns[0])
	assert.Equal(t, columnInfo{"test_table_2", "score", "double"}, columns[1])
	assert.Equal(t, columnInfo{"test_table_2", "active", "boolean"}, columns[2])
	assert.Equal(t, columnInfo{"test_table_2", "updated_at", "timestamp"}, columns[3])
}

func TestIntegration_ListColumns_WithWildcards(t *testing.T) {
	conn := integrationConn(t)
	columns := listColumns(t, conn, strPtr("AwsDataCatalog"), &testSchemaName, strPtr("test\\_table%"), strPtr("%\\_at"))
	assert.Equal(t, columnInfo{"test_table_1", "created_at", "timestamp"}, columns[0])
	assert.Equal(t, columnInfo{"test_table_2", "updated_at", "timestamp"}, columns[1])
}
