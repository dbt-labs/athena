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
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	athena "github.com/dbt-labs/athena/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getSetOptions is a helper to cast adbc.Database to adbc.GetSetOptions.
func getSetOptions(t *testing.T, db adbc.Database) adbc.GetSetOptions {
	t.Helper()
	gso, ok := db.(adbc.GetSetOptions)
	require.True(t, ok, "database does not implement adbc.GetSetOptions")
	return gso
}

func TestNewDriver(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)
	assert.NotNil(t, drv)
}

func TestNewDatabase_NoOptions(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	// Should succeed even with no options (validation deferred to Open)
	db, err := drv.NewDatabase(map[string]string{})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestNewDatabase_WithOptions(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionCatalog:        "AwsDataCatalog",
		athena.OptionSchema:         "default",
		athena.OptionOutputLocation: "s3://my-bucket/athena-results/",
		athena.OptionWorkGroup:      "primary",
		athena.OptionAuthType:       athena.AuthTypeDefault,
	})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestNewDatabase_InvalidAuthType(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	_, err := drv.NewDatabase(map[string]string{
		athena.OptionAuthType: "invalid_auth_type",
	})
	require.Error(t, err)
}

func TestGetSetOption(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:  "us-west-2",
		athena.OptionCatalog: "MyCatalog",
	})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	gso := getSetOptions(t, db)

	region, err := gso.GetOption(athena.OptionRegion)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", region)

	catalog, err := gso.GetOption(athena.OptionCatalog)
	require.NoError(t, err)
	assert.Equal(t, "MyCatalog", catalog)

	// Update an option
	err = gso.SetOption(athena.OptionRegion, "eu-west-1")
	require.NoError(t, err)

	region, err = gso.GetOption(athena.OptionRegion)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", region)
}

func TestAuthTypeAccessKey_MissingKey(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionOutputLocation: "s3://bucket/prefix/",
		athena.OptionAuthType:       athena.AuthTypeAccessKey,
		// Missing access key ID and secret key
	})
	require.NoError(t, err)
	defer db.Close()

	// Open should fail because credentials are incomplete
	_, err = db.Open(context.Background())
	require.Error(t, err)
}

func TestAuthTypeProfile_MissingProfileName(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:         "us-east-1",
		athena.OptionOutputLocation: "s3://bucket/prefix/",
		athena.OptionAuthType:       athena.AuthTypeProfile,
		// Missing profile name
	})
	require.NoError(t, err)
	defer db.Close()

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

// integrationConn opens a real Athena connection from environment variables and
// registers cleanup. Skips the test if ADBC_ATHENA_TESTS is unset.
func integrationConn(t *testing.T) adbc.Connection {
	t.Helper()
	if os.Getenv("ADBC_ATHENA_TESTS") == "" {
		t.Skip("set ADBC_ATHENA_TESTS=1 to run integration tests")
	}

	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-east-1"
	}
	outputLocation := os.Getenv("ATHENA_OUTPUT_LOCATION")
	// require.NotEmpty(t, outputLocation, "ATHENA_OUTPUT_LOCATION must be set for integration tests")

	catalog := os.Getenv("ATHENA_CATALOG")
	if catalog == "" {
		catalog = "AwsDataCatalog"
	}
	schema := os.Getenv("ATHENA_SCHEMA")
	if schema == "" {
		schema = "default"
	}

	opts := map[string]string{
		athena.OptionRegion:         region,
		athena.OptionOutputLocation: outputLocation,
		athena.OptionCatalog:        catalog,
		athena.OptionSchema:         schema,
		athena.OptionAuthType:       athena.AuthTypeDefault,
	}
	if profile := os.Getenv("AWS_PROFILE"); profile != "" {
		opts[athena.OptionAuthType] = athena.AuthTypeProfile
		opts[athena.OptionProfileName] = profile
	}

	drv := athena.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return conn
}

// runQuery is a helper that executes sql and returns the first record.
func runQuery(t *testing.T, conn adbc.Connection, sql string) arrow.Record {
	t.Helper()
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	t.Cleanup(func() { stmt.Close() })

	require.NoError(t, stmt.SetSqlQuery(sql))

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
  CAST('hello'           AS VARBINARY) AS bin_col,
  CAST(NULL              AS VARCHAR)   AS null_col,
  CAST(42                AS BIGINT)    AS bigint_col,
  CAST(7                 AS INTEGER)   AS int_col,
  CAST(3.14              AS REAL)      AS float_col,
  CAST(3.14              AS DOUBLE)    AS double_col,
  DECIMAL '3.14'                       AS decimal_col,
  TRUE                                 AS bool_col,
  DATE        '2024-01-15'             AS date_col,
  TIMESTAMP '2024-01-15 12:30:00.123'                  AS ts_3_col,
  TIMESTAMP '2024-01-15 12:30:00.123456'               AS ts_6_col,
  TIMESTAMP '2024-01-15 12:30:00.123456789'            AS ts_9_col,
  TIMESTAMP '2024-01-15 12:30:00.123 America/New_York' AS ts_tz_name_col,
  TIMESTAMP '2024-01-15 12:30:00.123 +03:45'           AS ts_tz_offset_col,
  TIME '12:30:45.123456'                               AS time_col,
  TIME '12:30:45.123456 +05:30'                        AS time_tz_col,
  INTERVAL '1' DAY + INTERVAL '12' HOUR                AS interval_ds_col,
  INTERVAL '9' YEAR + INTERVAL '3' MONTH               AS interval_ym_col,
  IPADDRESS '192.168.0.1'                              AS ipaddress_col,
  UUID '9409d3f1-01e6-4380-8a04-aecc50c7fa2e'          AS uuid_col,
  ARRAY[1, 2, 3]                       AS array_col,
  MAP(ARRAY['k'], ARRAY['v'])          AS map_col,
  CAST(MAP(ARRAY['k'], ARRAY['v'])     AS JSON) AS json_col,
  APPROX_SET(123)                      	 AS hll_col,
  CAST(APPROX_SET(123) AS P4HyperLogLog) AS p4hll_col,
  QDIGEST_AGG(123)                       AS qdigest_col,
  TDIGEST_AGG(123)                       AS tdigest_col
`
	rec := runQuery(t, conn, query)

	require.EqualValues(t, 28, rec.NumCols(), "expected 28 columns")
	require.EqualValues(t, 1, rec.NumRows(), "expected 1 row")

	schema := rec.Schema()

	decimalType := &arrow.Decimal128Type{Precision: 3, Scale: 2}

	// Scalar types map to their native Arrow types.
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(0).Type, "str_col")
	assert.Equal(t, arrow.BinaryTypes.Binary, schema.Field(1).Type, "bin_col")
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(2).Type, "null_col")
	assert.Equal(t, arrow.PrimitiveTypes.Int64, schema.Field(3).Type, "bigint_col")
	assert.Equal(t, arrow.PrimitiveTypes.Int32, schema.Field(4).Type, "int_col")
	assert.Equal(t, arrow.PrimitiveTypes.Float32, schema.Field(5).Type, "float_col")
	assert.Equal(t, arrow.PrimitiveTypes.Float64, schema.Field(6).Type, "double_col")
	assert.Equal(t, decimalType, schema.Field(7).Type, "decimal_col")
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, schema.Field(8).Type, "bool_col")
	assert.Equal(t, arrow.FixedWidthTypes.Date32, schema.Field(9).Type, "date_col")

	// Timestamp types map to nanosecond timestamp arrays, with or without time zone
	tsType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	tstzType := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}

	assert.Equal(t, tsType, schema.Field(10).Type, "ts_3_col")
	assert.Equal(t, tsType, schema.Field(11).Type, "ts_6_col")
	assert.Equal(t, tsType, schema.Field(12).Type, "ts_9_col")
	assert.Equal(t, tstzType, schema.Field(13).Type, "ts_tz_name_col")
	assert.Equal(t, tstzType, schema.Field(14).Type, "ts_tz_offset_col")

	// Time types
	assert.Equal(t, arrow.FixedWidthTypes.Time64us, schema.Field(15).Type, "time_col")
	assert.Equal(t, arrow.FixedWidthTypes.Time64us, schema.Field(16).Type, "time_tz_col")

	// Intervals
	assert.Equal(t, arrow.FixedWidthTypes.DayTimeInterval, schema.Field(17).Type, "interval_ds_col")
	assert.Equal(t, arrow.FixedWidthTypes.MonthInterval, schema.Field(18).Type, "interval_ym_col")

	// Special types are stringified
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(19).Type, "ipaddress_col")
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(20).Type, "uuid_col")

	// Nested types are stringified.
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(21).Type, "array_col")
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(22).Type, "map_col")
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(23).Type, "json_col")

	// Spot-check scalar values.
	assert.Equal(t, "hello", rec.Column(0).(*array.String).Value(0))
	assert.Equal(t, []byte("hello"), rec.Column(1).(*array.Binary).Value(0))
	assert.True(t, rec.Column(2).IsNull(0), "null_col should be null")
	assert.EqualValues(t, 42, rec.Column(3).(*array.Int64).Value(0))
	assert.EqualValues(t, 7, rec.Column(4).(*array.Int32).Value(0))
	assert.InDelta(t, 3.14, rec.Column(5).(*array.Float32).Value(0), 1e-2)
	assert.InDelta(t, 3.14, rec.Column(6).(*array.Float64).Value(0), 1e-9)
	assert.InDelta(t, 3.14, rec.Column(7).(*array.Decimal128).Value(0).ToFloat64(2), 1e-9)
	assert.True(t, rec.Column(8).(*array.Boolean).Value(0))
	assert.EqualValues(t, arrow.Date32(19737), rec.Column(9).(*array.Date32).Value(0), "date_col: days since epoch")

	// 2024-01-15 12:30:00 UTC in nanoseconds since epoch:
	// 19737 days * 86400 s/day = 1_705_276_800 s
	// + 12*3600 + 30*60 = 45_000 s
	// = 1_705_321_800 s total
	const baseNs = int64(1_705_321_800) * 1_000_000_000

	// Timestamps without time zone — stored as-is in nanoseconds.
	assert.EqualValues(t, baseNs+123_000_000, rec.Column(10).(*array.Timestamp).Value(0), "ts_3_col")
	assert.EqualValues(t, baseNs+123_456_000, rec.Column(11).(*array.Timestamp).Value(0), "ts_6_col")
	assert.EqualValues(t, baseNs+123_456_789, rec.Column(12).(*array.Timestamp).Value(0), "ts_9_col")

	// Timestamps with time zone — Athena normalizes to UTC before returning.
	// 12:30:00.123 America/New_York (EST, UTC-5 in January) = 17:30:00.123 UTC
	const estOffsetNs = 5 * 3600 * int64(1_000_000_000)
	assert.EqualValues(t, baseNs+estOffsetNs+123_000_000, rec.Column(13).(*array.Timestamp).Value(0), "ts_tz_name_col")
	// 12:30:00.123 +03:45 = 08:45:00.123 UTC (subtract 3h45m)
	const plus0345Ns = (3*3600 + 45*60) * int64(1_000_000_000)
	assert.EqualValues(t, baseNs-plus0345Ns+123_000_000, rec.Column(14).(*array.Timestamp).Value(0), "ts_tz_offset_col")

	// Time values
	// 12:30:45.123456 in microseconds since midnight
	const timeUs = int64(12*3600*1e6 + 30*60*1e6 + 45*1e6 + 123456)
	assert.EqualValues(t, timeUs, rec.Column(15).(*array.Time64).Value(0), "time_col")
	// 12:30:45.123456 +05:30 = 07:00:45.123456 UTC
	const timeTzUs = int64(7*3600*1e6 + 0*60*1e6 + 45*1e6 + 123456)
	assert.EqualValues(t, timeTzUs, rec.Column(16).(*array.Time64).Value(0), "time_tz_col")

	// Intervals
	assert.Equal(t, arrow.DayTimeInterval{Days: 1, Milliseconds: 12 * 3600 * 1000}, rec.Column(17).(*array.DayTimeInterval).Value(0))
	assert.Equal(t, arrow.MonthInterval(9*12+3), rec.Column(18).(*array.MonthInterval).Value(0))

	// Special types are stringified
	assert.Equal(t, "192.168.0.1", rec.Column(19).(*array.String).Value(0))
	assert.Equal(t, "9409d3f1-01e6-4380-8a04-aecc50c7fa2e", rec.Column(20).(*array.String).Value(0))

	// Nested columns must be strings.
	assert.Equal(t, "[1, 2, 3]", rec.Column(21).(*array.String).Value(0))
	assert.Equal(t, "{k=v}", rec.Column(22).(*array.String).Value(0))
	assert.Equal(t, "{\"k\":\"v\"}", rec.Column(23).(*array.String).Value(0))

	// Sketch types are returned as binary.
	assert.Equal(t, arrow.BinaryTypes.Binary, schema.Field(24).Type, "hll_col")
	assert.Equal(t, arrow.BinaryTypes.Binary, schema.Field(25).Type, "p4hll_col")
	assert.Equal(t, arrow.BinaryTypes.Binary, schema.Field(26).Type, "qdigest_col")
	assert.Equal(t, arrow.BinaryTypes.Binary, schema.Field(27).Type, "tdigest_col")
	assert.NotEmpty(t, rec.Column(24).(*array.Binary).Value(0), "hll_col")
	assert.NotEmpty(t, rec.Column(25).(*array.Binary).Value(0), "p4hll_col")
	assert.NotEmpty(t, rec.Column(26).(*array.Binary).Value(0), "qdigest_col")
	assert.NotEmpty(t, rec.Column(27).(*array.Binary).Value(0), "tdigest_col")
}
