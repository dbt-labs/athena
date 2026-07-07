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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string { return &s }

func TestAthenaTypeStringToArrow(t *testing.T) {
	tests := []struct {
		athenaType string
		arrowType  arrow.DataType
	}{
		{"varchar", arrow.BinaryTypes.String},
		{"string", arrow.BinaryTypes.String},
		{"char", arrow.BinaryTypes.String},
		{"bigint", arrow.PrimitiveTypes.Int64},
		{"integer", arrow.PrimitiveTypes.Int32},
		{"int", arrow.PrimitiveTypes.Int32},
		{"smallint", arrow.PrimitiveTypes.Int16},
		{"tinyint", arrow.PrimitiveTypes.Int8},
		{"double", arrow.PrimitiveTypes.Float64},
		{"float", arrow.PrimitiveTypes.Float32},
		{"real", arrow.PrimitiveTypes.Float32},
		{"boolean", arrow.FixedWidthTypes.Boolean},
		{"date", arrow.FixedWidthTypes.Date32},
		{"timestamp", &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{"timestamp with time zone", &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{"varbinary", arrow.BinaryTypes.Binary},
		{"binary", arrow.BinaryTypes.Binary},
		{"time", arrow.FixedWidthTypes.Time64us},
		{"time with time zone", arrow.FixedWidthTypes.Time64us},
		{"interval day to second", arrow.FixedWidthTypes.DayTimeInterval},
		{"interval year to month", arrow.FixedWidthTypes.MonthInterval},
		{"hyperloglog", arrow.BinaryTypes.Binary},
		{"p4hyperloglog", arrow.BinaryTypes.Binary},
		{"setdigest", arrow.BinaryTypes.Binary},
		{"qdigest", arrow.BinaryTypes.Binary},
		{"tdigest", arrow.BinaryTypes.Binary},
		{"decimal", arrow.BinaryTypes.String},
		{"array", arrow.BinaryTypes.String},
		{"unknown_type", arrow.BinaryTypes.String},
	}

	for _, tt := range tests {
		t.Run(tt.athenaType, func(t *testing.T) {
			got := athenaTypeStringToArrow(tt.athenaType)
			assert.Equal(t, tt.arrowType, got)
		})
	}
}

func TestBuildSchema(t *testing.T) {
	colInfo := []types.ColumnInfo{
		{Name: strPtr("id"), Type: strPtr("bigint")},
		{Name: strPtr("name"), Type: strPtr("varchar")},
		{Name: strPtr("active"), Type: strPtr("boolean")},
	}

	schema := buildSchema(colInfo)
	require.NotNil(t, schema)
	assert.Equal(t, 3, schema.NumFields())
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Int64, schema.Field(0).Type)
	assert.Equal(t, "name", schema.Field(1).Name)
	assert.Equal(t, arrow.BinaryTypes.String, schema.Field(1).Type)
	assert.Equal(t, "active", schema.Field(2).Name)
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, schema.Field(2).Type)

	// Each field must carry the raw Athena type string under "ATHENA:type".
	for i, want := range []string{"bigint", "varchar", "boolean"} {
		f := schema.Field(i)
		idx := f.Metadata.FindKey("ATHENA:type")
		require.NotEqual(t, -1, idx, "field %d missing ATHENA:type metadata", i)
		assert.Equal(t, want, f.Metadata.Values()[idx], "field %d ATHENA:type", i)
	}
}

func TestNewRecordReader_Empty(t *testing.T) {
	colInfo := []types.ColumnInfo{
		{Name: strPtr("n"), Type: strPtr("integer")},
	}

	rdr, err := newRecordReader(memory.DefaultAllocator, colInfo, nil)
	require.NoError(t, err)
	defer rdr.Release()

	schema := rdr.Schema()
	assert.Equal(t, 1, schema.NumFields())
	assert.Equal(t, "n", schema.Field(0).Name)
	assert.False(t, rdr.Next())
}

func TestNewRecordReader_WithRows(t *testing.T) {
	colInfo := []types.ColumnInfo{
		{Name: strPtr("id"), Type: strPtr("bigint")},
		{Name: strPtr("val"), Type: strPtr("varchar")},
	}

	rows := []types.Row{
		{Data: []types.Datum{{VarCharValue: strPtr("42")}, {VarCharValue: strPtr("hello")}}},
		{Data: []types.Datum{{VarCharValue: strPtr("99")}, {VarCharValue: strPtr("world")}}},
	}

	rdr, err := newRecordReader(memory.DefaultAllocator, colInfo, rows)
	require.NoError(t, err)
	defer rdr.Release()

	assert.True(t, rdr.Next())
	rec := rdr.Record()
	assert.Equal(t, int64(2), rec.NumRows())
	assert.Equal(t, int64(2), rec.NumCols())
}

func TestNewRecordReader_NullValues(t *testing.T) {
	colInfo := []types.ColumnInfo{
		{Name: strPtr("val"), Type: strPtr("integer")},
	}

	rows := []types.Row{
		{Data: []types.Datum{{VarCharValue: nil}}}, // NULL
	}

	rdr, err := newRecordReader(memory.DefaultAllocator, colInfo, rows)
	require.NoError(t, err)
	defer rdr.Release()

	assert.True(t, rdr.Next())
	rec := rdr.Record()
	col := rec.Column(0)
	assert.True(t, col.IsNull(0))
}

func TestCivilToDays(t *testing.T) {
	// Unix epoch = 0
	assert.Equal(t, int32(0), civilToDays(1970, 1, 1))
	// 1970-01-02 = day 1
	assert.Equal(t, int32(1), civilToDays(1970, 1, 2))
	// 2000-01-01
	assert.Equal(t, int32(10957), civilToDays(2000, 1, 1))
}

func TestParseDateToDays(t *testing.T) {
	days, err := parseDateToDays("1970-01-01")
	require.NoError(t, err)
	assert.Equal(t, int32(0), days)

	days, err = parseDateToDays("2000-01-01")
	require.NoError(t, err)
	assert.Equal(t, int32(10957), days)
}

func TestParseTimestampToNanos(t *testing.T) {
	// 1970-01-01 00:00:00 = 0 nanoseconds
	ns, err := parseTimestampToNanos("1970-01-01 00:00:00")
	require.NoError(t, err)
	assert.Equal(t, int64(0), ns)

	// 1970-01-01 00:00:01 = 1_000_000_000 nanoseconds
	ns, err = parseTimestampToNanos("1970-01-01 00:00:01")
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000_000), ns)

	// With fractional seconds — ".5" → 500_000_000 ns
	ns, err = parseTimestampToNanos("1970-01-01 00:00:00.5")
	require.NoError(t, err)
	assert.Equal(t, int64(500_000_000), ns)

	// Millisecond precision — ".123" → 123_000_000 ns
	ns, err = parseTimestampToNanos("1970-01-01 00:00:00.123")
	require.NoError(t, err)
	assert.Equal(t, int64(123_000_000), ns)

	// Microsecond precision — ".123456" → 123_456_000 ns
	ns, err = parseTimestampToNanos("1970-01-01 00:00:00.123456")
	require.NoError(t, err)
	assert.Equal(t, int64(123_456_000), ns)

	// Nanosecond precision — full 9 digits preserved
	ns, err = parseTimestampToNanos("1970-01-01 00:00:00.123456789")
	require.NoError(t, err)
	assert.Equal(t, int64(123_456_789), ns)

	// Timestamp with time zone — " UTC" suffix must be ignored
	ns, err = parseTimestampToNanos("1970-01-01 00:00:01.000000000 UTC")
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000_000), ns)

	// Timestamp with time zone — no fractional seconds, space+tz suffix
	ns, err = parseTimestampToNanos("1970-01-01 00:00:01 UTC")
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000_000), ns)

	// Timestamp with IANA time zone name suffix — midnight Eastern = 05:00 UTC
	ns, err = parseTimestampToNanos("1970-01-01 00:00:00.000000000 America/New_York")
	require.NoError(t, err)
	assert.Equal(t, int64(5*3600*1_000_000_000), ns)

	// Timestamp with time zone offset — midnight in +03:45 = previous day 20:15 UTC
	ns, err = parseTimestampToNanos("1970-01-01 00:00:00.000000000 +03:45")
	require.NoError(t, err)
	assert.Equal(t, int64(-(3*3600+45*60)*1_000_000_000), ns)
}

// TestBuildRecordBatch_AllTypes exercises appendValue for every Athena type
// through buildRecordBatch, verifying both the Arrow column type and the
// decoded value.
func TestBuildRecordBatch_AllTypes(t *testing.T) {
	tests := []struct {
		athenaType string
		val        string
		check      func(t *testing.T, col arrow.Array)
	}{
		{
			"bigint",
			"9223372036854775807",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.PrimitiveTypes.Int64, col.DataType())
				assert.EqualValues(t, int64(9223372036854775807), col.(*array.Int64).Value(0))
			},
		},
		{
			"varchar",
			"hello",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.String, col.DataType())
				assert.Equal(t, "hello", col.(*array.String).Value(0))
			},
		},
		{
			"integer",
			"123",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.PrimitiveTypes.Int32, col.DataType())
				assert.EqualValues(t, 123, col.(*array.Int32).Value(0))
			},
		},
		{
			"int",
			"-1",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.PrimitiveTypes.Int32, col.DataType())
				assert.EqualValues(t, -1, col.(*array.Int32).Value(0))
			},
		},
		{
			"smallint",
			"32767",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.PrimitiveTypes.Int16, col.DataType())
				assert.EqualValues(t, 32767, col.(*array.Int16).Value(0))
			},
		},
		{
			"tinyint",
			"127",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.PrimitiveTypes.Int8, col.DataType())
				assert.EqualValues(t, 127, col.(*array.Int8).Value(0))
			},
		},
		{
			"double",
			"3.14",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.PrimitiveTypes.Float64, col.DataType())
				assert.InDelta(t, 3.14, col.(*array.Float64).Value(0), 1e-9)
			},
		},
		{
			"float",
			"2.5",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.PrimitiveTypes.Float32, col.DataType())
				assert.InDelta(t, 2.5, col.(*array.Float32).Value(0), 1e-6)
			},
		},
		{
			"real",
			"1.0",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.PrimitiveTypes.Float32, col.DataType())
				assert.InDelta(t, 1.0, col.(*array.Float32).Value(0), 1e-6)
			},
		},
		{
			"boolean",
			"true",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Boolean, col.DataType())
				assert.True(t, col.(*array.Boolean).Value(0))
			},
		},
		{
			"date",
			"1970-01-01",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Date32, col.DataType())
				assert.EqualValues(t, 0, col.(*array.Date32).Value(0))
			},
		},
		{
			"date",
			"2000-01-01",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Date32, col.DataType())
				assert.EqualValues(t, 10957, col.(*array.Date32).Value(0))
			},
		},
		{
			"timestamp",
			"1970-01-01 00:00:01",
			func(t *testing.T, col arrow.Array) {
				dt := &arrow.TimestampType{Unit: arrow.Nanosecond}
				require.Equal(t, dt, col.DataType())
				assert.EqualValues(t, 1_000_000_000, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"timestamp",
			"1970-01-01 00:00:01.123",
			func(t *testing.T, col arrow.Array) {
				dt := &arrow.TimestampType{Unit: arrow.Nanosecond}
				require.Equal(t, dt, col.DataType())
				assert.EqualValues(t, 1_123_000_000, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"timestamp",
			"1970-01-01 00:00:01.123456",
			func(t *testing.T, col arrow.Array) {
				dt := &arrow.TimestampType{Unit: arrow.Nanosecond}
				require.Equal(t, dt, col.DataType())
				assert.EqualValues(t, 1_123_456_000, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"timestamp",
			"1970-01-01 00:00:01.123456789",
			func(t *testing.T, col arrow.Array) {
				dt := &arrow.TimestampType{Unit: arrow.Nanosecond}
				require.Equal(t, dt, col.DataType())
				assert.EqualValues(t, 1_123_456_789, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"timestamp with time zone",
			"1970-01-01 00:00:01.234 UTC",
			func(t *testing.T, col arrow.Array) {
				dt := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
				require.Equal(t, dt, col.DataType())
				assert.EqualValues(t, 1_234_000_000, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"timestamp with time zone",
			"1970-01-01 00:00:01.234 America/New_York",
			func(t *testing.T, col arrow.Array) {
				dt := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
				require.Equal(t, dt, col.DataType())
				// 00:00:01.234 Eastern = 05:00:01.234 UTC
				assert.EqualValues(t, 5*3600*1_000_000_000+1_234_000_000, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"timestamp with time zone",
			"1970-01-01 03:45:01.234 +03:45",
			func(t *testing.T, col arrow.Array) {
				dt := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
				require.Equal(t, dt, col.DataType())
				// 03:45:01.234 in +03:45 = 00:00:01.234 UTC
				assert.EqualValues(t, 1_234_000_000, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"varbinary",
			"68 65 6c 6c 6f",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.Binary, col.DataType())
				assert.Equal(t, []byte("hello"), col.(*array.Binary).Value(0))
			},
		},
		{
			"binary",
			"77 6f 72 6c 64",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.Binary, col.DataType())
				assert.Equal(t, []byte("world"), col.(*array.Binary).Value(0))
			},
		},
		{
			"time",
			"12:30:45.123456",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				// 12h30m45.123456s in microseconds
				expected := arrow.Time64(12*3600*1e6 + 30*60*1e6 + 45*1e6 + 123456)
				assert.Equal(t, expected, col.(*array.Time64).Value(0))
			},
		},
		{
			"time",
			"12:30:45",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				expected := arrow.Time64(12*3600*1e6 + 30*60*1e6 + 45*1e6)
				assert.Equal(t, expected, col.(*array.Time64).Value(0))
			},
		},
		{
			"time",
			"12:30:45.123",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				expected := arrow.Time64(12*3600*1e6 + 30*60*1e6 + 45*1e6 + 123000)
				assert.Equal(t, expected, col.(*array.Time64).Value(0))
			},
		},
		{
			"time",
			"00:00:00.000000",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				assert.Equal(t, arrow.Time64(0), col.(*array.Time64).Value(0))
			},
		},
		{
			"time with time zone",
			"12:30:45.123456 +05:30",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				// 12:30:45.123456 +05:30 = 07:00:45.123456 UTC
				expected := arrow.Time64(7*3600*1e6 + 0*60*1e6 + 45*1e6 + 123456)
				assert.Equal(t, expected, col.(*array.Time64).Value(0))
			},
		},
		{
			"time with time zone",
			"12:30:45+05:30",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				// 12:30:45+05:30 = 07:00:45 UTC
				expected := arrow.Time64(7*3600*1e6 + 0*60*1e6 + 45*1e6)
				assert.Equal(t, expected, col.(*array.Time64).Value(0))
			},
		},
		{
			"time with time zone",
			"12:30:45.123+05:30",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				// 12:30:45.123+05:30 = 07:00:45.123 UTC
				expected := arrow.Time64(7*3600*1e6 + 0*60*1e6 + 45*1e6 + 123000)
				assert.Equal(t, expected, col.(*array.Time64).Value(0))
			},
		},
		{
			"time with time zone",
			"12:30:45.123456+05:30",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				// 12:30:45.123456+05:30 = 07:00:45.123456 UTC (no space before offset)
				expected := arrow.Time64(7*3600*1e6 + 0*60*1e6 + 45*1e6 + 123456)
				assert.Equal(t, expected, col.(*array.Time64).Value(0))
			},
		},
		{
			"time with time zone",
			"08:00:00-03:00",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Time64us, col.DataType())
				// 08:00:00 -03:00 = 11:00:00 UTC
				expected := arrow.Time64(11 * 3600 * 1e6)
				assert.Equal(t, expected, col.(*array.Time64).Value(0))
			},
		},
		{
			"interval day to second",
			"1 12:30:45.123",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.DayTimeInterval, col.DataType())
				v := col.(*array.DayTimeInterval).Value(0)
				assert.EqualValues(t, 1, v.Days)
				assert.EqualValues(t, 12*3600*1000+30*60*1000+45*1000+123, v.Milliseconds)
			},
		},
		{
			"interval day to second",
			"0 00:00:00.000",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.DayTimeInterval, col.DataType())
				v := col.(*array.DayTimeInterval).Value(0)
				assert.EqualValues(t, 0, v.Days)
				assert.EqualValues(t, 0, v.Milliseconds)
			},
		},
		{
			"interval day to second",
			"30 05:00:00.000",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.DayTimeInterval, col.DataType())
				v := col.(*array.DayTimeInterval).Value(0)
				assert.EqualValues(t, 30, v.Days)
				assert.EqualValues(t, 5*3600*1000, v.Milliseconds)
			},
		},
		{
			"interval year to month",
			"9-3",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.MonthInterval, col.DataType())
				v := col.(*array.MonthInterval).Value(0)
				assert.EqualValues(t, 9*12+3, v)
			},
		},
		{
			"interval year to month",
			"0-0",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.MonthInterval, col.DataType())
				v := col.(*array.MonthInterval).Value(0)
				assert.EqualValues(t, 0, v)
			},
		},
		{
			"interval year to month",
			"1-6",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.MonthInterval, col.DataType())
				v := col.(*array.MonthInterval).Value(0)
				assert.EqualValues(t, 18, v)
			},
		},
		{
			// array is stringified
			"array",
			"[1, 2, 3]",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.String, col.DataType())
				assert.Equal(t, "[1, 2, 3]", col.(*array.String).Value(0))
			},
		},
		{
			// map is stringified
			"map",
			"{a=1}",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.String, col.DataType())
				assert.Equal(t, "{a=1}", col.(*array.String).Value(0))
			},
		},
		{
			// json is stringified
			"json",
			`{"k":"v"}`,
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.String, col.DataType())
				assert.Equal(t, `{"k":"v"}`, col.(*array.String).Value(0))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.athenaType+"/"+tt.val, func(t *testing.T) {
			colInfo := []types.ColumnInfo{{Name: strPtr("col"), Type: strPtr(tt.athenaType)}}
			rows := []types.Row{{Data: []types.Datum{{VarCharValue: strPtr(tt.val)}}}}

			schema := buildSchema(colInfo)
			batch, err := buildRecordBatch(memory.DefaultAllocator, schema, rows)
			require.NoError(t, err)
			defer batch.Release()

			require.EqualValues(t, 1, batch.NumRows())
			require.EqualValues(t, 1, batch.NumCols())
			tt.check(t, batch.Column(0))
		})
	}

	// Decimal requires Precision/Scale from ColumnInfo.
	t.Run("decimal/123.45", func(t *testing.T) {
		colInfo := []types.ColumnInfo{{Name: strPtr("col"), Type: strPtr("decimal"), Precision: 18, Scale: 2}}
		rows := []types.Row{{Data: []types.Datum{{VarCharValue: strPtr("123.45")}}}}

		schema := buildSchema(colInfo)
		batch, err := buildRecordBatch(memory.DefaultAllocator, schema, rows)
		require.NoError(t, err)
		defer batch.Release()

		require.EqualValues(t, 1, batch.NumRows())
		col := batch.Column(0)
		dt := &arrow.Decimal128Type{Precision: 18, Scale: 2}
		require.Equal(t, dt, col.DataType())
		assert.Equal(t, "123.45", col.(*array.Decimal128).Value(0).ToString(2))
	})
}
