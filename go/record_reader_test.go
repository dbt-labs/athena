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
		{"timestamp", arrow.FixedWidthTypes.Timestamp_us},
		{"timestamp with time zone", arrow.FixedWidthTypes.Timestamp_us},
		{"varbinary", arrow.BinaryTypes.Binary},
		{"binary", arrow.BinaryTypes.Binary},
		{"decimal(10,2)", arrow.BinaryTypes.String},
		{"array<int>", arrow.BinaryTypes.String},
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

func TestParseTimestampToMicros(t *testing.T) {
	// 1970-01-01 00:00:00 = 0 microseconds
	us, err := parseTimestampToMicros("1970-01-01 00:00:00")
	require.NoError(t, err)
	assert.Equal(t, int64(0), us)

	// 1970-01-01 00:00:01 = 1_000_000 microseconds
	us, err = parseTimestampToMicros("1970-01-01 00:00:01")
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000), us)

	// With fractional seconds
	us, err = parseTimestampToMicros("1970-01-01 00:00:00.5")
	require.NoError(t, err)
	assert.Equal(t, int64(500_000), us)

	// With 6-digit microseconds
	us, err = parseTimestampToMicros("1970-01-01 00:00:00.123456")
	require.NoError(t, err)
	assert.Equal(t, int64(123_456), us)

	// Timestamp with time zone — " UTC" suffix must be ignored
	us, err = parseTimestampToMicros("1970-01-01 00:00:01.000000 UTC")
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000), us)

	// Timestamp with time zone — no fractional seconds, space+tz suffix
	us, err = parseTimestampToMicros("1970-01-01 00:00:01 UTC")
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000), us)

	// Timestamp with IANA time zone name suffix
	us, err = parseTimestampToMicros("1970-01-01 00:00:00.000000 America/New_York")
	require.NoError(t, err)
	assert.Equal(t, int64(0), us)
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
			"1970-01-01 00:00:01.000000",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Timestamp_us, col.DataType())
				assert.EqualValues(t, 1_000_000, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"timestamp with time zone",
			"1970-01-01 00:00:01.000000 UTC",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FixedWidthTypes.Timestamp_us, col.DataType())
				assert.EqualValues(t, 1_000_000, col.(*array.Timestamp).Value(0))
			},
		},
		{
			"varbinary",
			"hello",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.Binary, col.DataType())
				assert.Equal(t, []byte("hello"), col.(*array.Binary).Value(0))
			},
		},
		{
			"binary",
			"world",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.Binary, col.DataType())
				assert.Equal(t, []byte("world"), col.(*array.Binary).Value(0))
			},
		},
		{
			// decimal is stringified
			"decimal(18,2)",
			"123.45",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.String, col.DataType())
				assert.Equal(t, "123.45", col.(*array.String).Value(0))
			},
		},
		{
			// array is stringified
			"array<int>",
			"[1, 2, 3]",
			func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BinaryTypes.String, col.DataType())
				assert.Equal(t, "[1, 2, 3]", col.(*array.String).Value(0))
			},
		},
		{
			// map is stringified
			"map<varchar,int>",
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
}
