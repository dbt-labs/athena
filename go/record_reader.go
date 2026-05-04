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
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// newRecordReader builds an array.RecordReader from Athena rows + column metadata.
// colInfo comes from ResultSetMetadata.ColumnInfo; rows must have the header already skipped.
func newRecordReader(alloc memory.Allocator, colInfo []types.ColumnInfo, rows []types.Row) (array.RecordReader, error) {
	schema := buildSchema(colInfo)

	if len(rows) == 0 {
		return array.NewRecordReader(schema, nil)
	}

	batch, err := buildRecordBatch(alloc, schema, rows)
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	return array.NewRecordReader(schema, []arrow.Record{batch})
}

// buildSchema converts Athena ColumnInfo slice to an Arrow schema.
// Each field carries "ATHENA:type" metadata containing the raw Athena type string,
// allowing consumers to reconstruct the original SQL type without lossy Arrow→SQL
// inference. See https://github.com/apache/arrow-adbc/issues/3449 for a proposal
// to standardize this metadata key across drivers.
func buildSchema(colInfo []types.ColumnInfo) *arrow.Schema {
	fields := make([]arrow.Field, len(colInfo))
	for i, col := range colInfo {
		name := ""
		if col.Name != nil {
			name = *col.Name
		}
		typeStr := ""
		if col.Type != nil {
			typeStr = *col.Type
		}
		meta := arrow.MetadataFrom(map[string]string{
			"ATHENA:type": typeStr,
		})
		fields[i] = arrow.Field{
			Name:     name,
			Type:     athenaColumnTypeToArrow(col),
			Nullable: true,
			Metadata: meta,
		}
	}
	return arrow.NewSchema(fields, nil)
}

// athenaColumnTypeToArrow maps a ColumnInfo's type to an Arrow DataType.
func athenaColumnTypeToArrow(col types.ColumnInfo) arrow.DataType {
	if col.Type == nil {
		return arrow.BinaryTypes.String
	}
	return athenaTypeStringToArrow(*col.Type)
}

// athenaTypeStringToArrow maps an Athena type string to an Arrow DataType.
func athenaTypeStringToArrow(t string) arrow.DataType {
	switch t {
	case "varchar", "string", "char":
		return arrow.BinaryTypes.String
	case "bigint":
		return arrow.PrimitiveTypes.Int64
	case "integer", "int":
		return arrow.PrimitiveTypes.Int32
	case "smallint":
		return arrow.PrimitiveTypes.Int16
	case "tinyint":
		return arrow.PrimitiveTypes.Int8
	case "double":
		return arrow.PrimitiveTypes.Float64
	case "float", "real":
		return arrow.PrimitiveTypes.Float32
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "date":
		return arrow.FixedWidthTypes.Date32
	case "timestamp", "timestamp with time zone":
		return arrow.FixedWidthTypes.Timestamp_us
	case "varbinary", "binary":
		return arrow.BinaryTypes.Binary
	default:
		// array, map, row, decimal, json — stringify
		return arrow.BinaryTypes.String
	}
}

// buildRecordBatch converts Athena rows into a single Arrow Record.
// Column types are derived from the Arrow schema rather than re-inspecting the
// raw Athena type strings, keeping the type mapping logic in one place.
func buildRecordBatch(alloc memory.Allocator, schema *arrow.Schema, rows []types.Row) (arrow.Record, error) {
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	for _, row := range rows {
		for ci := 0; ci < schema.NumFields(); ci++ {
			fb := bldr.Field(ci)
			if ci >= len(row.Data) {
				// Athena returned fewer fields than the schema declares — pad with null.
				fb.AppendNull()
				continue
			}
			field := row.Data[ci]
			val := ""
			isNull := field.VarCharValue == nil
			if !isNull {
				val = *field.VarCharValue
			}
			if err := appendValue(fb, schema.Field(ci).Type, val, isNull); err != nil {
				return nil, fmt.Errorf("column %d (%s): %w", ci, schema.Field(ci).Name, err)
			}
		}
	}

	return bldr.NewRecord(), nil
}

// appendValue appends a string-encoded value to the appropriate builder type,
// switching on the Arrow DataType rather than the original Athena type string.
func appendValue(bldr array.Builder, dt arrow.DataType, val string, isNull bool) error {
	if isNull {
		bldr.AppendNull()
		return nil
	}

	switch dt.ID() {
	case arrow.INT64:
		v, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
		bldr.(*array.Int64Builder).Append(v)
	case arrow.INT32:
		v, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return err
		}
		bldr.(*array.Int32Builder).Append(int32(v))
	case arrow.INT16:
		v, err := strconv.ParseInt(val, 10, 16)
		if err != nil {
			return err
		}
		bldr.(*array.Int16Builder).Append(int16(v))
	case arrow.INT8:
		v, err := strconv.ParseInt(val, 10, 8)
		if err != nil {
			return err
		}
		bldr.(*array.Int8Builder).Append(int8(v))
	case arrow.FLOAT64:
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return err
		}
		bldr.(*array.Float64Builder).Append(v)
	case arrow.FLOAT32:
		v, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return err
		}
		bldr.(*array.Float32Builder).Append(float32(v))
	case arrow.BOOL:
		v, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		bldr.(*array.BooleanBuilder).Append(v)
	case arrow.DATE32:
		days, err := parseDateToDays(val)
		if err != nil {
			return err
		}
		bldr.(*array.Date32Builder).Append(arrow.Date32(days))
	case arrow.TIMESTAMP:
		ms, err := parseTimestampToMillis(val)
		if err != nil {
			return err
		}
		bldr.(*array.TimestampBuilder).Append(arrow.Timestamp(ms))
	case arrow.BINARY:
		bldr.(*array.BinaryBuilder).Append([]byte(val))
	default:
		// STRING covers varchar, string, char, decimal, array, map, row, json, etc.
		bldr.(*array.StringBuilder).Append(val)
	}
	return nil
}

// parseDateToDays parses "YYYY-MM-DD" and returns days since Unix epoch (1970-01-01).
func parseDateToDays(s string) (int32, error) {
	if len(s) < 10 {
		return 0, fmt.Errorf("unexpected date format: %q", s)
	}
	year, err := strconv.Atoi(s[0:4])
	if err != nil {
		return 0, err
	}
	month, err := strconv.Atoi(s[5:7])
	if err != nil {
		return 0, err
	}
	day, err := strconv.Atoi(s[8:10])
	if err != nil {
		return 0, err
	}
	return civilToDays(year, month, day), nil
}

// civilToDays converts a Gregorian calendar date to days since Unix epoch.
// Algorithm: http://howardhinnant.github.io/date_algorithms.html days_from_civil
func civilToDays(y, m, d int) int32 {
	if m <= 2 {
		y--
		m += 9
	} else {
		m -= 3
	}
	era := y / 400
	if y < 0 {
		era = (y - 399) / 400
	}
	yoe := y - era*400
	doy := (153*m+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return int32(era*146097 + doe - 719468)
}

// parseTimestampToMillis parses an Athena timestamp string to milliseconds since Unix epoch.
// Athena timestamps use the format "YYYY-MM-DD HH:MM:SS[.fraction][ <tz>]" where fraction
// is a variable-length sequence of decimal digits; it is truncated to 3 digits (milliseconds).
// Timezone suffixes (e.g., " UTC", " America/New_York", "+00:00") are ignored — all
// timestamps are treated as UTC, consistent with Athena's behaviour for TIMESTAMP
// (which has no timezone) and TIMESTAMP WITH TIME ZONE (which Athena normalises to UTC).
func parseTimestampToMillis(s string) (int64, error) {
	if len(s) < 19 {
		return 0, fmt.Errorf("unexpected timestamp format: %q", s)
	}
	year, err := strconv.Atoi(s[0:4])
	if err != nil {
		return 0, err
	}
	month, err := strconv.Atoi(s[5:7])
	if err != nil {
		return 0, err
	}
	day, err := strconv.Atoi(s[8:10])
	if err != nil {
		return 0, err
	}
	hour, err := strconv.Atoi(s[11:13])
	if err != nil {
		return 0, err
	}
	min, err := strconv.Atoi(s[14:16])
	if err != nil {
		return 0, err
	}
	sec, err := strconv.Atoi(s[17:19])
	if err != nil {
		return 0, err
	}

	var fracMillis int64
	if len(s) > 19 && s[19] == '.' {
		// Fractional seconds start at position 20.
		frac := s[20:]
		// Truncate at any non-digit (e.g. space before timezone suffix).
		for i := 0; i < len(frac); i++ {
			if frac[i] < '0' || frac[i] > '9' {
				frac = frac[:i]
				break
			}
		}
		// Pad or truncate to exactly 3 digits (milliseconds).
		for len(frac) < 3 {
			frac += "0"
		}
		if len(frac) > 3 {
			frac = frac[:3]
		}
		fracMillis, err = strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, err
		}
	}
	// Any trailing timezone data after position 19 (or after the fractional part)
	// is intentionally ignored.

	days := civilToDays(year, month, day)
	totalMillis := int64(days)*86400*1_000 +
		int64(hour)*3600*1_000 +
		int64(min)*60*1_000 +
		int64(sec)*1_000 +
		fracMillis

	return totalMillis, nil
}
