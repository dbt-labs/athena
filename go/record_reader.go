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
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
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
	t := *col.Type
	if t == "decimal" {
		return &arrow.Decimal128Type{Precision: col.Precision, Scale: col.Scale}
	}
	return athenaTypeStringToArrow(t)
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
	case "time", "time with time zone":
		return arrow.FixedWidthTypes.Time64us
	case "timestamp":
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "timestamp with time zone":
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	case "varbinary", "binary":
		return arrow.BinaryTypes.Binary
	case "interval day to second":
		return arrow.FixedWidthTypes.DayTimeInterval
	case "interval year to month":
		return arrow.FixedWidthTypes.MonthInterval
	case "hyperloglog", "p4hyperloglog", "setdigest", "qdigest", "tdigest":
		return arrow.BinaryTypes.Binary
	default:
		// array, map, row, json — stringify
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
		ns, err := parseTimestampToNanos(val)
		if err != nil {
			return err
		}
		bldr.(*array.TimestampBuilder).Append(arrow.Timestamp(ns))
	case arrow.BINARY:
		b, err := hex.DecodeString(strings.ReplaceAll(val, " ", ""))
		if err != nil {
			return err
		}
		bldr.(*array.BinaryBuilder).Append(b)
	case arrow.DECIMAL128:
		decType := dt.(*arrow.Decimal128Type)
		n, err := decimal128.FromString(val, decType.Precision, decType.Scale)
		if err != nil {
			return err
		}
		bldr.(*array.Decimal128Builder).Append(n)
	case arrow.TIME64:
		v, err := parseTimeToMicros(val)
		if err != nil {
			return err
		}
		bldr.(*array.Time64Builder).Append(v)
	case arrow.INTERVAL_DAY_TIME:
		v, err := parseDayTimeInterval(val)
		if err != nil {
			return err
		}
		bldr.(*array.DayTimeIntervalBuilder).Append(v)
	case arrow.INTERVAL_MONTHS:
		v, err := parseMonthInterval(val)
		if err != nil {
			return err
		}
		bldr.(*array.MonthIntervalBuilder).Append(v)
	default:
		// STRING covers varchar, string, char, decimal, array, map, row, json, etc.
		bldr.(*array.StringBuilder).Append(val)
	}
	return nil
}

// parseTimeToMicros parses Athena's time string into microseconds since midnight.
// Handles both "HH:MM:SS.ffffff" (plain time) and "HH:MM:SS.ffffff TZ" (time with timezone).
// The timezone may be separated by a space or attached directly (e.g. "12:30:45.123+05:30").
// For time with timezone, the value is normalized to UTC.
func parseTimeToMicros(val string) (arrow.Time64, error) {
	timePart, offsetMicros, err := splitTimeAndOffset(val)
	if err != nil {
		return 0, err
	}

	t, err := arrow.Time64FromString(timePart, arrow.Microsecond)
	if err != nil {
		return 0, err
	}

	return t - arrow.Time64(offsetMicros), nil
}

func splitTimeAndOffset(val string) (string, int64, error) {
	// Check for space-separated timezone: "12:30:45.123 UTC" or "12:30:45.123 +05:30"
	if spaceIdx := strings.IndexByte(val, ' '); spaceIdx > 0 {
		offset, err := parseTimezoneOffset(val[spaceIdx+1:])
		return val[:spaceIdx], offset, err
	}

	// Check for offset attached directly after fractional seconds: "12:30:45.123+05:30" or "12:30:45.123-03:00"
	// The offset is always ±HH:MM (6 chars) at the end.
	if len(val) > 6 {
		offsetStart := len(val) - 6
		if val[offsetStart] == '+' || val[offsetStart] == '-' {
			offset, err := parseTimezoneOffset(val[offsetStart:])
			return val[:offsetStart], offset, err
		}
	}

	return val, 0, nil
}

func parseTimezoneOffset(tzStr string) (int64, error) {
	if tzStr == "UTC" || tzStr == "Z" {
		return 0, nil
	}
	if len(tzStr) == 6 && (tzStr[0] == '+' || tzStr[0] == '-') {
		h, err := strconv.ParseInt(tzStr[1:3], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("parsing timezone hours: %w", err)
		}
		m, err := strconv.ParseInt(tzStr[4:6], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("parsing timezone minutes: %w", err)
		}
		offset := (h*3600 + m*60) * 1_000_000
		if tzStr[0] == '-' {
			offset = -offset
		}
		return offset, nil
	}
	return 0, nil
}

// parseDayTimeInterval parses Athena's "D HH:MM:SS.mmm" format into an Arrow DayTimeInterval.
func parseDayTimeInterval(s string) (arrow.DayTimeInterval, error) {
	spaceIdx := strings.IndexByte(s, ' ')
	if spaceIdx < 0 {
		return arrow.DayTimeInterval{}, fmt.Errorf("unexpected interval day to second format: %q", s)
	}

	days, err := strconv.ParseInt(s[:spaceIdx], 10, 32)
	if err != nil {
		return arrow.DayTimeInterval{}, fmt.Errorf("parsing days in interval: %w", err)
	}

	timePart := s[spaceIdx+1:]
	if len(timePart) < 8 {
		return arrow.DayTimeInterval{}, fmt.Errorf("unexpected time format in interval: %q", timePart)
	}

	hours, err := strconv.ParseInt(timePart[0:2], 10, 32)
	if err != nil {
		return arrow.DayTimeInterval{}, err
	}
	mins, err := strconv.ParseInt(timePart[3:5], 10, 32)
	if err != nil {
		return arrow.DayTimeInterval{}, err
	}
	secs, err := strconv.ParseInt(timePart[6:8], 10, 32)
	if err != nil {
		return arrow.DayTimeInterval{}, err
	}

	ms := hours*3600000 + mins*60000 + secs*1000
	if len(timePart) > 8 && timePart[8] == '.' {
		frac := timePart[9:]
		for len(frac) < 3 {
			frac += "0"
		}
		fracMs, err := strconv.ParseInt(frac[:3], 10, 32)
		if err != nil {
			return arrow.DayTimeInterval{}, err
		}
		ms += fracMs
	}

	return arrow.DayTimeInterval{Days: int32(days), Milliseconds: int32(ms)}, nil
}

// parseMonthInterval parses Athena's "Y-M" format into an Arrow MonthInterval (total months).
func parseMonthInterval(s string) (arrow.MonthInterval, error) {
	dashIdx := strings.IndexByte(s, '-')
	if dashIdx < 0 {
		return 0, fmt.Errorf("unexpected interval year to month format: %q", s)
	}

	years, err := strconv.ParseInt(s[:dashIdx], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parsing years in interval: %w", err)
	}
	months, err := strconv.ParseInt(s[dashIdx+1:], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parsing months in interval: %w", err)
	}

	return arrow.MonthInterval(years*12 + months), nil
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

// parseTimestampToNanos parses an Athena timestamp string to nanoseconds since Unix epoch.
// Athena timestamps use the format "YYYY-MM-DD HH:MM:SS[.fraction][ <tz>]" where fraction
// is a variable-length sequence of up to 9 decimal digits (nanoseconds).
// If a timezone suffix is present (e.g., " UTC", " America/New_York", " +03:45"), the
// timestamp is converted to UTC. Plain timestamps (no suffix) are assumed UTC.
func parseTimestampToNanos(s string) (int64, error) {
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

	var fracNanos int64
	rest := s[19:]
	if len(rest) > 0 && rest[0] == '.' {
		rest = rest[1:]
		end := 0
		for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
			end++
		}
		frac := rest[:end]
		rest = rest[end:]
		// Pad or truncate to exactly 9 digits (nanoseconds).
		for len(frac) < 9 {
			frac += "0"
		}
		if len(frac) > 9 {
			frac = frac[:9]
		}
		fracNanos, err = strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, err
		}
	}

	days := civilToDays(year, month, day)
	totalNanos := int64(days)*86400*1_000_000_000 +
		int64(hour)*3600*1_000_000_000 +
		int64(min)*60*1_000_000_000 +
		int64(sec)*1_000_000_000 +
		fracNanos

	// Parse timezone suffix if present.
	rest = strings.TrimSpace(rest)
	if len(rest) > 0 {
		offsetNanos, err := parseTZOffsetNanos(rest, year, month, day, hour, min, sec)
		if err != nil {
			return 0, err
		}
		totalNanos -= offsetNanos
	}

	return totalNanos, nil
}

// parseTZOffsetNanos parses a timezone suffix and returns its UTC offset in nanoseconds.
// The offset follows the sign convention of ISO 8601: +03:45 means local is 3h45m ahead
// of UTC, so subtracting the offset from local time yields UTC.
// Supports: "UTC", "+HH:MM", "-HH:MM", and IANA zone names (e.g., "America/New_York").
// For IANA names, the offset is resolved at the given local time (year, month, day, etc.)
// to account for daylight saving transitions.
func parseTZOffsetNanos(tz string, year, month, day, hour, min, sec int) (int64, error) {
	if tz == "UTC" || tz == "utc" {
		return 0, nil
	}
	if len(tz) >= 6 && (tz[0] == '+' || tz[0] == '-') {
		sign := int64(1)
		if tz[0] == '-' {
			sign = -1
		}
		parts := strings.SplitN(tz[1:], ":", 2)
		if len(parts) != 2 {
			return 0, fmt.Errorf("unexpected timezone offset format: %q", tz)
		}
		h, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, err
		}
		m, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, err
		}
		return sign * (int64(h)*3600 + int64(m)*60) * 1_000_000_000, nil
	}
	// IANA timezone name — resolve offset at the given local time.
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return 0, fmt.Errorf("unknown timezone: %q: %w", tz, err)
	}
	t := time.Date(year, time.Month(month), day, hour, min, sec, 0, loc)
	_, offsetSec := t.Zone()
	return int64(offsetSec) * 1_000_000_000, nil
}
