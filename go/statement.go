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
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type statementImpl struct {
	driverbase.StatementImplBase

	conn  *connectionImpl
	query string
}

func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

func (s *statementImpl) Close() error {
	if s.conn == nil {
		return adbc.Error{
			Msg:  "[athena] statement already closed",
			Code: adbc.StatusInvalidState,
		}
	}
	s.conn = nil
	return nil
}

func (s *statementImpl) SetOption(key, val string) error {
	return s.StatementImplBase.SetOption(key, val)
}

func (s *statementImpl) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

func (s *statementImpl) Prepare(_ context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[athena] Athena does not support prepared statements",
	}
}

func (s *statementImpl) ExecuteSchema(_ context.Context) (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[athena] ExecuteSchema not implemented",
	}
}

// ExecuteQuery runs the SQL query on Athena, waits for completion, and returns
// an array.RecordReader over the results. Results are accumulated as one Arrow
// record batch per Athena result page to limit peak memory usage.
func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.conn == nil {
		return nil, -1, adbc.Error{
			Msg:  "[athena] statement already closed",
			Code: adbc.StatusInvalidState,
		}
	}
	if s.query == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[athena] no query set",
		}
	}

	execID, err := s.startQuery(ctx)
	if err != nil {
		return nil, -1, err
	}

	if err := s.waitForQuery(ctx, execID); err != nil {
		return nil, -1, err
	}

	rdr, err := s.buildPagedRecordReader(ctx, execID)
	if err != nil {
		return nil, -1, err
	}

	return rdr, -1, nil
}

// ExecuteUpdate runs a DML statement on Athena and returns -1 (rows affected unknown).
func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if s.conn == nil {
		return -1, adbc.Error{
			Msg:  "[athena] statement already closed",
			Code: adbc.StatusInvalidState,
		}
	}
	if s.query == "" {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[athena] no query set",
		}
	}

	execID, err := s.startQuery(ctx)
	if err != nil {
		return -1, err
	}

	if err := s.waitForQuery(ctx, execID); err != nil {
		return -1, err
	}

	return -1, nil
}

func (s *statementImpl) startQuery(ctx context.Context) (*string, error) {
	input := &athenaSDK.StartQueryExecutionInput{
		QueryString: &s.query,
		QueryExecutionContext: &types.QueryExecutionContext{
			Catalog:  nilIfEmpty(s.conn.catalog),
			Database: nilIfEmpty(s.conn.schema),
		},
	}
	if s.conn.db.s3StagingDir != "" {
		input.ResultConfiguration = &types.ResultConfiguration{
			OutputLocation: &s.conn.db.s3StagingDir,
		}
	}
	if s.conn.db.workGroup != "" {
		input.WorkGroup = &s.conn.db.workGroup
	}

	out, err := s.conn.athenaClient.StartQueryExecution(ctx, input)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("[athena] StartQueryExecution failed: %v", err),
		}
	}
	return out.QueryExecutionId, nil
}

func (s *statementImpl) waitForQuery(ctx context.Context, execID *string) error {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Best-effort: stop the running Athena query to avoid unnecessary cost.
			_, _ = s.conn.athenaClient.StopQueryExecution(context.Background(), &athenaSDK.StopQueryExecutionInput{
				QueryExecutionId: execID,
			})
			return adbc.Error{
				Code: adbc.StatusCancelled,
				Msg:  ctx.Err().Error(),
			}
		case <-timer.C:
		}

		out, err := s.conn.athenaClient.GetQueryExecution(ctx, &athenaSDK.GetQueryExecutionInput{
			QueryExecutionId: execID,
		})
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("[athena] GetQueryExecution failed: %v", err),
			}
		}

		state := out.QueryExecution.Status.State
		switch state {
		case types.QueryExecutionStateSucceeded:
			return nil
		case types.QueryExecutionStateFailed:
			reason := ""
			if out.QueryExecution.Status.StateChangeReason != nil {
				reason = *out.QueryExecution.Status.StateChangeReason
			}
			return adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("[athena] query failed: %s", reason),
			}
		case types.QueryExecutionStateCancelled:
			return adbc.Error{
				Code: adbc.StatusCancelled,
				Msg:  "[athena] query was cancelled",
			}
		default:
			// QUEUED or RUNNING — reset timer and poll again.
			timer.Reset(500 * time.Millisecond)
		}
	}
}

// buildPagedRecordReader fetches Athena results page-by-page and builds one
// Arrow record batch per page. All batches are accumulated before the
// RecordReader is returned; the caller receives an array.RecordReader that
// iterates over them one at a time. Memory usage scales with total result size
// since all pages must be fetched before returning.
func (s *statementImpl) buildPagedRecordReader(ctx context.Context, execID *string) (array.RecordReader, error) {
	input := &athenaSDK.GetQueryResultsInput{
		QueryExecutionId: execID,
	}

	var schema *arrow.Schema
	var colInfo []types.ColumnInfo
	var batches []arrow.Record
	firstPage := true

	paginator := athenaSDK.NewGetQueryResultsPaginator(s.conn.athenaClient, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			for _, b := range batches {
				b.Release()
			}
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("[athena] GetQueryResults failed: %v", err),
			}
		}

		if page.ResultSet == nil {
			continue
		}

		rows := page.ResultSet.Rows
		if firstPage {
			if page.ResultSet.ResultSetMetadata != nil {
				colInfo = page.ResultSet.ResultSetMetadata.ColumnInfo
			}
			// The first row of the first page is a header row — skip it.
			if len(rows) > 0 {
				rows = rows[1:]
			}
			schema = buildSchema(colInfo)
			firstPage = false
		}

		if len(rows) == 0 {
			continue
		}

		batch, err := buildRecordBatch(s.conn.Alloc, schema, colInfo, rows)
		if err != nil {
			for _, b := range batches {
				b.Release()
			}
			return nil, err
		}
		batches = append(batches, batch)
	}

	if schema == nil {
		// Query returned no metadata (e.g. DDL with no result set).
		schema = arrow.NewSchema(nil, nil)
	}

	// Note: do NOT release batches here; array.NewRecordReader retains them and
	// the caller is responsible for calling Release() on the returned RecordReader.
	return array.NewRecordReader(schema, batches)
}

func (s *statementImpl) Bind(_ context.Context, _ arrow.Record) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[athena] Bind not implemented",
	}
}

func (s *statementImpl) BindStream(_ context.Context, _ array.RecordReader) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[athena] BindStream not implemented",
	}
}

func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[athena] parameter schema detection not implemented",
	}
}

func (s *statementImpl) SetSubstraitPlan(_ []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[athena] Substrait plans not supported",
	}
}

func (s *statementImpl) ExecutePartitions(_ context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[athena] partitioned result sets not supported",
	}
}

// nilIfEmpty returns nil if s is empty, otherwise a pointer to s.
func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
