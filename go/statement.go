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
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
// an array.RecordReader that streams results one Athena page at a time. Only
// one page is held in memory at a time, so peak memory is bounded by page size.
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
	if s.conn.db.outputLocation != "" {
		input.ResultConfiguration = &types.ResultConfiguration{
			OutputLocation: &s.conn.db.outputLocation,
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
			// Use a short timeout so a network stall doesn't block indefinitely.
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			_, _ = s.conn.athenaClient.StopQueryExecution(stopCtx, &athenaSDK.StopQueryExecutionInput{
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

// pagingRecordReader is a lazy array.RecordReader that fetches one Athena result
// page per Next() call, keeping only a single page in memory at a time.
type pagingRecordReader struct {
	// refCount is first to guarantee 64-bit alignment on 32-bit architectures.
	refCount    atomic.Int64
	alloc       memory.Allocator
	schema      *arrow.Schema
	paginator   *athenaSDK.GetQueryResultsPaginator
	ctx         context.Context
	pending     []types.Row // data rows from the first page (header already stripped)
	pendingDone bool        // true once pending has been offered as a batch
	current     arrow.RecordBatch
	err         error
}

func newPagingRecordReader(
	ctx context.Context,
	alloc memory.Allocator,
	schema *arrow.Schema,
	paginator *athenaSDK.GetQueryResultsPaginator,
	firstPageRows []types.Row,
) *pagingRecordReader {
	r := &pagingRecordReader{
		alloc:     alloc,
		schema:    schema,
		paginator: paginator,
		ctx:       ctx,
		pending:   firstPageRows,
	}
	r.refCount.Store(1)
	return r
}

func (r *pagingRecordReader) Retain() { r.refCount.Add(1) }

func (r *pagingRecordReader) Release() {
	if r.refCount.Add(-1) == 0 {
		r.releaseCurrent()
	}
}

// releaseCurrent releases the current batch and sets it to nil.
func (r *pagingRecordReader) releaseCurrent() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

func (r *pagingRecordReader) Schema() *arrow.Schema { return r.schema }

func (r *pagingRecordReader) Err() error { return r.err }

// RecordBatch returns the current batch (valid after a successful Next() call).
func (r *pagingRecordReader) RecordBatch() arrow.RecordBatch { return r.current }

// Record is the deprecated alias for RecordBatch, retained for interface compatibility.
func (r *pagingRecordReader) Record() arrow.RecordBatch { return r.RecordBatch() }

// Next advances to the next batch. The first call returns a batch built from
// the rows pre-fetched during schema discovery (first page). Subsequent calls
// each fetch exactly one additional Athena result page.
func (r *pagingRecordReader) Next() bool {
	// Release the previous batch before fetching the next.
	r.releaseCurrent()

	// On the first Next() call, offer the rows already fetched from page 1.
	if !r.pendingDone {
		r.pendingDone = true
		if len(r.pending) > 0 {
			batch, err := buildRecordBatch(r.alloc, r.schema, r.pending)
			r.pending = nil
			if err != nil {
				r.err = err
				return false
			}
			r.current = batch
			return true
		}
		r.pending = nil
		// First page had no data rows; fall through to subsequent pages.
	}

	// Fetch subsequent pages one at a time.
	for r.paginator.HasMorePages() {
		page, err := r.paginator.NextPage(r.ctx)
		if err != nil {
			r.err = adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("[athena] GetQueryResults failed: %v", err),
			}
			return false
		}
		if page.ResultSet == nil || len(page.ResultSet.Rows) == 0 {
			continue
		}
		batch, err := buildRecordBatch(r.alloc, r.schema, page.ResultSet.Rows)
		if err != nil {
			r.err = err
			return false
		}
		r.current = batch
		return true
	}
	return false
}

// buildPagedRecordReader fetches the first Athena result page to obtain the schema,
// then returns a pagingRecordReader that streams subsequent pages one at a time.
// Only one page is held in memory at any point, avoiding OOM for large result sets.
func (s *statementImpl) buildPagedRecordReader(ctx context.Context, execID *string) (array.RecordReader, error) {
	input := &athenaSDK.GetQueryResultsInput{
		QueryExecutionId: execID,
	}
	paginator := athenaSDK.NewGetQueryResultsPaginator(s.conn.athenaClient, input)

	// Fetch pages until we find one with ResultSetMetadata to determine the schema.
	var schema *arrow.Schema
	var firstPageRows []types.Row
	firstPage := true

	for schema == nil && paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("[athena] GetQueryResults failed: %v", err),
			}
		}

		if page.ResultSet != nil && page.ResultSet.ResultSetMetadata != nil && len(page.ResultSet.ResultSetMetadata.ColumnInfo) > 0 {
			schema = buildSchema(page.ResultSet.ResultSetMetadata.ColumnInfo)
		}

		if page.ResultSet != nil {
			rows := page.ResultSet.Rows
			if firstPage {
				// The first row of the first page is a header row — skip it.
				if len(rows) > 0 {
					rows = rows[1:]
				}
				firstPage = false
			}
			firstPageRows = append(firstPageRows, rows...)
		}
	}

	if schema == nil {
		// No pages returned — DDL or empty result set.
		schema = arrow.NewSchema(nil, nil)
	}

	return newPagingRecordReader(ctx, s.conn.Alloc, schema, paginator, firstPageRows), nil
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
