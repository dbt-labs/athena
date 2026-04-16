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

//go:build driverlib

package main

// ADBC_EXPORTING is required on Windows, or else the symbols
// won't be accessible to the driver manager

// #cgo CFLAGS: -DADBC_EXPORTING
// #cgo CXXFLAGS: -std=c++17 -DADBC_EXPORTING
// #include "adbc.h"
// #include "utils.h"
// #include <errno.h>
// #include <stdint.h>
// #include <string.h>
//
// typedef const char cchar_t;
// typedef const uint8_t cuint8_t;
// typedef const uint32_t cuint32_t;
// typedef const struct AdbcError ConstAdbcError;
//
// int AthenaArrayStreamGetSchema(struct ArrowArrayStream*, struct ArrowSchema*);
// int AthenaArrayStreamGetNext(struct ArrowArrayStream*, struct ArrowArray*);
// const char* AthenaArrayStreamGetLastError(struct ArrowArrayStream*);
// void AthenaArrayStreamRelease(struct ArrowArrayStream*);
//
// int AthenaArrayStreamGetSchemaTrampoline(struct ArrowArrayStream*, struct ArrowSchema*);
// int AthenaArrayStreamGetNextTrampoline(struct ArrowArrayStream*, struct ArrowArray*);
//
// void releasePartitions(struct AdbcPartitions* partitions);
//
import "C"
import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/cgo"
	"strings"
	"sync/atomic"
	"unsafe"

	athena "github.com/dbt-labs/athena/go"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

// Must use malloc() to respect CGO rules
var drv = athena.NewDriver(mallocator.NewMallocator())

// Flag set if any method panic()ed - afterwards all calls to driver will fail
// since internal state of driver is unknown
var globalPoison atomic.Bool

const errPrefix = "[Athena] "

func setErr(err *C.struct_AdbcError, format string, vals ...interface{}) {
	if err == nil {
		return
	}

	if err.release != nil {
		C.AthenaerrRelease(err)
	}

	var msg string
	if strings.HasPrefix(format, errPrefix) {
		// If the error message already starts with the prefix, we don't
		// want to add it again.
		msg = fmt.Sprintf(format, vals...)
	} else {
		// Otherwise, we prepend the prefix to the error message.
		msg = errPrefix + fmt.Sprintf(format, vals...)
	}
	err.message = C.CString(msg)
	err.release = (*[0]byte)(C.Athena_release_error)
}

func setErrWithDetails(err *C.struct_AdbcError, adbcError adbc.Error) {
	if err == nil {
		return
	}

	if err.release != nil {
		C.AthenaerrRelease(err)
	}

	for i := range 5 {
		err.sqlstate[i] = C.char(adbcError.SqlState[i])
	}

	if err.vendor_code != C.ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
		// Caller is not interested in `private_data` if `vendor_code` is not
		// `ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA` so let's use the field for
		// `vendor_code` instead, but make sure it's not set to the value
		// that would indicate `private_data` is set.
		if adbcError.VendorCode != C.ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
			err.vendor_code = C.int(adbcError.VendorCode)
		}
		setErr(err, "%s", adbcError.Msg)
		return
	}

	numDetails := len(adbcError.Details)
	// If there are no details, but we have a `VendorCode`, let's override
	// `vendor_code` and not populate `private_data` with the error details.
	if numDetails == 0 && adbcError.VendorCode != 0 && adbcError.VendorCode != C.ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA {
		err.vendor_code = C.int(adbcError.VendorCode)
		setErr(err, "%s", adbcError.Msg)
		return
	}

	cErrPtr := C.calloc(C.sizeof_struct_AthenaError, C.size_t(1))
	cErr := (*C.struct_AthenaError)(cErrPtr)
	cErr.message = C.CString(adbcError.Msg)
	err.message = cErr.message
	err.release = (*[0]byte)(C.AthenaReleaseErrWithDetails)
	err.private_data = cErrPtr

	if numDetails > 0 {
		cErr.keys = (**C.cchar_t)(C.calloc(C.size_t(numDetails), C.size_t(unsafe.Sizeof((*C.cchar_t)(nil)))))
		cErr.values = (**C.cuint8_t)(C.calloc(C.size_t(numDetails), C.size_t(unsafe.Sizeof((*C.cuint8_t)(nil)))))
		cErr.lengths = (*C.size_t)(C.calloc(C.size_t(numDetails), C.sizeof_size_t))

		keys := fromCArr[*C.cchar_t](cErr.keys, numDetails)
		values := fromCArr[*C.cuint8_t](cErr.values, numDetails)
		lengths := fromCArr[C.size_t](cErr.lengths, numDetails)

		for i, detail := range adbcError.Details {
			keys[i] = C.CString(detail.Key())
			bytes, err := detail.Serialize()
			if err != nil {
				msg := err.Error()
				values[i] = (*C.cuint8_t)(unsafe.Pointer(C.CString(msg)))
				lengths[i] = C.size_t(len(msg))
			} else {
				values[i] = (*C.cuint8_t)(C.malloc(C.size_t(len(bytes))))
				sink := fromCArr[byte]((*byte)(values[i]), len(bytes))
				copy(sink, bytes)
				lengths[i] = C.size_t(len(bytes))
			}
		}
	} else {
		cErr.keys = nil
		cErr.values = nil
		cErr.lengths = nil
	}
	cErr.count = C.int(numDetails)
}

func errToAdbcErr(adbcerr *C.struct_AdbcError, err error) adbc.Status {
	if err == nil {
		return adbc.StatusOK
	}

	var adbcError adbc.Error
	if errors.As(err, &adbcError) {
		setErrWithDetails(adbcerr, adbcError)
		return adbcError.Code
	}

	setErr(adbcerr, "%s", err.Error())
	return adbc.StatusUnknown
}

// We panicked; make all API functions error and dump stack traces
func poison(err *C.struct_AdbcError, fname string, e interface{}) C.AdbcStatusCode {
	if !globalPoison.Swap(true) {
		// Only print stack traces on the first occurrence
		buf := make([]byte, 1<<20)
		length := runtime.Stack(buf, true)
		fmt.Fprintf(os.Stderr, "Athena driver panicked, stack traces:\n%s", buf[:length])
	}
	setErr(err, "%s: Go panic in Athena driver (see stderr): %#v", fname, e)
	return C.ADBC_STATUS_INTERNAL
}

func checkDBAlloc(db *C.struct_AdbcDatabase, err *C.struct_AdbcError, fname string) bool {
	if globalPoison.Load() {
		setErr(err, "%s: Go panicked, driver is in unknown state", fname)
		return false
	}
	if db == nil {
		setErr(err, "%s: database not allocated", fname)
		return false
	}
	if db.private_data == nil {
		setErr(err, "%s: database not allocated", fname)
		return false
	}
	return true
}

func checkDBInit(db *C.struct_AdbcDatabase, err *C.struct_AdbcError, fname string) *cDatabase {
	if !checkDBAlloc(db, err, fname) {
		return nil
	}
	cdb := getFromHandle[cDatabase](db.private_data)
	if cdb.db == nil {
		setErr(err, "%s: database not initialized", fname)
		return nil
	}

	return cdb
}

// Custom ArrowArrayStream export to support ADBC error data in ArrowArrayStream

type cArrayStream struct {
	rdr array.RecordReader
	// Must be C-allocated
	adbcErr *C.struct_AdbcError
	status  C.AdbcStatusCode
}

func (cStream *cArrayStream) maybeError() C.int {
	err := cStream.rdr.Err()
	if err != nil {
		if cStream.adbcErr != nil {
			C.AthenaerrRelease(cStream.adbcErr)
		} else {
			cStream.adbcErr = (*C.struct_AdbcError)(C.calloc(1, C.ADBC_ERROR_1_1_0_SIZE))
		}
		cStream.adbcErr.vendor_code = C.ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA
		cStream.status = C.AdbcStatusCode(errToAdbcErr(cStream.adbcErr, err))
		switch adbc.Status(cStream.status) {
		case adbc.StatusUnknown:
			return C.EIO
		case adbc.StatusNotImplemented:
			return C.ENOTSUP
		case adbc.StatusNotFound:
			return C.ENOENT
		case adbc.StatusAlreadyExists:
			return C.EEXIST
		case adbc.StatusInvalidArgument:
			return C.EINVAL
		case adbc.StatusInvalidState:
			return C.EINVAL
		case adbc.StatusInvalidData:
			return C.EIO
		case adbc.StatusIntegrity:
			return C.EIO
		case adbc.StatusInternal:
			return C.EIO
		case adbc.StatusIO:
			return C.EIO
		case adbc.StatusCancelled:
			return C.ECANCELED
		case adbc.StatusTimeout:
			return C.ETIMEDOUT
		case adbc.StatusUnauthenticated:
			return C.EACCES
		case adbc.StatusUnauthorized:
			return C.EACCES
		default:
			return C.EIO
		}
	}
	return 0
}

//export AthenaArrayStreamGetLastError
func AthenaArrayStreamGetLastError(stream *C.struct_ArrowArrayStream) *C.cchar_t {
	if stream == nil || stream.release != (*[0]byte)(C.AthenaArrayStreamRelease) || stream.private_data == nil {
		return nil
	}
	cStream := getFromHandle[cArrayStream](stream.private_data)
	if cStream.adbcErr != nil {
		return cStream.adbcErr.message
	}
	return nil
}

//export AthenaArrayStreamGetNext
func AthenaArrayStreamGetNext(stream *C.struct_ArrowArrayStream, array *C.struct_ArrowArray) C.int {
	if stream == nil || stream.release != (*[0]byte)(C.AthenaArrayStreamRelease) || stream.private_data == nil {
		return C.EINVAL
	}
	if array == nil {
		return C.EINVAL
	}
	cStream := getFromHandle[cArrayStream](stream.private_data)
	if cStream.rdr.Next() {
		cdata.ExportArrowRecordBatch(cStream.rdr.RecordBatch(), toCdataArray(array), nil)
		return 0
	}
	array.release = nil
	array.private_data = nil
	return cStream.maybeError()
}

//export AthenaArrayStreamGetSchema
func AthenaArrayStreamGetSchema(stream *C.struct_ArrowArrayStream, schema *C.struct_ArrowSchema) C.int {
	if stream == nil || stream.release != (*[0]byte)(C.AthenaArrayStreamRelease) || stream.private_data == nil {
		return C.EINVAL
	}
	if schema == nil {
		return C.EINVAL
	}
	cStream := getFromHandle[cArrayStream](stream.private_data)
	s := cStream.rdr.Schema()
	if s == nil {
		return cStream.maybeError()
	}
	cdata.ExportArrowSchema(s, toCdataSchema(schema))
	return 0
}

//export AthenaArrayStreamRelease
func AthenaArrayStreamRelease(stream *C.struct_ArrowArrayStream) {
	if stream == nil || stream.release != (*[0]byte)(C.AthenaArrayStreamRelease) || stream.private_data == nil {
		return
	}
	h := (*(*cgo.Handle)(stream.private_data))

	cStream := h.Value().(*cArrayStream)
	cStream.rdr.Release()
	if cStream.adbcErr != nil {
		C.AthenaerrRelease(cStream.adbcErr)
		C.free(unsafe.Pointer(cStream.adbcErr))
	}
	C.free(unsafe.Pointer(stream.private_data))
	stream.private_data = nil
	h.Delete()
}

//export AthenaErrorFromArrayStream
func AthenaErrorFromArrayStream(stream *C.struct_ArrowArrayStream, status *C.AdbcStatusCode) *C.struct_AdbcError {
	if stream == nil || stream.release != (*[0]byte)(C.AthenaArrayStreamRelease) || stream.private_data == nil {
		return nil
	}
	cStream := getFromHandle[cArrayStream](stream.private_data)
	if status != nil {
		*status = cStream.status
	}
	return cStream.adbcErr
}

func exportRecordReader(rdr array.RecordReader, stream *C.struct_ArrowArrayStream) {
	cStream := &cArrayStream{rdr: rdr, status: C.ADBC_STATUS_OK}
	stream.get_last_error = (*[0]byte)(C.AthenaArrayStreamGetLastError)
	stream.get_next = (*[0]byte)(C.AthenaArrayStreamGetNextTrampoline)
	stream.get_schema = (*[0]byte)(C.AthenaArrayStreamGetSchemaTrampoline)
	stream.release = (*[0]byte)(C.AthenaArrayStreamRelease)
	hndl := cgo.NewHandle(cStream)
	stream.private_data = createHandle(hndl)
	rdr.Retain()
}

type cDatabase struct {
	opts map[string]string
	db   adbc.Database
}

//export AthenaDatabaseGetOption
func AthenaDatabaseGetOption(db *C.struct_AdbcDatabase, key *C.cchar_t, value *C.char, length *C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseGetOption", e)
		}
	}()
	cdb := checkDBInit(db, err, "AdbcDatabaseGetOption")
	if cdb == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := cdb.db.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcDatabaseGetOption: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}
	val, e := opts.GetOption(C.GoString(key))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	return exportStringOption(val, value, length)
}

//export AthenaDatabaseGetOptionBytes
func AthenaDatabaseGetOptionBytes(db *C.struct_AdbcDatabase, key *C.cchar_t, value *C.uint8_t, length *C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseGetOptionBytes", e)
		}
	}()
	cdb := checkDBInit(db, err, "AdbcDatabaseGetOptionBytes")
	if cdb == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := cdb.db.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcDatabaseGetOptionBytes: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}
	val, e := opts.GetOptionBytes(C.GoString(key))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	return exportBytesOption(val, value, length)
}

//export AthenaDatabaseGetOptionDouble
func AthenaDatabaseGetOptionDouble(db *C.struct_AdbcDatabase, key *C.cchar_t, value *C.double, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseGetOptionDouble", e)
		}
	}()
	cdb := checkDBInit(db, err, "AdbcDatabaseGetOptionDouble")
	if cdb == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := cdb.db.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcDatabaseGetOptionDouble: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	val, e := opts.GetOptionDouble(C.GoString(key))
	*value = C.double(val)
	return C.AdbcStatusCode(errToAdbcErr(err, e))
}

//export AthenaDatabaseGetOptionInt
func AthenaDatabaseGetOptionInt(db *C.struct_AdbcDatabase, key *C.cchar_t, value *C.int64_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseGetOptionInt", e)
		}
	}()
	cdb := checkDBInit(db, err, "AdbcDatabaseGetOptionInt")
	if cdb == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := cdb.db.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcDatabaseGetOptionInt: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	val, e := opts.GetOptionInt(C.GoString(key))
	*value = C.int64_t(val)
	return C.AdbcStatusCode(errToAdbcErr(err, e))
}

//export AthenaDatabaseInit
func AthenaDatabaseInit(db *C.struct_AdbcDatabase, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseInit", e)
		}
	}()
	if !checkDBAlloc(db, err, "AdbcDatabaseInit") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	cdb := getFromHandle[cDatabase](db.private_data)

	if cdb.db != nil {
		setErr(err, "AdbcDatabaseInit: database already initialized")
		return C.ADBC_STATUS_INVALID_STATE
	}

	adb, aerr := drv.NewDatabase(cdb.opts)
	if aerr != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, aerr))
	}

	cdb.db = adb
	return C.ADBC_STATUS_OK
}

//export AthenaDatabaseNew
func AthenaDatabaseNew(db *C.struct_AdbcDatabase, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseNew", e)
		}
	}()
	if globalPoison.Load() {
		setErr(err, "AdbcDatabaseNew: Go panicked, driver is in unknown state")
		return C.ADBC_STATUS_INTERNAL
	}
	if db.private_data != nil {
		setErr(err, "AdbcDatabaseNew: database already allocated")
		return C.ADBC_STATUS_INVALID_STATE
	}
	dbobj := &cDatabase{opts: make(map[string]string)}
	hndl := cgo.NewHandle(dbobj)
	db.private_data = createHandle(hndl)
	return C.ADBC_STATUS_OK
}

//export AthenaDatabaseRelease
func AthenaDatabaseRelease(db *C.struct_AdbcDatabase, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseRelease", e)
		}
	}()
	if !checkDBAlloc(db, err, "AdbcDatabaseRelease") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	h := (*(*cgo.Handle)(db.private_data))

	cdb := h.Value().(*cDatabase)
	if cdb.db != nil {
		cdb.db.Close()
		cdb.db = nil
	}
	cdb.opts = nil
	if db.private_data != nil {
		C.free(unsafe.Pointer(db.private_data))
		db.private_data = nil
	}
	h.Delete()
	// manually trigger GC for two reasons:
	//  1. ASAN expects the release callback to be called before
	//     the process ends, but GC is not deterministic. So by manually
	//     triggering the GC we ensure the release callback gets called.
	//  2. Creates deterministic GC behavior by all Release functions
	//     triggering a garbage collection
	runtime.GC()
	return C.ADBC_STATUS_OK
}

//export AthenaDatabaseSetOption
func AthenaDatabaseSetOption(db *C.struct_AdbcDatabase, key, value *C.cchar_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseSetOption", e)
		}
	}()
	if !checkDBAlloc(db, err, "AdbcDatabaseSetOption") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	cdb := getFromHandle[cDatabase](db.private_data)

	k, v := C.GoString(key), C.GoString(value)
	if cdb.db != nil {
		opts, ok := cdb.db.(adbc.PostInitOptions)
		if !ok {
			setErr(err, "AdbcDatabaseSetOption: options are not supported")
			return C.ADBC_STATUS_NOT_IMPLEMENTED
		}
		return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOption(k, v)))
	} else {
		cdb.opts[k] = v
	}

	return C.ADBC_STATUS_OK
}

//export AthenaDatabaseSetOptionBytes
func AthenaDatabaseSetOptionBytes(db *C.struct_AdbcDatabase, key *C.cchar_t, value *C.cuint8_t, length C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseSetOptionBytes", e)
		}
	}()
	cdb := checkDBInit(db, err, "AdbcDatabaseSetOptionBytes")
	if cdb == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := cdb.db.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcDatabaseSetOptionBytes: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionBytes(C.GoString(key), fromCArr[byte](value, int(length)))))
}

//export AthenaDatabaseSetOptionDouble
func AthenaDatabaseSetOptionDouble(db *C.struct_AdbcDatabase, key *C.cchar_t, value C.double, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseSetOptionDouble", e)
		}
	}()
	cdb := checkDBInit(db, err, "AdbcDatabaseSetOptionDouble")
	if cdb == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := cdb.db.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcDatabaseSetOptionDouble: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionDouble(C.GoString(key), float64(value))))
}

//export AthenaDatabaseSetOptionInt
func AthenaDatabaseSetOptionInt(db *C.struct_AdbcDatabase, key *C.cchar_t, value C.int64_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcDatabaseSetOptionInt", e)
		}
	}()
	cdb := checkDBInit(db, err, "AdbcDatabaseSetOptionInt")
	if cdb == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := cdb.db.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcDatabaseSetOptionInt: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionInt(C.GoString(key), int64(value))))
}

type cConn struct {
	cancellableContext

	cnxn     adbc.Connection
	initArgs map[string]string
}

func checkConnAlloc(cnxn *C.struct_AdbcConnection, err *C.struct_AdbcError, fname string) bool {
	if globalPoison.Load() {
		setErr(err, "%s: Go panicked, driver is in unknown state", fname)
		return false
	}
	if cnxn == nil {
		setErr(err, "%s: connection not allocated", fname)
		return false
	}
	if cnxn.private_data == nil {
		setErr(err, "%s: connection not allocated", fname)
		return false
	}
	return true
}

func checkConnInit(cnxn *C.struct_AdbcConnection, err *C.struct_AdbcError, fname string) *cConn {
	if !checkConnAlloc(cnxn, err, fname) {
		return nil
	}
	conn := getFromHandle[cConn](cnxn.private_data)
	if conn.cnxn == nil {
		setErr(err, "%s: connection not initialized", fname)
		return nil
	}

	return conn
}

//export AthenaConnectionGetOption
func AthenaConnectionGetOption(db *C.struct_AdbcConnection, key *C.cchar_t, value *C.char, length *C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetOption", e)
		}
	}()
	conn := checkConnInit(db, err, "AdbcConnectionGetOption")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := conn.cnxn.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcConnectionGetOption: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}
	val, e := opts.GetOption(C.GoString(key))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	return exportStringOption(val, value, length)
}

//export AthenaConnectionGetOptionBytes
func AthenaConnectionGetOptionBytes(db *C.struct_AdbcConnection, key *C.cchar_t, value *C.uint8_t, length *C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetOptionBytes", e)
		}
	}()
	conn := checkConnInit(db, err, "AdbcConnectionGetOptionBytes")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := conn.cnxn.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcConnectionGetOptionBytes: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}
	val, e := opts.GetOptionBytes(C.GoString(key))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	return exportBytesOption(val, value, length)
}

//export AthenaConnectionGetOptionDouble
func AthenaConnectionGetOptionDouble(db *C.struct_AdbcConnection, key *C.cchar_t, value *C.double, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetOptionDouble", e)
		}
	}()
	conn := checkConnInit(db, err, "AdbcConnectionGetOptionDouble")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := conn.cnxn.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcConnectionGetOptionDouble: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	val, e := opts.GetOptionDouble(C.GoString(key))
	*value = C.double(val)
	return C.AdbcStatusCode(errToAdbcErr(err, e))
}

//export AthenaConnectionGetOptionInt
func AthenaConnectionGetOptionInt(db *C.struct_AdbcConnection, key *C.cchar_t, value *C.int64_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetOptionInt", e)
		}
	}()
	conn := checkConnInit(db, err, "AdbcConnectionGetOptionInt")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := conn.cnxn.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcConnectionGetOptionInt: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	val, e := opts.GetOptionInt(C.GoString(key))
	*value = C.int64_t(val)
	return C.AdbcStatusCode(errToAdbcErr(err, e))
}

//export AthenaConnectionNew
func AthenaConnectionNew(cnxn *C.struct_AdbcConnection, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionNew", e)
		}
	}()
	if cnxn == nil {
		setErr(err, "AdbcConnectionNew: connection is nil")
		return C.ADBC_STATUS_INVALID_ARGUMENT
	}
	if globalPoison.Load() {
		setErr(err, "AdbcConnectionNew: Go panicked, driver is in unknown state")
		return C.ADBC_STATUS_INTERNAL
	}
	if cnxn.private_data != nil {
		setErr(err, "AdbcConnectionNew: connection already allocated")
		return C.ADBC_STATUS_INVALID_STATE
	}

	hndl := cgo.NewHandle(&cConn{})
	cnxn.private_data = createHandle(hndl)
	return C.ADBC_STATUS_OK
}

//export AthenaConnectionSetOption
func AthenaConnectionSetOption(cnxn *C.struct_AdbcConnection, key, val *C.cchar_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionSetOption", e)
		}
	}()
	if !checkConnAlloc(cnxn, err, "AdbcConnectionSetOption") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	conn := getFromHandle[cConn](cnxn.private_data)

	if conn.cnxn == nil {
		// not yet initialized
		k, v := C.GoString(key), C.GoString(val)
		if conn.initArgs == nil {
			conn.initArgs = map[string]string{}
		}
		conn.initArgs[k] = v
		return C.ADBC_STATUS_OK
	}

	opts, ok := conn.cnxn.(adbc.PostInitOptions)
	if !ok {
		setErr(err, "AdbcConnectionSetOption: not supported post-init")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}
	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOption(C.GoString(key), C.GoString(val))))
}

//export AthenaConnectionSetOptionBytes
func AthenaConnectionSetOptionBytes(db *C.struct_AdbcConnection, key *C.cchar_t, value *C.cuint8_t, length C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionSetOptionBytes", e)
		}
	}()
	conn := checkConnInit(db, err, "AdbcConnectionSetOptionBytes")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := conn.cnxn.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcConnectionSetOptionBytes: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionBytes(C.GoString(key), fromCArr[byte](value, int(length)))))
}

//export AthenaConnectionSetOptionDouble
func AthenaConnectionSetOptionDouble(db *C.struct_AdbcConnection, key *C.cchar_t, value C.double, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionSetOptionDouble", e)
		}
	}()
	if !checkConnAlloc(db, err, "AdbcConnectionSetOptionDouble") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	conn := getFromHandle[cConn](db.private_data)

	if conn.cnxn == nil {
		k := C.GoString(key)
		if conn.initArgs == nil {
			conn.initArgs = map[string]string{}
		}
		conn.initArgs[k] = fmt.Sprintf("%g", float64(value))
		return C.ADBC_STATUS_OK
	}

	opts, ok := conn.cnxn.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcConnectionSetOptionDouble: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionDouble(C.GoString(key), float64(value))))
}

//export AthenaConnectionSetOptionInt
func AthenaConnectionSetOptionInt(db *C.struct_AdbcConnection, key *C.cchar_t, value C.int64_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionSetOptionInt", e)
		}
	}()
	if !checkConnAlloc(db, err, "AdbcConnectionSetOptionInt") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	conn := getFromHandle[cConn](db.private_data)

	if conn.cnxn == nil {
		k := C.GoString(key)
		if conn.initArgs == nil {
			conn.initArgs = map[string]string{}
		}
		conn.initArgs[k] = fmt.Sprintf("%d", int64(value))
		return C.ADBC_STATUS_OK
	}

	opts, ok := conn.cnxn.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcConnectionSetOptionInt: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionInt(C.GoString(key), int64(value))))
}

//export AthenaConnectionInit
func AthenaConnectionInit(cnxn *C.struct_AdbcConnection, db *C.struct_AdbcDatabase, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionInit", e)
		}
	}()
	if !checkConnAlloc(cnxn, err, "AdbcConnectionInit") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	conn := getFromHandle[cConn](cnxn.private_data)

	if conn.cnxn != nil {
		setErr(err, "AdbcConnectionInit: connection already initialized")
		return C.ADBC_STATUS_INVALID_STATE
	}
	cdb := checkDBInit(db, err, "AdbcConnectionInit")
	if cdb == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}
	c, e := cdb.db.Open(context.Background())
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	conn.cnxn = c

	if len(conn.initArgs) > 0 {
		// C allow SetOption before Init, Go doesn't allow options to Open so set them now
		opts, ok := conn.cnxn.(adbc.PostInitOptions)
		if !ok {
			setErr(err, "AdbcConnectionInit: options are not supported")
			return C.ADBC_STATUS_NOT_IMPLEMENTED
		}

		for k, v := range conn.initArgs {
			rawCode := errToAdbcErr(err, opts.SetOption(k, v))
			if rawCode != adbc.StatusOK {
				return C.AdbcStatusCode(rawCode)
			}
		}
		conn.initArgs = nil
	}

	return C.ADBC_STATUS_OK
}

//export AthenaConnectionRelease
func AthenaConnectionRelease(cnxn *C.struct_AdbcConnection, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionRelease", e)
		}
	}()
	if !checkConnAlloc(cnxn, err, "AdbcConnectionRelease") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	h := (*(*cgo.Handle)(cnxn.private_data))

	conn := h.Value().(*cConn)
	defer func() {
		conn.cancelContext()
		conn.cnxn = nil
		C.free(cnxn.private_data)
		cnxn.private_data = nil
		h.Delete()
		// manually trigger GC for two reasons:
		//  1. ASAN expects the release callback to be called before
		//     the process ends, but GC is not deterministic. So by manually
		//     triggering the GC we ensure the release callback gets called.
		//  2. Creates deterministic GC behavior by all Release functions
		//     triggering a garbage collection
		runtime.GC()
	}()
	if conn.cnxn == nil {
		return C.ADBC_STATUS_OK
	}
	return C.AdbcStatusCode(errToAdbcErr(err, conn.cnxn.Close()))
}

func fromCArr[T, CType any](ptr *CType, sz int) []T {
	if ptr == nil || sz == 0 {
		return nil
	}

	return unsafe.Slice((*T)(unsafe.Pointer(ptr)), sz)
}

func toCdataStream(ptr *C.struct_ArrowArrayStream) *cdata.CArrowArrayStream {
	return (*cdata.CArrowArrayStream)(unsafe.Pointer(ptr))
}

func toCdataSchema(ptr *C.struct_ArrowSchema) *cdata.CArrowSchema {
	return (*cdata.CArrowSchema)(unsafe.Pointer(ptr))
}

func toCdataArray(ptr *C.struct_ArrowArray) *cdata.CArrowArray {
	return (*cdata.CArrowArray)(unsafe.Pointer(ptr))
}

//export AthenaConnectionCancel
func AthenaConnectionCancel(cnxn *C.struct_AdbcConnection, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionCancel", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionCancel")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	conn.cancelContext()
	return C.ADBC_STATUS_OK
}

func toStrPtr(in *C.cchar_t) *string {
	if in == nil {
		return nil
	}

	out := C.GoString((*C.char)(in))
	return &out
}

func toStrSlice(in **C.cchar_t) []string {
	if in == nil {
		return nil
	}

	sz := unsafe.Sizeof(*in)

	out := make([]string, 0, 1)
	for *in != nil {
		out = append(out, C.GoString(*in))
		in = (**C.cchar_t)(unsafe.Add(unsafe.Pointer(in), sz))
	}
	return out
}

//export AthenaConnectionGetInfo
func AthenaConnectionGetInfo(cnxn *C.struct_AdbcConnection, codes *C.cuint32_t, len C.size_t, out *C.struct_ArrowArrayStream, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetInfo", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionGetInfo")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	infoCodes := fromCArr[adbc.InfoCode](codes, int(len))
	rdr, e := conn.cnxn.GetInfo(conn.newContext(), infoCodes)
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	defer rdr.Release()
	exportRecordReader(rdr, out)
	return C.ADBC_STATUS_OK
}

//export AthenaConnectionGetObjects
func AthenaConnectionGetObjects(cnxn *C.struct_AdbcConnection, depth C.int, catalog, dbSchema, tableName *C.cchar_t, tableType **C.cchar_t, columnName *C.cchar_t,
	out *C.struct_ArrowArrayStream, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetObjects", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionGetObjects")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	rdr, e := conn.cnxn.GetObjects(conn.newContext(), adbc.ObjectDepth(depth), toStrPtr(catalog), toStrPtr(dbSchema), toStrPtr(tableName), toStrPtr(columnName), toStrSlice(tableType))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	defer rdr.Release()
	exportRecordReader(rdr, out)
	return C.ADBC_STATUS_OK
}

//export AthenaConnectionGetStatistics
func AthenaConnectionGetStatistics(cnxn *C.struct_AdbcConnection, catalog, dbSchema, tableName *C.cchar_t, approximate C.char, out *C.struct_ArrowArrayStream, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetStatistics", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionGetStatistics")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	gs, ok := conn.cnxn.(adbc.ConnectionGetStatistics)
	if !ok {
		setErr(err, "AdbcConnectionGetStatistics: not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	rdr, e := gs.GetStatistics(conn.newContext(), toStrPtr(catalog), toStrPtr(dbSchema), toStrPtr(tableName), int(approximate) != 0)
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	defer rdr.Release()
	exportRecordReader(rdr, out)
	return C.ADBC_STATUS_OK
}

//export AthenaConnectionGetStatisticNames
func AthenaConnectionGetStatisticNames(cnxn *C.struct_AdbcConnection, out *C.struct_ArrowArrayStream, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetStatisticNames", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionGetStatisticNames")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	gs, ok := conn.cnxn.(adbc.ConnectionGetStatistics)
	if !ok {
		setErr(err, "AdbcConnectionGetStatisticNames: not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	rdr, e := gs.GetStatisticNames(conn.newContext())
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	defer rdr.Release()
	exportRecordReader(rdr, out)
	return C.ADBC_STATUS_OK
}

//export AthenaConnectionGetTableSchema
func AthenaConnectionGetTableSchema(cnxn *C.struct_AdbcConnection, catalog, dbSchema, tableName *C.cchar_t, schema *C.struct_ArrowSchema, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetTableSchema", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionGetTableSchema")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	sc, e := conn.cnxn.GetTableSchema(conn.newContext(), toStrPtr(catalog), toStrPtr(dbSchema), C.GoString(tableName))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	cdata.ExportArrowSchema(sc, toCdataSchema(schema))
	return C.ADBC_STATUS_OK
}

//export AthenaConnectionGetTableTypes
func AthenaConnectionGetTableTypes(cnxn *C.struct_AdbcConnection, out *C.struct_ArrowArrayStream, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionGetTableTypes", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionGetTableTypes")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	rdr, e := conn.cnxn.GetTableTypes(conn.newContext())
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	defer rdr.Release()
	exportRecordReader(rdr, out)
	return C.ADBC_STATUS_OK
}

//export AthenaConnectionReadPartition
func AthenaConnectionReadPartition(cnxn *C.struct_AdbcConnection, serialized *C.cuint8_t, serializedLen C.size_t, out *C.struct_ArrowArrayStream, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionReadPartition", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionReadPartition")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	rdr, e := conn.cnxn.ReadPartition(conn.newContext(), fromCArr[byte](serialized, int(serializedLen)))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	defer rdr.Release()
	exportRecordReader(rdr, out)
	return C.ADBC_STATUS_OK
}

//export AthenaConnectionCommit
func AthenaConnectionCommit(cnxn *C.struct_AdbcConnection, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionCommit", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionCommit")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	return C.AdbcStatusCode(errToAdbcErr(err, conn.cnxn.Commit(conn.newContext())))
}

//export AthenaConnectionRollback
func AthenaConnectionRollback(cnxn *C.struct_AdbcConnection, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcConnectionRollback", e)
		}
	}()
	conn := checkConnInit(cnxn, err, "AdbcConnectionRollback")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	return C.AdbcStatusCode(errToAdbcErr(err, conn.cnxn.Rollback(conn.newContext())))
}

type cStmt struct {
	cancellableContext

	stmt adbc.Statement
}

func checkStmtAlloc(stmt *C.struct_AdbcStatement, err *C.struct_AdbcError, fname string) bool {
	if globalPoison.Load() {
		setErr(err, "%s: Go panicked, driver is in unknown state", fname)
		return false
	}
	if stmt == nil {
		setErr(err, "%s: statement not allocated", fname)
		return false
	}
	if stmt.private_data == nil {
		setErr(err, "%s: statement not allocated", fname)
		return false
	}
	return true
}

func checkStmtInit(stmt *C.struct_AdbcStatement, err *C.struct_AdbcError, fname string) *cStmt {
	if !checkStmtAlloc(stmt, err, fname) {
		return nil
	}
	cStmt := getFromHandle[cStmt](stmt.private_data)
	if cStmt.stmt == nil {
		setErr(err, "%s: statement not initialized", fname)
		return nil
	}
	return cStmt
}

//export AthenaStatementGetOption
func AthenaStatementGetOption(db *C.struct_AdbcStatement, key *C.cchar_t, value *C.char, length *C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementGetOption", e)
		}
	}()
	st := checkStmtInit(db, err, "AdbcStatementGetOption")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := st.stmt.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcStatementGetOption: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}
	val, e := opts.GetOption(C.GoString(key))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	return exportStringOption(val, value, length)
}

//export AthenaStatementGetOptionBytes
func AthenaStatementGetOptionBytes(db *C.struct_AdbcStatement, key *C.cchar_t, value *C.uint8_t, length *C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementGetOptionBytes", e)
		}
	}()
	st := checkStmtInit(db, err, "AdbcStatementGetOptionBytes")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := st.stmt.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcStatementGetOptionBytes: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}
	val, e := opts.GetOptionBytes(C.GoString(key))
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	return exportBytesOption(val, value, length)
}

//export AthenaStatementGetOptionDouble
func AthenaStatementGetOptionDouble(db *C.struct_AdbcStatement, key *C.cchar_t, value *C.double, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementGetOptionDouble", e)
		}
	}()
	st := checkStmtInit(db, err, "AdbcStatementGetOptionDouble")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := st.stmt.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcStatementGetOptionDouble: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	val, e := opts.GetOptionDouble(C.GoString(key))
	*value = C.double(val)
	return C.AdbcStatusCode(errToAdbcErr(err, e))
}

//export AthenaStatementGetOptionInt
func AthenaStatementGetOptionInt(db *C.struct_AdbcStatement, key *C.cchar_t, value *C.int64_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementGetOptionInt", e)
		}
	}()
	st := checkStmtInit(db, err, "AdbcStatementGetOptionInt")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := st.stmt.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcStatementGetOptionInt: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	val, e := opts.GetOptionInt(C.GoString(key))
	*value = C.int64_t(val)
	return C.AdbcStatusCode(errToAdbcErr(err, e))
}

//export AthenaStatementNew
func AthenaStatementNew(cnxn *C.struct_AdbcConnection, stmt *C.struct_AdbcStatement, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementNew", e)
		}
	}()
	if stmt == nil {
		setErr(err, "AdbcStatementNew: statement is nil")
		return C.ADBC_STATUS_INVALID_ARGUMENT
	}
	if globalPoison.Load() {
		setErr(err, "AdbcStatementNew: Go panicked, driver is in unknown state")
		return C.ADBC_STATUS_INTERNAL
	}
	if stmt.private_data != nil {
		setErr(err, "AdbcStatementNew: statement already allocated")
		return C.ADBC_STATUS_INVALID_STATE
	}

	conn := checkConnInit(cnxn, err, "AdbcStatementNew")
	if conn == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	st, e := conn.cnxn.NewStatement()
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	hndl := cgo.NewHandle(&cStmt{stmt: st})
	stmt.private_data = createHandle(hndl)
	return C.ADBC_STATUS_OK
}

//export AthenaStatementRelease
func AthenaStatementRelease(stmt *C.struct_AdbcStatement, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementRelease", e)
		}
	}()
	if globalPoison.Load() {
		setErr(err, "AdbcStatementRelease: Go panicked, driver is in unknown state")
		return C.ADBC_STATUS_INTERNAL
	}
	if !checkStmtAlloc(stmt, err, "AdbcStatementRelease") {
		return C.ADBC_STATUS_INVALID_STATE
	}
	h := (*(*cgo.Handle)(stmt.private_data))

	st := h.Value().(*cStmt)
	defer func() {
		st.cancelContext()
		st.stmt = nil
		C.free(stmt.private_data)
		stmt.private_data = nil
		h.Delete()
		// manually trigger GC for two reasons:
		//  1. ASAN expects the release callback to be called before
		//     the process ends, but GC is not deterministic. So by manually
		//     triggering the GC we ensure the release callback gets called.
		//  2. Creates deterministic GC behavior by all Release functions
		//     triggering a garbage collection
		runtime.GC()
	}()
	if st.stmt == nil {
		return C.ADBC_STATUS_OK
	}
	return C.AdbcStatusCode(errToAdbcErr(err, st.stmt.Close()))
}

//export AthenaStatementCancel
func AthenaStatementCancel(stmt *C.struct_AdbcStatement, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementCancel", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementCancel")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	st.cancelContext()
	return C.ADBC_STATUS_OK
}

//export AthenaStatementPrepare
func AthenaStatementPrepare(stmt *C.struct_AdbcStatement, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementPrepare", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementPrepare")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	return C.AdbcStatusCode(errToAdbcErr(err, st.stmt.Prepare(st.newContext())))
}

//export AthenaStatementExecuteQuery
func AthenaStatementExecuteQuery(stmt *C.struct_AdbcStatement, out *C.struct_ArrowArrayStream, affected *C.int64_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementExecuteQuery", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementExecuteQuery")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	if out == nil {
		n, e := st.stmt.ExecuteUpdate(st.newContext())
		if e != nil {
			return C.AdbcStatusCode(errToAdbcErr(err, e))
		}

		if affected != nil {
			*affected = C.int64_t(n)
		}
	} else {
		rdr, n, e := st.stmt.ExecuteQuery(st.newContext())
		if e != nil {
			return C.AdbcStatusCode(errToAdbcErr(err, e))
		}

		if affected != nil {
			*affected = C.int64_t(n)
		}

		defer rdr.Release()
		exportRecordReader(rdr, out)
	}
	return C.ADBC_STATUS_OK
}

//export AthenaStatementExecuteSchema
func AthenaStatementExecuteSchema(stmt *C.struct_AdbcStatement, schema *C.struct_ArrowSchema, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementExecuteSchema", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementExecuteSchema")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	es, ok := st.stmt.(adbc.StatementExecuteSchema)
	if !ok {
		setErr(err, "AdbcStatementExecuteSchema: not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	sc, e := es.ExecuteSchema(st.newContext())
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	cdata.ExportArrowSchema(sc, toCdataSchema(schema))
	return C.ADBC_STATUS_OK
}

//export AthenaStatementSetSqlQuery
func AthenaStatementSetSqlQuery(stmt *C.struct_AdbcStatement, query *C.cchar_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementSetSqlQuery", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementSetSqlQuery")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	return C.AdbcStatusCode(errToAdbcErr(err, st.stmt.SetSqlQuery(C.GoString(query))))
}

//export AthenaStatementSetSubstraitPlan
func AthenaStatementSetSubstraitPlan(stmt *C.struct_AdbcStatement, plan *C.cuint8_t, length C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementSetSubstraitPlan", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementSetSubstraitPlan")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	return C.AdbcStatusCode(errToAdbcErr(err, st.stmt.SetSubstraitPlan(fromCArr[byte](plan, int(length)))))
}

//export AthenaStatementBind
func AthenaStatementBind(stmt *C.struct_AdbcStatement, values *C.struct_ArrowArray, schema *C.struct_ArrowSchema, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementBind", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementBind")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	rec, e := cdata.ImportCRecordBatch(toCdataArray(values), toCdataSchema(schema))
	if e != nil {
		// if there was an error, we need to manually release both inputs
		cdata.ReleaseCArrowArray(toCdataArray(values))
		cdata.ReleaseCArrowSchema(toCdataSchema(schema))
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	defer rec.Release()

	return C.AdbcStatusCode(errToAdbcErr(err, st.stmt.Bind(st.newContext(), rec)))
}

//export AthenaStatementBindStream
func AthenaStatementBindStream(stmt *C.struct_AdbcStatement, stream *C.struct_ArrowArrayStream, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementBindStream", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementBindStream")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	rdr, e := cdata.ImportCRecordReader(toCdataStream(stream), nil)
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}
	recRdr := rdr.(array.RecordReader)
	e = st.stmt.BindStream(st.newContext(), recRdr)
	if e != nil {
		recRdr.Release()
	}
	return C.AdbcStatusCode(errToAdbcErr(err, e))
}

//export AthenaStatementGetParameterSchema
func AthenaStatementGetParameterSchema(stmt *C.struct_AdbcStatement, schema *C.struct_ArrowSchema, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementGetParameterSchema", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementGetParameterSchema")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	sc, e := st.stmt.GetParameterSchema()
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	cdata.ExportArrowSchema(sc, toCdataSchema(schema))
	return C.ADBC_STATUS_OK
}

//export AthenaStatementSetOption
func AthenaStatementSetOption(stmt *C.struct_AdbcStatement, key, value *C.cchar_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementSetOption", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementSetOption")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	return C.AdbcStatusCode(errToAdbcErr(err, st.stmt.SetOption(C.GoString(key), C.GoString(value))))
}

//export AthenaStatementSetOptionBytes
func AthenaStatementSetOptionBytes(db *C.struct_AdbcStatement, key *C.cchar_t, value *C.cuint8_t, length C.size_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementSetOptionBytes", e)
		}
	}()
	st := checkStmtInit(db, err, "AdbcStatementSetOptionBytes")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := st.stmt.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcStatementSetOptionBytes: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionBytes(C.GoString(key), fromCArr[byte](value, int(length)))))
}

//export AthenaStatementSetOptionDouble
func AthenaStatementSetOptionDouble(db *C.struct_AdbcStatement, key *C.cchar_t, value C.double, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementSetOptionDouble", e)
		}
	}()
	st := checkStmtInit(db, err, "AdbcStatementSetOptionDouble")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := st.stmt.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcStatementSetOptionDouble: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionDouble(C.GoString(key), float64(value))))
}

//export AthenaStatementSetOptionInt
func AthenaStatementSetOptionInt(db *C.struct_AdbcStatement, key *C.cchar_t, value C.int64_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementSetOptionInt", e)
		}
	}()
	st := checkStmtInit(db, err, "AdbcStatementSetOptionInt")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	opts, ok := st.stmt.(adbc.GetSetOptions)
	if !ok {
		setErr(err, "AdbcStatementSetOptionInt: options are not supported")
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	return C.AdbcStatusCode(errToAdbcErr(err, opts.SetOptionInt(C.GoString(key), int64(value))))
}

//export releasePartitions
func releasePartitions(partitions *C.struct_AdbcPartitions) {
	if partitions.private_data == nil {
		return
	}

	C.free(unsafe.Pointer(partitions.partitions))
	C.free(unsafe.Pointer(partitions.partition_lengths))
	C.free(partitions.private_data)
	partitions.partitions = nil
	partitions.partition_lengths = nil
	partitions.private_data = nil
}

//export AthenaStatementExecutePartitions
func AthenaStatementExecutePartitions(stmt *C.struct_AdbcStatement, schema *C.struct_ArrowSchema, partitions *C.struct_AdbcPartitions, affected *C.int64_t, err *C.struct_AdbcError) (code C.AdbcStatusCode) {
	defer func() {
		if e := recover(); e != nil {
			code = poison(err, "AdbcStatementExecutePartitions", e)
		}
	}()
	st := checkStmtInit(stmt, err, "AdbcStatementExecutePartitions")
	if st == nil {
		return C.ADBC_STATUS_INVALID_STATE
	}

	sc, part, n, e := st.stmt.ExecutePartitions(st.newContext())
	if e != nil {
		return C.AdbcStatusCode(errToAdbcErr(err, e))
	}

	if partitions == nil {
		setErr(err, "AdbcStatementExecutePartitions: partitions output struct is null")
		return C.ADBC_STATUS_INVALID_ARGUMENT
	}

	if affected != nil {
		*affected = C.int64_t(n)
	}

	if sc != nil && schema != nil {
		cdata.ExportArrowSchema(sc, toCdataSchema(schema))
	}

	partitions.num_partitions = C.size_t(part.NumPartitions)
	partitions.partitions = (**C.cuint8_t)(C.malloc(C.size_t(unsafe.Sizeof((*C.uint8_t)(nil)) * uintptr(part.NumPartitions))))
	partitions.partition_lengths = (*C.size_t)(C.malloc(C.size_t(unsafe.Sizeof(C.size_t(0)) * uintptr(part.NumPartitions))))

	// Copy into C-allocated memory to avoid violating CGO rules
	totalLen := 0
	for _, p := range part.PartitionIDs {
		totalLen += len(p)
	}
	partitions.private_data = C.calloc(C.size_t(totalLen), C.size_t(1))
	dst := fromCArr[byte]((*byte)(partitions.private_data), totalLen)

	partIDs := fromCArr[*C.cuint8_t](partitions.partitions, int(partitions.num_partitions))
	partLens := fromCArr[C.size_t](partitions.partition_lengths, int(partitions.num_partitions))
	for i, p := range part.PartitionIDs {
		partIDs[i] = (*C.cuint8_t)(&dst[0])
		copy(dst, p)
		dst = dst[len(p):]
		partLens[i] = C.size_t(len(p))
	}

	partitions.release = (*[0]byte)(C.releasePartitions)
	return C.ADBC_STATUS_OK
}

//export AdbcDriverAthenaInit
func AdbcDriverAthenaInit(version C.int, rawDriver *C.void, err *C.struct_AdbcError) C.AdbcStatusCode {
	if rawDriver == nil {
		setErr(err, "AdbcDriverAthenaInit: driver output struct is null")
		return C.ADBC_STATUS_INVALID_ARGUMENT
	}
	driver := (*C.struct_AdbcDriver)(unsafe.Pointer(rawDriver))

	switch version {
	case C.ADBC_VERSION_1_0_0:
		sink := fromCArr[byte]((*byte)(unsafe.Pointer(driver)), C.ADBC_DRIVER_1_0_0_SIZE)
		memory.Set(sink, 0)
	case C.ADBC_VERSION_1_1_0:
		sink := fromCArr[byte]((*byte)(unsafe.Pointer(driver)), C.ADBC_DRIVER_1_1_0_SIZE)
		memory.Set(sink, 0)
	default:
		setErr(err, "Only version 1.0.0/1.1.0 supported, got %d", int(version))
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	driver.release = (*[0]byte)(C.AthenaDriverRelease)
	driver.DatabaseInit = (*[0]byte)(C.AthenaDatabaseInit)
	driver.DatabaseNew = (*[0]byte)(C.AthenaDatabaseNew)
	driver.DatabaseRelease = (*[0]byte)(C.AthenaDatabaseRelease)
	driver.DatabaseSetOption = (*[0]byte)(C.AthenaDatabaseSetOption)

	driver.ConnectionNew = (*[0]byte)(C.AthenaConnectionNew)
	driver.ConnectionInit = (*[0]byte)(C.AthenaConnectionInit)
	driver.ConnectionRelease = (*[0]byte)(C.AthenaConnectionRelease)
	driver.ConnectionSetOption = (*[0]byte)(C.AthenaConnectionSetOption)
	driver.ConnectionGetInfo = (*[0]byte)(C.AthenaConnectionGetInfo)
	driver.ConnectionGetObjects = (*[0]byte)(C.AthenaConnectionGetObjects)
	driver.ConnectionGetTableSchema = (*[0]byte)(C.AthenaConnectionGetTableSchema)
	driver.ConnectionGetTableTypes = (*[0]byte)(C.AthenaConnectionGetTableTypes)
	driver.ConnectionReadPartition = (*[0]byte)(C.AthenaConnectionReadPartition)
	driver.ConnectionCommit = (*[0]byte)(C.AthenaConnectionCommit)
	driver.ConnectionRollback = (*[0]byte)(C.AthenaConnectionRollback)

	driver.StatementNew = (*[0]byte)(C.AthenaStatementNew)
	driver.StatementRelease = (*[0]byte)(C.AthenaStatementRelease)
	driver.StatementSetOption = (*[0]byte)(C.AthenaStatementSetOption)
	driver.StatementSetSqlQuery = (*[0]byte)(C.AthenaStatementSetSqlQuery)
	driver.StatementSetSubstraitPlan = (*[0]byte)(C.AthenaStatementSetSubstraitPlan)
	driver.StatementBind = (*[0]byte)(C.AthenaStatementBind)
	driver.StatementBindStream = (*[0]byte)(C.AthenaStatementBindStream)
	driver.StatementExecuteQuery = (*[0]byte)(C.AthenaStatementExecuteQuery)
	driver.StatementExecutePartitions = (*[0]byte)(C.AthenaStatementExecutePartitions)
	driver.StatementGetParameterSchema = (*[0]byte)(C.AthenaStatementGetParameterSchema)
	driver.StatementPrepare = (*[0]byte)(C.AthenaStatementPrepare)

	if version == C.ADBC_VERSION_1_1_0 {
		driver.ErrorGetDetailCount = (*[0]byte)(C.AthenaErrorGetDetailCount)
		driver.ErrorGetDetail = (*[0]byte)(C.AthenaErrorGetDetail)
		driver.ErrorFromArrayStream = (*[0]byte)(C.AthenaErrorFromArrayStream)

		driver.DatabaseGetOption = (*[0]byte)(C.AthenaDatabaseGetOption)
		driver.DatabaseGetOptionBytes = (*[0]byte)(C.AthenaDatabaseGetOptionBytes)
		driver.DatabaseGetOptionDouble = (*[0]byte)(C.AthenaDatabaseGetOptionDouble)
		driver.DatabaseGetOptionInt = (*[0]byte)(C.AthenaDatabaseGetOptionInt)
		driver.DatabaseSetOptionBytes = (*[0]byte)(C.AthenaDatabaseSetOptionBytes)
		driver.DatabaseSetOptionDouble = (*[0]byte)(C.AthenaDatabaseSetOptionDouble)
		driver.DatabaseSetOptionInt = (*[0]byte)(C.AthenaDatabaseSetOptionInt)

		driver.ConnectionCancel = (*[0]byte)(C.AthenaConnectionCancel)
		driver.ConnectionGetOption = (*[0]byte)(C.AthenaConnectionGetOption)
		driver.ConnectionGetOptionBytes = (*[0]byte)(C.AthenaConnectionGetOptionBytes)
		driver.ConnectionGetOptionDouble = (*[0]byte)(C.AthenaConnectionGetOptionDouble)
		driver.ConnectionGetOptionInt = (*[0]byte)(C.AthenaConnectionGetOptionInt)
		driver.ConnectionGetStatistics = (*[0]byte)(C.AthenaConnectionGetStatistics)
		driver.ConnectionGetStatisticNames = (*[0]byte)(C.AthenaConnectionGetStatisticNames)
		driver.ConnectionSetOptionBytes = (*[0]byte)(C.AthenaConnectionSetOptionBytes)
		driver.ConnectionSetOptionDouble = (*[0]byte)(C.AthenaConnectionSetOptionDouble)
		driver.ConnectionSetOptionInt = (*[0]byte)(C.AthenaConnectionSetOptionInt)

		driver.StatementCancel = (*[0]byte)(C.AthenaStatementCancel)
		driver.StatementExecuteSchema = (*[0]byte)(C.AthenaStatementExecuteSchema)
		driver.StatementGetOption = (*[0]byte)(C.AthenaStatementGetOption)
		driver.StatementGetOptionBytes = (*[0]byte)(C.AthenaStatementGetOptionBytes)
		driver.StatementGetOptionDouble = (*[0]byte)(C.AthenaStatementGetOptionDouble)
		driver.StatementGetOptionInt = (*[0]byte)(C.AthenaStatementGetOptionInt)
		driver.StatementSetOptionBytes = (*[0]byte)(C.AthenaStatementSetOptionBytes)
		driver.StatementSetOptionDouble = (*[0]byte)(C.AthenaStatementSetOptionDouble)
		driver.StatementSetOptionInt = (*[0]byte)(C.AthenaStatementSetOptionInt)
	}

	return C.ADBC_STATUS_OK
}

// Allocate a new cgo.Handle and store its address in a heap-allocated
// uintptr_t.  Experimentally, this was found to be necessary, else
// something (the Go runtime?) would corrupt (garbage-collect?) the
// handle.
func createHandle(hndl cgo.Handle) unsafe.Pointer {
	// uintptr_t* hptr = malloc(sizeof(uintptr_t));
	hptr := (*C.uintptr_t)(C.calloc(C.sizeof_uintptr_t, C.size_t(1)))
	// *hptr = (uintptr)hndl;
	*hptr = C.uintptr_t(uintptr(hndl))
	return unsafe.Pointer(hptr)
}

func getFromHandle[T any](ptr unsafe.Pointer) *T {
	// uintptr_t* hptr = (uintptr_t*)ptr;
	hptr := (*C.uintptr_t)(ptr)
	return cgo.Handle((uintptr)(*hptr)).Value().(*T)
}

// fillStringOption writes val into buf (if buf is non-nil and large enough)
// and returns the required buffer size including the NUL terminator.
// This is the pure-Go core of exportStringOption and is tested directly.
func fillStringOption(val string, buf []byte) int {
	needed := len(val) + 1
	if buf != nil && needed <= len(buf) {
		copy(buf, val)
		buf[len(val)] = 0
	}
	return needed
}

func exportStringOption(val string, out *C.char, length *C.size_t) C.AdbcStatusCode {
	if length == nil {
		return C.ADBC_STATUS_INVALID_ARGUMENT
	}
	var buf []byte
	if out != nil {
		buf = fromCArr[byte]((*byte)(unsafe.Pointer(out)), int(*length))
	}
	*length = C.size_t(fillStringOption(val, buf))
	return C.ADBC_STATUS_OK
}

func exportBytesOption(val []byte, out *C.uint8_t, length *C.size_t) C.AdbcStatusCode {
	if length == nil {
		return C.ADBC_STATUS_INVALID_ARGUMENT
	}
	var sink []byte
	if out != nil {
		sink = fromCArr[byte]((*byte)(out), int(*length))
	}
	if len(val) <= len(sink) {
		copy(sink, val)
	}
	*length = C.size_t(len(val))
	return C.ADBC_STATUS_OK
}

type cancellableContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *cancellableContext) newContext() context.Context {
	c.cancelContext()
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c.ctx
}

func (c *cancellableContext) cancelContext() {
	if c.cancel != nil {
		c.cancel()
	}
	c.ctx = nil
	c.cancel = nil
}

func main() {}
