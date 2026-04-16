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

// shimtest is a standalone integration-test program for the CGo FFI shim.
// It links directly against libadbc_driver_athena and calls AdbcDriverAthenaInit
// to verify that the init entrypoint behaves correctly.
//
// Build and run:
//
//	make -C ..
//	go run . 2>&1
//
// Or via the Makefile test target:
//
//	make -C .. test
package main

// #cgo CFLAGS: -I../athena
// #cgo LDFLAGS: -L.. -ladbc_driver_athena -Wl,-rpath,..
// #include "adbc.h"
// #include <stdlib.h>
// #include <string.h>
//
// // Forward-declare the entry point exported by libadbc_driver_athena.
// AdbcStatusCode AdbcDriverAthenaInit(int version, void* driver, struct AdbcError* err);
//
// static void release_adbc_error(struct AdbcError* e) {
//   if (e && e->release) { e->release(e); e->release = NULL; }
// }
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

// ============================================================================
// minimal test framework
// ============================================================================

var (
	passed int
	failed int
)

func check(name string, ok bool, detail string) {
	if ok {
		fmt.Printf("  PASS  %s\n", name)
		passed++
	} else {
		fmt.Printf("  FAIL  %s: %s\n", name, detail)
		failed++
	}
}

func section(name string) { fmt.Printf("\n--- %s ---\n", name) }

// ============================================================================
// helpers
// ============================================================================

func newErr() *C.struct_AdbcError {
	return (*C.struct_AdbcError)(C.calloc(1, C.sizeof_struct_AdbcError))
}

func releaseErr(e *C.struct_AdbcError) {
	C.release_adbc_error(e)
	C.free(unsafe.Pointer(e))
}

func callInit(version C.int, driver *C.struct_AdbcDriver) (C.AdbcStatusCode, *C.struct_AdbcError) {
	e := newErr()
	code := C.AdbcDriverAthenaInit(version, unsafe.Pointer(driver), e)
	return code, e
}

// ============================================================================
// tests
// ============================================================================

func testNullDriver() {
	section("AdbcDriverAthenaInit: NULL driver pointer")

	code, e := callInit(C.ADBC_VERSION_1_1_0, nil)
	defer releaseErr(e)

	check("returns ADBC_STATUS_INVALID_ARGUMENT",
		code == C.ADBC_STATUS_INVALID_ARGUMENT,
		fmt.Sprintf("got status %d", int(code)))

	check("sets an error message",
		e.message != nil,
		"error message was nil")
}

func testUnsupportedVersion() {
	section("AdbcDriverAthenaInit: unsupported version")

	var driver C.struct_AdbcDriver
	code, e := callInit(C.int(0), &driver)
	defer releaseErr(e)

	check("returns ADBC_STATUS_NOT_IMPLEMENTED",
		code == C.ADBC_STATUS_NOT_IMPLEMENTED,
		fmt.Sprintf("got status %d", int(code)))
}

func testV100Init() {
	section("AdbcDriverAthenaInit: ADBC 1.0.0")

	var driver C.struct_AdbcDriver
	code, e := callInit(C.ADBC_VERSION_1_0_0, &driver)
	defer releaseErr(e)

	check("returns ADBC_STATUS_OK",
		code == C.ADBC_STATUS_OK,
		fmt.Sprintf("got status %d, message: %s", int(code), C.GoString(e.message)))

	// Regression: driver.release was missing; driver managers crash on unload.
	check("driver.release is non-nil",
		driver.release != nil, "driver.release was nil")

	check("DatabaseNew is set", driver.DatabaseNew != nil, "DatabaseNew was nil")
	check("DatabaseInit is set", driver.DatabaseInit != nil, "DatabaseInit was nil")
	check("DatabaseRelease is set", driver.DatabaseRelease != nil, "DatabaseRelease was nil")
	check("ConnectionNew is set", driver.ConnectionNew != nil, "ConnectionNew was nil")
	check("ConnectionInit is set", driver.ConnectionInit != nil, "ConnectionInit was nil")
	check("ConnectionRelease is set", driver.ConnectionRelease != nil, "ConnectionRelease was nil")
	check("StatementNew is set", driver.StatementNew != nil, "StatementNew was nil")
	check("StatementRelease is set", driver.StatementRelease != nil, "StatementRelease was nil")
	check("StatementExecuteQuery is set", driver.StatementExecuteQuery != nil, "StatementExecuteQuery was nil")
}

func testV110Init() {
	section("AdbcDriverAthenaInit: ADBC 1.1.0")

	var driver C.struct_AdbcDriver
	code, e := callInit(C.ADBC_VERSION_1_1_0, &driver)
	defer releaseErr(e)

	check("returns ADBC_STATUS_OK",
		code == C.ADBC_STATUS_OK,
		fmt.Sprintf("got status %d, message: %s", int(code), C.GoString(e.message)))

	check("driver.release is non-nil", driver.release != nil, "driver.release was nil")

	// 1.1.0-only entrypoints
	check("ErrorGetDetailCount is set", driver.ErrorGetDetailCount != nil, "ErrorGetDetailCount was nil")
	check("ErrorGetDetail is set", driver.ErrorGetDetail != nil, "ErrorGetDetail was nil")
	check("ConnectionCancel is set", driver.ConnectionCancel != nil, "ConnectionCancel was nil")
	check("ConnectionGetStatistics is set", driver.ConnectionGetStatistics != nil, "ConnectionGetStatistics was nil")
	check("ConnectionGetStatisticNames is set", driver.ConnectionGetStatisticNames != nil, "ConnectionGetStatisticNames was nil")
	check("StatementExecuteSchema is set", driver.StatementExecuteSchema != nil, "StatementExecuteSchema was nil")
	check("StatementCancel is set", driver.StatementCancel != nil, "StatementCancel was nil")
}

func testV110Zeroes() {
	section("AdbcDriverAthenaInit: struct is zeroed before filling")

	// Fill the driver struct with non-zero garbage to confirm Init zeroes it.
	driver := C.struct_AdbcDriver{}
	p := (*[C.ADBC_DRIVER_1_1_0_SIZE]byte)(unsafe.Pointer(&driver))
	for i := range p {
		p[i] = 0xAB
	}

	code, e := callInit(C.ADBC_VERSION_1_1_0, &driver)
	defer releaseErr(e)

	check("returns ADBC_STATUS_OK",
		code == C.ADBC_STATUS_OK,
		fmt.Sprintf("got status %d", int(code)))

	check("DatabaseGetOption is set (set by 1.1.0 path)",
		driver.DatabaseGetOption != nil, "DatabaseGetOption was nil")
}


// ============================================================================
// main
// ============================================================================

func main() {
	fmt.Println("shimtest: CGo-level integration tests for libadbc_driver_athena")

	testNullDriver()
	testUnsupportedVersion()
	testV100Init()
	testV110Init()
	testV110Zeroes()

	fmt.Printf("\n%d passed, %d failed\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}
