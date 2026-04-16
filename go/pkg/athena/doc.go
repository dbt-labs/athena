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

// Package main is the CGo FFI shim for the Athena ADBC driver.
// It is only compiled when the "driverlib" build tag is set and
// produces libadbc_driver_athena.so via `make` in the parent directory.
//
// To build:
//
//	cd go/pkg && make
//
// To run tests (unit + shim integration):
//
//	cd go/pkg && make test
package main
