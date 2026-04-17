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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// fillStringOption (pure-Go core of exportStringOption)
// ============================================================================

func TestFillStringOption_ReturnsRequiredLength(t *testing.T) {
	n := fillStringOption("hello", nil)
	assert.Equal(t, 6, n, "required length must be len(val)+1 for NUL terminator")
}

func TestFillStringOption_NilBuf_DoesNotCrash(t *testing.T) {
	// Regression: old exportStringOption wrote unconditionally before nil guard.
	n := fillStringOption("hello", nil)
	assert.Equal(t, 6, n)
}

func TestFillStringOption_BufferTooSmall_DoesNotWrite(t *testing.T) {
	buf := []byte{0xFF, 0xFF}
	n := fillStringOption("hello", buf)
	assert.Equal(t, 6, n, "returns required length even when buffer is too small")
	assert.Equal(t, byte(0xFF), buf[0], "buffer must not be modified when too small")
	assert.Equal(t, byte(0xFF), buf[1], "buffer must not be modified when too small")
}

func TestFillStringOption_WritesStringAndNul(t *testing.T) {
	buf := make([]byte, 6)
	n := fillStringOption("hello", buf)
	assert.Equal(t, 6, n)
	assert.Equal(t, []byte("hello\x00"), buf)
}

func TestFillStringOption_NulAtCorrectIndex(t *testing.T) {
	// Regression: the old exportStringOption wrote NUL at index lenWithTerminator
	// (i.e. len(val)+1) instead of len(val), causing an off-by-one write.
	val := "ab"
	buf := make([]byte, len(val)+3) // intentionally larger than strictly needed
	for i := range buf {
		buf[i] = 0xFF
	}

	fillStringOption(val, buf)

	assert.Equal(t, byte('a'), buf[0])
	assert.Equal(t, byte('b'), buf[1])
	assert.Equal(t, byte(0x00), buf[2], "NUL must be placed at index len(val), not len(val)+1")
	assert.Equal(t, byte(0xFF), buf[3], "byte beyond NUL must not be written")
}

func TestFillStringOption_EmptyString(t *testing.T) {
	buf := make([]byte, 2)
	buf[0] = 0xFF
	n := fillStringOption("", buf)
	assert.Equal(t, 1, n, "empty string requires 1 byte (just the NUL)")
	assert.Equal(t, byte(0x00), buf[0])
}

func TestFillStringOption_ExactFit(t *testing.T) {
	val := "hi"
	buf := make([]byte, 3) // exactly len(val)+1
	n := fillStringOption(val, buf)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("hi\x00"), buf)
}
