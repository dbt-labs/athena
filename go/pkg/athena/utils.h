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

// clang-format off
//go:build driverlib
// clang-format on

#pragma once

#include <stdlib.h>
#include "adbc.h"

struct AdbcError* AthenaErrorFromArrayStream(struct ArrowArrayStream*,
                                             AdbcStatusCode*);
AdbcStatusCode AthenaDatabaseGetOption(struct AdbcDatabase*, const char*, char*,
                                       size_t*, struct AdbcError*);
AdbcStatusCode AthenaDatabaseGetOptionBytes(struct AdbcDatabase*, const char*,
                                            uint8_t*, size_t*, struct AdbcError*);
AdbcStatusCode AthenaDatabaseGetOptionDouble(struct AdbcDatabase*, const char*,
                                             double*, struct AdbcError*);
AdbcStatusCode AthenaDatabaseGetOptionInt(struct AdbcDatabase*, const char*, int64_t*,
                                          struct AdbcError*);
AdbcStatusCode AthenaDatabaseInit(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode AthenaDatabaseNew(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode AthenaDatabaseRelease(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode AthenaDatabaseSetOption(struct AdbcDatabase* db, const char* key,
                                       const char* value, struct AdbcError* err);
AdbcStatusCode AthenaDatabaseSetOptionBytes(struct AdbcDatabase*, const char*,
                                            const uint8_t*, size_t, struct AdbcError*);
AdbcStatusCode AthenaDatabaseSetOptionDouble(struct AdbcDatabase*, const char*, double,
                                             struct AdbcError*);
AdbcStatusCode AthenaDatabaseSetOptionInt(struct AdbcDatabase*, const char*, int64_t,
                                          struct AdbcError*);

AdbcStatusCode AthenaConnectionCancel(struct AdbcConnection*, struct AdbcError*);
AdbcStatusCode AthenaConnectionCommit(struct AdbcConnection* cnxn,
                                      struct AdbcError* err);
AdbcStatusCode AthenaConnectionGetInfo(struct AdbcConnection* cnxn,
                                       const uint32_t* codes, size_t len,
                                       struct ArrowArrayStream* out,
                                       struct AdbcError* err);
AdbcStatusCode AthenaConnectionGetObjects(
    struct AdbcConnection* cnxn, int depth, const char* catalog, const char* dbSchema,
    const char* tableName, const char** tableType, const char* columnName,
    struct ArrowArrayStream* out, struct AdbcError* err);
AdbcStatusCode AthenaConnectionGetOption(struct AdbcConnection*, const char*, char*,
                                         size_t*, struct AdbcError*);
AdbcStatusCode AthenaConnectionGetOptionBytes(struct AdbcConnection*, const char*,
                                              uint8_t*, size_t*, struct AdbcError*);
AdbcStatusCode AthenaConnectionGetOptionDouble(struct AdbcConnection*, const char*,
                                               double*, struct AdbcError*);
AdbcStatusCode AthenaConnectionGetOptionInt(struct AdbcConnection*, const char*,
                                            int64_t*, struct AdbcError*);
AdbcStatusCode AthenaConnectionGetStatistics(struct AdbcConnection*, const char*,
                                             const char*, const char*, char,
                                             struct ArrowArrayStream*,
                                             struct AdbcError*);
AdbcStatusCode AthenaConnectionGetStatisticNames(struct AdbcConnection*,
                                                 struct ArrowArrayStream*,
                                                 struct AdbcError*);
AdbcStatusCode AthenaConnectionGetTableSchema(
    struct AdbcConnection* cnxn, const char* catalog, const char* dbSchema,
    const char* tableName, struct ArrowSchema* schema, struct AdbcError* err);
AdbcStatusCode AthenaConnectionGetTableTypes(struct AdbcConnection* cnxn,
                                             struct ArrowArrayStream* out,
                                             struct AdbcError* err);
AdbcStatusCode AthenaConnectionInit(struct AdbcConnection* cnxn,
                                    struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode AthenaConnectionNew(struct AdbcConnection* cnxn, struct AdbcError* err);
AdbcStatusCode AthenaConnectionReadPartition(struct AdbcConnection* cnxn,
                                             const uint8_t* serialized,
                                             size_t serializedLen,
                                             struct ArrowArrayStream* out,
                                             struct AdbcError* err);
AdbcStatusCode AthenaConnectionRelease(struct AdbcConnection* cnxn,
                                       struct AdbcError* err);
AdbcStatusCode AthenaConnectionRollback(struct AdbcConnection* cnxn,
                                        struct AdbcError* err);
AdbcStatusCode AthenaConnectionSetOption(struct AdbcConnection* cnxn, const char* key,
                                         const char* val, struct AdbcError* err);
AdbcStatusCode AthenaConnectionSetOptionBytes(struct AdbcConnection*, const char*,
                                              const uint8_t*, size_t,
                                              struct AdbcError*);
AdbcStatusCode AthenaConnectionSetOptionDouble(struct AdbcConnection*, const char*,
                                               double, struct AdbcError*);
AdbcStatusCode AthenaConnectionSetOptionInt(struct AdbcConnection*, const char*,
                                            int64_t, struct AdbcError*);

AdbcStatusCode AthenaStatementBind(struct AdbcStatement* stmt,
                                   struct ArrowArray* values,
                                   struct ArrowSchema* schema, struct AdbcError* err);
AdbcStatusCode AthenaStatementBindStream(struct AdbcStatement* stmt,
                                         struct ArrowArrayStream* stream,
                                         struct AdbcError* err);
AdbcStatusCode AthenaStatementCancel(struct AdbcStatement*, struct AdbcError*);
AdbcStatusCode AthenaStatementExecuteQuery(struct AdbcStatement* stmt,
                                           struct ArrowArrayStream* out,
                                           int64_t* affected, struct AdbcError* err);
AdbcStatusCode AthenaStatementExecutePartitions(struct AdbcStatement* stmt,
                                                struct ArrowSchema* schema,
                                                struct AdbcPartitions* partitions,
                                                int64_t* affected,
                                                struct AdbcError* err);
AdbcStatusCode AthenaStatementExecutePartitionsTrampoline(
    struct AdbcStatement* stmt, struct ArrowSchema* schema,
    struct AdbcPartitions* partitions, int64_t* affected, struct AdbcError* err);
AdbcStatusCode AthenaStatementExecuteSchema(struct AdbcStatement*, struct ArrowSchema*,
                                            struct AdbcError*);
AdbcStatusCode AthenaStatementGetOption(struct AdbcStatement*, const char*, char*,
                                        size_t*, struct AdbcError*);
AdbcStatusCode AthenaStatementGetOptionBytes(struct AdbcStatement*, const char*,
                                             uint8_t*, size_t*, struct AdbcError*);
AdbcStatusCode AthenaStatementGetOptionDouble(struct AdbcStatement*, const char*,
                                              double*, struct AdbcError*);
AdbcStatusCode AthenaStatementGetOptionInt(struct AdbcStatement*, const char*,
                                           int64_t*, struct AdbcError*);
AdbcStatusCode AthenaStatementGetParameterSchema(struct AdbcStatement* stmt,
                                                 struct ArrowSchema* schema,
                                                 struct AdbcError* err);
AdbcStatusCode AthenaStatementNew(struct AdbcConnection* cnxn,
                                  struct AdbcStatement* stmt, struct AdbcError* err);
AdbcStatusCode AthenaStatementPrepare(struct AdbcStatement* stmt,
                                      struct AdbcError* err);
AdbcStatusCode AthenaStatementRelease(struct AdbcStatement* stmt,
                                      struct AdbcError* err);
AdbcStatusCode AthenaStatementSetOption(struct AdbcStatement* stmt, const char* key,
                                        const char* value, struct AdbcError* err);
AdbcStatusCode AthenaStatementSetOptionBytes(struct AdbcStatement*, const char*,
                                             const uint8_t*, size_t,
                                             struct AdbcError*);
AdbcStatusCode AthenaStatementSetOptionDouble(struct AdbcStatement*, const char*,
                                              double, struct AdbcError*);
AdbcStatusCode AthenaStatementSetOptionInt(struct AdbcStatement*, const char*, int64_t,
                                           struct AdbcError*);
AdbcStatusCode AthenaStatementSetSqlQuery(struct AdbcStatement* stmt,
                                          const char* query, struct AdbcError* err);
AdbcStatusCode AthenaStatementSetSubstraitPlan(struct AdbcStatement* stmt,
                                               const uint8_t* plan, size_t length,
                                               struct AdbcError* err);

AdbcStatusCode AdbcDriverAthenaInit(int version, void* rawDriver,
                                    struct AdbcError* err);

static inline void AthenaerrRelease(struct AdbcError* error) {
  if (error->release) {
    error->release(error);
    error->release = NULL;
  }
}

void Athena_release_error(struct AdbcError* error);

struct AthenaError {
  char* message;
  char** keys;
  uint8_t** values;
  size_t* lengths;
  int count;
};

void AthenaReleaseErrWithDetails(struct AdbcError* error);

int AthenaErrorGetDetailCount(const struct AdbcError* error);
struct AdbcErrorDetail AthenaErrorGetDetail(const struct AdbcError* error, int index);

int AthenaArrayStreamGetSchemaTrampoline(struct ArrowArrayStream* stream,
                                         struct ArrowSchema* out);
int AthenaArrayStreamGetNextTrampoline(struct ArrowArrayStream* stream,
                                       struct ArrowArray* out);
