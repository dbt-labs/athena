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

// clang-format off

// clang-format on

#include "utils.h"

#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

void Athena_release_error(struct AdbcError* error) {
  free(error->message);
  error->message = NULL;
  error->release = NULL;
}

void AthenaReleaseErrWithDetails(struct AdbcError* error) {
  if (!error || error->release != AthenaReleaseErrWithDetails ||
      !error->private_data) {
    return;
  }

  struct AthenaError* details =
      (struct AthenaError*) error->private_data;
  for (int i = 0; i < details->count; i++) {
    free(details->keys[i]);
    free(details->values[i]);
  }
  free(details->keys);
  free(details->values);
  free(details->lengths);
  free(details);

  free(error->message);
  error->message = NULL;
  error->release = NULL;
  error->private_data = NULL;
}

int AthenaErrorGetDetailCount(const struct AdbcError* error) {
  if (!error || error->release != AthenaReleaseErrWithDetails ||
      !error->private_data) {
    return 0;
  }

  return ((struct AthenaError*) error->private_data)->count;
}

struct AdbcErrorDetail AthenaErrorGetDetail(const struct AdbcError* error,
                                            int index) {
  if (!error || error->release != AthenaReleaseErrWithDetails ||
      !error->private_data) {
    return (struct AdbcErrorDetail){NULL, NULL, 0};
  }
  struct AthenaError* details = (struct AthenaError*) error->private_data;
  if (index < 0 || index >= details->count) {
    return (struct AdbcErrorDetail){NULL, NULL, 0};
  }

  return (struct AdbcErrorDetail){
    .key = details->keys[index],
    .value = details->values[index],
    .value_length = details->lengths[index]
  };
}

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
int AdbcErrorGetDetailCount(const struct AdbcError* error) {
  return AthenaErrorGetDetailCount(error);
}

struct AdbcErrorDetail AdbcErrorGetDetail(const struct AdbcError* error, int index) {
  return AthenaErrorGetDetail(error, index);
}

const struct AdbcError* AdbcErrorFromArrayStream(struct ArrowArrayStream* stream,
                                                 AdbcStatusCode* status) {
  return AthenaErrorFromArrayStream(stream, status);
}

AdbcStatusCode AdbcDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                     char* value, size_t* length,
                                     struct AdbcError* error) {
  return AthenaDatabaseGetOption(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          uint8_t* value, size_t* length,
                                          struct AdbcError* error) {
  return AthenaDatabaseGetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double* value, struct AdbcError* error) {
  return AthenaDatabaseGetOptionDouble(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t* value, struct AdbcError* error) {
  return AthenaDatabaseGetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return AthenaDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return AthenaDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return AthenaDatabaseRelease(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return AthenaDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          const uint8_t* value, size_t length,
                                          struct AdbcError* error) {
  return AthenaDatabaseSetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseSetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double value, struct AdbcError* error) {
  return AthenaDatabaseSetOptionDouble(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t value, struct AdbcError* error) {
  return AthenaDatabaseSetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcConnectionCancel(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return AthenaConnectionCancel(connection, error);
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return AthenaConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     const uint32_t* info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream* out,
                                     struct AdbcError* error) {
  if (out) memset(out, 0, sizeof(*out));
  return AthenaConnectionGetInfo(connection, info_codes, info_codes_length,
                                 out, error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_type,
                                        const char* column_name,
                                        struct ArrowArrayStream* out,
                                        struct AdbcError* error) {
  if (out) memset(out, 0, sizeof(*out));
  return AthenaConnectionGetObjects(connection, depth, catalog, db_schema, table_name,
                                    table_type, column_name, out, error);
}

AdbcStatusCode AdbcConnectionGetOption(struct AdbcConnection* connection, const char* key,
                                       char* value, size_t* length,
                                       struct AdbcError* error) {
  return AthenaConnectionGetOption(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, uint8_t* value,
                                            size_t* length, struct AdbcError* error) {
  return AthenaConnectionGetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double* value,
                                             struct AdbcError* error) {
  return AthenaConnectionGetOptionDouble(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t* value,
                                          struct AdbcError* error) {
  return AthenaConnectionGetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetStatistics(struct AdbcConnection* connection,
                                           const char* catalog, const char* db_schema,
                                           const char* table_name, char approximate,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return AthenaConnectionGetStatistics(connection, catalog, db_schema, table_name,
                                       approximate, out, error);
}

AdbcStatusCode AdbcConnectionGetStatisticNames(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  return AthenaConnectionGetStatisticNames(connection, out, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  if (schema) memset(schema, 0, sizeof(*schema));
  return AthenaConnectionGetTableSchema(connection, catalog, db_schema, table_name,
                                        schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  if (out) memset(out, 0, sizeof(*out));
  return AthenaConnectionGetTableTypes(connection, out, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return AthenaConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return AthenaConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  if (out) memset(out, 0, sizeof(*out));
  return AthenaConnectionReadPartition(connection, serialized_partition,
                                       serialized_length, out, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return AthenaConnectionRelease(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return AthenaConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return AthenaConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, const uint8_t* value,
                                            size_t length, struct AdbcError* error) {
  return AthenaConnectionSetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionSetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double value,
                                             struct AdbcError* error) {
  return AthenaConnectionSetOptionDouble(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t value,
                                          struct AdbcError* error) {
  return AthenaConnectionSetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcStatementCancel(struct AdbcStatement* statement,
                                   struct AdbcError* error) {
  return AthenaStatementCancel(statement, error);
}

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return AthenaStatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return AthenaStatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              struct ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return AthenaStatementExecutePartitionsTrampoline(
    statement, schema, partitions, rows_affected, error);
}

AdbcStatusCode AthenaStatementExecutePartitionsTrampoline(
    struct AdbcStatement* statement,
    struct ArrowSchema* schema,
    struct AdbcPartitions* partitions,
    int64_t* rows_affected,
    struct AdbcError* error) {
  if (schema) memset(schema, 0, sizeof(*schema));
  if (partitions) memset(partitions, 0, sizeof(*partitions));
  return AthenaStatementExecutePartitions(statement, schema, partitions,
                                          rows_affected, error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  if (out) memset(out, 0, sizeof(*out));
  return AthenaStatementExecuteQuery(statement, out, rows_affected, error);
}

AdbcStatusCode AdbcStatementExecuteSchema(struct AdbcStatement* statement,
                                          struct ArrowSchema* schema,
                                          struct AdbcError* error) {
  if (schema) memset(schema, 0, sizeof(*schema));
  return AthenaStatementExecuteSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementGetOption(struct AdbcStatement* statement, const char* key,
                                      char* value, size_t* length,
                                      struct AdbcError* error) {
  return AthenaStatementGetOption(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, uint8_t* value,
                                           size_t* length, struct AdbcError* error) {
  return AthenaStatementGetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double* value,
                                            struct AdbcError* error) {
  return AthenaStatementGetOptionDouble(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionInt(struct AdbcStatement* statement,
                                         const char* key, int64_t* value,
                                         struct AdbcError* error) {
  return AthenaStatementGetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  if (schema) memset(schema, 0, sizeof(*schema));
  return AthenaStatementGetParameterSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return AthenaStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return AthenaStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return AthenaStatementRelease(statement, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return AthenaStatementSetSqlQuery(statement, query, error);
}

AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement* statement,
                                             const uint8_t* plan, size_t length,
                                             struct AdbcError* error) {
  return AthenaStatementSetSubstraitPlan(statement, plan, length, error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return AthenaStatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, const uint8_t* value,
                                           size_t length, struct AdbcError* error) {
  return AthenaStatementSetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementSetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double value,
                                            struct AdbcError* error) {
  return AthenaStatementSetOptionDouble(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionInt(struct AdbcStatement* statement,
                                         const char* key, int64_t value,
                                         struct AdbcError* error) {
  return AthenaStatementSetOptionInt(statement, key, value, error);
}

/* Do not export the common AdbcDriverInit entrypoint from this driver. */
#endif  // !defined(ADBC_NO_COMMON_ENTRYPOINTS)

int AthenaArrayStreamGetSchema(struct ArrowArrayStream*, struct ArrowSchema*);
int AthenaArrayStreamGetNext(struct ArrowArrayStream*, struct ArrowArray*);

AdbcStatusCode AthenaDriverRelease(struct AdbcDriver* driver,
                                    struct AdbcError* err) {
  (void)driver;
  (void)err;
  return ADBC_STATUS_OK;
}

int AthenaArrayStreamGetSchemaTrampoline(struct ArrowArrayStream* stream,
                                         struct ArrowSchema* out) {
  // XXX(https://github.com/apache/arrow-adbc/issues/729)
  memset(out, 0, sizeof(*out));
  return AthenaArrayStreamGetSchema(stream, out);
}

int AthenaArrayStreamGetNextTrampoline(struct ArrowArrayStream* stream,
                                       struct ArrowArray* out) {
  // XXX(https://github.com/apache/arrow-adbc/issues/729)
  memset(out, 0, sizeof(*out));
  return AthenaArrayStreamGetNext(stream, out);
}

#ifdef __cplusplus
}
#endif
