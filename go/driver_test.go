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

package athena_test

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	athena "github.com/dbt-labs/athena/go"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getSetOptions is a helper to cast adbc.Database to adbc.GetSetOptions.
func getSetOptions(t *testing.T, db adbc.Database) adbc.GetSetOptions {
	t.Helper()
	gso, ok := db.(adbc.GetSetOptions)
	require.True(t, ok, "database does not implement adbc.GetSetOptions")
	return gso
}

func TestNewDriver(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)
	assert.NotNil(t, drv)
}

func TestNewDatabase_NoOptions(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	// Should succeed even with no options (validation deferred to Open)
	db, err := drv.NewDatabase(map[string]string{})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestNewDatabase_WithOptions(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:       "us-east-1",
		athena.OptionCatalog:      "AwsDataCatalog",
		athena.OptionSchema:       "default",
		athena.OptionS3StagingDir: "s3://my-bucket/athena-results/",
		athena.OptionWorkGroup:    "primary",
		athena.OptionAuthType:     athena.AuthTypeDefault,
	})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestNewDatabase_InvalidAuthType(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	_, err := drv.NewDatabase(map[string]string{
		athena.OptionAuthType: "invalid_auth_type",
	})
	require.Error(t, err)
}

func TestGetSetOption(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:  "us-west-2",
		athena.OptionCatalog: "MyCatalog",
	})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	gso := getSetOptions(t, db)

	region, err := gso.GetOption(athena.OptionRegion)
	require.NoError(t, err)
	assert.Equal(t, "us-west-2", region)

	catalog, err := gso.GetOption(athena.OptionCatalog)
	require.NoError(t, err)
	assert.Equal(t, "MyCatalog", catalog)

	// Update an option
	err = gso.SetOption(athena.OptionRegion, "eu-west-1")
	require.NoError(t, err)

	region, err = gso.GetOption(athena.OptionRegion)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", region)
}

func TestAuthTypeAccessKey_MissingKey(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:       "us-east-1",
		athena.OptionS3StagingDir: "s3://bucket/prefix/",
		athena.OptionAuthType:     athena.AuthTypeAccessKey,
		// Missing access key ID and secret key
	})
	require.NoError(t, err)
	defer db.Close()

	// Open should fail because credentials are incomplete
	_, err = db.Open(context.Background())
	require.Error(t, err)
}

func TestAuthTypeProfile_MissingProfileName(t *testing.T) {
	drv := athena.NewDriver(memory.DefaultAllocator)

	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:       "us-east-1",
		athena.OptionS3StagingDir: "s3://bucket/prefix/",
		athena.OptionAuthType:     athena.AuthTypeProfile,
		// Missing profile name
	})
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Open(context.Background())
	require.Error(t, err)
}

func TestAllAuthTypeConstants(t *testing.T) {
	assert.Equal(t, "iam", athena.AuthTypeDefault)
	assert.Equal(t, "access_key", athena.AuthTypeAccessKey)
	assert.Equal(t, "profile", athena.AuthTypeProfile)
}

func TestAllOptionConstants(t *testing.T) {
	assert.Equal(t, "athena.region", athena.OptionRegion)
	assert.Equal(t, "athena.catalog", athena.OptionCatalog)
	assert.Equal(t, "athena.schema", athena.OptionSchema)
	assert.Equal(t, "athena.s3_staging_dir", athena.OptionS3StagingDir)
	assert.Equal(t, "athena.work_group", athena.OptionWorkGroup)
	assert.Equal(t, "athena.auth_type", athena.OptionAuthType)
	assert.Equal(t, "athena.aws.access_key_id", athena.OptionAccessKeyID)
	assert.Equal(t, "athena.aws.secret_access_key", athena.OptionSecretKey)
	assert.Equal(t, "athena.aws.session_token", athena.OptionSessionToken)
	assert.Equal(t, "athena.aws.profile", athena.OptionProfileName)
}

func TestIntegration(t *testing.T) {
	if os.Getenv("ADBC_ATHENA_TESTS") == "" {
		t.Skip("set ADBC_ATHENA_TESTS=1 to run integration tests")
	}

	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-east-1"
	}
	s3Dir := os.Getenv("ATHENA_S3_STAGING_DIR")
	require.NotEmpty(t, s3Dir, "ATHENA_S3_STAGING_DIR must be set for integration tests")

	drv := athena.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{
		athena.OptionRegion:       region,
		athena.OptionS3StagingDir: s3Dir,
		athena.OptionAuthType:     athena.AuthTypeDefault,
	})
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	err = stmt.SetSqlQuery("SELECT 1 AS n")
	require.NoError(t, err)

	rdr, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	defer rdr.Release()

	assert.True(t, rdr.Next())
	rec := rdr.Record()
	assert.EqualValues(t, 1, rec.NumCols())
}
