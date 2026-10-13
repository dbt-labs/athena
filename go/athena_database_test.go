// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newConfigTestDB is a database with static credentials, so that building its
// AWS config and clients needs neither a network nor a credential file.
func newConfigTestDB(t *testing.T) *databaseImpl {
	d := newTestDB(t, nil)
	d.region = "eu-west-1"
	d.authType = AuthTypeAccessKey
	d.accessKeyID = "AKIDBASE"
	d.secretKey = "secret"
	return d
}

// mockSTS records the AssumeRole call and returns fixed credentials.
type mockSTS struct {
	input *sts.AssumeRoleInput
}

func (m *mockSTS) AssumeRole(_ context.Context, in *sts.AssumeRoleInput, _ ...func(*sts.Options)) (*sts.AssumeRoleOutput, error) {
	m.input = in
	return &sts.AssumeRoleOutput{Credentials: &ststypes.Credentials{
		AccessKeyId:     aws.String("AKIDASSUMED"),
		SecretAccessKey: aws.String("secret"),
		SessionToken:    aws.String("token"),
		Expiration:      aws.Time(time.Now().Add(time.Hour)),
	}}, nil
}

func TestAPIRetries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		retries  int
		attempts int
	}{
		{"unset, the SDK default", -1, 3},
		{"no retries", 0, 1},
		{"five retries", 5, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newConfigTestDB(t)
			d.maxAPIRetries = tc.retries
			cfg, err := d.buildAWSConfig(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tc.attempts, d.athenaClient(cfg).Options().Retryer.MaxAttempts(), "athena")
			assert.Equal(t, tc.attempts, d.stsClient(cfg).(*sts.Client).Options().Retryer.MaxAttempts(), "sts")
		})
	}
}

func TestEndpoints(t *testing.T) {
	d := newConfigTestDB(t)
	cfg, err := d.buildAWSConfig(context.Background())
	require.NoError(t, err)
	assert.Nil(t, d.athenaClient(cfg).Options().BaseEndpoint, "athena, unset")
	assert.Nil(t, d.stsClient(cfg).(*sts.Client).Options().BaseEndpoint, "sts, unset")

	d.athenaEndpointURL = "https://athena.vpce.example"
	d.stsEndpointURL = "https://sts.vpce.example"
	assert.Equal(t, "https://athena.vpce.example", aws.ToString(d.athenaClient(cfg).Options().BaseEndpoint))
	assert.Equal(t, "https://sts.vpce.example", aws.ToString(d.stsClient(cfg).(*sts.Client).Options().BaseEndpoint))
}

// TestEndpoints_SDKEnvironment checks the SDK's service-specific endpoint
// variables apply when the options are unset, and the options win over them.
func TestEndpoints_SDKEnvironment(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL_ATHENA", "https://athena.env.example")
	t.Setenv("AWS_ENDPOINT_URL_STS", "https://sts.env.example")
	d := newConfigTestDB(t)
	cfg, err := d.buildAWSConfig(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "https://athena.env.example", aws.ToString(d.athenaClient(cfg).Options().BaseEndpoint))
	assert.Equal(t, "https://sts.env.example", aws.ToString(d.stsClient(cfg).(*sts.Client).Options().BaseEndpoint))

	d.athenaEndpointURL = "https://athena.vpce.example"
	d.stsEndpointURL = "https://sts.vpce.example"
	assert.Equal(t, "https://athena.vpce.example", aws.ToString(d.athenaClient(cfg).Options().BaseEndpoint))
	assert.Equal(t, "https://sts.vpce.example", aws.ToString(d.stsClient(cfg).(*sts.Client).Options().BaseEndpoint))
}

func TestRoleARN_AssumesTheRole(t *testing.T) {
	stsClient := &mockSTS{}
	d := newConfigTestDB(t)
	d.roleARN = "arn:aws:iam::123456789012:role/dbt"
	d.roleExternalID = "ext-1"
	d.roleSessionName = "dbt-athena"
	d.roleDuration = 15 * time.Minute
	d.testSTSClient = stsClient

	cfg, err := d.buildAWSConfig(context.Background())
	require.NoError(t, err)
	creds, err := d.athenaClient(cfg).Options().Credentials.Retrieve(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "AKIDASSUMED", creds.AccessKeyID, "the Athena client signs with the assumed role")
	require.NotNil(t, stsClient.input)
	assert.Equal(t, "arn:aws:iam::123456789012:role/dbt", aws.ToString(stsClient.input.RoleArn))
	assert.Equal(t, "ext-1", aws.ToString(stsClient.input.ExternalId))
	assert.Equal(t, "dbt-athena", aws.ToString(stsClient.input.RoleSessionName))
	assert.EqualValues(t, 900, aws.ToInt32(stsClient.input.DurationSeconds))
}

func TestNoRoleARN_KeepsTheBaseCredentials(t *testing.T) {
	stsClient := &mockSTS{}
	d := newConfigTestDB(t)
	d.testSTSClient = stsClient

	cfg, err := d.buildAWSConfig(context.Background())
	require.NoError(t, err)
	creds, err := d.athenaClient(cfg).Options().Credentials.Retrieve(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "AKIDBASE", creds.AccessKeyID)
	assert.Nil(t, stsClient.input, "no AssumeRole without a role")
}

func TestPollInterval(t *testing.T) {
	mock := &mockAthenaClient{
		startQueryExecutionFn: func(_ context.Context, _ *athenaSDK.StartQueryExecutionInput, _ ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error) {
			return &athenaSDK.StartQueryExecutionOutput{QueryExecutionId: strp("exec-poll")}, nil
		},
		getQueryExecutionFn: succeedAfterN(3),
	}
	stmt := newTestStmt(t, mock)
	stmt.conn.db.pollInterval = 50 * time.Millisecond
	require.NoError(t, stmt.SetSqlQuery("DROP TABLE t"))

	start := time.Now()
	_, err := stmt.ExecuteUpdate(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err)
	// Three RUNNING answers, so three waits of the poll interval before SUCCEEDED;
	// the old fixed 500ms would take 1.5s.
	assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
}
