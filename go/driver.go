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

// Package athena is an ADBC Driver Implementation for AWS Athena.
package athena

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// OptionRegion is the AWS region (e.g. "us-east-1").
	OptionRegion = "athena.region"
	// OptionCatalog is the Glue catalog / dbt "database".
	OptionCatalog = "athena.catalog"
	// OptionSchema is the default Glue database / dbt "schema".
	OptionSchema = "athena.schema"
	// OptionOutputLocation is the S3 output location (s3://bucket/prefix/).
	OptionOutputLocation = "athena.output_location"
	// OptionWorkGroup is the Athena workgroup name.
	OptionWorkGroup = "athena.work_group"
	// OptionAuthType selects the AWS authentication method.
	OptionAuthType = "athena.auth_type"
	// OptionAccessKeyID is the AWS access key ID for static credentials.
	OptionAccessKeyID = "athena.aws.access_key_id"
	// OptionSecretKey is the AWS secret access key for static credentials.
	OptionSecretKey = "athena.aws.secret_access_key"
	// OptionSessionToken is the optional AWS session token.
	OptionSessionToken = "athena.aws.session_token"
	// OptionProfileName is the named AWS profile to use.
	OptionProfileName = "athena.aws.profile"
	// OptionRoleARN is an IAM role to assume on top of the credentials of the
	// selected auth type. Every AWS client of the driver uses the assumed role.
	OptionRoleARN = "athena.aws.role_arn"
	// OptionRoleExternalID is the external ID passed to AssumeRole.
	OptionRoleExternalID = "athena.aws.role_external_id"
	// OptionRoleSessionName is the AssumeRole session name.
	OptionRoleSessionName = "athena.aws.role_session_name"
	// OptionRoleDuration is the lifetime of the assumed role's credentials, as a
	// Go duration ("1h"); the SDK refreshes them before they expire.
	OptionRoleDuration = "athena.aws.role_duration"
	// OptionMaxAPIRetries is how many times the SDK retries a failed AWS API
	// request, the first attempt not included (SDK default 2). It applies to
	// the API calls only, never to a query that ran and failed.
	OptionMaxAPIRetries = "athena.aws.max_api_retries"
	// OptionAthenaEndpointURL overrides the endpoint of the Athena API, e.g. a
	// VPC interface endpoint without private DNS.
	OptionAthenaEndpointURL = "athena.aws.athena_endpoint_url"
	// OptionSTSEndpointURL overrides the endpoint of the STS API, used to assume
	// OptionRoleARN.
	OptionSTSEndpointURL = "athena.aws.sts_endpoint_url"
	// OptionPollInterval is the interval between query status checks, as a Go
	// duration (default "500ms").
	OptionPollInterval = "athena.poll_interval"

	// AuthTypeDefault uses the default AWS credential chain (env vars, instance profile, etc.).
	AuthTypeDefault = "iam"
	// AuthTypeAccessKey uses static key/secret credentials.
	AuthTypeAccessKey = "access_key"
	// AuthTypeProfile uses a named AWS shared-config profile.
	AuthTypeProfile = "profile"
)

var infoVendorVersion string
var driverVersion = "dev"

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			driverVersion = info.Main.Version
		}
		for _, dep := range info.Deps {
			if dep.Path == "github.com/aws/aws-sdk-go-v2/service/athena" {
				infoVendorVersion = fmt.Sprintf("aws-sdk-go-v2/service/athena %s", dep.Version)
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Athena ADBC driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Athena")
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Athena Driver",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
		adbc.InfoVendorVersion:   infoVendorVersion,
	})
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}

	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		authType:         AuthTypeDefault,
		pollInterval:     defaultPollInterval,
		maxAPIRetries:    -1,
	}

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
