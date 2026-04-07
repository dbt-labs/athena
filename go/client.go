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

	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
)

// athenaClientAPI abstracts the AWS Athena SDK client to allow injection of
// test doubles. *athenaSDK.Client satisfies this interface implicitly.
//
// Each paginator constructor in the SDK (e.g. NewGetQueryResultsPaginator)
// accepts a single-method interface for its operation; athenaClientAPI is a
// superset of all of them, so any value of this type can be passed directly
// to those constructors.
type athenaClientAPI interface {
	GetTableMetadata(ctx context.Context, params *athenaSDK.GetTableMetadataInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetTableMetadataOutput, error)
	ListDataCatalogs(ctx context.Context, params *athenaSDK.ListDataCatalogsInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListDataCatalogsOutput, error)
	ListDatabases(ctx context.Context, params *athenaSDK.ListDatabasesInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListDatabasesOutput, error)
	ListTableMetadata(ctx context.Context, params *athenaSDK.ListTableMetadataInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.ListTableMetadataOutput, error)
	StartQueryExecution(ctx context.Context, params *athenaSDK.StartQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.StartQueryExecutionOutput, error)
	StopQueryExecution(ctx context.Context, params *athenaSDK.StopQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.StopQueryExecutionOutput, error)
	GetQueryExecution(ctx context.Context, params *athenaSDK.GetQueryExecutionInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryExecutionOutput, error)
	GetQueryResults(ctx context.Context, params *athenaSDK.GetQueryResultsInput, optFns ...func(*athenaSDK.Options)) (*athenaSDK.GetQueryResultsOutput, error)
}
