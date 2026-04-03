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
	"fmt"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	athenaClient *athenaSDK.Client
	db           *databaseImpl

	// catalog and schema are per-connection copies of the database defaults,
	// so that SetCurrentCatalog/SetCurrentDbSchema on one connection does not
	// affect sibling connections opened from the same database.
	catalog string
	schema  string
}

func (c *connectionImpl) Close() error {
	c.athenaClient = nil
	c.db = nil
	return nil
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		conn:              c,
	}, nil
}

// GetTableSchema uses Athena's GetTableMetadata API to return an Arrow schema.
func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	cat := c.db.catalog
	if catalog != nil && *catalog != "" {
		cat = *catalog
	}
	sch := c.db.schema
	if dbSchema != nil && *dbSchema != "" {
		sch = *dbSchema
	}

	if cat == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "catalog is required for GetTableSchema",
		}
	}
	if sch == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "schema is required for GetTableSchema",
		}
	}

	out, err := c.athenaClient.GetTableMetadata(ctx, &athenaSDK.GetTableMetadataInput{
		CatalogName:  &cat,
		DatabaseName: &sch,
		TableName:    &tableName,
	})
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("GetTableMetadata failed: %v", err),
		}
	}

	fields := make([]arrow.Field, 0, len(out.TableMetadata.Columns))
	for _, col := range out.TableMetadata.Columns {
		name := ""
		if col.Name != nil {
			name = *col.Name
		}
		dt := athenaTypeToArrow(col.Type)
		fields = append(fields, arrow.Field{Name: name, Type: dt, Nullable: true})
	}

	return arrow.NewSchema(fields, nil), nil
}

// CurrentNamespacer interface implementation.

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.db.catalog, nil
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.db.schema, nil
}

func (c *connectionImpl) SetCurrentCatalog(catalog string) error {
	c.db.catalog = catalog
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	c.db.schema = schema
	return nil
}

// TableTypeLister interface implementation.

func (c *connectionImpl) ListTableTypes(_ context.Context) ([]string, error) {
	return []string{"EXTERNAL_TABLE", "MANAGED_TABLE", "VIRTUAL_VIEW"}, nil
}

// DbObjectsEnumerator interface implementation.

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	if c.db.catalog != "" {
		if catalogFilter != nil && *catalogFilter != "" && c.db.catalog != *catalogFilter {
			return nil, nil
		}
		return []string{c.db.catalog}, nil
	}

	listInput := &athenaSDK.ListDataCatalogsInput{}
	paginator := athenaSDK.NewListDataCatalogsPaginator(c.athenaClient, listInput)

	var catalogs []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListDataCatalogs failed: %v", err),
			}
		}
		for _, dc := range page.DataCatalogsSummary {
			if dc.CatalogName == nil {
				continue
			}
			if catalogFilter != nil && *catalogFilter != "" && *dc.CatalogName != *catalogFilter {
				continue
			}
			catalogs = append(catalogs, *dc.CatalogName)
		}
	}
	return catalogs, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	input := &athenaSDK.ListDatabasesInput{
		CatalogName: &catalog,
	}
	paginator := athenaSDK.NewListDatabasesPaginator(c.athenaClient, input)

	var schemas []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListDatabases failed: %v", err),
			}
		}
		for _, db := range page.DatabaseList {
			if db.Name == nil {
				continue
			}
			if schemaFilter != nil && *schemaFilter != "" && *db.Name != *schemaFilter {
				continue
			}
			schemas = append(schemas, *db.Name)
		}
	}
	return schemas, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, _ *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	input := &athenaSDK.ListTableMetadataInput{
		CatalogName:  &catalog,
		DatabaseName: &schema,
	}
	if tableFilter != nil && *tableFilter != "" {
		input.Expression = tableFilter
	}

	paginator := athenaSDK.NewListTableMetadataPaginator(c.athenaClient, input)

	var tables []driverbase.TableInfo
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListTableMetadata failed: %v", err),
			}
		}
		for _, tbl := range page.TableMetadataList {
			if tbl.Name == nil {
				continue
			}
			tableType := "EXTERNAL_TABLE"
			if tbl.TableType != nil {
				tableType = *tbl.TableType
			}

			ti := driverbase.TableInfo{
				TableName: *tbl.Name,
				TableType: tableType,
			}

			if includeColumns {
				cols := make([]driverbase.ColumnInfo, 0, len(tbl.Columns))
				for i, col := range tbl.Columns {
					colName := ""
					if col.Name != nil {
						colName = *col.Name
					}
					typeName := ""
					if col.Type != nil {
						typeName = *col.Type
					}
					pos := int32(i + 1)
					cols = append(cols, driverbase.ColumnInfo{
						ColumnName:      colName,
						OrdinalPosition: &pos,
						XdbcTypeName:    &typeName,
					})
				}
				ti.TableColumns = cols
			}

			tables = append(tables, ti)
		}
	}
	return tables, nil
}

// athenaTypeToArrow converts an Athena column type string to an Arrow DataType.
func athenaTypeToArrow(t *string) arrow.DataType {
	if t == nil {
		return arrow.BinaryTypes.String
	}
	return athenaTypeStringToArrow(*t)
}
