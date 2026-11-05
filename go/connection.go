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
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	athenaTypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	glueSDK "github.com/aws/aws-sdk-go-v2/service/glue"
	glueTypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	athenaClient athenaClientAPI
	glueClient   glueClientAPI
	db           *databaseImpl

	// catalog and schema are per-connection copies of the database defaults,
	// so that SetCurrentCatalog/SetCurrentDbSchema on one connection does not
	// affect sibling connections opened from the same database.
	catalog string
	schema  string
}

func (c *connectionImpl) workGroup() *string {
	if c.db.workGroup == "" {
		return nil
	}
	return &c.db.workGroup
}

func (c *connectionImpl) Close(_ context.Context) error {
	c.athenaClient = nil
	c.db = nil
	return nil
}

func (c *connectionImpl) NewStatement(_ context.Context) (adbc.StatementWithContext, error) {
	return &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		conn:              c,
	}, nil
}

// GetTableSchema uses Athena's GetTableMetadata API to return an Arrow schema.
func (c *connectionImpl) GetTableSchema(ctx context.Context, catalogName *string, schemaName *string, tableName string) (*arrow.Schema, error) {
	if catalogName == nil || *catalogName == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "catalog is required for GetTableSchema",
		}
	}
	if schemaName == nil || *schemaName == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "schema is required for GetTableSchema",
		}
	}

	out, err := c.athenaClient.GetTableMetadata(ctx, &athenaSDK.GetTableMetadataInput{
		CatalogName:  catalogName,
		DatabaseName: schemaName,
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

func (c *connectionImpl) GetCurrentCatalog(_ context.Context) (string, error) {
	return c.catalog, nil
}

func (c *connectionImpl) GetCurrentDbSchema(_ context.Context) (string, error) {
	return c.schema, nil
}

func (c *connectionImpl) SetCurrentCatalog(_ context.Context, catalog string) error {
	c.catalog = catalog
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(_ context.Context, schema string) error {
	c.schema = schema
	return nil
}

// TableTypeLister interface implementation.

const (
	TableTypeExternal = "EXTERNAL_TABLE"
	TableTypeView     = "VIRTUAL_VIEW"
)

var knownTableTypes = []string{TableTypeExternal, TableTypeView}

func (c *connectionImpl) ListTableTypes(_ context.Context) ([]string, error) {
	return knownTableTypes, nil
}

// DbObjectsEnumerator interface implementation.

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	if catalogFilter != nil && *catalogFilter == "" {
		return []string{}, nil
	}

	wildcards, err := hasWildcards(catalogFilter)
	if err != nil {
		return nil, err
	}
	if catalogFilter == nil || wildcards {
		catalogPattern, err := likePatternToRegex(catalogFilter)
		if err != nil {
			return nil, err
		}
		if catalogPattern == nil {
			catalogPattern = regexp.MustCompile("^.*$")
		}
		return c.listCatalogs(ctx, catalogPattern)
	} else if *catalogFilter == "AwsDataCatalog" {
		return []string{*catalogFilter}, nil
	} else {
		catalog, err := c.checkCatalog(ctx, catalogFilter)
		if err != nil {
			return nil, err
		} else if catalog != nil {
			return []string{*catalog}, nil
		} else {
			return nil, nil
		}
	}
}

func (c *connectionImpl) checkCatalog(ctx context.Context, catalogName *string) (*string, error) {
	unescaped := unescapeLikePattern(*catalogName)

	athenaResponse, err := c.athenaClient.GetDataCatalog(ctx, &athenaSDK.GetDataCatalogInput{
		Name:      &unescaped,
		WorkGroup: c.workGroup(),
	})
	var invalidRequestException *athenaTypes.InvalidRequestException
	if err != nil {
		if !errors.As(err, &invalidRequestException) {
			return nil, err
		}
	} else {
		return athenaResponse.DataCatalog.Name, nil
	}

	glueResponse, err := c.glueClient.GetCatalog(ctx, &glueSDK.GetCatalogInput{
		CatalogId: &unescaped,
	})
	var entityNotFoundException *glueTypes.EntityNotFoundException
	if err != nil {
		if !errors.As(err, &entityNotFoundException) {
			return nil, err
		}
	} else if glueResponse.Catalog != nil && glueResponse.Catalog.CatalogId != nil {
		name := *glueResponse.Catalog.CatalogId
		if _, after, ok := strings.Cut(name, ":"); ok {
			name = after
		}
		return &name, nil
	}
	return nil, nil
}

func (c *connectionImpl) listCatalogs(ctx context.Context, catalogPattern *regexp.Regexp) ([]string, error) {
	catalogs, err := c.listAthenaCatalogs(ctx, catalogPattern)
	if err != nil {
		return nil, err
	}

	glueCatalogs, err := c.listGlueCatalogs(ctx, catalogPattern)
	if err != nil {
		return nil, err
	}
	catalogs = append(catalogs, glueCatalogs...)

	return catalogs, nil
}

func (c *connectionImpl) listAthenaCatalogs(ctx context.Context, catalogPattern *regexp.Regexp) ([]string, error) {
	listInput := &athenaSDK.ListDataCatalogsInput{
		WorkGroup: c.workGroup(),
	}
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
			if dc.CatalogName != nil && catalogPattern.MatchString(*dc.CatalogName) {
				catalogs = append(catalogs, *dc.CatalogName)
			}
		}
	}
	return catalogs, nil
}

func (c *connectionImpl) listGlueCatalogs(ctx context.Context, catalogPattern *regexp.Regexp) ([]string, error) {
	glueInput := &glueSDK.GetCatalogsInput{Recursive: true}
	glueOut, err := c.glueClient.GetCatalogs(ctx, glueInput)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("Glue GetCatalogs failed: %v", err),
		}
	}

	var catalogs []string
	for _, cat := range glueOut.CatalogList {
		if cat.CatalogId == nil {
			continue
		}
		name := *cat.CatalogId
		if _, after, ok := strings.Cut(name, ":"); ok {
			name = after
		}
		if catalogPattern.MatchString(name) {
			catalogs = append(catalogs, name)
		}
	}
	return catalogs, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	if catalog == "" || (schemaFilter != nil && *schemaFilter == "") {
		return []string{}, nil
	}

	schemaHasWildcards, err := hasWildcards(schemaFilter)
	if err != nil {
		return nil, err
	}
	if schemaFilter != nil && !schemaHasWildcards {
		return c.checkSchema(ctx, catalog, schemaFilter)
	}
	return c.listSchemas(ctx, catalog, schemaFilter)
}

func (c *connectionImpl) checkSchema(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	unescaped := unescapeLikePattern(*schemaFilter)

	out, err := c.athenaClient.GetDatabase(ctx, &athenaSDK.GetDatabaseInput{
		CatalogName:  &catalog,
		DatabaseName: &unescaped,
		WorkGroup:    c.workGroup(),
	})
	if err != nil {
		var metadataErr *athenaTypes.MetadataException
		if errors.As(err, &metadataErr) {
			return nil, nil
		}
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("GetDatabase failed: %v", err),
		}
	}
	if out.Database != nil && out.Database.Name != nil {
		return []string{*out.Database.Name}, nil
	}
	return nil, nil
}

func (c *connectionImpl) listSchemas(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	schemaPattern, err := likePatternToRegex(schemaFilter)
	if err != nil {
		return nil, err
	}
	if schemaPattern == nil {
		schemaPattern = regexp.MustCompile("^.*$")
	}
	input := &athenaSDK.ListDatabasesInput{
		CatalogName: &catalog,
		WorkGroup:   c.workGroup(),
	}
	paginator := athenaSDK.NewListDatabasesPaginator(c.athenaClient, input)

	var schemas []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			var metadataErr *athenaTypes.MetadataException
			if errors.As(err, &metadataErr) {
				return nil, nil
			}
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListDatabases failed: %v", err),
			}
		}
		for _, db := range page.DatabaseList {
			if db.Name != nil && schemaPattern.MatchString(*db.Name) {
				schemas = append(schemas, *db.Name)
			}
		}
	}
	return schemas, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalogName string, schemaName string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	if tableFilter != nil && *tableFilter == "" {
		return []driverbase.TableInfo{}, nil
	}

	tableHasWildcards, err := hasWildcards(tableFilter)
	if err != nil {
		return nil, err
	}
	if tableFilter != nil && !tableHasWildcards {
		return c.checkTable(ctx, catalogName, schemaName, tableFilter, columnFilter, includeColumns)
	}
	return c.listTables(ctx, catalogName, schemaName, tableFilter, columnFilter, includeColumns)
}

func (c *connectionImpl) checkTable(ctx context.Context, catalogName string, schemaName string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	unescaped := unescapeLikePattern(*tableFilter)

	out, err := c.athenaClient.GetTableMetadata(ctx, &athenaSDK.GetTableMetadataInput{
		CatalogName:  &catalogName,
		DatabaseName: &schemaName,
		TableName:    &unescaped,
		WorkGroup:    c.workGroup(),
	})
	if err != nil {
		var metadataErr *athenaTypes.MetadataException
		if errors.As(err, &metadataErr) {
			return nil, nil
		}
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("GetTableMetadata failed: %v", err),
		}
	}

	tbl := out.TableMetadata
	if tbl == nil || tbl.Name == nil {
		return nil, nil
	}

	ti := tableMetadataToTableInfo(tbl, columnFilter, includeColumns)
	return []driverbase.TableInfo{ti}, nil
}

func (c *connectionImpl) listTables(ctx context.Context, catalogName string, schemaName string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	input := &athenaSDK.ListTableMetadataInput{
		CatalogName:  &catalogName,
		DatabaseName: &schemaName,
		WorkGroup:    c.workGroup(),
	}
	if tableFilter != nil {
		tableFilterExpression, err := likePatternToRegex(tableFilter)
		if err != nil {
			return nil, err
		}
		expressionStr := tableFilterExpression.String()
		input.Expression = &expressionStr
	}

	paginator := athenaSDK.NewListTableMetadataPaginator(c.athenaClient, input)

	var tables []driverbase.TableInfo
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			var metadataErr *athenaTypes.MetadataException
			if errors.As(err, &metadataErr) {
				return nil, nil
			}
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListTableMetadata failed: %v", err),
			}
		}
		for _, tbl := range page.TableMetadataList {
			if tbl.Name == nil {
				continue
			}
			ti := tableMetadataToTableInfo(&tbl, columnFilter, includeColumns)
			tables = append(tables, ti)
		}
	}
	return tables, nil
}

func normalizeTableType(t *string) string {
	if t == nil || !slices.Contains(knownTableTypes, *t) {
		return TableTypeExternal
	}
	return *t
}

func tableMetadataToTableInfo(tbl *athenaTypes.TableMetadata, columnFilter *string, includeColumns bool) driverbase.TableInfo {
	tableType := normalizeTableType(tbl.TableType)

	ti := driverbase.TableInfo{
		TableName: *tbl.Name,
		TableType: tableType,
	}

	if includeColumns && (columnFilter == nil || *columnFilter != "") {
		columnPattern, _ := likePatternToRegex(columnFilter)
		if columnPattern == nil {
			columnPattern = regexp.MustCompile("^.*$")
		}
		cols := make([]driverbase.ColumnInfo, 0, len(tbl.Columns))
		for i, col := range tbl.Columns {
			colName := ""
			if col.Name != nil {
				colName = *col.Name
			}
			if !columnPattern.MatchString(colName) {
				continue
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

	return ti
}

func hasWildcards(likePattern *string) (bool, error) {
	if likePattern == nil {
		return false, nil
	}
	isEscape := false
	for i := 0; i < len(*likePattern); i++ {
		ch := (*likePattern)[i]
		if !isEscape && ch == '\\' {
			isEscape = true
		} else if !isEscape && (ch == '_' || ch == '%') {
			return true, nil
		} else {
			isEscape = false
		}
	}
	if isEscape {
		return false, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "pattern cannot end with an escape",
		}
	}
	return false, nil
}

func unescapeLikePattern(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			out.WriteByte(s[i])
		} else {
			out.WriteByte(s[i])
		}
	}
	return out.String()
}

func likePatternToRegex(likePattern *string) (*regexp.Regexp, error) {
	if likePattern == nil {
		return regexp.MustCompile("^.*$"), nil
	}
	pat := *likePattern
	var out strings.Builder
	out.Grow(len(pat) * 2)
	out.WriteString("(?i)^")

	isEscape := false
	for i := 0; i < len(pat); i++ {
		ch := pat[i]
		if ch == '\\' && !isEscape {
			isEscape = true
		} else if (ch == '%' || ch == '_') {
			if (isEscape) {
				out.WriteByte(ch)
				isEscape = false;
			} else {
				out.WriteByte('.')
				if ch == '%' {
					out.WriteByte('*')
					for i+1 < len(pat) && pat[i+1] == ch {
						i++
					}
				}
			}
		} else if (isEscape || strings.ContainsRune("?+.[]{}()^$|*\\<>=-!", rune(ch))) {
			out.WriteByte('\\')
			out.WriteByte(ch)
			isEscape = false;
		} else {
			out.WriteByte(ch)
		}
	}
	if isEscape {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "pattern cannot end with an escape",
		}
	}
	out.WriteByte('$')
	r, err := regexp.Compile(out.String())
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("could not compile pattern to regexp: %v", err),
		}
	}
	return r, nil
}

// athenaTypeToArrow converts an Athena column type string to an Arrow DataType.
func athenaTypeToArrow(t *string) arrow.DataType {
	if t == nil {
		return arrow.BinaryTypes.String
	}
	return athenaTypeStringToArrow(*t)
}
