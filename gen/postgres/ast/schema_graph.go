package ast

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ettle/strcase"
	"github.com/uthereal/scheme/genproto/spec/core/shared"
	"github.com/uthereal/scheme/genproto/spec/postgres"
)

// SchemaGraph handles resolving PostgreSQL data types into target generator
// types while tracking any required package imports and nested definitions
// (Enums, Domains, Composites).
type SchemaGraph struct {
	Imports    map[string]struct{}
	Enums      map[string]*EnumData
	Composites map[string]*ModelData
	Domains    map[string]*DomainData
	Models     map[string]*ModelData

	enumTally   map[string]int
	compTally   map[string]int
	domainTally map[string]int
}

// NewSchemaGraph creates a new schema graph with initialized maps and builds
// the entire model graph based on the provided schema.
func NewSchemaGraph(pgSchema *postgres.PostgresDatabase) (*SchemaGraph, error) {
	if pgSchema == nil {
		return nil, fmt.Errorf("postgres schema cannot be nil")
	}

	sg := &SchemaGraph{
		Imports:     make(map[string]struct{}),
		Enums:       make(map[string]*EnumData),
		Composites:  make(map[string]*ModelData),
		Domains:     make(map[string]*DomainData),
		Models:      make(map[string]*ModelData),
		enumTally:   make(map[string]int),
		compTally:   make(map[string]int),
		domainTally: make(map[string]int),
	}

	err := sg.buildModelGraph(pgSchema)
	if err != nil {
		return nil, err
	}

	return sg, nil
}

// AddImport registers a required import package.
func (sg *SchemaGraph) AddImport(pkg string) {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	sg.Imports[pkg] = struct{}{}
}

// ImportList returns a slice of all tracked imports.
func (sg *SchemaGraph) ImportList() []string {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	list := make([]string, 0, len(sg.Imports))
	for pkg := range sg.Imports {
		list = append(list, pkg)
	}
	sort.Strings(list)
	return list
}

// EnumList returns a slice of all mapped enums.
func (sg *SchemaGraph) EnumList() []*EnumData {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	list := make([]*EnumData, 0, len(sg.Enums))
	for _, v := range sg.Enums {
		list = append(list, v)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list
}

// CompositeList returns a slice of all mapped composites.
func (sg *SchemaGraph) CompositeList() []*ModelData {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	list := make([]*ModelData, 0, len(sg.Composites))
	for _, v := range sg.Composites {
		list = append(list, v)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].StructNameExported < list[j].StructNameExported
	})
	return list
}

// DomainList returns a slice of all mapped domains.
func (sg *SchemaGraph) DomainList() []*DomainData {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	list := make([]*DomainData, 0, len(sg.Domains))
	for _, v := range sg.Domains {
		list = append(list, v)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list
}

// ModelList returns a slice of all mapped models.
func (sg *SchemaGraph) ModelList() []*ModelData {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	list := make([]*ModelData, 0, len(sg.Models))
	for _, v := range sg.Models {
		list = append(list, v)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].StructNameExported < list[j].StructNameExported
	})
	return list
}

// buildModelGraph parses the entire PostgreSQL database, mapping all tables,
// columns, and relationships into a unified graph of ModelData structures.
func (sg *SchemaGraph) buildModelGraph(
	pgSchema *postgres.PostgresDatabase,
) error {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	if pgSchema == nil {
		return fmt.Errorf("postgres schema cannot be nil")
	}

	// Pass 1: Tally table name frequencies to resolve collisions
	tally := make(map[string]int)
	seenEnums := make(map[string]struct{})
	seenComps := make(map[string]struct{})
	seenDomains := make(map[string]struct{})

	var walkDataType func(dt *postgres.DataType, currentSchema string)
	walkDataType = func(dt *postgres.DataType, currentSchema string) {
		if dt == nil || dt.GetType() == nil {
			return
		}
		switch t := dt.GetType().(type) {
		case *postgres.DataType_EnumType:
			schema := t.EnumType.GetSchema()
			if schema == "" {
				schema = currentSchema
			}
			name := t.EnumType.GetName()
			key := schema + "." + name
			_, ok := seenEnums[key]
			if !ok {
				seenEnums[key] = struct{}{}
				sg.enumTally[name]++
			}
		case *postgres.DataType_CompositeType:
			schema := t.CompositeType.GetSchema()
			if schema == "" {
				schema = currentSchema
			}
			name := t.CompositeType.GetName()
			key := schema + "." + name
			_, ok := seenComps[key]
			if !ok {
				seenComps[key] = struct{}{}
				sg.compTally[name]++
			}
			for _, f := range t.CompositeType.GetFields() {
				walkDataType(f.GetType(), currentSchema)
			}
		case *postgres.DataType_DomainType:
			schema := t.DomainType.GetSchema()
			if schema == "" {
				schema = currentSchema
			}
			name := t.DomainType.GetName()
			key := schema + "." + name
			_, ok := seenDomains[key]
			if !ok {
				seenDomains[key] = struct{}{}
				sg.domainTally[name]++
			}
			if t.DomainType.GetBaseType() != nil {
				walkDataType(t.DomainType.GetBaseType(), currentSchema)
			}
		case *postgres.DataType_ArrayType:
			if t.ArrayType.GetElementType() != nil {
				walkDataType(t.ArrayType.GetElementType(), currentSchema)
			}
		case *postgres.DataType_CustomRangeType:
			if t.CustomRangeType.GetBaseType() != nil {
				walkDataType(t.CustomRangeType.GetBaseType(), currentSchema)
			}
		}
	}

	for _, schema := range pgSchema.GetSchemas() {
		schemaName := schema.GetName()
		for _, table := range schema.GetTables() {
			tally[table.GetName()]++
			for _, col := range table.GetColumns() {
				walkDataType(col.GetType(), schemaName)
			}
		}
	}

	// Pass 2: Map tables and columns into structs
	for _, schema := range pgSchema.GetSchemas() {
		schemaName := schema.GetName()
		for _, table := range schema.GetTables() {
			tableName := table.GetName()

			// Resolve naming collisions
			var structNameExported string
			if tally[tableName] > 1 {
				structNameExported = TableNameToStructName(schemaName + "_" + tableName)
			} else {
				structNameExported = TableNameToStructName(tableName)
			}
			structNamePrivate := strcase.ToGoCamel(structNameExported)
			columns := table.GetColumns()

			modelType := &ModelData{
				StructNameExported: structNameExported,
				StructNamePrivate:  structNamePrivate,
				TableName:          tableName,
				SchemaName:         schemaName,
				TableFullName:      "\"" + schemaName + "\".\"" + tableName + "\"",
				Fields:             make([]ModelDataField, 0, len(columns)),
				Edges:              make([]EdgeData, 0),
			}

			for _, col := range columns {
				goType, err := sg.MapColumnType(col, schemaName)
				if err != nil {
					return fmt.Errorf(
						"failed to map column %s -> %w", col.GetName(), err,
					)
				}

				colName := col.GetName()
				modelType.Fields = append(modelType.Fields, ModelDataField{
					Name:       ColumnNameToFieldName(colName),
					ColumnName: colName,
					Type:       goType,
					ColumnType: sg.getColumnCategory(col.GetType(), schemaName),
				})
			}

			fullKey := schemaName + "." + tableName
			sg.Models[fullKey] = modelType
		}
	}

	// Pass 3: Hydrate relationships (Edges)
	for _, schema := range pgSchema.GetSchemas() {
		schemaName := schema.GetName()
		for _, table := range schema.GetTables() {
			tableName := table.GetName()
			fullKey := schemaName + "." + tableName

			sourceModel, ok := sg.Models[fullKey]
			if !ok {
				return fmt.Errorf("failed to find model for table %s", fullKey)
			}

			for _, rel := range table.GetRelations() {
				targetRaw := rel.GetTargetTable()
				var targetSchema, targetTable string
				if strings.Contains(targetRaw, ".") {
					parts := strings.SplitN(targetRaw, ".", 2)
					targetSchema, targetTable = parts[0], parts[1]
				} else {
					targetSchema, targetTable = schemaName, targetRaw
				}
				targetFullKey := targetSchema + "." + targetTable

				targetModel, ok := sg.Models[targetFullKey]
				if !ok {
					continue
				}

				targetStructName := targetModel.StructNameExported

				relCols := rel.GetColumns()
				localCols := make([]string, 0, len(relCols))
				targetCols := make([]string, 0, len(relCols))
				localFields := make([]string, 0, len(relCols))
				targetFields := make([]string, 0, len(relCols))

				for _, colMap := range relCols {
					localCols = append(localCols, colMap.GetSourceColumn())
					targetCols = append(targetCols, colMap.GetTargetColumn())
					localFields = append(
						localFields, ColumnNameToFieldName(colMap.GetSourceColumn()),
					)
					targetFields = append(
						targetFields, ColumnNameToFieldName(colMap.GetTargetColumn()),
					)
				}

				relType := rel.GetType()
				isSlice := relType == shared.RelationType_RELATION_TYPE_ONE_TO_MANY ||
					relType == shared.RelationType_RELATION_TYPE_MANY_TO_MANY

				sourceModel.Edges = append(sourceModel.Edges, EdgeData{
					Name:          targetStructName,
					NameLower:     strings.ToLower(targetStructName),
					TargetModel:   targetStructName,
					IsSlice:       isSlice,
					LocalColumns:  localCols,
					TargetColumns: targetCols,
					LocalFields:   localFields,
					TargetFields:  targetFields,
					IsBackRef:     false,
				})

				pluralName := strcase.ToGoPascal(tableName)
				if tally[tableName] > 1 {
					pluralName = strcase.ToGoPascal(schemaName + "_" + tableName)
				} else {
					pluralName = strcase.ToGoPascal(tableName)
				}

				backRefIsSlice := relType != shared.RelationType_RELATION_TYPE_ONE_TO_ONE
				targetModel.Edges = append(targetModel.Edges, EdgeData{
					Name:          pluralName,
					NameLower:     strings.ToLower(pluralName),
					TargetModel:   sourceModel.StructNameExported,
					IsSlice:       backRefIsSlice,
					LocalColumns:  targetCols,
					TargetColumns: localCols,
					LocalFields:   targetFields,
					TargetFields:  localFields,
					IsBackRef:     true,
				})
			}
		}
	}

	return nil
}

// MapColumnType maps a PostgreSQL column definition to its corresponding
// Go type, handling nullability.
func (sg *SchemaGraph) MapColumnType(
	col *postgres.Column, currentSchema string,
) (string, error) {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	dt := col.GetType()
	if dt == nil {
		return "", errors.New("column type cannot be nil")
	}

	goType, err := sg.MapDataType(dt, currentSchema)
	if err != nil {
		return "", err
	}

	if col.GetIsNullable() && !sg.IsNillableGoType(dt) {
		goType = "*" + goType
	}

	return goType, nil
}

// IsNillableGoType recursively determines if the underlying Go type for a given
// PostgreSQL data type is natively nillable (like a slice or json.RawMessage).
func (sg *SchemaGraph) IsNillableGoType(dt *postgres.DataType) bool {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	if dt == nil {
		return false
	}
	switch t := dt.GetType().(type) {
	case *postgres.DataType_ByteaType,
		*postgres.DataType_JsonType,
		*postgres.DataType_JsonbType,
		*postgres.DataType_JsonpathType,
		*postgres.DataType_ArrayType:
		return true
	case *postgres.DataType_DomainType:
		if t.DomainType != nil {
			return sg.IsNillableGoType(t.DomainType.GetBaseType())
		}
	}
	return false
}

// MapDataType maps a PostgreSQL data type to its corresponding Go type,
// recursively handling nested types and registering required imports and
// definitions.
func (sg *SchemaGraph) MapDataType(
	dt *postgres.DataType, currentSchema string,
) (string, error) {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	if dt == nil {
		return "", errors.New("postgres.DataType cannot be nil")
	}
	switch t := dt.GetType().(type) {
	// 1. Numeric Types
	case *postgres.DataType_SmallintType,
		*postgres.DataType_SmallserialType:
		return "int16", nil
	case *postgres.DataType_IntegerType,
		*postgres.DataType_SerialType:
		return "int32", nil
	case *postgres.DataType_BigintType,
		*postgres.DataType_BigserialType:
		return "int64", nil
	case *postgres.DataType_DecimalType,
		*postgres.DataType_NumericType,
		*postgres.DataType_MoneyType:
		sg.AddImport("github.com/shopspring/decimal")
		return "decimal.Decimal", nil
	case *postgres.DataType_RealType:
		return "float32", nil
	case *postgres.DataType_DoublePrecisionType:
		return "float64", nil

	// 2. Character & String Types
	case *postgres.DataType_VarcharType,
		*postgres.DataType_CharType,
		*postgres.DataType_TextType:
		return "string", nil

	// 3. Binary Data Types
	case *postgres.DataType_ByteaType:
		return "[]byte", nil

	// 4. Date/Time Types
	case *postgres.DataType_TimestampType,
		*postgres.DataType_TimestamptzType,
		*postgres.DataType_DateType,
		*postgres.DataType_TimeType,
		*postgres.DataType_TimetzType:
		sg.AddImport("time")
		return "time.Time", nil
	case *postgres.DataType_IntervalType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Interval", nil

	// 5. Boolean Type
	case *postgres.DataType_BooleanType:
		return "bool", nil

	// 6. Enumerated Types
	case *postgres.DataType_EnumType:
		targetSchema := t.EnumType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.EnumType.GetName()
		enumName := strcase.ToGoPascal(name)
		if sg.enumTally[name] > 1 {
			enumName = strcase.ToGoPascal(targetSchema + "_" + name)
		}
		_, seen := sg.Enums[enumName]
		if !seen {
			enum, err := sg.ExtractEnumDefinition(t, enumName)
			if err != nil {
				return "", err
			}
			sg.Enums[enumName] = &enum
		}
		return enumName, nil

	// 7. Geometric Types
	case *postgres.DataType_PointType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Point", nil
	case *postgres.DataType_LineType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Line", nil
	case *postgres.DataType_LsegType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Lseg", nil
	case *postgres.DataType_BoxType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Box", nil
	case *postgres.DataType_PathType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Path", nil
	case *postgres.DataType_PolygonType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Polygon", nil
	case *postgres.DataType_CircleType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Circle", nil

	// 8. Network Address Types
	case *postgres.DataType_InetType:
		sg.AddImport("net/netip")
		return "netip.Addr", nil
	case *postgres.DataType_CidrType:
		sg.AddImport("net/netip")
		return "netip.Prefix", nil
	case *postgres.DataType_MacaddrType,
		*postgres.DataType_Macaddr8Type:
		sg.AddImport("net")
		return "net.HardwareAddr", nil

	// 9. Bit String Types
	case *postgres.DataType_BitType,
		*postgres.DataType_BitVaryingType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Bits", nil

	// 10. Text Search Types
	case *postgres.DataType_TsvectorType,
		*postgres.DataType_TsqueryType:
		return "string", nil

	// 11. UUID Type
	case *postgres.DataType_UuidType:
		sg.AddImport("github.com/google/uuid")
		return "uuid.UUID", nil

	// 12. XML Type
	case *postgres.DataType_XmlType:
		return "string", nil

	// 13. JSON Types
	case *postgres.DataType_JsonType,
		*postgres.DataType_JsonbType,
		*postgres.DataType_JsonpathType:
		sg.AddImport("encoding/json")
		return "json.RawMessage", nil

	// 14. Arrays
	case *postgres.DataType_ArrayType:
		elemType, err := sg.MapDataType(t.ArrayType.GetElementType(), currentSchema)
		if err != nil {
			return "", fmt.Errorf("failed to map array element type -> %w", err)
		}

		dims := int32(1)
		if t.ArrayType != nil && t.ArrayType.Dimensions != nil {
			dims = *t.ArrayType.Dimensions
		}
		if dims < 1 || dims > 10 {
			return "", fmt.Errorf("invalid array dimensions -> %d", dims)
		}

		return strings.Repeat("[]", int(dims)) + elemType, nil

	// 15. Composite / Range / Domain Types
	case *postgres.DataType_CompositeType:
		targetSchema := t.CompositeType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.CompositeType.GetName()
		compName := "Composite" + strcase.ToGoPascal(name)
		if sg.compTally[name] > 1 {
			compName = "Composite" + strcase.ToGoPascal(targetSchema+"_"+name)
		}
		_, seen := sg.Composites[compName]
		if !seen {
			comp, err := sg.ExtractCompositeDefinition(t, compName, currentSchema)
			if err != nil {
				return "", err
			}
			sg.Composites[compName] = &comp
		}
		return compName, nil

	case *postgres.DataType_Int4RangeType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Range[int32]", nil
	case *postgres.DataType_Int8RangeType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return "pgtype.Range[int64]", nil
	case *postgres.DataType_NumRangeType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		sg.AddImport("github.com/shopspring/decimal")
		return "pgtype.Range[decimal.Decimal]", nil
	case *postgres.DataType_TsRangeType,
		*postgres.DataType_TstzRangeType,
		*postgres.DataType_DateRangeType:
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		sg.AddImport("time")
		return "pgtype.Range[time.Time]", nil
	case *postgres.DataType_CustomRangeType:
		baseType := t.CustomRangeType.GetBaseType()
		baseGoType, err := sg.MapDataType(baseType, currentSchema)
		if err != nil {
			return "", fmt.Errorf("failed to map custom range base type -> %w", err)
		}
		sg.AddImport("github.com/jackc/pgx/v5/pgtype")
		return fmt.Sprintf("pgtype.Range[%s]", baseGoType), nil
	case *postgres.DataType_DomainType:
		targetSchema := t.DomainType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.DomainType.GetName()
		domainName := "Domain" + strcase.ToGoPascal(name)
		if sg.domainTally[name] > 1 {
			domainName = "Domain" + strcase.ToGoPascal(targetSchema+"_"+name)
		}
		_, seen := sg.Domains[domainName]
		if !seen {
			dom, err := sg.ExtractDomainDefinition(t, domainName, currentSchema)
			if err != nil {
				return "", err
			}
			sg.Domains[domainName] = &dom
		}
		return domainName, nil

	// 16. OID & pg_lsn
	case *postgres.DataType_OidType:
		return "uint32", nil
	case *postgres.DataType_PgLsnType:
		return "uint64", nil
	default:
		return "", fmt.Errorf("unsupported postgres data type -> %T", t)
	}
}

// ExtractEnumDefinition extracts the definition of a PostgreSQL ENUM type
// into an EnumData structure.
func (sg *SchemaGraph) ExtractEnumDefinition(
	t *postgres.DataType_EnumType, enumName string,
) (enum EnumData, err error) {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	if t == nil || t.EnumType == nil {
		return EnumData{}, errors.New("enum type definition cannot be nil")
	}
	if enumName == "" {
		return EnumData{}, errors.New("enum name cannot be empty")
	}

	enum = EnumData{
		Name:   enumName,
		Values: make([]EnumDataValue, 0, len(t.EnumType.GetValues())),
	}
	for _, val := range t.EnumType.GetValues() {
		enum.Values = append(enum.Values, EnumDataValue{
			Name:  enumName + strcase.ToGoPascal(val),
			Value: val,
		})
	}
	return enum, nil
}

// ExtractCompositeDefinition extracts the definition of a PostgreSQL composite
// type into a ModelData structure.
func (sg *SchemaGraph) ExtractCompositeDefinition(
	t *postgres.DataType_CompositeType, compName string, currentSchema string,
) (ModelData, error) {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	if t == nil || t.CompositeType == nil {
		return ModelData{}, errors.New("composite type definition cannot be nil")
	}
	if compName == "" {
		return ModelData{}, errors.New("composite name cannot be empty")
	}

	compositeModel := ModelData{
		StructNameExported: compName,
		StructNamePrivate:  strcase.ToGoCamel(compName),
		Fields: make(
			[]ModelDataField, 0, len(t.CompositeType.GetFields()),
		),
	}

	for _, field := range t.CompositeType.GetFields() {
		fGoType, err := sg.MapDataType(field.GetType(), currentSchema)
		if err != nil {
			return ModelData{}, fmt.Errorf(
				"failed to map composite field %s -> %w", field.GetName(), err,
			)
		}

		compositeModel.Fields = append(compositeModel.Fields, ModelDataField{
			Name:       ColumnNameToFieldName(field.GetName()),
			ColumnName: field.GetName(),
			Type:       fGoType,
			ColumnType: sg.getColumnCategory(field.GetType(), currentSchema),
		})
	}
	return compositeModel, nil
}

// ExtractDomainDefinition extracts the definition of a PostgreSQL domain type
// into a DomainData structure.
func (sg *SchemaGraph) ExtractDomainDefinition(
	t *postgres.DataType_DomainType, domainName string, currentSchema string,
) (DomainData, error) {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	if t == nil || t.DomainType == nil {
		return DomainData{}, errors.New("domain type definition cannot be nil")
	}
	if domainName == "" {
		return DomainData{}, errors.New("domain name cannot be empty")
	}

	baseGoType, err := sg.MapDataType(t.DomainType.GetBaseType(), currentSchema)
	if err != nil {
		return DomainData{}, fmt.Errorf(
			"failed to map domain %s base type -> %w",
			t.DomainType.GetName(), err,
		)
	}

	dom := DomainData{
		Name:     domainName,
		BaseType: baseGoType,
	}
	return dom, nil
}

// getColumnCategory resolves the ColumnCategory for a given DataType, properly
// reflecting any collision-safe naming rules applied during parsing.
func (sg *SchemaGraph) getColumnCategory(
	dt *postgres.DataType, currentSchema string,
) ColumnCategory {
	if dt == nil || dt.GetType() == nil {
		return ColumnCategory{Name: colTypeUnsupported}
	}
	switch t := dt.GetType().(type) {
	case *postgres.DataType_SmallintType,
		*postgres.DataType_SmallserialType:
		return ColumnCategory{Name: colTypeNumber, Type: "int16"}
	case *postgres.DataType_IntegerType,
		*postgres.DataType_SerialType:
		return ColumnCategory{Name: colTypeNumber, Type: "int32"}
	case *postgres.DataType_BigintType,
		*postgres.DataType_BigserialType:
		return ColumnCategory{Name: colTypeNumber, Type: "int64"}
	case *postgres.DataType_DecimalType,
		*postgres.DataType_NumericType,
		*postgres.DataType_MoneyType:
		return ColumnCategory{Name: colTypeNumber, Type: "decimal.Decimal"}
	case *postgres.DataType_RealType:
		return ColumnCategory{Name: colTypeNumber, Type: "float32"}
	case *postgres.DataType_DoublePrecisionType:
		return ColumnCategory{Name: colTypeNumber, Type: "float64"}
	case *postgres.DataType_OidType:
		return ColumnCategory{Name: colTypeNumber, Type: "uint32"}
	case *postgres.DataType_PgLsnType:
		return ColumnCategory{Name: colTypeNumber, Type: "uint64"}
	case *postgres.DataType_VarcharType,
		*postgres.DataType_CharType,
		*postgres.DataType_TextType,
		*postgres.DataType_TsvectorType,
		*postgres.DataType_TsqueryType,
		*postgres.DataType_XmlType:
		return ColumnCategory{Name: colTypeString, Type: "string"}
	case *postgres.DataType_BooleanType:
		return ColumnCategory{Name: colTypeBoolean, Type: "bool"}
	case *postgres.DataType_TimestampType,
		*postgres.DataType_TimestamptzType,
		*postgres.DataType_DateType,
		*postgres.DataType_TimeType,
		*postgres.DataType_TimetzType:
		return ColumnCategory{Name: colTypeTime, Type: "time.Time"}
	case *postgres.DataType_IntervalType:
		return ColumnCategory{Name: colTypeTime, Type: "pgtype.Interval"}
	case *postgres.DataType_ByteaType:
		return ColumnCategory{Name: colTypeByte, Type: "[]byte"}
	case *postgres.DataType_EnumType:
		targetSchema := t.EnumType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.EnumType.GetName()
		enumName := strcase.ToGoPascal(name)
		if sg.enumTally[name] > 1 {
			enumName = strcase.ToGoPascal(targetSchema + "_" + name)
		}
		return ColumnCategory{
			Name: colTypeEnum,
			Type: enumName,
		}
	case *postgres.DataType_UuidType:
		return ColumnCategory{Name: colTypeUUID, Type: "uuid.UUID"}
	case *postgres.DataType_JsonType,
		*postgres.DataType_JsonbType,
		*postgres.DataType_JsonpathType:
		return ColumnCategory{Name: colTypeJSON, Type: "json.RawMessage"}
	case *postgres.DataType_ArrayType:
		return ColumnCategory{Name: colTypeArray, Type: "any"}
	case *postgres.DataType_InetType,
		*postgres.DataType_CidrType:
		return ColumnCategory{Name: colTypeNetworkAddress, Type: "netip.Addr"}
	case *postgres.DataType_MacaddrType,
		*postgres.DataType_Macaddr8Type:
		return ColumnCategory{
			Name: colTypeNetworkAddress,
			Type: "net.HardwareAddr",
		}
	case *postgres.DataType_PointType:
		return ColumnCategory{Name: colTypeGeometric, Type: "pgtype.Point"}
	case *postgres.DataType_LineType:
		return ColumnCategory{Name: colTypeGeometric, Type: "pgtype.Line"}
	case *postgres.DataType_LsegType:
		return ColumnCategory{Name: colTypeGeometric, Type: "pgtype.Lseg"}
	case *postgres.DataType_BoxType:
		return ColumnCategory{Name: colTypeGeometric, Type: "pgtype.Box"}
	case *postgres.DataType_PathType:
		return ColumnCategory{Name: colTypeGeometric, Type: "pgtype.Path"}
	case *postgres.DataType_PolygonType:
		return ColumnCategory{Name: colTypeGeometric, Type: "pgtype.Polygon"}
	case *postgres.DataType_CircleType:
		return ColumnCategory{Name: colTypeGeometric, Type: "pgtype.Circle"}
	case *postgres.DataType_Int4RangeType:
		return ColumnCategory{Name: colTypeRange, Type: "pgtype.Range[int32]"}
	case *postgres.DataType_Int8RangeType:
		return ColumnCategory{Name: colTypeRange, Type: "pgtype.Range[int64]"}
	case *postgres.DataType_NumRangeType:
		return ColumnCategory{
			Name: colTypeRange,
			Type: "pgtype.Range[decimal.Decimal]",
		}
	case *postgres.DataType_TsRangeType,
		*postgres.DataType_TstzRangeType,
		*postgres.DataType_DateRangeType:
		return ColumnCategory{
			Name: colTypeRange,
			Type: "pgtype.Range[time.Time]",
		}
	case *postgres.DataType_CustomRangeType:
		return ColumnCategory{Name: colTypeRange, Type: "any"}
	case *postgres.DataType_CompositeType:
		targetSchema := t.CompositeType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.CompositeType.GetName()
		compName := strcase.ToGoPascal(name)
		if sg.compTally[name] > 1 {
			compName = strcase.ToGoPascal(targetSchema + "_" + name)
		}
		return ColumnCategory{
			Name: columnCategoryName(string(colTypeComposite) + compName + "Column"),
			Type: "Composite" + compName,
		}
	case *postgres.DataType_DomainType:
		targetSchema := t.DomainType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.DomainType.GetName()
		domainName := strcase.ToGoPascal(name)
		if sg.domainTally[name] > 1 {
			domainName = strcase.ToGoPascal(targetSchema + "_" + name)
		}
		baseCat := sg.getColumnCategory(t.DomainType.GetBaseType(), currentSchema)
		baseCat.Type = "Domain" + domainName
		return baseCat
	case *postgres.DataType_BitType,
		*postgres.DataType_BitVaryingType:
		return ColumnCategory{Name: colTypeBitString, Type: "pgtype.Bits"}
	default:
		return ColumnCategory{Name: colTypeUnsupported}
	}
}
