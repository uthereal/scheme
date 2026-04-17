package ast

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ettle/strcase"
	"github.com/uthereal/scheme/gen"
	"github.com/uthereal/scheme/genproto/core"
	"github.com/uthereal/scheme/genproto/postgres"
)

// NewSchemaGraphGo creates a Go-specific representation of the generic
// SchemaGraph. This specialized graph is used by Go templates to generate
// type-safe code, incorporating language-specific primitives, imports,
// and naming conventions that are not present in the generic AST.
//
// It performs three distinct passes to ensure models and their relationships
// are correctly initialized and mapped to Go primitives:
//
//  1. Pass 1: Shell Object Stubbing. It creates the initial *ModelGo wrapper
//     for every table model and pre-registers them in a hash map. This ensures
//     all Go models are referenceable via O(1) lookups.
//
//  2. Pass 2: Field Type Resolution. It iterates over the generated shells
//     and correctly maps every native column (Fields) into its respective
//     Go primitive or standard library struct (e.g. uuid.UUID, time.Time).
//
//  3. Pass 3: Edge (Relationship) Hydration. With all fields fully mapped,
//     this pass iterates over the models again to map their foreign-key
//     relationships (Edges) pointing securely to their already-populated
//     target model structures, enabling O(1) foreign key field evaluations.
func NewSchemaGraphGo(
	db *core.Database,
	graph *SchemaGraph,
	lang gen.Language,
) (*SchemaGraphGo, error) {
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	if graph == nil {
		return nil, errors.New("graph cannot be nil")
	}

	if lang.Validate == nil {
		panic("language validation function cannot be nil")
	}

	err := lang.Validate(lang.Options)
	if err != nil {
		return nil, fmt.Errorf("failed to validate language -> %w", err)
	}

	goPkgName := db.GetName()
	if goPkgName == "" {
		return nil, errors.New("database name cannot be empty")
	}
	goPkgName = strcase.ToSnake(goPkgName)

	goPkgPath := ""
	if lang.Options.GoPackagePath != "" {
		goPkgPath = lang.Options.GoPackagePath + "/postgres/" +
			goPkgName
	}

	sgg := &SchemaGraphGo{
		Graph:            graph,
		GoPkgName:        goPkgName,
		GoPkgPath:        goPkgPath,
		Imports:          make(map[string]struct{}),
		Models:           make([]*ModelGo, 0, len(graph.Models)),
		Enums:            make([]*EnumGo, 0),
		Domains:          make([]*DomainGo, 0),
		Composites:       make([]*CompositeGo, 0),
		ActiveCategories: make(map[string]bool),
	}

	// Pass 1: Shell Object Stubbing.
	// Pre-build a map for model resolution by PascalCase name to avoid
	// O(M^2) latency when processing edges later.
	modelGoByName := make(map[string]*ModelGo, len(graph.Models))

	for _, m := range graph.Models {
		modelGo := &ModelGo{
			Model:             m,
			StructNamePrivate: strcase.ToGoCamel(m.Name),
			Fields:            make([]*FieldGo, 0, len(m.Fields)),
			Edges:             make([]*EdgeGo, 0, len(m.Edges)),
		}
		sgg.Models = append(sgg.Models, modelGo)
		modelGoByName[m.Name] = modelGo
	}
	slices.SortFunc(sgg.Models, func(a *ModelGo, b *ModelGo) int {
		return cmp.Compare(a.Name, b.Name)
	})

	// Pass 2: Field Type Resolution.
	// Map generic postgres types to specific Go types (e.g. 'int32',
	// 'uuid.UUID') and calculate nullability pointers before attempting
	// to resolve any relational edges.
	for _, modelGo := range sgg.Models {
		m := modelGo.Model
		for _, f := range m.Fields {
			goType, isPtr, err := sgg.mapColumnType(f, m.SchemaName)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to map column type -> %w",
					err,
				)
			}
			modelGo.Fields = append(modelGo.Fields, &FieldGo{
				Field:      f,
				GoBaseType: goType,
				IsPtr:      isPtr,
				Category: sgg.getColumnCategory(
					f.Type, m.SchemaName,
				),
			})
		}
	}

	// Pass 3: Edge (Relationship) Hydration.
	// Resolve cross-model foreign key relationships. This must occur after
	// Pass 2 because resolving an Edge relies on knowing the exact Go field
	// types and pointer states of both the local and target models.
	for _, modelGo := range sgg.Models {
		m := modelGo.Model
		for _, e := range m.Edges {
			targetModelGo := modelGoByName[e.TargetModel]
			eg := sgg.resolveEdgeGo(modelGo, e, targetModelGo)
			modelGo.Edges = append(modelGo.Edges, eg)
		}
	}
	for _, e := range graph.Enums {
		sgg.Enums = append(sgg.Enums, &EnumGo{Enum: e})
	}
	slices.SortFunc(sgg.Enums, func(a *EnumGo, b *EnumGo) int {
		return cmp.Compare(a.Name, b.Name)
	})

	for _, d := range graph.Domains {
		baseTypeGo, err := sgg.mapDataType(d.BaseType, "")
		if err != nil {
			return nil, fmt.Errorf(
				"failed to map domain base type for %s -> %w",
				d.Name, err,
			)
		}
		sgg.Domains = append(sgg.Domains, &DomainGo{
			Domain:     d,
			BaseGoType: baseTypeGo,
		})
	}
	slices.SortFunc(sgg.Domains, func(a *DomainGo, b *DomainGo) int {
		return cmp.Compare(a.Name, b.Name)
	})

	for _, c := range graph.Composites {
		compGo := &ModelGo{
			Model:             (*Model)(c),
			StructNamePrivate: strcase.ToGoCamel(c.Name),
			Fields:            make([]*FieldGo, 0, len(c.Fields)),
		}
		for _, f := range c.Fields {
			goType, isPtr, err := sgg.mapColumnType(f, c.SchemaName)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to map column type -> %w",
					err,
				)
			}
			compGo.Fields = append(compGo.Fields, &FieldGo{
				Field:      f,
				GoBaseType: goType,
				IsPtr:      isPtr,
				Category: sgg.getColumnCategory(
					f.Type, c.SchemaName,
				),
			})
		}
		sgg.Composites = append(sgg.Composites, (*CompositeGo)(compGo))
	}
	slices.SortFunc(sgg.Composites, func(a *CompositeGo, b *CompositeGo) int {
		return cmp.Compare(a.Name, b.Name)
	})

	for _, f := range graph.Functions {
		funcGo := &FunctionGo{
			Function:     f,
			NameExported: strcase.ToGoPascal(f.Name),
		}
		for _, arg := range f.Arguments {
			goType, err := sgg.mapDataType(arg.Type, "")
			if err != nil {
				return nil, fmt.Errorf("failed to map function arg type -> %w", err)
			}
			funcGo.ArgumentsGo = append(funcGo.ArgumentsGo, FunctionArgumentGo{
				Name:   arg.Name,
				GoType: goType,
				Index:  len(funcGo.ArgumentsGo) + 1,
			})
		}
		if f.ReturnType != nil {
			goType, err := sgg.mapDataType(f.ReturnType, "")
			if err != nil {
				return nil, fmt.Errorf("failed to map function return type -> %w", err)
			}
			funcGo.ReturnTypeGo = goType
		} else {
			funcGo.ReturnTypeGo = ""
		}
		sgg.Functions = append(sgg.Functions, funcGo)
	}
	slices.SortFunc(sgg.Functions, func(a *FunctionGo, b *FunctionGo) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return sgg, nil
}

func (sgg *SchemaGraphGo) ImportList() []string {
	list := make([]string, 0, len(sgg.Imports))
	for pkg := range sgg.Imports {
		list = append(list, pkg)
	}
	slices.Sort(list)
	return list
}

// mapColumnType determines the appropriate Go type and nullability for a
// specific model field. It also automatically registers any required
// external package imports (like 'uuid' or 'decimal') into the graph's
// import tracking set to ensure the generated file compiles.
func (sgg *SchemaGraphGo) mapColumnType(
	col *Field,
	currentSchema string,
) (string, bool, error) {
	if sgg == nil {
		panic("SchemaGraphGo receiver cannot be nil")
	}
	dt := col.Type
	if dt == nil {
		return "", false, errors.New("column type cannot be nil")
	}

	goType, err := sgg.mapDataType(dt, currentSchema)
	if err != nil {
		return "", false, fmt.Errorf("failed mapping -> %w", err)
	}

	isPtr := false
	if col.IsNullable && !sgg.isNillableGoType(dt, currentSchema) {
		isPtr = true
	}

	return goType, isPtr, nil
}

func (sgg *SchemaGraphGo) isNillableGoType(
	dt *postgres.DataType,
	currentSchema string,
) bool {
	if sgg == nil {
		panic("SchemaGraphGo receiver cannot be nil")
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
			targetSchema := t.DomainType.GetSchema()
			if targetSchema == "" {
				targetSchema = currentSchema
			}
			key := targetSchema + "." + t.DomainType.GetName()
			dom, ok := sgg.Graph.Domains[key]
			if ok {
				return sgg.isNillableGoType(
					dom.BaseType, targetSchema,
				)
			}
		}
	}
	return false
}

func (sgg *SchemaGraphGo) mapDataType(
	dt *postgres.DataType,
	currentSchema string,
) (string, error) {
	if sgg == nil {
		panic("SchemaGraphGo receiver cannot be nil")
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
		sgg.Imports["github.com/shopspring/decimal"] = struct{}{}
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
		sgg.Imports["time"] = struct{}{}
		return "time.Time", nil
	case *postgres.DataType_IntervalType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
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
		key := targetSchema + "." + name
		enum, seen := sgg.Graph.Enums[key]
		if !seen {
			return "", fmt.Errorf("unknown enum type -> %s", key)
		}
		return enum.Name, nil

	// 7. Geometric Types
	case *postgres.DataType_PointType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Point", nil
	case *postgres.DataType_LineType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Line", nil
	case *postgres.DataType_LsegType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Lseg", nil
	case *postgres.DataType_BoxType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Box", nil
	case *postgres.DataType_PathType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Path", nil
	case *postgres.DataType_PolygonType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Polygon", nil
	case *postgres.DataType_CircleType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Circle", nil

	// 8. Network Address Types
	case *postgres.DataType_InetType:
		sgg.Imports["net/netip"] = struct{}{}
		return "netip.Addr", nil
	case *postgres.DataType_CidrType:
		sgg.Imports["net/netip"] = struct{}{}
		return "netip.Prefix", nil
	case *postgres.DataType_MacaddrType,
		*postgres.DataType_Macaddr8Type:
		sgg.Imports["net"] = struct{}{}
		return "net.HardwareAddr", nil

	// 9. Bit String Types
	case *postgres.DataType_BitType,
		*postgres.DataType_BitVaryingType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Bits", nil

	// 10. Text Search Types
	case *postgres.DataType_TsvectorType,
		*postgres.DataType_TsqueryType:
		return "string", nil

	// 11. UUID Type
	case *postgres.DataType_UuidType:
		sgg.Imports["github.com/google/uuid"] = struct{}{}
		return "uuid.UUID", nil

	// 12. XML Type
	case *postgres.DataType_XmlType:
		return "string", nil

	// 13. JSON Types
	case *postgres.DataType_JsonType,
		*postgres.DataType_JsonbType,
		*postgres.DataType_JsonpathType:
		sgg.Imports["encoding/json"] = struct{}{}
		return "json.RawMessage", nil

	// 14. Arrays
	case *postgres.DataType_ArrayType:
		elem := t.ArrayType.GetElementType()
		elemType, err := sgg.mapDataType(elem, currentSchema)
		if err != nil {
			return "", fmt.Errorf(
				"failed to map array element type -> %w",
				err,
			)
		}

		dims := int32(1)
		if t.ArrayType != nil && t.ArrayType.Dimensions != nil {
			dims = *t.ArrayType.Dimensions
		}
		if dims < 1 || dims > 10 {
			return "", fmt.Errorf("invalid dims -> %d", dims)
		}

		return strings.Repeat("[]", int(dims)) + elemType, nil

	// 15. Composite / Range / Domain Types
	case *postgres.DataType_CompositeType:
		targetSchema := t.CompositeType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.CompositeType.GetName()
		key := targetSchema + "." + name
		comp, seen := sgg.Graph.Composites[key]
		if !seen {
			return "", fmt.Errorf("unknown composite -> %s", key)
		}
		return comp.Name, nil

	case *postgres.DataType_Int4RangeType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Range[int32]", nil
	case *postgres.DataType_Int8RangeType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return "pgtype.Range[int64]", nil
	case *postgres.DataType_NumRangeType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		sgg.Imports["github.com/shopspring/decimal"] = struct{}{}
		return "pgtype.Range[decimal.Decimal]", nil
	case *postgres.DataType_TsRangeType,
		*postgres.DataType_TstzRangeType,
		*postgres.DataType_DateRangeType:
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		sgg.Imports["time"] = struct{}{}
		return "pgtype.Range[time.Time]", nil
	case *postgres.DataType_CustomRangeType:
		baseType := t.CustomRangeType.GetBaseType()
		baseGoType, err := sgg.mapDataType(baseType, currentSchema)
		if err != nil {
			return "", fmt.Errorf(
				"failed to map custom range base type -> %w",
				err,
			)
		}
		sgg.Imports["github.com/jackc/pgx/v5/pgtype"] = struct{}{}
		return fmt.Sprintf("pgtype.Range[%s]", baseGoType), nil
	case *postgres.DataType_DomainType:
		targetSchema := t.DomainType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.DomainType.GetName()
		key := targetSchema + "." + name
		dom, seen := sgg.Graph.Domains[key]
		if !seen {
			return "", fmt.Errorf("unknown domain type -> %s", key)
		}
		return dom.Name, nil

	// 16. OID & pg_lsn
	case *postgres.DataType_OidType:
		return "uint32", nil
	case *postgres.DataType_PgLsnType:
		return "uint64", nil
	default:
		return "", fmt.Errorf("unsupported postgres data type -> %T", t)
	}
}

func (sgg *SchemaGraphGo) resolveEdgeGo(
	modelGo *ModelGo,
	e *Edge,
	targetModelGo *ModelGo,
) *EdgeGo {
	fieldTypes := make([]string, 0, len(e.LocalFields))
	localIsNullable := make([]bool, 0, len(e.LocalFields))
	targetIsNullable := make([]bool, 0, len(e.LocalFields))

	for i := 0; i < len(e.LocalFields); i++ {
		localFieldType := ""
		localFieldPtr := false
		for _, f := range modelGo.Fields {
			if f.Name == e.LocalFields[i] {
				localFieldType = f.GoBaseType
				localFieldPtr = f.IsPtr
				break
			}
		}

		targetFieldPtr := false
		if targetModelGo != nil {
			for _, f := range targetModelGo.Fields {
				if f.Name == e.TargetFields[i] {
					targetFieldPtr = f.IsPtr
					break
				}
			}
		}

		fieldTypes = append(fieldTypes, localFieldType)
		localIsNullable = append(localIsNullable, localFieldPtr)
		targetIsNullable = append(targetIsNullable, targetFieldPtr)
	}

	return &EdgeGo{
		Edge:             e,
		FieldTypes:       fieldTypes,
		LocalIsNullable:  localIsNullable,
		TargetIsNullable: targetIsNullable,
	}
}

func (sgg *SchemaGraphGo) getColumnCategory(
	dt *postgres.DataType, currentSchema string,
) ColumnCategory {
	cat := sgg.resolveColumnCategory(dt, currentSchema)
	sgg.ActiveCategories[cat.Name.String()] = true
	return cat
}

func (sgg *SchemaGraphGo) resolveColumnCategory(
	dt *postgres.DataType, currentSchema string,
) ColumnCategory {
	if dt == nil || dt.GetType() == nil {
		return ColumnCategory{Name: ColTypeUnsupported}
	}
	switch t := dt.GetType().(type) {
	case *postgres.DataType_SmallintType,
		*postgres.DataType_SmallserialType:
		return ColumnCategory{Name: ColTypeNumber, Type: "int16"}
	case *postgres.DataType_IntegerType,
		*postgres.DataType_SerialType:
		return ColumnCategory{Name: ColTypeNumber, Type: "int32"}
	case *postgres.DataType_BigintType,
		*postgres.DataType_BigserialType:
		return ColumnCategory{Name: ColTypeNumber, Type: "int64"}
	case *postgres.DataType_DecimalType,
		*postgres.DataType_NumericType,
		*postgres.DataType_MoneyType:
		return ColumnCategory{
			Name: ColTypeNumber,
			Type: "decimal.Decimal",
		}
	case *postgres.DataType_RealType:
		return ColumnCategory{Name: ColTypeNumber, Type: "float32"}
	case *postgres.DataType_DoublePrecisionType:
		return ColumnCategory{Name: ColTypeNumber, Type: "float64"}
	case *postgres.DataType_OidType:
		return ColumnCategory{Name: ColTypeNumber, Type: "uint32"}
	case *postgres.DataType_PgLsnType:
		return ColumnCategory{Name: ColTypeNumber, Type: "uint64"}
	case *postgres.DataType_VarcharType,
		*postgres.DataType_CharType,
		*postgres.DataType_TextType,
		*postgres.DataType_TsvectorType,
		*postgres.DataType_TsqueryType,
		*postgres.DataType_XmlType:
		return ColumnCategory{Name: ColTypeString, Type: "string"}
	case *postgres.DataType_BooleanType:
		return ColumnCategory{Name: ColTypeBoolean, Type: "bool"}
	case *postgres.DataType_TimestampType,
		*postgres.DataType_TimestamptzType,
		*postgres.DataType_DateType,
		*postgres.DataType_TimeType,
		*postgres.DataType_TimetzType:
		return ColumnCategory{Name: ColTypeTime, Type: "time.Time"}
	case *postgres.DataType_IntervalType:
		return ColumnCategory{
			Name: ColTypeTime,
			Type: "pgtype.Interval",
		}
	case *postgres.DataType_ByteaType:
		return ColumnCategory{Name: ColTypeByte, Type: "[]byte"}
	case *postgres.DataType_EnumType:
		targetSchema := t.EnumType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.EnumType.GetName()
		key := targetSchema + "." + name
		enum, ok := sgg.Graph.Enums[key]
		if !ok {
			return ColumnCategory{Name: ColTypeUnsupported}
		}
		return ColumnCategory{
			Name: ColTypeEnum,
			Type: enum.Name,
		}
	case *postgres.DataType_UuidType:
		return ColumnCategory{Name: ColTypeUUID, Type: "uuid.UUID"}
	case *postgres.DataType_JsonType,
		*postgres.DataType_JsonbType,
		*postgres.DataType_JsonpathType:
		return ColumnCategory{
			Name: ColTypeJSON,
			Type: "json.RawMessage",
		}
	case *postgres.DataType_ArrayType:
		return ColumnCategory{Name: ColTypeArray, Type: "any"}
	case *postgres.DataType_InetType,
		*postgres.DataType_CidrType:
		return ColumnCategory{
			Name: ColTypeNetworkAddress,
			Type: "netip.Addr",
		}
	case *postgres.DataType_MacaddrType,
		*postgres.DataType_Macaddr8Type:
		return ColumnCategory{
			Name: ColTypeNetworkAddress,
			Type: "net.HardwareAddr",
		}
	case *postgres.DataType_PointType:
		return ColumnCategory{
			Name: ColTypeGeometric,
			Type: "pgtype.Point",
		}
	case *postgres.DataType_LineType:
		return ColumnCategory{
			Name: ColTypeGeometric,
			Type: "pgtype.Line",
		}
	case *postgres.DataType_LsegType:
		return ColumnCategory{
			Name: ColTypeGeometric,
			Type: "pgtype.Lseg",
		}
	case *postgres.DataType_BoxType:
		return ColumnCategory{
			Name: ColTypeGeometric,
			Type: "pgtype.Box",
		}
	case *postgres.DataType_PathType:
		return ColumnCategory{
			Name: ColTypeGeometric,
			Type: "pgtype.Path",
		}
	case *postgres.DataType_PolygonType:
		return ColumnCategory{
			Name: ColTypeGeometric,
			Type: "pgtype.Polygon",
		}
	case *postgres.DataType_CircleType:
		return ColumnCategory{
			Name: ColTypeGeometric,
			Type: "pgtype.Circle",
		}
	case *postgres.DataType_Int4RangeType:
		return ColumnCategory{
			Name: ColTypeRange,
			Type: "pgtype.Range[int32]",
		}
	case *postgres.DataType_Int8RangeType:
		return ColumnCategory{
			Name: ColTypeRange,
			Type: "pgtype.Range[int64]",
		}
	case *postgres.DataType_NumRangeType:
		return ColumnCategory{
			Name: ColTypeRange,
			Type: "pgtype.Range[decimal.Decimal]",
		}
	case *postgres.DataType_TsRangeType,
		*postgres.DataType_TstzRangeType,
		*postgres.DataType_DateRangeType:
		return ColumnCategory{
			Name: ColTypeRange,
			Type: "pgtype.Range[time.Time]",
		}
	case *postgres.DataType_CustomRangeType:
		return ColumnCategory{Name: ColTypeRange, Type: "any"}
	case *postgres.DataType_CompositeType:
		targetSchema := t.CompositeType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.CompositeType.GetName()
		key := targetSchema + "." + name
		comp, ok := sgg.Graph.Composites[key]
		if !ok {
			return ColumnCategory{Name: ColTypeUnsupported}
		}
		ccName := string(ColTypeComposite) + comp.Name + "Column"
		return ColumnCategory{
			Name: columnCategoryName(ccName),
			Type: comp.Name,
		}
	case *postgres.DataType_DomainType:
		targetSchema := t.DomainType.GetSchema()
		if targetSchema == "" {
			targetSchema = currentSchema
		}
		name := t.DomainType.GetName()
		key := targetSchema + "." + name
		dom, ok := sgg.Graph.Domains[key]
		if !ok {
			return ColumnCategory{Name: ColTypeUnsupported}
		}
		baseCat := sgg.resolveColumnCategory(dom.BaseType, targetSchema)
		baseCat.Type = dom.Name
		return baseCat
	case *postgres.DataType_BitType,
		*postgres.DataType_BitVaryingType:
		return ColumnCategory{
			Name: ColTypeBitString,
			Type: "pgtype.Bits",
		}
	default:
		return ColumnCategory{Name: ColTypeUnsupported}
	}
}
