package ast

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ettle/strcase"
	"github.com/jinzhu/inflection"
	"github.com/uthereal/scheme/genproto/core/shared"
	"github.com/uthereal/scheme/genproto/postgres"
)

// NewSchemaGraph creates a new schema graph with initialized maps and builds
// the entire model graph based on the provided schema.
func NewSchemaGraph(pgSchema *postgres.PostgresDatabase) (*SchemaGraph, error) {
	if pgSchema == nil {
		return nil, errors.New("postgres schema cannot be nil")
	}

	sg := &SchemaGraph{
		Enums:      make(map[string]*Enum),
		Composites: make(map[string]*Composite),
		Domains:    make(map[string]*Domain),
		Models:     make(map[string]*Model),
	}

	err := sg.buildModelGraph(pgSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to build model graph -> %w", err)
	}

	return sg, nil
}

// buildModelGraph constructs the internal model graph from a PostgreSQL
// database schema.
//
// It performs four distinct passes to ensure all types are correctly
// initialized and named:
//
//  1. Pass 1: Global Tallying. It scans all schemas to count the frequency of
//     table and type names (Enums, Composites, Domains). This is required
//     because we must know if a name is globally unique across all database
//     schemas BEFORE we can decide on its final struct name.
//
//  2. Pass 2: Name Resolution. It uses the tallies from Pass 1 to resolve
//     naming collisions by deciding on final struct names. If a name is
//     used in multiple schemas, it is prefixed with the schema name.
//
//  3. Pass 3: Object Mapping. It initializes all Model, Enum, and Domain
//     structs using the resolved names, ensuring every type is available
//     in the SchemaGraph's maps for the next pass.
//
//  4. Pass 4: Relationship (Edge) hydration. It maps foreign key constraints
//     between models. This must be a separate pass because an Edge connects
//     a source model to a target model; we need both models to be fully
//     initialized and their names resolved before we can link them.
func (sg *SchemaGraph) buildModelGraph(
	pgSchema *postgres.PostgresDatabase,
) error {
	if sg == nil {
		panic("SchemaGraph receiver cannot be nil")
	}
	if pgSchema == nil {
		return errors.New("postgres schema cannot be nil")
	}

	// Pass 1: Global Tallying.
	// We count the occurrences of each SQL name across all schemas. This
	// is required because we must know if a name is globally unique across
	// all database schemas BEFORE we can decide on its final struct
	// name.
	nameTally := make(map[string]int)
	for _, schema := range pgSchema.GetSchemas() {
		for _, table := range schema.GetTables() {
			nameTally[table.GetName()]++
		}
		for _, enumDef := range schema.GetEnums() {
			nameTally[enumDef.GetName()]++
		}
		for _, domainDef := range schema.GetDomains() {
			nameTally[domainDef.GetName()]++
		}
		for _, compDef := range schema.GetComposites() {
			nameTally[compDef.GetName()]++
		}
	}

	// Pass 2: Name Resolution.
	// We use the tallies from Pass 1 to resolve naming collisions. If an
	// object's name is used in multiple schemas, we prefix it with the
	// schema name. We store these resolved names in lookup maps to decouple
	// name resolution from object initialization. We also ensure that the
	// resulting names are globally unique.
	resTableNames := make(map[string]string)
	resEnumNames := make(map[string]string)
	resDomainNames := make(map[string]string)
	resCompNames := make(map[string]string)

	uniqueNames := make(map[string]string)
	for _, schema := range pgSchema.GetSchemas() {
		schemaName := schema.GetName()

		for _, table := range schema.GetTables() {
			name := table.GetName()
			key := schemaName + "." + name
			baseName := name
			if nameTally[name] > 1 {
				baseName = schemaName + "_" + name
			}
			finalName := inflection.Singular(
				strcase.ToGoPascal(baseName),
			)

			oldKey, exists := uniqueNames[finalName]
			if exists {
				return fmt.Errorf(
					"table naming collision: %s and %s -> %s",
					oldKey, key, finalName,
				)
			}
			uniqueNames[finalName] = key
			resTableNames[key] = finalName
		}

		for _, enumDef := range schema.GetEnums() {
			name := enumDef.GetName()
			key := schemaName + "." + name
			baseName := name
			if nameTally[name] > 1 {
				baseName = schemaName + "_" + name
			}
			finalName := strcase.ToGoPascal(baseName)

			oldKey, exists := uniqueNames[finalName]
			if exists {
				return fmt.Errorf(
					"enum naming collision: %s and %s -> %s",
					oldKey, key, finalName,
				)
			}
			uniqueNames[finalName] = key
			resEnumNames[key] = finalName
		}

		for _, domainDef := range schema.GetDomains() {
			name := domainDef.GetName()
			key := schemaName + "." + name
			baseName := name
			if nameTally[name] > 1 {
				baseName = schemaName + "_" + name
			}
			finalName := strcase.ToGoPascal(baseName)

			oldKey, exists := uniqueNames[finalName]
			if exists {
				return fmt.Errorf(
					"domain naming collision: %s and %s -> %s",
					oldKey, key, finalName,
				)
			}
			uniqueNames[finalName] = key
			resDomainNames[key] = finalName
		}

		for _, compDef := range schema.GetComposites() {
			name := compDef.GetName()
			key := schemaName + "." + name
			baseName := name
			if nameTally[name] > 1 {
				baseName = schemaName + "_" + name
			}
			finalName := strcase.ToGoPascal(baseName)

			oldKey, exists := uniqueNames[finalName]
			if exists {
				return fmt.Errorf(
					"composite naming collision: %s and %s -> %s",
					oldKey, key, finalName,
				)
			}
			uniqueNames[finalName] = key
			resCompNames[key] = finalName
		}
	}

	// Pass 3: Object Mapping.
	// We initialize all Enum, Domain, Composite, and Table Model structs.
	// This ensures that every type is available in the SchemaGraph's maps
	// before we establish relationships in the final pass.
	for _, schema := range pgSchema.GetSchemas() {
		schemaName := schema.GetName()

		for _, enumDef := range schema.GetEnums() {
			name := enumDef.GetName()
			key := schemaName + "." + name
			_, ok := sg.Enums[key]
			if ok {
				continue
			}

			vals := make([]EnumValue, 0, len(enumDef.GetValues()))
			for _, v := range enumDef.GetValues() {
				vals = append(vals, EnumValue{
					Name: v, Value: v,
				})
			}
			sg.Enums[key] = &Enum{
				Name:   resEnumNames[key],
				Values: vals,
			}
		}

		for _, domainDef := range schema.GetDomains() {
			name := domainDef.GetName()
			key := schemaName + "." + name
			_, ok := sg.Domains[key]
			if ok {
				continue
			}

			sg.Domains[key] = &Domain{
				Name:     resDomainNames[key],
				BaseType: domainDef.GetBaseType(),
			}
		}

		for _, compDef := range schema.GetComposites() {
			name := compDef.GetName()
			key := schemaName + "." + name
			_, ok := sg.Composites[key]
			if ok {
				continue
			}

			fields := make([]*Field, 0, len(compDef.GetFields()))
			for _, f := range compDef.GetFields() {
				// PostgreSQL composite fields are always nullable.
				// @link https://www.postgresql.org/docs/current/rowtypes.html
				//   "Attributes of a composite type cannot have constraints"
				fields = append(fields, &Field{
					Name:       strcase.ToGoPascal(f.GetName()),
					ColumnName: f.GetName(),
					Type:       f.GetType(),
					IsNullable: true,
				})
			}
			sg.Composites[key] = &Composite{
				Name:       resCompNames[key],
				TableName:  name,
				SchemaName: schemaName,
				Fields:     fields,
			}
		}

		for _, table := range schema.GetTables() {
			tableName := table.GetName()
			key := schemaName + "." + tableName

			columns := table.GetColumns()
			modelType := &Model{
				Name:       resTableNames[key],
				TableName:  tableName,
				SchemaName: schemaName,
				TableFullName: fmt.Sprintf(
					"\"%s\".\"%s\"", schemaName, tableName,
				),
				Fields: make([]*Field, 0, len(columns)),
				Edges:  make([]*Edge, 0),
			}

			fields := make([]*Field, 0, len(columns))
			for _, col := range columns {
				fName := strcase.ToGoPascal(col.GetName())
				fields = append(fields, &Field{
					Name:       fName,
					ColumnName: col.GetName(),
					Type:       col.GetType(),
					IsNullable: col.GetIsNullable(),
				})
			}
			modelType.Fields = fields
			sg.Models[key] = modelType
		}
	}

	// Pass 4: Relationship Hydration.
	// We establish the links between Table Models based on foreign key
	// relations. This MUST be the final pass because an Edge requires both
	// the source and target Models to be already instantiated and named.
	// This also allows us to correctly name back-references using the
	// target Model's resolved name.
	for _, schema := range pgSchema.GetSchemas() {
		schemaName := schema.GetName()

		for _, table := range schema.GetTables() {
			tableName := table.GetName()
			modelKey := schemaName + "." + tableName

			sourceModel, ok := sg.Models[modelKey]
			if !ok {
				return fmt.Errorf(
					"failed to find model for %s", modelKey,
				)
			}

			for _, rel := range table.GetRelations() {
				targetRaw := rel.GetTargetTable()
				var targetSchema, targetTable string
				if strings.Contains(targetRaw, ".") {
					parts := strings.SplitN(
						targetRaw, ".", 2,
					)
					targetSchema = parts[0]
					targetTable = parts[1]
				} else {
					targetSchema = schemaName
					targetTable = targetRaw
				}
				targetKey := targetSchema + "." + targetTable

				targetModel, ok := sg.Models[targetKey]
				if !ok {
					continue
				}

				targetStructName := targetModel.Name

				relCols := rel.GetColumns()
				localCols := make([]string, 0, len(relCols))
				targetCols := make([]string, 0, len(relCols))
				localFields := make([]string, 0, len(relCols))
				targetFields := make([]string, 0, len(relCols))

				for _, colMap := range relCols {
					lCol := colMap.GetSourceColumn()
					tCol := colMap.GetTargetColumn()
					localCols = append(localCols, lCol)
					targetCols = append(targetCols, tCol)
					localFields = append(
						localFields,
						strcase.ToGoPascal(lCol),
					)
					targetFields = append(
						targetFields,
						strcase.ToGoPascal(tCol),
					)
				}

				relType := rel.GetType()
				relOTM := shared.
					RelationType_RELATION_TYPE_ONE_TO_MANY
				relMTM := shared.
					RelationType_RELATION_TYPE_MANY_TO_MANY
				isSlice := relType == relOTM || relType == relMTM

				edgeName := strcase.ToGoPascal(rel.GetName())
				if edgeName == "" {
					edgeName = targetStructName
				}

				sourceModel.Edges = append(sourceModel.Edges, &Edge{
					Name:          edgeName,
					NameLower:     strings.ToLower(edgeName),
					TargetModel:   targetStructName,
					IsSlice:       isSlice,
					LocalColumns:  localCols,
					TargetColumns: targetCols,
					LocalFields:   localFields,
					TargetFields:  targetFields,
					IsBackRef:     false,
				})

				backRefName := resTableNames[modelKey]
				if rel.GetName() != "" {
					backRefName = strcase.ToGoPascal(
						rel.GetName(),
					) + backRefName
				}

				relMTO := shared.
					RelationType_RELATION_TYPE_MANY_TO_ONE
				backRefIsSlice := relType == relMTO || relType == relMTM

				if backRefIsSlice {
					backRefName = inflection.Plural(backRefName)
				}

				targetModel.Edges = append(targetModel.Edges, &Edge{
					Name:          backRefName,
					NameLower:     strings.ToLower(backRefName),
					TargetModel:   sourceModel.Name,
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
