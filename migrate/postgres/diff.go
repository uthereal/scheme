package postgres

import (
	"fmt"
	"sort"
	"strings"

	"github.com/uthereal/scheme/genproto/postgres"
)

// Differ represents the state reconciliation engine that computes required
// DDL actions to bring a LiveState into alignment with a target
// PostgresDatabase.
type Differ struct {
	Actions []MigrationAction
	live    *LiveState
	target  *postgres.PostgresDatabase
}

type renameOp struct {
	Prev string
	New  string
	Ref  any
}

// NewDiffer initializes a state reconciliation engine. It clones the provided
// LiveState to ensure the original memory state is completely preserved during
// diff calculation.
func NewDiffer(
	live *LiveState, target *postgres.PostgresDatabase,
) (*Differ, error) {
	if live == nil {
		return nil, fmt.Errorf("live state cannot be nil")
	}
	if target == nil {
		return nil, fmt.Errorf("target database cannot be nil")
	}

	return &Differ{
		Actions: make([]MigrationAction, 0),
		live:    live,
		target:  target,
	}, nil
}

// Plan executes the state reconciliation, generating an ordered sequence of
// MigrationActions required to migrate the live database. It returns the
// actions and a new simulated LiveState representing the database after
// the actions are applied.
func (d *Differ) Plan() error {
	if d == nil {
		return fmt.Errorf("differ cannot be nil")
	}

	var err error

	err = d.planSchemas()
	if err != nil {
		return fmt.Errorf("failed to plan schemas -> %w", err)
	}

	err = d.planEnums()
	if err != nil {
		return fmt.Errorf("failed to plan enums -> %w", err)
	}

	err = d.planComposites()
	if err != nil {
		return fmt.Errorf("failed to plan composites -> %w", err)
	}

	err = d.planDomains()
	if err != nil {
		return fmt.Errorf("failed to plan domains -> %w", err)
	}

	err = d.planTables()
	if err != nil {
		return fmt.Errorf("failed to plan tables -> %w", err)
	}

	err = d.planPrimaryKeys()
	if err != nil {
		return fmt.Errorf("failed to plan primary keys -> %w", err)
	}

	err = d.planIndexes()
	if err != nil {
		return fmt.Errorf("failed to plan indexes -> %w", err)
	}

	err = d.planForeignKeys()
	if err != nil {
		return fmt.Errorf("failed to plan foreign keys -> %w", err)
	}

	return nil
}

func (d *Differ) planSchemas() error {
	var renames []renameOp
	for _, targetSchema := range d.target.GetSchemas() {
		tName := targetSchema.GetName()
		pName := targetSchema.GetNamePrevious()
		if pName != "" && pName != tName {
			renames = append(renames, renameOp{Prev: pName, New: tName})
		}
	}

	for len(renames) > 0 {
		progress := false
		for i := 0; i < len(renames); i++ {
			op := renames[i]
			_, exists := d.live.Schemas[op.New]
			if !exists {
				sqlStr := fmt.Sprintf(
					"ALTER SCHEMA %q RENAME TO %q;", op.Prev, op.New,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectSchema,
					Name:       op.New,
					SQL:        sqlStr,
				})

				d.live.Schemas[op.New] = d.live.Schemas[op.Prev]
				d.live.Schemas[op.New].Name = op.New
				delete(d.live.Schemas, op.Prev)

				renames = append(renames[:i], renames[i+1:]...)
				progress = true
				break
			}
		}
		if !progress {
			var collisionOp *renameOp
			for i := range renames {
				isTargetInRenames := false
				for j := range renames {
					if renames[j].Prev == renames[i].New {
						isTargetInRenames = true
						break
					}
				}
				if !isTargetInRenames {
					collisionOp = &renames[i]
					break
				}
			}
			if collisionOp != nil {
				return fmt.Errorf("schema rename collision -> %s", collisionOp.New)
			}

			op := renames[0]
			tmpName := "scheme_tmp_" + op.Prev
			sqlStr := fmt.Sprintf(
				"ALTER SCHEMA %q RENAME TO %q;", op.Prev, tmpName,
			)
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectSchema,
				Name:       tmpName,
				SQL:        sqlStr,
			})

			d.live.Schemas[tmpName] = d.live.Schemas[op.Prev]
			d.live.Schemas[tmpName].Name = tmpName
			delete(d.live.Schemas, op.Prev)

			renames[0].Prev = tmpName
		}
	}

	for _, targetSchema := range d.target.GetSchemas() {
		tName := targetSchema.GetName()
		_, exists := d.live.Schemas[tName]
		if !exists {
			sqlStr := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q;", tName)
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeCreate,
				ObjectType: ObjectSchema,
				Name:       tName,
				SQL:        sqlStr,
			})
			d.live.Schemas[tName] = &LiveSchema{
				Name:       tName,
				Tables:     make(map[string]*LiveTable),
				Enums:      make(map[string]*LiveEnum),
				Composites: make(map[string]*LiveComposite),
				Domains:    make(map[string]*LiveDomain),
			}
		}
	}
	return nil
}

func (d *Differ) planEnums() error {
	for _, targetSchema := range d.target.GetSchemas() {
		schemaName := targetSchema.GetName()
		liveSchema, ok := d.live.Schemas[schemaName]
		if !ok {
			continue
		}

		protected := make(map[string]struct{})
		var renames []renameOp
		var creates []*postgres.DataType_EnumType

		for _, targetTable := range targetSchema.GetTables() {
			for _, col := range targetTable.GetColumns() {
				dt := col.GetType()
				if dt == nil {
					continue
				}
				enumType, ok := dt.GetType().(*postgres.DataType_EnumType)
				if !ok || enumType.EnumType == nil {
					continue
				}

				name := enumType.EnumType.GetName()
				pName := enumType.EnumType.GetNamePrevious()
				eSchema := enumType.EnumType.GetSchema()
				if eSchema == "" {
					eSchema = schemaName
				}
				if eSchema != schemaName {
					continue
				}

				creates = append(creates, enumType)
				if pName != "" && pName != name {
					protected[pName] = struct{}{}
					renames = append(renames, renameOp{
						Prev: pName, New: name, Ref: enumType,
					})
				} else {
					protected[name] = struct{}{}
				}
			}
		}

		var liveNames []string
		for k := range liveSchema.Enums {
			liveNames = append(liveNames, k)
		}
		sort.Strings(liveNames)

		for _, liveName := range liveNames {
			_, exists := protected[liveName]
			if !exists {
				sqlStr := fmt.Sprintf(
					"DROP TYPE %q.%q CASCADE;", schemaName, liveName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectEnum,
					Schema:        schemaName,
					Name:          liveName,
					SQL:           sqlStr,
					IsDestructive: true,
				})
				delete(liveSchema.Enums, liveName)
			}
		}

		for len(renames) > 0 {
			progress := false
			for i := 0; i < len(renames); i++ {
				op := renames[i]
				_, exists := liveSchema.Enums[op.New]
				if !exists {
					sqlStr := fmt.Sprintf(
						"ALTER TYPE %q.%q RENAME TO %q;",
						schemaName, op.Prev, op.New,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeRename,
						ObjectType: ObjectEnum,
						Schema:     schemaName,
						Name:       op.New,
						SQL:        sqlStr,
					})

					liveSchema.Enums[op.New] = liveSchema.Enums[op.Prev]
					liveSchema.Enums[op.New].Name = op.New
					delete(liveSchema.Enums, op.Prev)

					renames = append(renames[:i], renames[i+1:]...)
					progress = true
					break
				}
			}
			if !progress {
				var collisionOp *renameOp
				for i := range renames {
					isTargetInRenames := false
					for j := range renames {
						if renames[j].Prev == renames[i].New {
							isTargetInRenames = true
							break
						}
					}
					if !isTargetInRenames {
						collisionOp = &renames[i]
						break
					}
				}
				if collisionOp != nil {
					return fmt.Errorf("enum rename collision -> %s", collisionOp.New)
				}

				op := renames[0]
				tmpName := "scheme_tmp_" + op.Prev
				sqlStr := fmt.Sprintf(
					"ALTER TYPE %q.%q RENAME TO %q;",
					schemaName, op.Prev, tmpName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectEnum,
					Schema:     schemaName,
					Name:       tmpName,
					SQL:        sqlStr,
				})

				liveSchema.Enums[tmpName] = liveSchema.Enums[op.Prev]
				liveSchema.Enums[tmpName].Name = tmpName
				delete(liveSchema.Enums, op.Prev)

				renames[0].Prev = tmpName
			}
		}

		for _, enumType := range creates {
			name := enumType.EnumType.GetName()
			liveEnum, exists := liveSchema.Enums[name]
			if !exists {
				var valBuilder strings.Builder
				for i, val := range enumType.EnumType.GetValues() {
					if i > 0 {
						valBuilder.WriteString(", ")
					}
					valBuilder.WriteString(fmt.Sprintf("'%s'", val))
				}
				sqlStr := fmt.Sprintf(
					"CREATE TYPE %q.%q AS ENUM (%s);",
					schemaName, name, valBuilder.String(),
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeCreate,
					ObjectType: ObjectEnum,
					Schema:     schemaName,
					Name:       name,
					SQL:        sqlStr,
				})

				liveSchema.Enums[name] = &LiveEnum{
					Name:   name,
					Values: enumType.EnumType.GetValues(),
				}
			} else {
				for _, targetVal := range enumType.EnumType.GetValues() {
					found := false
					for _, liveVal := range liveEnum.Values {
						if targetVal == liveVal {
							found = true
							break
						}
					}
					if !found {
						sqlStr := fmt.Sprintf(
							"ALTER TYPE %q.%q ADD VALUE '%s';",
							schemaName, name, targetVal,
						)
						d.Actions = append(d.Actions, MigrationAction{
							Type:       ActionTypeAlter,
							ObjectType: ObjectEnum,
							Schema:     schemaName,
							Name:       name,
							SQL:        sqlStr,
						})
						liveEnum.Values = append(liveEnum.Values, targetVal)
					}
				}
			}
		}
	}
	return nil
}

func (d *Differ) planComposites() error {
	for _, targetSchema := range d.target.GetSchemas() {
		schemaName := targetSchema.GetName()
		liveSchema, ok := d.live.Schemas[schemaName]
		if !ok {
			continue
		}

		protected := make(map[string]struct{})
		var renames []renameOp
		var creates []*postgres.DataType_CompositeType

		for _, targetTable := range targetSchema.GetTables() {
			for _, col := range targetTable.GetColumns() {
				dt := col.GetType()
				if dt == nil {
					continue
				}
				compType, ok := dt.GetType().(*postgres.DataType_CompositeType)
				if !ok || compType.CompositeType == nil {
					continue
				}

				name := compType.CompositeType.GetName()
				pName := compType.CompositeType.GetNamePrevious()
				eSchema := compType.CompositeType.GetSchema()
				if eSchema == "" {
					eSchema = schemaName
				}
				if eSchema != schemaName {
					continue
				}

				creates = append(creates, compType)
				if pName != "" && pName != name {
					protected[pName] = struct{}{}
					renames = append(renames, renameOp{
						Prev: pName, New: name, Ref: compType,
					})
				} else {
					protected[name] = struct{}{}
				}
			}
		}

		var liveNames []string
		for k := range liveSchema.Composites {
			liveNames = append(liveNames, k)
		}
		sort.Strings(liveNames)

		for _, liveName := range liveNames {
			_, exists := protected[liveName]
			if !exists {
				sqlStr := fmt.Sprintf(
					"DROP TYPE %q.%q CASCADE;", schemaName, liveName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectComposite,
					Schema:        schemaName,
					Name:          liveName,
					SQL:           sqlStr,
					IsDestructive: true,
				})
				delete(liveSchema.Composites, liveName)
			}
		}

		for len(renames) > 0 {
			progress := false
			for i := 0; i < len(renames); i++ {
				op := renames[i]
				_, exists := liveSchema.Composites[op.New]
				if !exists {
					sqlStr := fmt.Sprintf(
						"ALTER TYPE %q.%q RENAME TO %q;",
						schemaName, op.Prev, op.New,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeRename,
						ObjectType: ObjectComposite,
						Schema:     schemaName,
						Name:       op.New,
						SQL:        sqlStr,
					})

					liveSchema.Composites[op.New] = liveSchema.Composites[op.Prev]
					liveSchema.Composites[op.New].Name = op.New
					delete(liveSchema.Composites, op.Prev)

					renames = append(renames[:i], renames[i+1:]...)
					progress = true
					break
				}
			}
			if !progress {
				var collisionOp *renameOp
				for i := range renames {
					isTargetInRenames := false
					for j := range renames {
						if renames[j].Prev == renames[i].New {
							isTargetInRenames = true
							break
						}
					}
					if !isTargetInRenames {
						collisionOp = &renames[i]
						break
					}
				}
				if collisionOp != nil {
					return fmt.Errorf("composite rename collision -> %s", collisionOp.New)
				}

				op := renames[0]
				tmpName := "scheme_tmp_" + op.Prev
				sqlStr := fmt.Sprintf(
					"ALTER TYPE %q.%q RENAME TO %q;",
					schemaName, op.Prev, tmpName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectComposite,
					Schema:     schemaName,
					Name:       tmpName,
					SQL:        sqlStr,
				})

				liveSchema.Composites[tmpName] = liveSchema.Composites[op.Prev]
				liveSchema.Composites[tmpName].Name = tmpName
				delete(liveSchema.Composites, op.Prev)

				renames[0].Prev = tmpName
			}
		}

		for _, compType := range creates {
			name := compType.CompositeType.GetName()
			liveComp, exists := liveSchema.Composites[name]
			if !exists {
				var valBuilder strings.Builder
				for i, field := range compType.CompositeType.GetFields() {
					if i > 0 {
						valBuilder.WriteString(", ")
					}
					pgType, err := resolvePGType(field.GetType())
					if err != nil {
						return err
					}
					valBuilder.WriteString(fmt.Sprintf(
						"%q %s", field.GetName(), pgType,
					))
				}
				sqlStr := fmt.Sprintf(
					"CREATE TYPE %q.%q AS (%s);",
					schemaName, name, valBuilder.String(),
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeCreate,
					ObjectType: ObjectComposite,
					Schema:     schemaName,
					Name:       name,
					SQL:        sqlStr,
				})

				liveSchema.Composites[name] = &LiveComposite{
					Name:   name,
					Fields: make(map[string]*LiveCompositeField),
				}
			} else {
				targetFieldMap := make(map[string]struct{})
				for _, targetField := range compType.CompositeType.GetFields() {
					targetFieldMap[targetField.GetName()] = struct{}{}
					_, existsLiveField := liveComp.Fields[targetField.GetName()]
					if !existsLiveField {
						pgType, err := resolvePGType(targetField.GetType())
						if err != nil {
							return err
						}
						sqlStr := fmt.Sprintf(
							"ALTER TYPE %q.%q ADD ATTRIBUTE %q %s;",
							schemaName, name, targetField.GetName(), pgType,
						)
						d.Actions = append(d.Actions, MigrationAction{
							Type:       ActionTypeAlter,
							ObjectType: ObjectComposite,
							Schema:     schemaName,
							Name:       name,
							SQL:        sqlStr,
						})
					}
				}

				var liveFieldNames []string
				for k := range liveComp.Fields {
					liveFieldNames = append(liveFieldNames, k)
				}
				sort.Strings(liveFieldNames)

				for _, liveFieldName := range liveFieldNames {
					_, exists := targetFieldMap[liveFieldName]
					if !exists {
						sqlStr := fmt.Sprintf(
							"ALTER TYPE %q.%q DROP ATTRIBUTE %q CASCADE;",
							schemaName, name, liveFieldName,
						)
						actionName := fmt.Sprintf("%s.%s", name, liveFieldName)
						d.Actions = append(d.Actions, MigrationAction{
							Type:          ActionTypeDrop,
							ObjectType:    ObjectComposite,
							Schema:        schemaName,
							Name:          actionName,
							SQL:           sqlStr,
							IsDestructive: true,
						})
					}
				}
			}
		}
	}
	return nil
}

func (d *Differ) planDomains() error {
	for _, targetSchema := range d.target.GetSchemas() {
		schemaName := targetSchema.GetName()
		liveSchema, ok := d.live.Schemas[schemaName]
		if !ok {
			continue
		}

		protected := make(map[string]struct{})
		var renames []renameOp
		var creates []*postgres.DataType_DomainType

		for _, targetTable := range targetSchema.GetTables() {
			for _, col := range targetTable.GetColumns() {
				dt := col.GetType()
				if dt == nil {
					continue
				}
				domType, ok := dt.GetType().(*postgres.DataType_DomainType)
				if !ok || domType.DomainType == nil {
					continue
				}

				name := domType.DomainType.GetName()
				pName := domType.DomainType.GetNamePrevious()
				eSchema := domType.DomainType.GetSchema()
				if eSchema == "" {
					eSchema = schemaName
				}
				if eSchema != schemaName {
					continue
				}

				creates = append(creates, domType)
				if pName != "" && pName != name {
					protected[pName] = struct{}{}
					renames = append(renames, renameOp{
						Prev: pName, New: name, Ref: domType,
					})
				} else {
					protected[name] = struct{}{}
				}
			}
		}

		var liveNames []string
		for k := range liveSchema.Domains {
			liveNames = append(liveNames, k)
		}
		sort.Strings(liveNames)

		for _, liveName := range liveNames {
			_, exists := protected[liveName]
			if !exists {
				sqlStr := fmt.Sprintf(
					"DROP DOMAIN %q.%q CASCADE;", schemaName, liveName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectDomain,
					Schema:        schemaName,
					Name:          liveName,
					SQL:           sqlStr,
					IsDestructive: true,
				})
				delete(liveSchema.Domains, liveName)
			}
		}

		for len(renames) > 0 {
			progress := false
			for i := 0; i < len(renames); i++ {
				op := renames[i]
				_, exists := liveSchema.Domains[op.New]
				if !exists {
					sqlStr := fmt.Sprintf(
						"ALTER DOMAIN %q.%q RENAME TO %q;",
						schemaName, op.Prev, op.New,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeRename,
						ObjectType: ObjectDomain,
						Schema:     schemaName,
						Name:       op.New,
						SQL:        sqlStr,
					})

					liveSchema.Domains[op.New] = liveSchema.Domains[op.Prev]
					liveSchema.Domains[op.New].Name = op.New
					delete(liveSchema.Domains, op.Prev)

					renames = append(renames[:i], renames[i+1:]...)
					progress = true
					break
				}
			}
			if !progress {
				var collisionOp *renameOp
				for i := range renames {
					isTargetInRenames := false
					for j := range renames {
						if renames[j].Prev == renames[i].New {
							isTargetInRenames = true
							break
						}
					}
					if !isTargetInRenames {
						collisionOp = &renames[i]
						break
					}
				}
				if collisionOp != nil {
					return fmt.Errorf("domain rename collision -> %s", collisionOp.New)
				}

				op := renames[0]
				tmpName := "scheme_tmp_" + op.Prev
				sqlStr := fmt.Sprintf(
					"ALTER DOMAIN %q.%q RENAME TO %q;",
					schemaName, op.Prev, tmpName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectDomain,
					Schema:     schemaName,
					Name:       tmpName,
					SQL:        sqlStr,
				})

				liveSchema.Domains[tmpName] = liveSchema.Domains[op.Prev]
				liveSchema.Domains[tmpName].Name = tmpName
				delete(liveSchema.Domains, op.Prev)

				renames[0].Prev = tmpName
			}
		}

		for _, domType := range creates {
			name := domType.DomainType.GetName()
			_, exists := liveSchema.Domains[name]
			if !exists {
				pgType, err := resolvePGType(domType.DomainType.GetBaseType())
				if err != nil {
					return err
				}

				sqlStr := fmt.Sprintf(
					"CREATE DOMAIN %q.%q AS %s;",
					schemaName, name, pgType,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeCreate,
					ObjectType: ObjectDomain,
					Schema:     schemaName,
					Name:       name,
					SQL:        sqlStr,
				})

				liveSchema.Domains[name] = &LiveDomain{
					Name:     name,
					BaseType: pgType,
				}
			}
		}
	}
	return nil
}

func (d *Differ) planTables() error {
	for _, targetSchema := range d.target.GetSchemas() {
		schemaName := targetSchema.GetName()
		liveSchema, ok := d.live.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"in-memory live schema missing -> %s", schemaName,
			)
		}

		protected := make(map[string]struct{})
		var renames []renameOp

		for _, targetTable := range targetSchema.GetTables() {
			tName := targetTable.GetName()
			pName := targetTable.GetNamePrevious()
			if pName != "" && pName != tName {
				protected[pName] = struct{}{}
				renames = append(renames, renameOp{
					Prev: pName, New: tName, Ref: targetTable,
				})
			} else {
				protected[tName] = struct{}{}
			}
		}

		var liveNames []string
		for k := range liveSchema.Tables {
			liveNames = append(liveNames, k)
		}
		sort.Strings(liveNames)

		for _, liveName := range liveNames {
			_, exists := protected[liveName]
			if !exists {
				sqlStr := fmt.Sprintf(
					"DROP TABLE %q.%q CASCADE;", schemaName, liveName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectTable,
					Schema:        schemaName,
					Name:          liveName,
					SQL:           sqlStr,
					IsDestructive: true,
				})
				delete(liveSchema.Tables, liveName)
			}
		}

		for len(renames) > 0 {
			progress := false
			for i := 0; i < len(renames); i++ {
				op := renames[i]
				_, exists := liveSchema.Tables[op.New]
				if !exists {
					sqlStr := fmt.Sprintf(
						"ALTER TABLE %q.%q RENAME TO %q;",
						schemaName, op.Prev, op.New,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeRename,
						ObjectType: ObjectTable,
						Schema:     schemaName,
						Name:       op.New,
						SQL:        sqlStr,
					})

					liveSchema.Tables[op.New] = liveSchema.Tables[op.Prev]
					liveSchema.Tables[op.New].Name = op.New
					delete(liveSchema.Tables, op.Prev)

					renames = append(renames[:i], renames[i+1:]...)
					progress = true
					break
				}
			}
			if !progress {
				var collisionOp *renameOp
				for i := range renames {
					isTargetInRenames := false
					for j := range renames {
						if renames[j].Prev == renames[i].New {
							isTargetInRenames = true
							break
						}
					}
					if !isTargetInRenames {
						collisionOp = &renames[i]
						break
					}
				}
				if collisionOp != nil {
					return fmt.Errorf("table rename collision -> %s", collisionOp.New)
				}

				op := renames[0]
				tmpName := "scheme_tmp_" + op.Prev
				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q RENAME TO %q;",
					schemaName, op.Prev, tmpName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectTable,
					Schema:     schemaName,
					Name:       tmpName,
					SQL:        sqlStr,
				})

				liveSchema.Tables[tmpName] = liveSchema.Tables[op.Prev]
				liveSchema.Tables[tmpName].Name = tmpName
				delete(liveSchema.Tables, op.Prev)

				renames[0].Prev = tmpName
			}
		}

		for _, targetTable := range targetSchema.GetTables() {
			tName := targetTable.GetName()
			liveTable, exists := liveSchema.Tables[tName]
			if !exists {
				err := d.buildCreateTable(schemaName, targetTable)
				if err != nil {
					return err
				}
				liveSchema.Tables[tName] = &LiveTable{
					Name:    tName,
					Columns: make(map[string]*LiveColumn),
				}
			} else {
				err := d.planColumns(schemaName, liveTable, targetTable)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *Differ) planColumns(
	schema string, liveTable *LiveTable, targetTable *postgres.Table,
) error {
	tableName := targetTable.GetName()

	protected := make(map[string]struct{})
	var renames []renameOp

	for _, targetCol := range targetTable.GetColumns() {
		cName := targetCol.GetName()
		pName := targetCol.GetNamePrevious()
		if pName != "" && pName != cName {
			protected[pName] = struct{}{}
			renames = append(renames, renameOp{
				Prev: pName, New: cName, Ref: targetCol,
			})
		} else {
			protected[cName] = struct{}{}
		}
	}

	var liveColNames []string
	for k := range liveTable.Columns {
		liveColNames = append(liveColNames, k)
	}
	sort.Strings(liveColNames)

	for _, liveColName := range liveColNames {
		_, exists := protected[liveColName]
		if !exists {
			sqlStr := fmt.Sprintf(
				"ALTER TABLE %q.%q DROP COLUMN %q CASCADE;",
				schema, tableName, liveColName,
			)
			actionName := fmt.Sprintf("%s.%s", tableName, liveColName)
			d.Actions = append(d.Actions, MigrationAction{
				Type:          ActionTypeDrop,
				ObjectType:    ObjectColumn,
				Schema:        schema,
				Name:          actionName,
				SQL:           sqlStr,
				IsDestructive: true,
			})
			delete(liveTable.Columns, liveColName)
		}
	}

	for len(renames) > 0 {
		progress := false
		for i := 0; i < len(renames); i++ {
			op := renames[i]
			_, exists := liveTable.Columns[op.New]
			if !exists {
				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q RENAME COLUMN %q TO %q;",
					schema, tableName, op.Prev, op.New,
				)
				actionName := fmt.Sprintf("%s.%s", tableName, op.New)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectColumn,
					Schema:     schema,
					Name:       actionName,
					SQL:        sqlStr,
				})

				liveTable.Columns[op.New] = liveTable.Columns[op.Prev]
				liveTable.Columns[op.New].Name = op.New
				delete(liveTable.Columns, op.Prev)

				renames = append(renames[:i], renames[i+1:]...)
				progress = true
				break
			}
		}
		if !progress {
			var collisionOp *renameOp
			for i := range renames {
				isTargetInRenames := false
				for j := range renames {
					if renames[j].Prev == renames[i].New {
						isTargetInRenames = true
						break
					}
				}
				if !isTargetInRenames {
					collisionOp = &renames[i]
					break
				}
			}
			if collisionOp != nil {
				return fmt.Errorf("column rename collision -> %s", collisionOp.New)
			}

			op := renames[0]
			tmpName := "scheme_tmp_" + op.Prev
			sqlStr := fmt.Sprintf(
				"ALTER TABLE %q.%q RENAME COLUMN %q TO %q;",
				schema, tableName, op.Prev, tmpName,
			)
			actionName := fmt.Sprintf("%s.%s", tableName, tmpName)
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectColumn,
				Schema:     schema,
				Name:       actionName,
				SQL:        sqlStr,
			})

			liveTable.Columns[tmpName] = liveTable.Columns[op.Prev]
			liveTable.Columns[tmpName].Name = tmpName
			delete(liveTable.Columns, op.Prev)

			renames[0].Prev = tmpName
		}
	}

	for _, targetCol := range targetTable.GetColumns() {
		colName := targetCol.GetName()
		liveCol, exists := liveTable.Columns[colName]

		targetNullable := targetCol.GetIsNullable()
		for _, pk := range targetTable.GetPrimaryKeys() {
			if pk == colName {
				targetNullable = false
				break
			}
		}

		if !exists {
			pgType, err := resolvePGType(targetCol.GetType())
			if err != nil {
				return err
			}
			nullMod := " NOT NULL"
			defaultMod := ""
			defVal := targetCol.GetDefaultValue()
			if defVal != "" {
				defaultMod = " DEFAULT " + defVal
			}
			if targetNullable {
				nullMod = ""
			}

			sqlStr := fmt.Sprintf(
				"ALTER TABLE %q.%q ADD COLUMN %q %s%s%s;",
				schema, tableName, colName, pgType, nullMod, defaultMod,
			)
			actionName := fmt.Sprintf("%s.%s", tableName, colName)
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeCreate,
				ObjectType: ObjectColumn,
				Schema:     schema,
				Name:       actionName,
				SQL:        sqlStr,
			})
		} else {
			if liveCol.IsNullable && !targetNullable {
				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q ALTER COLUMN %q SET NOT NULL;",
					schema, tableName, colName,
				)
				actionName := fmt.Sprintf("%s.%s", tableName, colName)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeAlter,
					ObjectType: ObjectColumn,
					Schema:     schema,
					Name:       actionName,
					SQL:        sqlStr,
				})
			} else if !liveCol.IsNullable && targetNullable {
				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q ALTER COLUMN %q DROP NOT NULL;",
					schema, tableName, colName,
				)
				actionName := fmt.Sprintf("%s.%s", tableName, colName)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeAlter,
					ObjectType: ObjectColumn,
					Schema:     schema,
					Name:       actionName,
					SQL:        sqlStr,
				})
			}
		}
	}

	return nil
}

func (d *Differ) buildCreateTable(
	schema string, table *postgres.Table,
) error {
	var sql strings.Builder
	sqlStr := fmt.Sprintf("CREATE TABLE %q.%q (\n", schema, table.GetName())
	sql.WriteString(sqlStr)

	for i, col := range table.GetColumns() {
		if i > 0 {
			sql.WriteString(",\n")
		}

		pgType, err := resolvePGType(col.GetType())
		if err != nil {
			return err
		}

		nullMod := " NOT NULL"
		defaultMod := ""
		defVal := col.GetDefaultValue()
		if defVal != "" {
			defaultMod = " DEFAULT " + defVal
		}
		targetNullable := col.GetIsNullable()
		for _, pk := range table.GetPrimaryKeys() {
			if pk == col.GetName() {
				targetNullable = false
				break
			}
		}

		if targetNullable {
			nullMod = ""
		}

		sql.WriteString(
			fmt.Sprintf("  %q %s%s%s", col.GetName(), pgType, nullMod, defaultMod),
		)
	}

	sql.WriteString("\n);")

	d.Actions = append(d.Actions, MigrationAction{
		Type:       ActionTypeCreate,
		ObjectType: ObjectTable,
		Schema:     schema,
		Name:       table.GetName(),
		SQL:        sql.String(),
	})

	return nil
}

func (d *Differ) planPrimaryKeys() error {
	for _, targetSchema := range d.target.GetSchemas() {
		schemaName := targetSchema.GetName()
		liveSchema, ok := d.live.Schemas[schemaName]
		if !ok {
			continue
		}

		for _, targetTable := range targetSchema.GetTables() {
			tableName := targetTable.GetName()
			liveTable, ok := liveSchema.Tables[tableName]
			if !ok {
				continue
			}

			targetPKs := targetTable.GetPrimaryKeys()
			hasTargetPK := len(targetPKs) > 0
			hasLivePK := liveTable.PrimaryKey != nil &&
				len(liveTable.PrimaryKey.Columns) > 0

			pkMismatch := false
			if hasTargetPK != hasLivePK {
				pkMismatch = true
			} else if hasTargetPK && hasLivePK {
				if len(targetPKs) != len(liveTable.PrimaryKey.Columns) {
					pkMismatch = true
				} else {
					for i, col := range targetPKs {
						if col != liveTable.PrimaryKey.Columns[i] {
							pkMismatch = true
							break
						}
					}
				}
			}

			if pkMismatch {
				if hasLivePK {
					sqlStr := fmt.Sprintf(
						"ALTER TABLE %q.%q DROP CONSTRAINT %q;",
						schemaName, tableName, liveTable.PrimaryKey.Name,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:          ActionTypeDrop,
						ObjectType:    ObjectPrimaryKey,
						Schema:        schemaName,
						Name:          liveTable.PrimaryKey.Name,
						SQL:           sqlStr,
						IsDestructive: false,
					})
				}

				if hasTargetPK {
					var colBuilder strings.Builder
					for i, pk := range targetPKs {
						if i > 0 {
							colBuilder.WriteString(", ")
						}
						colBuilder.WriteString(fmt.Sprintf("%q", pk))
					}

					pkName := fmt.Sprintf("%s_pkey", tableName)
					sqlStr := fmt.Sprintf(
						"ALTER TABLE %q.%q ADD CONSTRAINT %q PRIMARY KEY (%s);",
						schemaName, tableName, pkName, colBuilder.String(),
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeCreate,
						ObjectType: ObjectPrimaryKey,
						Schema:     schemaName,
						Name:       pkName,
						SQL:        sqlStr,
					})
				}
			}
		}
	}
	return nil
}

func (d *Differ) planIndexes() error {
	for _, targetSchema := range d.target.GetSchemas() {
		schemaName := targetSchema.GetName()
		liveSchema, ok := d.live.Schemas[schemaName]
		if !ok {
			continue
		}

		for _, targetTable := range targetSchema.GetTables() {
			tableName := targetTable.GetName()
			liveTable, ok := liveSchema.Tables[tableName]
			if !ok {
				continue
			}

			protected := make(map[string]struct{})
			var renames []renameOp

			for _, targetIndex := range targetTable.GetIndexes() {
				iName := targetIndex.GetName()
				pName := targetIndex.GetNamePrevious()
				if pName != "" && pName != iName {
					protected[pName] = struct{}{}
					renames = append(renames, renameOp{
						Prev: pName, New: iName, Ref: targetIndex,
					})
				} else {
					protected[iName] = struct{}{}
				}
			}

			var liveIdxNames []string
			for k := range liveTable.Indexes {
				liveIdxNames = append(liveIdxNames, k)
			}
			sort.Strings(liveIdxNames)

			for _, liveIdxName := range liveIdxNames {
				_, exists := protected[liveIdxName]
				if !exists {
					sqlStr := fmt.Sprintf(
						"DROP INDEX %q.%q;", schemaName, liveIdxName,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:          ActionTypeDrop,
						ObjectType:    ObjectIndex,
						Schema:        schemaName,
						Name:          liveIdxName,
						SQL:           sqlStr,
						IsDestructive: false,
					})
					delete(liveTable.Indexes, liveIdxName)
				}
			}

			for len(renames) > 0 {
				progress := false
				for i := 0; i < len(renames); i++ {
					op := renames[i]
					_, exists := liveTable.Indexes[op.New]
					if !exists {
						sqlStr := fmt.Sprintf(
							"ALTER INDEX %q.%q RENAME TO %q;",
							schemaName, op.Prev, op.New,
						)
						d.Actions = append(d.Actions, MigrationAction{
							Type:       ActionTypeRename,
							ObjectType: ObjectIndex,
							Schema:     schemaName,
							Name:       op.New,
							SQL:        sqlStr,
						})

						liveTable.Indexes[op.New] = liveTable.Indexes[op.Prev]
						liveTable.Indexes[op.New].Name = op.New
						delete(liveTable.Indexes, op.Prev)

						renames = append(renames[:i], renames[i+1:]...)
						progress = true
						break
					}
				}
				if !progress {
					var collisionOp *renameOp
					for i := range renames {
						isTargetInRenames := false
						for j := range renames {
							if renames[j].Prev == renames[i].New {
								isTargetInRenames = true
								break
							}
						}
						if !isTargetInRenames {
							collisionOp = &renames[i]
							break
						}
					}
					if collisionOp != nil {
						return fmt.Errorf("index rename collision -> %s", collisionOp.New)
					}

					op := renames[0]
					tmpName := "scheme_tmp_" + op.Prev
					sqlStr := fmt.Sprintf(
						"ALTER INDEX %q.%q RENAME TO %q;",
						schemaName, op.Prev, tmpName,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeRename,
						ObjectType: ObjectIndex,
						Schema:     schemaName,
						Name:       tmpName,
						SQL:        sqlStr,
					})

					liveTable.Indexes[tmpName] = liveTable.Indexes[op.Prev]
					liveTable.Indexes[tmpName].Name = tmpName
					delete(liveTable.Indexes, op.Prev)

					renames[0].Prev = tmpName
				}
			}

			for _, targetIndex := range targetTable.GetIndexes() {
				idxName := targetIndex.GetName()
				_, exists := liveTable.Indexes[idxName]
				if !exists {
					var colBuilder strings.Builder
					for i, col := range targetIndex.GetColumns() {
						if i > 0 {
							colBuilder.WriteString(", ")
						}
						colBuilder.WriteString(fmt.Sprintf("%q", col.GetName()))
					}

					uniqueMod := ""
					if targetIndex.GetIsUnique() {
						uniqueMod = "UNIQUE "
					}

					sqlStr := fmt.Sprintf(
						"CREATE %sINDEX %q ON %q.%q (%s);",
						uniqueMod, idxName, schemaName, tableName,
						colBuilder.String(),
					)

					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeCreate,
						ObjectType: ObjectIndex,
						Schema:     schemaName,
						Name:       idxName,
						SQL:        sqlStr,
					})
				}
			}
		}
	}
	return nil
}

func (d *Differ) planForeignKeys() error {
        for _, targetSchema := range d.target.GetSchemas() {
                schemaName := targetSchema.GetName()
                liveSchema, ok := d.live.Schemas[schemaName]
                if !ok {
                        continue
                }

                for _, targetTable := range targetSchema.GetTables() {
                        tableName := targetTable.GetName()
                        liveTable, ok := liveSchema.Tables[tableName]
                        if !ok {
                                continue
                        }

                        protected := make(map[string]struct{})
                        var renames []renameOp
                        targetFKMap := make(map[string]struct{})

                        for _, targetFK := range targetTable.GetForeignKeys() {
                                fkName := targetFK.GetName()
                                pName := targetFK.GetNamePrevious()
                                targetFKMap[fkName] = struct{}{}

                                if pName != "" && pName != fkName {
                                        protected[pName] = struct{}{}
                                        renames = append(renames, renameOp{
                                                Prev: pName, New: fkName, Ref: targetFK,
                                        })
                                } else {
                                        protected[fkName] = struct{}{}
                                }
                        }

                        var liveFKNames []string
                        for k := range liveTable.ForeignKeys {
                                liveFKNames = append(liveFKNames, k)
                        }
                        sort.Strings(liveFKNames)

                        for _, liveFKName := range liveFKNames {
                                _, exists := protected[liveFKName]
                                if !exists {
                                        sqlStr := fmt.Sprintf(
                                                "ALTER TABLE %q.%q DROP CONSTRAINT %q;",
                                                schemaName, tableName, liveFKName,
                                        )
                                        d.Actions = append(d.Actions, MigrationAction{
                                                Type:          ActionTypeDrop,
                                                ObjectType:    ObjectForeignKey,
                                                Schema:        schemaName,
                                                Name:          liveFKName,
                                                SQL:           sqlStr,
                                                IsDestructive: false,
                                        })
                                        delete(liveTable.ForeignKeys, liveFKName)
                                }
                        }

                        for len(renames) > 0 {
                                progress := false
                                for i := 0; i < len(renames); i++ {
                                        op := renames[i]
                                        _, exists := liveTable.ForeignKeys[op.New]
                                        if !exists {
                                                sqlStr := fmt.Sprintf(
                                                        "ALTER TABLE %q.%q RENAME CONSTRAINT %q TO %q;",
                                                        schemaName, tableName, op.Prev, op.New,
                                                )
                                                d.Actions = append(d.Actions, MigrationAction{
                                                        Type:       ActionTypeRename,
                                                        ObjectType: ObjectForeignKey,
                                                        Schema:     schemaName,
                                                        Name:       op.New,
                                                        SQL:        sqlStr,
                                                })

                                                liveTable.ForeignKeys[op.New] = liveTable.ForeignKeys[op.Prev]
                                                liveTable.ForeignKeys[op.New].Name = op.New
                                                delete(liveTable.ForeignKeys, op.Prev)

                                                renames = append(renames[:i], renames[i+1:]...)
                                                progress = true
                                                break
                                        }
                                }
                                if !progress {
                                        var collisionOp *renameOp
                                        for i := range renames {
                                                isTargetInRenames := false
                                                for j := range renames {
                                                        if renames[j].Prev == renames[i].New {
                                                                isTargetInRenames = true
                                                                break
                                                        }
                                                }
                                                if !isTargetInRenames {
                                                        collisionOp = &renames[i]
                                                        break
                                                }
                                        }
                                        if collisionOp != nil {
                                                return fmt.Errorf("foreign key rename collision -> %s", collisionOp.New)
                                        }

                                        op := renames[0]
                                        tmpName := "scheme_tmp_fk_" + op.Prev
                                        sqlStr := fmt.Sprintf(
                                                "ALTER TABLE %q.%q RENAME CONSTRAINT %q TO %q;",
                                                schemaName, tableName, op.Prev, tmpName,
                                        )
                                        d.Actions = append(d.Actions, MigrationAction{
                                                Type:       ActionTypeRename,
                                                ObjectType: ObjectForeignKey,
                                                Schema:     schemaName,
                                                Name:       tmpName,
                                                SQL:        sqlStr,
                                        })
                                        renames[0].Prev = tmpName
                                        liveTable.ForeignKeys[tmpName] = liveTable.ForeignKeys[op.Prev]
                                        liveTable.ForeignKeys[tmpName].Name = tmpName
                                        delete(liveTable.ForeignKeys, op.Prev)
                                }
                        }

                        for _, targetFK := range targetTable.GetForeignKeys() {
                                fkName := targetFK.GetName()
                                _, exists := liveTable.ForeignKeys[fkName]
                                if !exists {
					var localCols strings.Builder
					var targetCols strings.Builder

					for i, mapping := range targetFK.GetColumns() {
						if i > 0 {
							localCols.WriteString(", ")
							targetCols.WriteString(", ")
						}
						localCols.WriteString(
							fmt.Sprintf("%q", mapping.GetSourceColumn()),
						)
						targetCols.WriteString(
							fmt.Sprintf("%q", mapping.GetTargetColumn()),
						)
					}

					targetTableRef := targetFK.GetTargetTable()
					if !strings.Contains(targetTableRef, ".") {
					        targetTableRef = fmt.Sprintf(
					                "%q.%q", schemaName, targetTableRef,
					        )
					} else {
					        parts := strings.SplitN(targetTableRef, ".", 2)
					        targetTableRef = fmt.Sprintf(
					                "%q.%q", parts[0], parts[1],
					        )
					}

					onUpdate := ""
					switch targetFK.GetOnUpdate().String() {
					case "FOREIGN_KEY_ACTION_NO_ACTION":
					        onUpdate = " ON UPDATE NO ACTION"
					case "FOREIGN_KEY_ACTION_RESTRICT":
					        onUpdate = " ON UPDATE RESTRICT"
					case "FOREIGN_KEY_ACTION_CASCADE":
					        onUpdate = " ON UPDATE CASCADE"
					case "FOREIGN_KEY_ACTION_SET_NULL":
					        onUpdate = " ON UPDATE SET NULL"
					case "FOREIGN_KEY_ACTION_SET_DEFAULT":
					        onUpdate = " ON UPDATE SET DEFAULT"
					}

					onDelete := ""
					switch targetFK.GetOnDelete().String() {
					case "FOREIGN_KEY_ACTION_NO_ACTION":
					        onDelete = " ON DELETE NO ACTION"
					case "FOREIGN_KEY_ACTION_RESTRICT":
					        onDelete = " ON DELETE RESTRICT"
					case "FOREIGN_KEY_ACTION_CASCADE":
					        onDelete = " ON DELETE CASCADE"
					case "FOREIGN_KEY_ACTION_SET_NULL":
					        onDelete = " ON DELETE SET NULL"
					case "FOREIGN_KEY_ACTION_SET_DEFAULT":
					        onDelete = " ON DELETE SET DEFAULT"
					}

					sqlStr := fmt.Sprintf(
					        "ALTER TABLE %q.%q ADD CONSTRAINT %q FOREIGN KEY (%s) REFERENCES %s (%s)%s%s;",
					        schemaName, tableName, fkName,
					        localCols.String(), targetTableRef, targetCols.String(),
					        onUpdate, onDelete,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeCreate,
						ObjectType: ObjectForeignKey,
						Schema:     schemaName,
						Name:       fkName,
						SQL:        sqlStr,
					})
				}
			}

			liveFKNames = []string{}
			for k := range liveTable.ForeignKeys {
				liveFKNames = append(liveFKNames, k)
			}
			sort.Strings(liveFKNames)

			for _, liveFKName := range liveFKNames {
				_, exists := targetFKMap[liveFKName]
				if !exists {
					sqlStr := fmt.Sprintf(
						"ALTER TABLE %q.%q DROP CONSTRAINT %q;",
						schemaName, tableName, liveFKName,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:          ActionTypeDrop,
						ObjectType:    ObjectForeignKey,
						Schema:        schemaName,
						Name:          liveFKName,
						SQL:           sqlStr,
						IsDestructive: false,
					})
				}
			}
		}
	}
	return nil
}

// resolvePGType translates the robust AST data types back into raw PostgreSQL
// string types for DDL execution.
func resolvePGType(dt *postgres.DataType) (string, error) {
	if dt == nil {
		return "", fmt.Errorf("data type cannot be nil")
	}

	switch t := dt.GetType().(type) {
	case *postgres.DataType_UuidType:
		return "uuid", nil
	case *postgres.DataType_IntegerType:
		return "integer", nil
	case *postgres.DataType_SmallintType:
		return "smallint", nil
	case *postgres.DataType_BigintType:
		return "bigint", nil
	case *postgres.DataType_VarcharType:
		if t.VarcharType != nil && t.VarcharType.Length != nil {
			return fmt.Sprintf("varchar(%d)", *t.VarcharType.Length), nil
		}
		return "varchar", nil
	case *postgres.DataType_TextType:
		return "text", nil
	case *postgres.DataType_BooleanType:
		return "boolean", nil
	case *postgres.DataType_TimestampType:
		return "timestamp", nil
	case *postgres.DataType_TimestamptzType:
		return "timestamptz", nil
	default:
		return "text /* FIXME: unsupported structural mapping */", nil
	}
}
