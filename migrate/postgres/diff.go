package postgres

import (
	"fmt"
	"slices"
	"strings"
)

// Differ provides the state reconciliation engine.
type Differ struct {
	Actions []MigrationAction
	live    *DatabaseState
	scratch *DatabaseState
	target  *DatabaseState
}

// pendingRename tracks an object being renamed across two distinct phases to
// avoid temporary namespace collisions during a complex migration.
type pendingRename struct {
	targetName string
	tempName   string
	oldName    string
}

// ComputeDiff executes state reconciliation between a live database state and a
// desired target state, returning the sequence of MigrationActions required.
func ComputeDiff(
	live *DatabaseState,
	target *DatabaseState,
) ([]MigrationAction, error) {
	if live == nil || target == nil {
		return nil, fmt.Errorf("live and target states cannot be nil")
	}

	d := &Differ{
		Actions: make([]MigrationAction, 0),
		live:    live,
		scratch: live.Clone(),
		target:  target,
	}

	err := d.planSchemas()
	if err != nil {
		return nil, err
	}

	err = d.planEnums()
	if err != nil {
		return nil, err
	}

	err = d.planComposites()
	if err != nil {
		return nil, err
	}

	err = d.planDomains()
	if err != nil {
		return nil, err
	}

	err = d.planTables()
	if err != nil {
		return nil, err
	}

	err = d.planColumns()
	if err != nil {
		return nil, err
	}

	err = d.planPrimaryKeys()
	if err != nil {
		return nil, err
	}

	err = d.planIndexes()
	if err != nil {
		return nil, err
	}

	err = d.planForeignKeys()
	if err != nil {
		return nil, err
	}

	return d.Actions, nil
}

// planSchemas evaluates schema lifecycle operations.
//  1. Identifies renames using the NamePrevious field, parking them in a
//     temp namespace.
//  2. Completes all parked renames to their final target namespace.
//  3. Creates any new target schemas that do not exist natively yet.
//  4. Drops any live database schemas that no longer exist in the target state.
func (d *Differ) planSchemas() error {
	targetNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		targetNames = append(targetNames, k)
	}
	slices.Sort(targetNames)

	// Step 1: Move required renames to temp names to free up namespace
	pendingRenames := make(map[string]pendingRename)
	for _, tName := range targetNames {
		targetSchema, ok := d.target.Schemas[tName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				tName,
			)
		}

		pName := targetSchema.NamePrevious
		if pName == "" || pName == tName {
			continue
		}

		_, ok = d.scratch.Schemas[pName]
		if !ok {
			continue
		}

		tmpName := "scheme_tmp_schema_" + pName
		d.Actions = append(d.Actions, MigrationAction{
			Type:       ActionTypeRename,
			ObjectType: ObjectSchema,
			Name:       tmpName,
			SQL:        fmt.Sprintf("ALTER SCHEMA %q RENAME TO %q;", pName, tmpName),
		})

		d.scratch.Schemas[tmpName] = d.scratch.Schemas[pName]
		d.scratch.Schemas[tmpName].Name = tmpName
		delete(d.scratch.Schemas, pName)

		pendingRenames[tName] = pendingRename{
			tempName:   tmpName,
			oldName:    pName,
			targetName: tName,
		}
	}

	// Step 2: Move all pending renames to their final names
	for _, tName := range targetNames {
		info, isRenaming := pendingRenames[tName]
		if !isRenaming {
			continue
		}

		d.Actions = append(d.Actions, MigrationAction{
			Type:       ActionTypeRename,
			ObjectType: ObjectSchema,
			Name:       tName,
			SQL: fmt.Sprintf(
				"ALTER SCHEMA %q RENAME TO %q;",
				info.tempName, tName,
			),
		})

		d.scratch.Schemas[tName] = d.scratch.Schemas[info.tempName]
		d.scratch.Schemas[tName].Name = tName
		delete(d.scratch.Schemas, info.tempName)
	}

	// Step 3: Handle creation of new schemas
	for _, tName := range targetNames {
		_, exists := d.scratch.Schemas[tName]
		if exists {
			continue
		}

		sqlStr := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q;", tName)
		d.Actions = append(d.Actions, MigrationAction{
			Type:       ActionTypeCreate,
			ObjectType: ObjectSchema,
			Name:       tName,
			SQL:        sqlStr,
		})
		d.scratch.Schemas[tName] = &SchemaState{
			Name:       tName,
			Tables:     make(map[string]*TableState),
			Enums:      make(map[string]*EnumState),
			Composites: make(map[string]*CompositeState),
			Domains:    make(map[string]*DomainState),
		}
	}

	// Step 4: Delete schemas that do not exist in the current d.target
	scratchNames := make([]string, 0, len(d.scratch.Schemas))
	for k := range d.scratch.Schemas {
		scratchNames = append(scratchNames, k)
	}
	slices.Sort(scratchNames)

	for _, sName := range scratchNames {
		_, exists := d.target.Schemas[sName]
		if exists {
			continue
		}

		d.Actions = append(d.Actions, MigrationAction{
			Type:          ActionTypeDrop,
			ObjectType:    ObjectSchema,
			Name:          sName,
			SQL:           fmt.Sprintf("DROP SCHEMA %q CASCADE;", sName),
			IsDestructive: true,
		})
		delete(d.scratch.Schemas, sName)
	}

	return nil
}

// planEnums evaluates enum lifecycle operations.
//  1. Identifies renames using the NamePrevious field, parking them in a
//     temp namespace.
//  2. Completes all parked renames to their final target namespace.
//  3. Alters existing enums to add new values.
//  4. Creates any new target enums that do not exist natively yet.
//  5. Drops any live database enums that no longer exist in the target state.
func (d *Differ) planEnums() error {
	schemaNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		schemaNames = append(schemaNames, k)
	}
	slices.Sort(schemaNames)

	for _, schemaName := range schemaNames {
		targetSchema, ok := d.target.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				schemaName,
			)
		}

		liveSchema, ok := d.scratch.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q missing from scratch.Schemas "+
					"(you may need to call planSchemas() first)", schemaName,
			)
		}

		targetNames := make([]string, 0, len(targetSchema.Enums))
		for k := range targetSchema.Enums {
			targetNames = append(targetNames, k)
		}
		slices.Sort(targetNames)

		// Step 1: Move required renames to temp names to free up namespace
		pendingRenames := make(map[string]pendingRename)
		for _, tName := range targetNames {
			targetEnum, ok := targetSchema.Enums[tName]

			if !ok {
				return fmt.Errorf(
					"enum %q not found in target schema %q",
					tName, schemaName,
				)
			}

			pName := targetEnum.NamePrevious
			if pName == "" || pName == tName {
				continue
			}

			_, ok = liveSchema.Enums[pName]
			if !ok {
				continue
			}

			tmpName := "scheme_tmp_enum_" + pName
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectEnum,
				Schema:     schemaName,
				Name:       tmpName,
				SQL: fmt.Sprintf(
					"ALTER TYPE %q.%q RENAME TO %q;",
					schemaName, pName, tmpName,
				),
			})

			liveSchema.Enums[tmpName] = liveSchema.Enums[pName]
			liveSchema.Enums[tmpName].Name = tmpName
			delete(liveSchema.Enums, pName)

			pendingRenames[tName] = pendingRename{
				tempName:   tmpName,
				oldName:    pName,
				targetName: tName,
			}
		}

		// Step 2: Move all pending renames to their final names
		for _, tName := range targetNames {
			info, isRenaming := pendingRenames[tName]
			if !isRenaming {
				continue
			}

			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectEnum,
				Schema:     schemaName,
				Name:       tName,
				SQL: fmt.Sprintf(
					"ALTER TYPE %q.%q RENAME TO %q;",
					schemaName, info.tempName, tName,
				),
			})

			liveSchema.Enums[tName] = liveSchema.Enums[info.tempName]
			liveSchema.Enums[tName].Name = tName
			delete(liveSchema.Enums, info.tempName)
		}

		// Step 3a: Alter existing enums to add new values
		for _, tName := range targetNames {
			targetEnum, ok := targetSchema.Enums[tName]

			if !ok {
				return fmt.Errorf(
					"enum %q not found in target schema %q",
					tName, schemaName,
				)
			}

			liveEnum, exists := liveSchema.Enums[tName]
			if !exists {
				continue
			}

			for _, targetVal := range targetEnum.Values {
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
						schemaName, tName, targetVal,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeAlter,
						ObjectType: ObjectEnum,
						Schema:     schemaName,
						Name:       tName,
						SQL:        sqlStr,
					})
					liveEnum.Values = append(liveEnum.Values, targetVal)
				}
			}
		}

		// Step 3b: Alter existing enums to destructively remove unmapped values.
		// PostgreSQL does not natively support ALTER TYPE ... DROP VALUE.

		// Step 4: Handle creation of new enums
		for _, tName := range targetNames {
			targetEnum, ok := targetSchema.Enums[tName]

			if !ok {
				return fmt.Errorf(
					"enum %q not found in target schema %q",
					tName, schemaName,
				)
			}

			_, exists := liveSchema.Enums[tName]
			if exists {
				continue
			}

			vals := make([]string, 0, len(targetEnum.Values))
			for _, val := range targetEnum.Values {
				vals = append(vals, fmt.Sprintf("'%s'", val))
			}
			sqlStr := fmt.Sprintf(
				"CREATE TYPE %q.%q AS ENUM (%s);",
				schemaName, tName, strings.Join(vals, ", "),
			)
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeCreate,
				ObjectType: ObjectEnum,
				Schema:     schemaName,
				Name:       tName,
				SQL:        sqlStr,
			})
			liveSchema.Enums[tName] = &EnumState{
				Name:   tName,
				Values: targetEnum.Values,
			}
		}

		// Step 5: Drop live enums that no longer exist in target
		liveNames := make([]string, 0, len(liveSchema.Enums))
		for k := range liveSchema.Enums {
			liveNames = append(liveNames, k)
		}
		slices.Sort(liveNames)

		for _, liveName := range liveNames {
			_, exists := targetSchema.Enums[liveName]
			if exists {
				continue
			}

			sqlStr := fmt.Sprintf("DROP TYPE %q.%q CASCADE;", schemaName, liveName)
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

	return nil
}

// planComposites evaluates composite type lifecycle operations.
//  1. Identifies renames using the NamePrevious field, parking them in a
//     temp namespace.
//  2. Completes all parked renames to their final target namespace.
//  3. Alters existing composites to add or drop attributes.
//  4. Creates any new target composites that do not exist natively yet.
//  5. Drops any live database composites that no longer exist in the target
//     state.
func (d *Differ) planComposites() error {
	schemaNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		schemaNames = append(schemaNames, k)
	}
	slices.Sort(schemaNames)

	for _, schemaName := range schemaNames {
		targetSchema, ok := d.target.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				schemaName,
			)
		}

		liveSchema, ok := d.scratch.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q missing from scratch.Schemas "+
					"(you may need to call planSchemas() first)", schemaName,
			)
		}

		targetNames := make([]string, 0, len(targetSchema.Composites))
		for k := range targetSchema.Composites {
			targetNames = append(targetNames, k)
		}
		slices.Sort(targetNames)

		// Step 1: Move required renames to temp names to free up namespace
		pendingRenames := make(map[string]pendingRename)
		for _, tName := range targetNames {
			targetComp, ok := targetSchema.Composites[tName]
			if !ok {
				return fmt.Errorf(
					"composite %q not found in target schema %q",
					tName, schemaName,
				)
			}

			pName := targetComp.NamePrevious
			if pName == "" || pName == tName {
				continue
			}

			_, ok = liveSchema.Composites[pName]
			if !ok {
				continue
			}

			tmpName := "scheme_tmp_comp_" + pName
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectComposite,
				Schema:     schemaName,
				Name:       tmpName,
				SQL: fmt.Sprintf(
					"ALTER TYPE %q.%q RENAME TO %q;",
					schemaName, pName, tmpName,
				),
			})

			liveSchema.Composites[tmpName] = liveSchema.Composites[pName]
			liveSchema.Composites[tmpName].Name = tmpName
			delete(liveSchema.Composites, pName)

			pendingRenames[tName] = pendingRename{
				tempName:   tmpName,
				oldName:    pName,
				targetName: tName,
			}
		}

		// Step 2: Move all pending renames to their final names
		for _, tName := range targetNames {
			info, isRenaming := pendingRenames[tName]
			if !isRenaming {
				continue
			}

			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectComposite,
				Schema:     schemaName,
				Name:       tName,
				SQL: fmt.Sprintf(
					"ALTER TYPE %q.%q RENAME TO %q;",
					schemaName, info.tempName, tName,
				),
			})

			liveSchema.Composites[tName] = liveSchema.Composites[info.tempName]
			liveSchema.Composites[tName].Name = tName
			delete(liveSchema.Composites, info.tempName)
		}

		// Step 3a: Alter existing composites to add new attributes
		for _, tName := range targetNames {
			targetComp, ok := targetSchema.Composites[tName]
			if !ok {
				return fmt.Errorf(
					"composite %q not found in target schema %q",
					tName, schemaName,
				)
			}

			liveComp, exists := liveSchema.Composites[tName]
			if !exists {
				continue
			}

			targetFields := make([]string, 0, len(targetComp.Fields))
			for k := range targetComp.Fields {
				targetFields = append(targetFields, k)
			}
			slices.SortFunc(targetFields, func(a string, b string) int {
				return targetComp.Fields[a].Position - targetComp.Fields[b].Position
			})

			for _, fName := range targetFields {
				targetField, ok := targetComp.Fields[fName]
				if !ok {
					return fmt.Errorf(
						"field %q not found in target composite %q",
						fName, tName,
					)
				}

				_, existsLiveField := liveComp.Fields[fName]
				if !existsLiveField {
					sqlStr := fmt.Sprintf(
						"ALTER TYPE %q.%q ADD ATTRIBUTE %q %s;",
						schemaName, tName, targetField.Name, targetField.DataType,
					)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeAlter,
						ObjectType: ObjectComposite,
						Schema:     schemaName,
						Name:       tName,
						SQL:        sqlStr,
					})
					liveComp.Fields[fName] = &CompositeFieldState{
						Name:     targetField.Name,
						DataType: targetField.DataType,
					}
				}
			}
		}

		// Step 3b: Alter existing composites to drop removed attributes
		for _, tName := range targetNames {
			targetComp, ok := targetSchema.Composites[tName]
			if !ok {
				return fmt.Errorf(
					"composite %q not found in target schema %q",
					tName, schemaName,
				)
			}

			liveComp, exists := liveSchema.Composites[tName]
			if !exists {
				continue
			}

			targetFieldMap := make(map[string]struct{})
			for k := range targetComp.Fields {
				targetFieldMap[k] = struct{}{}
			}

			liveFieldNames := make([]string, 0, len(liveComp.Fields))
			for k := range liveComp.Fields {
				liveFieldNames = append(liveFieldNames, k)
			}
			slices.Sort(liveFieldNames)

			for _, liveFieldName := range liveFieldNames {
				_, exists := targetFieldMap[liveFieldName]
				if exists {
					continue
				}

				sqlStr := fmt.Sprintf(
					"ALTER TYPE %q.%q DROP ATTRIBUTE %q CASCADE;",
					schemaName, tName, liveFieldName,
				)
				actionName := fmt.Sprintf("%s.%s", tName, liveFieldName)
				d.Actions = append(d.Actions, MigrationAction{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectComposite,
					Schema:        schemaName,
					Name:          actionName,
					SQL:           sqlStr,
					IsDestructive: true,
				})
				delete(liveComp.Fields, liveFieldName)
			}
		}

		// Step 4: Handle creation of new composites
		for _, tName := range targetNames {
			targetComp, ok := targetSchema.Composites[tName]
			if !ok {
				return fmt.Errorf(
					"composite %q not found in target schema %q",
					tName, schemaName,
				)
			}

			_, exists := liveSchema.Composites[tName]
			if exists {
				continue
			}

			fields := make([]string, 0, len(targetComp.Fields))
			for k := range targetComp.Fields {
				fields = append(fields, k)
			}
			slices.SortFunc(fields, func(a string, b string) int {
				return targetComp.Fields[a].Position - targetComp.Fields[b].Position
			})

			vals := make([]string, 0, len(fields))
			for _, fName := range fields {
				field, ok := targetComp.Fields[fName]
				if !ok {
					return fmt.Errorf(
						"field %q not found in target composite %q",
						fName, tName,
					)
				}
				vals = append(vals, fmt.Sprintf("%q %s", field.Name, field.DataType))
			}

			sqlStr := fmt.Sprintf(
				"CREATE TYPE %q.%q AS (%s);",
				schemaName, tName, strings.Join(vals, ", "),
			)
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeCreate,
				ObjectType: ObjectComposite,
				Schema:     schemaName,
				Name:       tName,
				SQL:        sqlStr,
			})

			liveSchema.Composites[tName] = &CompositeState{
				Name:   tName,
				Fields: make(map[string]*CompositeFieldState),
			}
		}

		// Step 5: Drop live composites that no longer exist in target
		liveNames := make([]string, 0, len(liveSchema.Composites))
		for k := range liveSchema.Composites {
			liveNames = append(liveNames, k)
		}
		slices.Sort(liveNames)

		for _, liveName := range liveNames {
			_, exists := targetSchema.Composites[liveName]
			if exists {
				continue
			}

			sqlStr := fmt.Sprintf("DROP TYPE %q.%q CASCADE;", schemaName, liveName)
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

	return nil
}

// planDomains evaluates domain lifecycle operations.
//  1. Identifies renames using the NamePrevious field, parking them in a
//     temp namespace.
//  2. Completes all parked renames to their final target namespace.
//  3. Alters existing domains (recreating if the underlying data type
//     changes).
//  4. Creates any new target domains that do not exist natively yet.
//  5. Drops any live database domains that no longer exist in the target state.
func (d *Differ) planDomains() error {
	schemaNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		schemaNames = append(schemaNames, k)
	}
	slices.Sort(schemaNames)

	for _, schemaName := range schemaNames {
		targetSchema, ok := d.target.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				schemaName,
			)
		}

		liveSchema, ok := d.scratch.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q missing from scratch.Schemas "+
					"(you may need to call planSchemas() first)", schemaName,
			)
		}

		targetNames := make([]string, 0, len(targetSchema.Domains))
		for k := range targetSchema.Domains {
			targetNames = append(targetNames, k)
		}
		slices.Sort(targetNames)

		// Step 1: Move required renames to temp names to free up namespace
		pendingRenames := make(map[string]pendingRename)
		for _, tName := range targetNames {
			targetDom, ok := targetSchema.Domains[tName]
			if !ok {
				return fmt.Errorf(
					"domain %q not found in target schema %q",
					tName, schemaName,
				)
			}

			pName := targetDom.NamePrevious
			if pName == "" || pName == tName {
				continue
			}

			_, ok = liveSchema.Domains[pName]
			if !ok {
				continue
			}

			tmpName := "scheme_tmp_dom_" + pName
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectDomain,
				Schema:     schemaName,
				Name:       tmpName,
				SQL: fmt.Sprintf(
					"ALTER DOMAIN %q.%q RENAME TO %q;",
					schemaName, pName, tmpName,
				),
			})

			liveSchema.Domains[tmpName] = liveSchema.Domains[pName]
			liveSchema.Domains[tmpName].Name = tmpName
			delete(liveSchema.Domains, pName)

			pendingRenames[tName] = pendingRename{
				tempName:   tmpName,
				oldName:    pName,
				targetName: tName,
			}
		}

		// Step 2: Move all pending renames to their final names
		for _, tName := range targetNames {
			info, isRenaming := pendingRenames[tName]
			if !isRenaming {
				continue
			}

			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectDomain,
				Schema:     schemaName,
				Name:       tName,
				SQL: fmt.Sprintf(
					"ALTER DOMAIN %q.%q RENAME TO %q;",
					schemaName, info.tempName, tName,
				),
			})

			liveSchema.Domains[tName] = liveSchema.Domains[info.tempName]
			liveSchema.Domains[tName].Name = tName
			delete(liveSchema.Domains, info.tempName)
		}

		// Step 3: Alter existing domains (recreating if the underlying data type
		//     changes)
		for _, tName := range targetNames {
			targetDom, ok := targetSchema.Domains[tName]
			if !ok {
				return fmt.Errorf(
					"domain %q not found in target schema %q",
					tName, schemaName,
				)
			}

			liveDom, exists := liveSchema.Domains[tName]
			if !exists {
				continue
			}

			if liveDom.DataType != targetDom.DataType {
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeDrop,
					ObjectType: ObjectDomain,
					Schema:     schemaName,
					Name:       tName,
					SQL: fmt.Sprintf(
						"DROP DOMAIN %q.%q CASCADE;",
						schemaName, tName,
					),
					IsDestructive: true,
				})
				sqlStr := fmt.Sprintf(
					"CREATE DOMAIN %q.%q AS %s;",
					schemaName, tName, targetDom.DataType,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeCreate,
					ObjectType: ObjectDomain,
					Schema:     schemaName,
					Name:       tName,
					SQL:        sqlStr,
				})
				liveDom.DataType = targetDom.DataType
			}
		}

		// Step 4: Handle creation of new domains
		for _, tName := range targetNames {
			targetDom, ok := targetSchema.Domains[tName]
			if !ok {
				return fmt.Errorf(
					"domain %q not found in target schema %q",
					tName, schemaName,
				)
			}

			_, exists := liveSchema.Domains[tName]
			if exists {
				continue
			}

			sqlStr := fmt.Sprintf(
				"CREATE DOMAIN %q.%q AS %s;",
				schemaName, tName, targetDom.DataType,
			)
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeCreate,
				ObjectType: ObjectDomain,
				Schema:     schemaName,
				Name:       tName,
				SQL:        sqlStr,
			})

			liveSchema.Domains[tName] = &DomainState{
				Name:     tName,
				DataType: targetDom.DataType,
			}
		}

		// Step 5: Drop live domains that no longer exist in target
		liveNames := make([]string, 0, len(liveSchema.Domains))
		for k := range liveSchema.Domains {
			liveNames = append(liveNames, k)
		}
		slices.Sort(liveNames)

		for _, liveName := range liveNames {
			_, exists := targetSchema.Domains[liveName]
			if exists {
				continue
			}

			sqlStr := fmt.Sprintf("DROP DOMAIN %q.%q CASCADE;", schemaName, liveName)
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

	return nil
}

// planTables evaluates table lifecycle operations.
//  1. Identifies and completes renames using the NamePrevious field.
//  2. Creates any new target tables that do not exist natively yet.
//  3. Drops any live database tables that no longer exist in the target state.
func (d *Differ) planTables() error {
	schemaNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		schemaNames = append(schemaNames, k)
	}
	slices.Sort(schemaNames)

	for _, schemaName := range schemaNames {
		targetSchema, ok := d.target.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				schemaName,
			)
		}

		liveSchema, ok := d.scratch.Schemas[schemaName]
		if !ok {
			return fmt.Errorf("in-memory live schema missing -> %s", schemaName)
		}

		targetNames := make([]string, 0, len(targetSchema.Tables))
		for k := range targetSchema.Tables {
			targetNames = append(targetNames, k)
		}
		slices.Sort(targetNames)

		// Step 1: Move required renames to temp names to free up namespace
		pendingRenames := make(map[string]pendingRename)
		for _, tName := range targetNames {
			targetTable, ok := targetSchema.Tables[tName]
			if !ok {
				return fmt.Errorf(
					"table %q not found in target schema %q",
					tName, schemaName,
				)
			}

			pName := targetTable.NamePrevious
			if pName == "" || pName == tName {
				continue
			}

			_, ok = liveSchema.Tables[pName]
			if !ok {
				continue
			}

			tmpName := "scheme_tmp_table_" + pName
			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectTable,
				Schema:     schemaName,
				Name:       tmpName,
				SQL: fmt.Sprintf(
					"ALTER TABLE %q.%q RENAME TO %q;",
					schemaName, pName, tmpName,
				),
			})

			liveSchema.Tables[tmpName] = liveSchema.Tables[pName]
			liveSchema.Tables[tmpName].Name = tmpName
			delete(liveSchema.Tables, pName)

			pendingRenames[tName] = pendingRename{
				tempName:   tmpName,
				oldName:    pName,
				targetName: tName,
			}
		}

		// Step 2: Move all pending renames to their final names
		for _, tName := range targetNames {
			info, isRenaming := pendingRenames[tName]
			if !isRenaming {
				continue
			}

			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeRename,
				ObjectType: ObjectTable,
				Schema:     schemaName,
				Name:       tName,
				SQL: fmt.Sprintf(
					"ALTER TABLE %q.%q RENAME TO %q;",
					schemaName, info.tempName, tName,
				),
			})

			liveSchema.Tables[tName] = liveSchema.Tables[info.tempName]
			liveSchema.Tables[tName].Name = tName
			delete(liveSchema.Tables, info.tempName)
		}

		// Step 3: Handle creation of new tables
		for _, tName := range targetNames {
			_, ok := targetSchema.Tables[tName]
			if !ok {
				return fmt.Errorf(
					"table %q not found in target schema %q",
					tName, schemaName,
				)
			}

			_, exists := liveSchema.Tables[tName]
			if exists {
				continue
			}

			d.Actions = append(d.Actions, MigrationAction{
				Type:       ActionTypeCreate,
				ObjectType: ObjectTable,
				Schema:     schemaName,
				Name:       tName,
				SQL:        fmt.Sprintf("CREATE TABLE %q.%q ();", schemaName, tName),
			})

			liveSchema.Tables[tName] = &TableState{
				Name:        tName,
				Columns:     make(map[string]*ColumnState),
				Indexes:     make(map[string]*IndexState),
				ForeignKeys: make(map[string]*ForeignKeyState),
			}
		}

		// Step 4: Drop live tables that no longer exist in target
		liveNames := make([]string, 0, len(liveSchema.Tables))
		for k := range liveSchema.Tables {
			liveNames = append(liveNames, k)
		}
		slices.Sort(liveNames)

		for _, liveName := range liveNames {
			_, exists := targetSchema.Tables[liveName]
			if exists {
				continue
			}

			sqlStr := fmt.Sprintf("DROP TABLE %q.%q CASCADE;", schemaName, liveName)
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
	return nil
}

// planColumns evaluates column lifecycle operations for all tables.
//  1. Identifies renames using the NamePrevious field, parking them in a
//     temp namespace.
//  2. Completes all parked renames to their final target namespace.
//  3. Alters existing columns (type, nullability, defaults).
//  4. Creates any new target columns that do not exist natively yet.
//  5. Drops any live database columns that no longer exist in the target state.
func (d *Differ) planColumns() error {
	schemaNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		schemaNames = append(schemaNames, k)
	}
	slices.Sort(schemaNames)

	for _, schemaName := range schemaNames {
		targetSchema, ok := d.target.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				schemaName,
			)
		}

		liveSchema, ok := d.scratch.Schemas[schemaName]
		if !ok {
			continue
		}

		targetTableNames := make([]string, 0, len(targetSchema.Tables))
		for k := range targetSchema.Tables {
			targetTableNames = append(targetTableNames, k)
		}
		slices.Sort(targetTableNames)

		for _, tName := range targetTableNames {
			targetTable, ok := targetSchema.Tables[tName]
			if !ok {
				return fmt.Errorf(
					"table %q not found in target schema %q",
					tName, schemaName,
				)
			}

			liveTable, ok := liveSchema.Tables[tName]
			if !ok {
				continue
			}

			targetColNames := make([]string, 0, len(targetTable.Columns))
			for k := range targetTable.Columns {
				targetColNames = append(targetColNames, k)
			}
			slices.Sort(targetColNames)

			// Step 1: Move required renames to temp names to free up namespace
			pendingRenames := make(map[string]pendingRename)
			for _, cName := range targetColNames {
				targetCol, ok := targetTable.Columns[cName]
				if !ok {
					return fmt.Errorf(
						"column %q not found in target table %q",
						cName, tName,
					)
				}

				pName := targetCol.NamePrevious
				if pName == "" || pName == cName {
					continue
				}

				_, ok = liveTable.Columns[pName]
				if !ok {
					continue
				}

				tmpName := "scheme_tmp_col_" + pName
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectColumn,
					Schema:     schemaName,
					Name:       fmt.Sprintf("%s.%s", tName, tmpName),
					SQL: fmt.Sprintf(
						"ALTER TABLE %q.%q RENAME COLUMN %q TO %q;",
						schemaName, tName, pName, tmpName,
					),
				})

				liveTable.Columns[tmpName] = liveTable.Columns[pName]
				liveTable.Columns[tmpName].Name = tmpName
				delete(liveTable.Columns, pName)

				pendingRenames[cName] = pendingRename{
					tempName:   tmpName,
					oldName:    pName,
					targetName: cName,
				}
			}

			// Step 2: Move all pending renames to their final names
			for _, cName := range targetColNames {
				info, isRenaming := pendingRenames[cName]
				if !isRenaming {
					continue
				}

				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectColumn,
					Schema:     schemaName,
					Name:       fmt.Sprintf("%s.%s", tName, cName),
					SQL: fmt.Sprintf(
						"ALTER TABLE %q.%q RENAME COLUMN %q TO %q;",
						schemaName, tName, info.tempName, cName,
					),
				})

				liveTable.Columns[cName] = liveTable.Columns[info.tempName]
				liveTable.Columns[cName].Name = cName
				delete(liveTable.Columns, info.tempName)
			}

			// Step 3: Alter existing columns
			for _, colName := range targetColNames {
				targetCol, ok := targetTable.Columns[colName]
				if !ok {
					return fmt.Errorf(
						"column %q not found in target table %q",
						colName, tName,
					)
				}

				liveCol, exists := liveTable.Columns[colName]
				if !exists {
					continue
				}

				if liveCol.DataType != targetCol.DataType {
					sqlStr := fmt.Sprintf(
						"ALTER TABLE %q.%q ALTER COLUMN %q TYPE %s;",
						schemaName, tName, colName, targetCol.DataType,
					)
					actionName := fmt.Sprintf("%s.%s", tName, colName)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeAlter,
						ObjectType: ObjectColumn,
						Schema:     schemaName,
						Name:       actionName,
						SQL:        sqlStr,
					})
					liveCol.DataType = targetCol.DataType
				}

				if liveCol.IsNullable != targetCol.IsNullable {
					actionWord := "SET"
					if targetCol.IsNullable {
						actionWord = "DROP"
					}
					sqlStr := fmt.Sprintf(
						"ALTER TABLE %q.%q ALTER COLUMN %q %s NOT NULL;",
						schemaName, tName, colName, actionWord,
					)
					actionName := fmt.Sprintf("%s.%s", tName, colName)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeAlter,
						ObjectType: ObjectColumn,
						Schema:     schemaName,
						Name:       actionName,
						SQL:        sqlStr,
					})
					liveCol.IsNullable = targetCol.IsNullable
				}

				targetDefault, liveDefault := "", ""
				if targetCol.ColumnDefault != nil {
					targetDefault = *targetCol.ColumnDefault
				}
				if liveCol.ColumnDefault != nil {
					liveDefault = *liveCol.ColumnDefault
				}

				if targetCol.IsAutoIncrement && targetDefault == "" &&
					strings.HasPrefix(liveDefault, "nextval(") {
					targetDefault = liveDefault
				}

				if targetDefault != liveDefault {
					mod := "DROP DEFAULT"
					if targetDefault != "" {
						mod = "SET DEFAULT " + targetDefault
					}
					sqlStr := fmt.Sprintf(
						"ALTER TABLE %q.%q ALTER COLUMN %q %s;",
						schemaName, tName, colName, mod,
					)
					actionName := fmt.Sprintf("%s.%s", tName, colName)
					d.Actions = append(d.Actions, MigrationAction{
						Type:       ActionTypeAlter,
						ObjectType: ObjectColumn,
						Schema:     schemaName,
						Name:       actionName,
						SQL:        sqlStr,
					})

					if targetDefault == "" {
						liveCol.ColumnDefault = nil
					} else {
						liveCol.ColumnDefault = new(targetDefault)
					}
				}
			}

			// Step 4: Handle creation of new columns
			for _, colName := range targetColNames {
				targetCol, ok := targetTable.Columns[colName]
				if !ok {
					return fmt.Errorf(
						"column %q not found in target table %q",
						colName, tName,
					)
				}

				_, exists := liveTable.Columns[colName]
				if exists {
					continue
				}

				nullMod := " NOT NULL"
				if targetCol.IsNullable {
					nullMod = ""
				}
				defaultMod := ""
				if targetCol.ColumnDefault != nil {
					defaultMod = " DEFAULT " + *targetCol.ColumnDefault
				}

				dataTypeStr := string(targetCol.DataType)
				autoIncMod := ""
				if targetCol.IsAutoIncrement {
					autoIncMod = " GENERATED BY DEFAULT AS IDENTITY"
					defaultMod = ""
				}

				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q ADD COLUMN %q %s%s%s%s;",
					schemaName, tName, colName, dataTypeStr, nullMod, defaultMod, autoIncMod,
				)
				actionName := fmt.Sprintf("%s.%s", tName, colName)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeCreate,
					ObjectType: ObjectColumn,
					Schema:     schemaName,
					Name:       actionName,
					SQL:        sqlStr,
				})

				liveTable.Columns[colName] = &ColumnState{
					Name:          targetCol.Name,
					DataType:      targetCol.DataType,
					IsNullable:    targetCol.IsNullable,
					ColumnDefault: targetCol.ColumnDefault,
				}
			}

			// Step 5: Drop live columns that no longer exist in target
			liveColNames := make([]string, 0, len(liveTable.Columns))
			for k := range liveTable.Columns {
				liveColNames = append(liveColNames, k)
			}
			slices.Sort(liveColNames)

			for _, liveColName := range liveColNames {
				_, exists := targetTable.Columns[liveColName]
				if exists {
					continue
				}

				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q DROP COLUMN %q CASCADE;",
					schemaName, tName, liveColName,
				)
				actionName := fmt.Sprintf("%s.%s", tName, liveColName)
				d.Actions = append(d.Actions, MigrationAction{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectColumn,
					Schema:        schemaName,
					Name:          actionName,
					SQL:           sqlStr,
					IsDestructive: true,
				})
				delete(liveTable.Columns, liveColName)
			}
		}
	}
	return nil
}

// planPrimaryKeys evaluates primary key lifecycle operations.
//  1. Drops any live primary keys that no longer exist or have mismatched
//     column definitions.
//  2. Creates any new target primary keys that do not exist natively yet.
func (d *Differ) planPrimaryKeys() error {
	schemaNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		schemaNames = append(schemaNames, k)
	}
	slices.Sort(schemaNames)

	for _, schemaName := range schemaNames {
		targetSchema, ok := d.target.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				schemaName,
			)
		}

		liveSchema, ok := d.scratch.Schemas[schemaName]
		if !ok {
			continue
		}

		targetNames := make([]string, 0, len(targetSchema.Tables))
		for k := range targetSchema.Tables {
			targetNames = append(targetNames, k)
		}
		slices.Sort(targetNames)

		for _, tName := range targetNames {
			targetTable, ok := targetSchema.Tables[tName]
			if !ok {
				return fmt.Errorf(
					"table %q not found in target schema %q",
					tName, schemaName,
				)
			}

			liveTable, ok := liveSchema.Tables[tName]
			if !ok {
				continue
			}

			hasTargetPK := targetTable.PrimaryKey != nil &&
				len(targetTable.PrimaryKey.Columns) > 0
			hasLivePK := liveTable.PrimaryKey != nil &&
				len(liveTable.PrimaryKey.Columns) > 0

			if !hasTargetPK && !hasLivePK {
				continue
			}

			pkMismatch := hasTargetPK != hasLivePK
			if !pkMismatch {
				targetLen := len(targetTable.PrimaryKey.Columns)
				liveLen := len(liveTable.PrimaryKey.Columns)
				if targetLen != liveLen {
					pkMismatch = true
				} else {
					for i, col := range targetTable.PrimaryKey.Columns {
						if col != liveTable.PrimaryKey.Columns[i] {
							pkMismatch = true
							break
						}
					}
				}
			}

			if !pkMismatch {
				continue
			}

			if hasLivePK {
				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q DROP CONSTRAINT %q;",
					schemaName, tName, liveTable.PrimaryKey.Name,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeDrop,
					ObjectType: ObjectPrimaryKey,
					Schema:     schemaName,
					Name:       liveTable.PrimaryKey.Name,
					SQL:        sqlStr,
				})
				liveTable.PrimaryKey = nil
			}
			if hasTargetPK {
				pkCols := make([]string, 0, len(targetTable.PrimaryKey.Columns))
				for _, pk := range targetTable.PrimaryKey.Columns {
					pkCols = append(pkCols, fmt.Sprintf("%q", pk))
				}
				pkName := targetTable.PrimaryKey.Name
				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q ADD CONSTRAINT %q PRIMARY KEY (%s);",
					schemaName, tName, pkName, strings.Join(pkCols, ", "),
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeCreate,
					ObjectType: ObjectPrimaryKey,
					Schema:     schemaName,
					Name:       pkName,
					SQL:        sqlStr,
				})
				liveTable.PrimaryKey = &PrimaryKeyState{
					Name:    targetTable.PrimaryKey.Name,
					Columns: targetTable.PrimaryKey.Columns,
				}
			}
		}
	}

	return nil
}

// planIndexes evaluates index lifecycle operations.
//  1. Identifies renames using the NamePrevious field, parking them in a
//     temp namespace.
//  2. Completes all parked renames to their final target namespace.
//  3. Drops existing indexes that have changed structurally.
//  4. Creates any new target indexes that do not exist natively yet.
//  5. Drops any live database indexes that no longer exist in the target state.
func (d *Differ) planIndexes() error {
	schemaNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		schemaNames = append(schemaNames, k)
	}
	slices.Sort(schemaNames)

	for _, schemaName := range schemaNames {
		targetSchema, ok := d.target.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				schemaName,
			)
		}

		liveSchema, ok := d.scratch.Schemas[schemaName]
		if !ok {
			continue
		}

		targetTableNames := make([]string, 0, len(targetSchema.Tables))
		for k := range targetSchema.Tables {
			targetTableNames = append(targetTableNames, k)
		}
		slices.Sort(targetTableNames)

		for _, tName := range targetTableNames {
			targetTable, ok := targetSchema.Tables[tName]
			if !ok {
				return fmt.Errorf(
					"table %q not found in target schema %q",
					tName, schemaName,
				)
			}

			liveTable, ok := liveSchema.Tables[tName]
			if !ok {
				continue
			}

			targetIdxNames := make([]string, 0, len(targetTable.Indexes))
			for k := range targetTable.Indexes {
				targetIdxNames = append(targetIdxNames, k)
			}
			slices.Sort(targetIdxNames)

			// Step 1: Move required renames to temp names to free up namespace
			pendingRenames := make(map[string]pendingRename)
			for _, idxName := range targetIdxNames {
				targetIndex, ok := targetTable.Indexes[idxName]
				if !ok {
					return fmt.Errorf(
						"index %q not found in target table %q",
						idxName, tName,
					)
				}

				pName := targetIndex.NamePrevious
				if pName == "" || pName == idxName {
					continue
				}

				_, ok = liveTable.Indexes[pName]
				if !ok {
					continue
				}

				tmpName := "scheme_tmp_idx_" + pName
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectIndex,
					Schema:     schemaName,
					Name:       tmpName,
					SQL: fmt.Sprintf(
						"ALTER INDEX %q.%q RENAME TO %q;",
						schemaName, pName, tmpName,
					),
				})

				liveTable.Indexes[tmpName] = liveTable.Indexes[pName]
				liveTable.Indexes[tmpName].Name = tmpName
				delete(liveTable.Indexes, pName)

				pendingRenames[idxName] = pendingRename{
					tempName:   tmpName,
					oldName:    pName,
					targetName: idxName,
				}
			}

			// Step 2: Move all pending renames to their final names
			for _, idxName := range targetIdxNames {
				info, isRenaming := pendingRenames[idxName]
				if !isRenaming {
					continue
				}

				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectIndex,
					Schema:     schemaName,
					Name:       idxName,
					SQL: fmt.Sprintf(
						"ALTER INDEX %q.%q RENAME TO %q;",
						schemaName, info.tempName, idxName,
					),
				})

				liveTable.Indexes[idxName] = liveTable.Indexes[info.tempName]
				liveTable.Indexes[idxName].Name = idxName
				delete(liveTable.Indexes, info.tempName)
			}

			// Step 3: Drop existing indexes that have changed structurally
			for _, idxName := range targetIdxNames {
				targetIndex, ok := targetTable.Indexes[idxName]
				if !ok {
					return fmt.Errorf(
						"index %q not found in target table %q",
						idxName, tName,
					)
				}

				liveIdx, exists := liveTable.Indexes[idxName]
				if !exists {
					continue
				}

				changed := liveIdx.IsUnique != targetIndex.IsUnique ||
					len(liveIdx.Columns) != len(targetIndex.Columns)

				if !changed {
					for i, col := range targetIndex.Columns {
						if liveIdx.Columns[i] != col {
							changed = true
							break
						}
					}
				}

				if !changed {
					continue
				}

				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeDrop,
					ObjectType: ObjectIndex,
					Schema:     schemaName,
					Name:       idxName,
					SQL: fmt.Sprintf(
						"DROP INDEX %q.%q;", schemaName, idxName,
					),
				})
				delete(liveTable.Indexes, idxName)
			}

			// Step 4: Handle creation of new indexes
			for _, idxName := range targetIdxNames {
				targetIndex, ok := targetTable.Indexes[idxName]
				if !ok {
					return fmt.Errorf(
						"index %q not found in target table %q",
						idxName, tName,
					)
				}
				_, exists := liveTable.Indexes[idxName]

				if exists {
					continue
				}

				idxCols := make([]string, 0, len(targetIndex.Columns))
				for _, col := range targetIndex.Columns {
					idxCols = append(idxCols, fmt.Sprintf("%q", col))
				}
				uniqueMod := ""
				if targetIndex.IsUnique {
					uniqueMod = "UNIQUE "
				}

				sqlStr := fmt.Sprintf(
					"CREATE %sINDEX %q ON %q.%q (%s);",
					uniqueMod, idxName, schemaName, tName, strings.Join(idxCols, ", "),
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeCreate,
					ObjectType: ObjectIndex,
					Schema:     schemaName,
					Name:       idxName,
					SQL:        sqlStr,
				})
				liveTable.Indexes[idxName] = &IndexState{
					Name:     targetIndex.Name,
					IsUnique: targetIndex.IsUnique,
					Columns:  targetIndex.Columns,
				}
			}

			// Step 5: Drop live indexes that no longer exist in target
			liveIdxNames := make([]string, 0, len(liveTable.Indexes))
			for k := range liveTable.Indexes {
				liveIdxNames = append(liveIdxNames, k)
			}
			slices.Sort(liveIdxNames)

			for _, liveIdxName := range liveIdxNames {
				_, exists := targetTable.Indexes[liveIdxName]
				if exists {
					continue
				}

				sqlStr := fmt.Sprintf("DROP INDEX %q.%q;", schemaName, liveIdxName)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeDrop,
					ObjectType: ObjectIndex,
					Schema:     schemaName,
					Name:       liveIdxName,
					SQL:        sqlStr,
				})
				delete(liveTable.Indexes, liveIdxName)
			}
		}
	}
	return nil
}

// planForeignKeys evaluates foreign key lifecycle operations.
//  1. Identifies renames using the NamePrevious field, parking them in a
//     temp namespace.
//  2. Completes all parked renames to their final target namespace.
//  3. Drops existing foreign keys that have changed structurally.
//  4. Creates any new target foreign keys that do not exist natively yet.
//  5. Drops any live database foreign keys that no longer exist in the target
//     state.
func (d *Differ) planForeignKeys() error {
	schemaNames := make([]string, 0, len(d.target.Schemas))
	for k := range d.target.Schemas {
		schemaNames = append(schemaNames, k)
	}
	slices.Sort(schemaNames)

	for _, schemaName := range schemaNames {
		targetSchema, ok := d.target.Schemas[schemaName]
		if !ok {
			return fmt.Errorf(
				"schema %q loaded from target.Schemas now not found in target.Schemas",
				schemaName,
			)
		}

		liveSchema, ok := d.scratch.Schemas[schemaName]
		if !ok {
			continue
		}

		targetTableNames := make([]string, 0, len(targetSchema.Tables))
		for k := range targetSchema.Tables {
			targetTableNames = append(targetTableNames, k)
		}
		slices.Sort(targetTableNames)

		for _, tName := range targetTableNames {
			targetTable, ok := targetSchema.Tables[tName]
			if !ok {
				return fmt.Errorf(
					"table %q not found in target schema %q",
					tName, schemaName,
				)
			}

			liveTable, ok := liveSchema.Tables[tName]
			if !ok {
				continue
			}

			targetFKNames := make([]string, 0, len(targetTable.ForeignKeys))
			for k := range targetTable.ForeignKeys {
				targetFKNames = append(targetFKNames, k)
			}
			slices.Sort(targetFKNames)

			// Step 1: Move required renames to temp names to free up namespace
			pendingRenames := make(map[string]pendingRename)
			for _, fkName := range targetFKNames {
				targetFK, ok := targetTable.ForeignKeys[fkName]
				if !ok {
					return fmt.Errorf(
						"foreign key %q not found in target table %q",
						fkName, tName,
					)
				}

				pName := targetFK.NamePrevious
				if pName == "" || pName == fkName {
					continue
				}

				_, ok = liveTable.ForeignKeys[pName]
				if !ok {
					continue
				}

				tmpName := "scheme_tmp_fk_" + pName
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectForeignKey,
					Schema:     schemaName,
					Name:       tmpName,
					SQL: fmt.Sprintf(
						"ALTER TABLE %q.%q RENAME CONSTRAINT %q TO %q;",
						schemaName, tName, pName, tmpName,
					),
				})

				liveTable.ForeignKeys[tmpName] = liveTable.ForeignKeys[pName]
				liveTable.ForeignKeys[tmpName].Name = tmpName
				delete(liveTable.ForeignKeys, pName)

				pendingRenames[fkName] = pendingRename{
					tempName:   tmpName,
					oldName:    pName,
					targetName: fkName,
				}
			}

			// Step 2: Move all pending renames to their final names
			for _, fkName := range targetFKNames {
				info, isRenaming := pendingRenames[fkName]
				if !isRenaming {
					continue
				}

				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeRename,
					ObjectType: ObjectForeignKey,
					Schema:     schemaName,
					Name:       fkName,
					SQL: fmt.Sprintf(
						"ALTER TABLE %q.%q RENAME CONSTRAINT %q TO %q;",
						schemaName, tName, info.tempName, fkName,
					),
				})

				liveTable.ForeignKeys[fkName] = liveTable.ForeignKeys[info.tempName]
				liveTable.ForeignKeys[fkName].Name = fkName
				delete(liveTable.ForeignKeys, info.tempName)
			}

			// Step 3: Drop existing foreign keys that have changed structurally
			for _, fkName := range targetFKNames {
				targetFK, ok := targetTable.ForeignKeys[fkName]
				if !ok {
					return fmt.Errorf(
						"foreign key %q not found in target table %q",
						fkName, tName,
					)
				}

				liveFK, exists := liveTable.ForeignKeys[fkName]
				if !exists {
					continue
				}

				changed := liveFK.OnUpdate != targetFK.OnUpdate ||
					liveFK.OnDelete != targetFK.OnDelete ||
					liveFK.TargetSchema != targetFK.TargetSchema ||
					liveFK.TargetTable != targetFK.TargetTable ||
					len(liveFK.ColsLocal) != len(targetFK.ColsLocal)

				if !changed {
					for i, localCol := range targetFK.ColsLocal {
						if liveFK.ColsLocal[i] != localCol ||
							liveFK.ColsTarget[i] != targetFK.ColsTarget[i] {
							changed = true
							break
						}
					}
				}

				if !changed {
					continue
				}

				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeDrop,
					ObjectType: ObjectForeignKey,
					Schema:     schemaName,
					Name:       fkName,
					SQL: fmt.Sprintf(
						"ALTER TABLE %q.%q DROP CONSTRAINT %q;",
						schemaName, tName, fkName,
					),
				})
				delete(liveTable.ForeignKeys, fkName)
			}

			// Step 4: Handle creation of new foreign keys
			for _, fkName := range targetFKNames {
				targetFK, ok := targetTable.ForeignKeys[fkName]
				if !ok {
					return fmt.Errorf(
						"foreign key %q not found in target table %q",
						fkName, tName,
					)
				}

				_, exists := liveTable.ForeignKeys[fkName]
				if exists {
					continue
				}

				localCols := make([]string, 0, len(targetFK.ColsLocal))
				targetCols := make([]string, 0, len(targetFK.ColsTarget))
				for i, localCol := range targetFK.ColsLocal {
					localCols = append(localCols, fmt.Sprintf("%q", localCol))
					targetCols = append(targetCols, fmt.Sprintf("%q", targetFK.ColsTarget[i]))
				}

				onUpdate := ""
				if targetFK.OnUpdate != "NO ACTION" {
					onUpdate = " ON UPDATE " + targetFK.OnUpdate.String()
				}
				onDelete := ""
				if targetFK.OnDelete != "NO ACTION" {
					onDelete = " ON DELETE " + targetFK.OnDelete.String()
				}

				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q ADD CONSTRAINT %q FOREIGN KEY (%s) "+
						"REFERENCES %q.%q (%s)%s%s;",
					schemaName, tName, fkName, strings.Join(localCols, ", "),
					targetFK.TargetSchema, targetFK.TargetTable,
					strings.Join(targetCols, ", "), onUpdate, onDelete,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeCreate,
					ObjectType: ObjectForeignKey,
					Schema:     schemaName,
					Name:       fkName,
					SQL:        sqlStr,
				})
				liveTable.ForeignKeys[fkName] = &ForeignKeyState{
					Name:         targetFK.Name,
					ColsLocal:    targetFK.ColsLocal,
					ColsTarget:   targetFK.ColsTarget,
					TargetSchema: targetFK.TargetSchema,
					TargetTable:  targetFK.TargetTable,
					OnUpdate:     targetFK.OnUpdate,
					OnDelete:     targetFK.OnDelete,
				}
			}

			// Step 5: Drop live foreign keys that no longer exist in target
			liveFKNames := make([]string, 0, len(liveTable.ForeignKeys))
			for k := range liveTable.ForeignKeys {
				liveFKNames = append(liveFKNames, k)
			}
			slices.Sort(liveFKNames)

			for _, liveFKName := range liveFKNames {
				_, exists := targetTable.ForeignKeys[liveFKName]
				if exists {
					continue
				}

				sqlStr := fmt.Sprintf(
					"ALTER TABLE %q.%q DROP CONSTRAINT %q;",
					schemaName, tName, liveFKName,
				)
				d.Actions = append(d.Actions, MigrationAction{
					Type:       ActionTypeDrop,
					ObjectType: ObjectForeignKey,
					Schema:     schemaName,
					Name:       liveFKName,
					SQL:        sqlStr,
				})
				delete(liveTable.ForeignKeys, liveFKName)
			}
		}
	}

	return nil
}
