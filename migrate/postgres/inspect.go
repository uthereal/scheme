package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

// LiveState represents an aggregated dump of the current structural
// layout of a PostgreSQL database.

type LiveComposite struct {
	Name   string
	Fields map[string]*LiveCompositeField
}

type LiveCompositeField struct {
	Name     string
	DataType string
	Position int
}

type LiveDomain struct {
	Name     string
	BaseType string
}

type LivePrimaryKey struct {
	Name    string
	Columns []string
}

type LiveEnum struct {
	Name   string
	Values []string
}

type LiveIndex struct {
	Name     string
	Columns  []string
	IsUnique bool
}

type LiveForeignKey struct {
	Name         string
	TargetTable  string
	TargetSchema string
	LocalCols    []string
	TargetCols   []string
}

type LiveState struct {
	Schemas map[string]*LiveSchema
}

type LiveSchema struct {
	Name       string
	Tables     map[string]*LiveTable
	Enums      map[string]*LiveEnum
	Composites map[string]*LiveComposite
	Domains    map[string]*LiveDomain
}

type LiveTable struct {
	Name        string
	Columns     map[string]*LiveColumn
	PrimaryKey  *LivePrimaryKey
	Indexes     map[string]*LiveIndex
	ForeignKeys map[string]*LiveForeignKey
}

type LiveColumn struct {
	Name          string
	DataType      string
	IsNullable    bool
	ColumnDefault *string
}

// Inspect queries the provided database connection's information_schema
// to construct an accurate, localized map of all current schemas, tables,
// and columns.
func Inspect(ctx context.Context, db *sql.DB) (*LiveState, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}

	state := &LiveState{
		Schemas: make(map[string]*LiveSchema),
	}

	schemaQuery := `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
	`
	sRows, err := db.QueryContext(ctx, schemaQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query schemas -> %w", err)
	}
	defer func(sRows *sql.Rows) {
		_ = sRows.Close()
	}(sRows)

	for sRows.Next() {
		var name string
		err = sRows.Scan(&name)
		if err != nil {
			return nil, fmt.Errorf("failed to scan schema -> %w", err)
		}
		state.Schemas[name] = &LiveSchema{
			Name:       name,
			Tables:     make(map[string]*LiveTable),
			Enums:      make(map[string]*LiveEnum),
			Composites: make(map[string]*LiveComposite),
			Domains:    make(map[string]*LiveDomain),
		}
	}
	err = sRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating schemas -> %w", err)
	}

	tableQuery := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
	`
	tRows, err := db.QueryContext(ctx, tableQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables -> %w", err)
	}
	defer func(tRows *sql.Rows) {
		_ = tRows.Close()
	}(tRows)

	for tRows.Next() {
		var schema, name string
		err = tRows.Scan(&schema, &name)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table -> %w", err)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			continue
		}

		s.Tables[name] = &LiveTable{
			Name:        name,
			Columns:     make(map[string]*LiveColumn),
			Indexes:     make(map[string]*LiveIndex),
			ForeignKeys: make(map[string]*LiveForeignKey),
		}
	}
	err = tRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating tables -> %w", err)
	}

	colQuery := `
		SELECT table_schema, table_name, column_name,
		data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
	`
	cRows, err := db.QueryContext(ctx, colQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns -> %w", err)
	}
	defer func(cRows *sql.Rows) {
		_ = cRows.Close()
	}(cRows)

	for cRows.Next() {
		var schema, table, colName, dataType, isNullable string
		var colDefault *string

		err = cRows.Scan(
			&schema, &table, &colName, &dataType, &isNullable, &colDefault,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column -> %w", err)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			continue
		}
		t, ok := s.Tables[table]
		if !ok {
			continue
		}

		t.Columns[colName] = &LiveColumn{
			Name:          colName,
			DataType:      dataType,
			IsNullable:    isNullable == "YES",
			ColumnDefault: colDefault,
		}
	}
	err = cRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating columns -> %w", err)
	}

	enumQuery := `
		SELECT n.nspname, t.typname, e.enumlabel
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, t.typname, e.enumsortorder
	`
	eRows, err := db.QueryContext(ctx, enumQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query enums -> %w", err)
	}
	defer func(eRows *sql.Rows) {
		_ = eRows.Close()
	}(eRows)

	for eRows.Next() {
		var schema, name, label string
		err = eRows.Scan(&schema, &name, &label)
		if err != nil {
			return nil, fmt.Errorf("failed to scan enum -> %w", err)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			continue
		}

		en, ok := s.Enums[name]
		if !ok {
			en = &LiveEnum{Name: name, Values: []string{}}
			s.Enums[name] = en
		}
		en.Values = append(en.Values, label)
	}
	err = eRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating enums -> %w", err)
	}

	compQuery := `
		SELECT n.nspname, t.typname, a.attname,
		       pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnum
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_attribute a ON a.attrelid = t.typrelid
		JOIN pg_class c ON c.oid = t.typrelid
		WHERE t.typtype = 'c' AND c.relkind = 'c'
		AND a.attnum > 0 AND NOT a.attisdropped
		AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, t.typname, a.attnum
	`
	compRows, err := db.QueryContext(ctx, compQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query composites -> %w", err)
	}
	defer func(compRows *sql.Rows) {
		_ = compRows.Close()
	}(compRows)

	for compRows.Next() {
		var schema, comp, field, fType string
		var pos int
		err = compRows.Scan(&schema, &comp, &field, &fType, &pos)
		if err != nil {
			return nil, fmt.Errorf("failed to scan composite -> %w", err)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			continue
		}

		c, ok := s.Composites[comp]
		if !ok {
			c = &LiveComposite{Name: comp, Fields: make(map[string]*LiveCompositeField)}
			s.Composites[comp] = c
		}
		c.Fields[field] = &LiveCompositeField{
			Name:     field,
			DataType: fType,
			Position: pos,
		}
	}
	err = compRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating composites -> %w", err)
	}

	domQuery := `
		SELECT n.nspname, t.typname,
		pg_catalog.format_type(t.typbasetype, t.typtypmod)
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typtype = 'd'
		AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
	`
	domRows, err := db.QueryContext(ctx, domQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query domains -> %w", err)
	}
	defer func(domRows *sql.Rows) {
		_ = domRows.Close()
	}(domRows)

	for domRows.Next() {
		var schema, name, baseType string
		err = domRows.Scan(&schema, &name, &baseType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan domain -> %w", err)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			continue
		}
		s.Domains[name] = &LiveDomain{Name: name, BaseType: baseType}
	}
	err = domRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating domains -> %w", err)
	}

	pkQuery := `
		SELECT tc.table_schema, tc.table_name, tc.constraint_name, kc.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kc
		ON tc.constraint_name = kc.constraint_name
		AND tc.table_schema = kc.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY tc.table_schema, tc.table_name, kc.ordinal_position
	`
	pkRows, err := db.QueryContext(ctx, pkQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary keys -> %w", err)
	}
	defer func(pkRows *sql.Rows) {
		_ = pkRows.Close()
	}(pkRows)

	for pkRows.Next() {
		var schema, table, constraint, colName string
		err = pkRows.Scan(&schema, &table, &constraint, &colName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pk -> %w", err)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			continue
		}
		t, ok := s.Tables[table]
		if !ok {
			continue
		}

		if t.PrimaryKey == nil {
			t.PrimaryKey = &LivePrimaryKey{Name: constraint, Columns: []string{}}
		}
		t.PrimaryKey.Columns = append(t.PrimaryKey.Columns, colName)
	}
	err = pkRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating pks -> %w", err)
	}

	idxQuery := `
		SELECT n.nspname, c.relname, i.relname, ix.indisunique, a.attname
		FROM pg_index ix
		JOIN pg_class c ON c.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		AND a.attnum = ANY(ix.indkey)
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		AND ix.indisprimary = false
		ORDER BY n.nspname, c.relname, i.relname, a.attnum
	`
	iRows, err := db.QueryContext(ctx, idxQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes -> %w", err)
	}
	defer func(iRows *sql.Rows) {
		_ = iRows.Close()
	}(iRows)

	for iRows.Next() {
		var schema, table, idxName, colName string
		var isUnique bool
		err = iRows.Scan(&schema, &table, &idxName, &isUnique, &colName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan index -> %w", err)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			continue
		}
		t, ok := s.Tables[table]
		if !ok {
			continue
		}

		ix, ok := t.Indexes[idxName]
		if !ok {
			ix = &LiveIndex{
				Name:     idxName,
				IsUnique: isUnique,
				Columns:  []string{},
			}
			t.Indexes[idxName] = ix
		}
		ix.Columns = append(ix.Columns, colName)
	}
	err = iRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating indexes -> %w", err)
	}

	return state, nil
}
