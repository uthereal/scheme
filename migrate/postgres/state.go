package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/uthereal/scheme/genproto/core/shared"
	"github.com/uthereal/scheme/genproto/postgres"
	"github.com/uthereal/scheme/migrate"
	"golang.org/x/sync/errgroup"
)

// CompositeState represents a composite type definition.
type CompositeState struct {
	Name         string
	NamePrevious string
	Fields       map[string]*CompositeFieldState
}

// CompositeFieldState represents a single field within a composite type.
type CompositeFieldState struct {
	Name     string
	DataType migrate.DatabaseDataType
	Position int
}

// DomainState represents a domain type definition.
type DomainState struct {
	Name         string
	NamePrevious string
	DataType     migrate.DatabaseDataType
}

// PrimaryKeyState represents a primary key constraint.
type PrimaryKeyState struct {
	Name    string
	Columns []string
}

// EnumState represents an enumerated type definition.
type EnumState struct {
	Name         string
	NamePrevious string
	Values       []string
}

// IndexState represents a database index definition.
type IndexState struct {
	Name         string
	NamePrevious string
	Columns      []string
	IsUnique     bool
}

// ForeignKeyState represents a foreign key constraint.
type ForeignKeyState struct {
	Name         string
	NamePrevious string
	ColsLocal    []string
	ColsTarget   []string
	TargetSchema string
	TargetTable  string
	OnUpdate     migrate.DatabaseForeignKeyAction
	OnDelete     migrate.DatabaseForeignKeyAction
}

// DatabaseState represents the complete structural state of a database.
type DatabaseState struct {
	Schemas map[string]*SchemaState
}

// SchemaState represents a collection of database objects within a namespace.
type SchemaState struct {
	Name         string
	NamePrevious string
	Tables       map[string]*TableState
	Enums        map[string]*EnumState
	Composites   map[string]*CompositeState
	Domains      map[string]*DomainState
}

// TableState represents a relational database table.
type TableState struct {
	Name         string
	NamePrevious string
	Columns      map[string]*ColumnState
	PrimaryKey   *PrimaryKeyState
	Indexes      map[string]*IndexState
	ForeignKeys  map[string]*ForeignKeyState
}

// ColumnState represents a single column within a table.
type ColumnState struct {
	Name            string
	NamePrevious    string
	DataType        migrate.DatabaseDataType
	IsNullable      bool
	IsAutoIncrement bool
	ColumnDefault   *string
}

const (
	querySchemas = `
		SELECT schema_name AS schema
		FROM information_schema.schemata
		WHERE schema_name != ALL($1)
	`
	queryTables = `
		SELECT table_schema AS schema, table_name AS "table"
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		AND table_schema != ALL($1)
	`
	queryColumns = `
		SELECT
			n.nspname AS schema,
			c.relname AS "table",
			a.attname AS "column",
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
			NOT a.attnotnull AS nullable,
			pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS "default",
			a.attidentity AS identity
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE c.relkind = 'r'
		AND a.attnum > 0
		AND NOT a.attisdropped
		AND n.nspname != ALL($1)
	`
	queryEnums = `
		SELECT n.nspname AS schema, t.typname AS enum, e.enumlabel AS label
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname != ALL($1)
		ORDER BY n.nspname, t.typname, e.enumsortorder
	`
	queryComposites = `
		SELECT
			n.nspname AS schema, t.typname AS composite, a.attname AS field,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
			a.attnum AS position
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_attribute a ON a.attrelid = t.typrelid
		JOIN pg_class c ON c.oid = t.typrelid
		WHERE t.typtype = 'c' AND c.relkind = 'c'
		AND a.attnum > 0 AND NOT a.attisdropped
		AND n.nspname != ALL($1)
	`
	queryDomains = `
		SELECT
			n.nspname AS schema, t.typname AS domain,
			pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base_type
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typtype = 'd'
		AND n.nspname != ALL($1)
	`
	queryPrimaryKeys = `
		SELECT
			tc.table_schema AS schema, tc.table_name AS "table",
			tc.constraint_name AS constraint, kc.column_name AS "column"
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kc
		ON tc.constraint_name = kc.constraint_name
		AND tc.table_schema = kc.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		AND tc.table_schema != ALL($1)
		ORDER BY tc.table_schema, tc.table_name, kc.ordinal_position
	`
	queryIndexes = `
		SELECT
			n.nspname AS schema, c.relname AS "table", i.relname AS index,
			ix.indisunique AS is_unique, a.attname AS "column"
		FROM pg_index ix
		JOIN pg_class c ON c.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
		JOIN pg_attribute a
		ON a.attrelid = c.oid AND a.attnum = k.attnum
		WHERE n.nspname != ALL($1)
		AND ix.indisprimary = false
		ORDER BY n.nspname, c.relname, i.relname, k.ord
	`
	queryForeignKeys = `
		SELECT
			tc.table_schema AS schema,
			tc.table_name AS "table",
			tc.constraint_name AS constraint,
			kcu.column_name AS local_column,
			rc.unique_constraint_schema AS target_schema,
			ccu.table_name AS target_table,
			ccu.column_name AS target_column,
			rc.update_rule,
			rc.delete_rule
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.referential_constraints AS rc
			ON rc.constraint_name = tc.constraint_name
			AND rc.constraint_schema = tc.table_schema
		JOIN information_schema.key_column_usage AS kcu
			ON kcu.constraint_name = tc.constraint_name
			AND kcu.table_schema = tc.table_schema
		JOIN information_schema.key_column_usage AS ccu
			ON ccu.constraint_name = rc.unique_constraint_name
			AND ccu.table_schema = rc.unique_constraint_schema
			AND ccu.ordinal_position = kcu.position_in_unique_constraint
		WHERE tc.constraint_type = 'FOREIGN KEY'
		AND tc.table_schema != ALL($1)
		ORDER BY
			tc.table_schema, tc.table_name,
			tc.constraint_name, kcu.ordinal_position;
	`
)

// internalSchemas are Postgres system schemas that are to be ignored during
// inspection.
var internalSchemas = []string{
	"pg_catalog",
	"information_schema",
	"pg_toast",
}

var fkActionMap = map[shared.ForeignKeyAction]migrate.DatabaseForeignKeyAction{
	shared.ForeignKeyAction_FOREIGN_KEY_ACTION_UNSPECIFIED: "NO ACTION",
	shared.ForeignKeyAction_FOREIGN_KEY_ACTION_NO_ACTION:   "NO ACTION",
	shared.ForeignKeyAction_FOREIGN_KEY_ACTION_RESTRICT:    "RESTRICT",
	shared.ForeignKeyAction_FOREIGN_KEY_ACTION_CASCADE:     "CASCADE",
	shared.ForeignKeyAction_FOREIGN_KEY_ACTION_SET_NULL:    "SET NULL",
	shared.ForeignKeyAction_FOREIGN_KEY_ACTION_SET_DEFAULT: "SET DEFAULT",
}

// NewDatabaseStateFromProto converts a PostgresDatabase AST into a pure
// DatabaseState model.
func NewDatabaseStateFromProto(
	proto *postgres.PostgresDatabase,
) (*DatabaseState, error) {
	if proto == nil {
		return nil, fmt.Errorf("postgres database cannot be nil")
	}

	schemas := proto.GetSchemas()
	state := &DatabaseState{
		Schemas: make(map[string]*SchemaState, len(schemas)),
	}

	var mu sync.Mutex
	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(8)

	for _, s := range schemas {
		s := s // Capture loop variable for goroutine
		g.Go(func() error {
			schemaState := &SchemaState{
				Name:         s.GetName(),
				NamePrevious: s.GetNamePrevious(),
				Tables:       make(map[string]*TableState, len(s.GetTables())),
				Enums:        make(map[string]*EnumState, len(s.GetEnums())),
				Composites:   make(map[string]*CompositeState, len(s.GetComposites())),
				Domains:      make(map[string]*DomainState, len(s.GetDomains())),
			}

			for _, e := range s.GetEnums() {
				schemaState.Enums[e.GetName()] = &EnumState{
					Name:         e.GetName(),
					NamePrevious: e.GetNamePrevious(),
					Values:       e.GetValues(),
				}
			}

			for _, d := range s.GetDomains() {
				dbType, err := ToDatabaseDataType(d.GetBaseType())
				if err != nil {
					return err
				}
				schemaState.Domains[d.GetName()] = &DomainState{
					Name:         d.GetName(),
					NamePrevious: d.GetNamePrevious(),
					DataType:     dbType,
				}
			}

			for _, c := range s.GetComposites() {
				compState := &CompositeState{
					Name:         c.GetName(),
					NamePrevious: c.GetNamePrevious(),
					Fields:       make(map[string]*CompositeFieldState, len(c.GetFields())),
				}
				for i, f := range c.GetFields() {
					dbType, err := ToDatabaseDataType(f.GetType())
					if err != nil {
						return err
					}
					compState.Fields[f.GetName()] = &CompositeFieldState{
						Name:     f.GetName(),
						DataType: dbType,
						Position: i + 1,
					}
				}
				schemaState.Composites[c.GetName()] = compState
			}

			for _, t := range s.GetTables() {
				tableState := &TableState{
					Name:         t.GetName(),
					NamePrevious: t.GetNamePrevious(),
					Columns:      make(map[string]*ColumnState, len(t.GetColumns())),
					Indexes:      make(map[string]*IndexState, len(t.GetIndexes())),
					ForeignKeys:  make(map[string]*ForeignKeyState, len(t.GetForeignKeys())),
				}

				targetNullableMap := make(map[string]bool, len(t.GetPrimaryKeys()))
				for _, pk := range t.GetPrimaryKeys() {
					targetNullableMap[pk] = false
				}

				for _, c := range t.GetColumns() {
					dbType, err := ToDatabaseDataType(c.GetType())
					if err != nil {
						return err
					}

					isNullable := c.GetIsNullable()
					_, isPK := targetNullableMap[c.GetName()]
					if isPK && isNullable {
						return fmt.Errorf(
							"primary key column %q in table %q cannot be nullable",
							c.GetName(), t.GetName(),
						)
					}

					var defVal *string
					v := c.GetDefaultValue()
					if v != "" {
						idx := strings.LastIndex(v, "::")
						if idx != -1 {
							v = v[:idx]
						}
						defVal = new(v)
					}

					isAutoInc := false
					switch c.GetType().GetType().(type) {
					case *postgres.DataType_SmallserialType,
						*postgres.DataType_SerialType,
						*postgres.DataType_BigserialType:
						isAutoInc = true
					}

					tableState.Columns[c.GetName()] = &ColumnState{
						Name:            c.GetName(),
						NamePrevious:    c.GetNamePrevious(),
						DataType:        dbType,
						IsNullable:      isNullable,
						IsAutoIncrement: isAutoInc,
						ColumnDefault:   defVal,
					}
				}

				if len(t.GetPrimaryKeys()) > 0 {
					tableState.PrimaryKey = &PrimaryKeyState{
						Name:    t.GetName() + "_pkey",
						Columns: t.GetPrimaryKeys(),
					}
				}

				for _, idx := range t.GetIndexes() {
					cols := make([]string, 0, len(idx.GetColumns()))
					for _, c := range idx.GetColumns() {
						cols = append(cols, c.GetName())
					}
					tableState.Indexes[idx.GetName()] = &IndexState{
						Name:         idx.GetName(),
						NamePrevious: idx.GetNamePrevious(),
						Columns:      cols,
						IsUnique:     idx.GetIsUnique(),
					}
				}

				for _, fk := range t.GetForeignKeys() {
					localCols := make([]string, 0, len(fk.GetColumns()))
					targetCols := make([]string, 0, len(fk.GetColumns()))
					for _, mapping := range fk.GetColumns() {
						localCols = append(localCols, mapping.GetSourceColumn())
						targetCols = append(targetCols, mapping.GetTargetColumn())
					}

					onUpdate, ok := fkActionMap[fk.GetOnUpdate()]
					if !ok {
						onUpdate = fkActionMap[shared.ForeignKeyAction_FOREIGN_KEY_ACTION_UNSPECIFIED]
					}

					onDelete, ok := fkActionMap[fk.GetOnDelete()]
					if !ok {
						onDelete = fkActionMap[shared.ForeignKeyAction_FOREIGN_KEY_ACTION_UNSPECIFIED]
					}

					targetRef := fk.GetTargetTable()
					targetSchema := s.GetName()
					targetTable := targetRef
					if strings.Contains(targetRef, ".") {
						parts := strings.SplitN(targetRef, ".", 2)
						targetSchema = parts[0]
						targetTable = parts[1]
					}

					tableState.ForeignKeys[fk.GetName()] = &ForeignKeyState{
						Name:         fk.GetName(),
						NamePrevious: fk.GetNamePrevious(),
						ColsLocal:    localCols,
						ColsTarget:   targetCols,
						TargetSchema: targetSchema,
						TargetTable:  targetTable,
						OnUpdate:     onUpdate,
						OnDelete:     onDelete,
					}
				}

				schemaState.Tables[t.GetName()] = tableState
			}

			mu.Lock()
			state.Schemas[s.GetName()] = schemaState
			mu.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	return state, nil
}

// NewDatabaseStateFromDb performs a comprehensive structural analysis of a live
// PostgreSQL database
// database by querying the information_schema and pg_catalog views.
// It extracts schema-level definitions (enums, domains, composites)
// and table-level structures (columns, primary keys, indexes, foreign keys).
// This live state is required for calculating structural diffs between the
// current database and the target textproto schema.
func NewDatabaseStateFromDb(
	ctx context.Context, db *sql.DB,
) (*DatabaseState, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}

	ctxTransaction, cancelCtxTransaction := context.WithTimeout(
		ctx, 5*time.Minute,
	)
	defer cancelCtxTransaction()

	tx, err := db.BeginTx(ctxTransaction, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction -> %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	_, err = tx.ExecContext(ctxTransaction, "SET LOCAL search_path = '';")
	if err != nil {
		return nil, fmt.Errorf("failed to set empty search_path -> %w", err)
	}

	state := &DatabaseState{
		Schemas: make(map[string]*SchemaState),
	}

	ctxSchemasQuery, cancelCtxSchemasQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxSchemasQuery()
	sRows, err := tx.QueryContext(ctxSchemasQuery, querySchemas, internalSchemas)
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
			return nil, fmt.Errorf(
				"failed to scan schema -> %w", err,
			)
		}
		state.Schemas[name] = &SchemaState{
			Name:       name,
			Tables:     make(map[string]*TableState),
			Enums:      make(map[string]*EnumState),
			Composites: make(map[string]*CompositeState),
			Domains:    make(map[string]*DomainState),
		}
	}
	err = sRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating schemas -> %w", err)
	}

	ctxTablesQuery, cancelCtxTablesQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxTablesQuery()
	tRows, err := tx.QueryContext(ctxTablesQuery, queryTables, internalSchemas)
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
			return nil, fmt.Errorf(
				"failed to scan table -> %w", err,
			)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			return nil, fmt.Errorf("schema %q not found", schema)
		}

		s.Tables[name] = &TableState{
			Name:        name,
			Columns:     make(map[string]*ColumnState),
			Indexes:     make(map[string]*IndexState),
			ForeignKeys: make(map[string]*ForeignKeyState),
		}
	}
	err = tRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating tables -> %w", err)
	}

	ctxColumnsQuery, cancelCtxColumnsQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxColumnsQuery()
	cRows, err := tx.QueryContext(ctxColumnsQuery, queryColumns, internalSchemas)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns -> %w", err)
	}
	defer func(cRows *sql.Rows) {
		_ = cRows.Close()
	}(cRows)

	for cRows.Next() {
		var schema, table, colName, dataType string
		var isNullable bool
		var colDefault *string
		var attIdentity string

		err = cRows.Scan(
			&schema, &table, &colName, &dataType, &isNullable,
			&colDefault, &attIdentity,
		)

		isAutoInc := false
		if attIdentity == "a" || attIdentity == "d" {
			isAutoInc = true
		}
		if colDefault != nil {
			v := *colDefault
			idx := strings.LastIndex(v, "::")
			if idx != -1 {
				v = v[:idx]
			}
			colDefault = &v
			if strings.HasPrefix(*colDefault, "nextval(") {
				isAutoInc = true
			}
		}
		if err != nil {
			return nil, fmt.Errorf(
				"failed to scan column -> %w", err,
			)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			return nil, fmt.Errorf("schema %q not found", schema)
		}
		t, ok := s.Tables[table]
		if !ok {
			return nil, fmt.Errorf(
				"table %q not found in schema %q", table, schema,
			)
		}

		t.Columns[colName] = &ColumnState{
			Name:            colName,
			DataType:        migrate.DatabaseDataType(dataType),
			IsNullable:      isNullable,
			IsAutoIncrement: isAutoInc,
			ColumnDefault:   colDefault,
		}
	}
	err = cRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating columns -> %w", err)
	}

	ctxEnumsQuery, cancelCtxEnumsQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxEnumsQuery()
	eRows, err := tx.QueryContext(ctxEnumsQuery, queryEnums, internalSchemas)
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
			return nil, fmt.Errorf("schema %q not found", schema)
		}

		en, ok := s.Enums[name]
		if !ok {
			en = &EnumState{Name: name, Values: []string{}}
			s.Enums[name] = en
		}
		en.Values = append(en.Values, label)
	}
	err = eRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating enums -> %w", err)
	}

	ctxCompositesQuery, cancelCtxCompositesQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxCompositesQuery()
	compRows, err := tx.QueryContext(
		ctxCompositesQuery, queryComposites, internalSchemas,
	)
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
			return nil, fmt.Errorf(
				"failed to scan composite -> %w", err,
			)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			return nil, fmt.Errorf("schema %q not found", schema)
		}

		c, ok := s.Composites[comp]
		if !ok {
			c = &CompositeState{
				Name:   comp,
				Fields: make(map[string]*CompositeFieldState),
			}
			s.Composites[comp] = c
		}
		c.Fields[field] = &CompositeFieldState{
			Name:     field,
			DataType: migrate.DatabaseDataType(fType),
			Position: pos,
		}
	}
	err = compRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating composites -> %w", err)
	}

	ctxDomainsQuery, cancelCtxDomainsQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxDomainsQuery()
	domRows, err := tx.QueryContext(
		ctxDomainsQuery, queryDomains, internalSchemas,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query domains -> %w", err)
	}
	defer func(domRows *sql.Rows) {
		_ = domRows.Close()
	}(domRows)

	for domRows.Next() {
		var schema, name, dataType string
		err = domRows.Scan(&schema, &name, &dataType)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to scan domain -> %w", err,
			)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			return nil, fmt.Errorf("schema %q not found", schema)
		}
		s.Domains[name] = &DomainState{
			Name:     name,
			DataType: migrate.DatabaseDataType(dataType),
		}
	}
	err = domRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating domains -> %w", err)
	}

	ctxPrimaryKeysQuery, cancelCtxPrimaryKeysQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxPrimaryKeysQuery()
	pkRows, err := tx.QueryContext(
		ctxPrimaryKeysQuery, queryPrimaryKeys, internalSchemas,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to query primary keys -> %w", err,
		)
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
			return nil, fmt.Errorf("schema %q not found", schema)
		}
		t, ok := s.Tables[table]
		if !ok {
			return nil, fmt.Errorf(
				"table %q not found in schema %q", table, schema,
			)
		}

		if t.PrimaryKey == nil {
			t.PrimaryKey = &PrimaryKeyState{
				Name:    constraint,
				Columns: []string{},
			}
		}
		t.PrimaryKey.Columns = append(t.PrimaryKey.Columns, colName)
	}
	err = pkRows.Err()
	if err != nil {
		return nil, fmt.Errorf("failed iterating pks -> %w", err)
	}

	ctxIndexesQuery, cancelCtxIndexesQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxIndexesQuery()
	iRows, err := tx.QueryContext(ctxIndexesQuery, queryIndexes, internalSchemas)
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
			return nil, fmt.Errorf(
				"failed to scan index -> %w", err,
			)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			return nil, fmt.Errorf("schema %q not found", schema)
		}
		t, ok := s.Tables[table]
		if !ok {
			return nil, fmt.Errorf(
				"table %q not found in schema %q", table, schema,
			)
		}

		ix, ok := t.Indexes[idxName]
		if !ok {
			ix = &IndexState{
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
	ctxForeignKeysQuery, cancelCtxForeignKeysQuery := context.WithTimeout(
		ctxTransaction, 30*time.Second,
	)
	defer cancelCtxForeignKeysQuery()
	fkRows, err := tx.QueryContext(
		ctxForeignKeysQuery, queryForeignKeys, internalSchemas,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to query foreign keys -> %w", err,
		)
	}
	defer func(fkRows *sql.Rows) {
		_ = fkRows.Close()
	}(fkRows)

	for fkRows.Next() {
		var (
			schema, table, constraint, localCol  string
			targetSchema, targetTable, targetCol string
			upRule, delRule                      string
		)
		err = fkRows.Scan(
			&schema, &table, &constraint, &localCol,
			&targetSchema, &targetTable, &targetCol,
			&upRule, &delRule,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to scan foreign key -> %w", err,
			)
		}

		s, ok := state.Schemas[schema]
		if !ok {
			return nil, fmt.Errorf("schema %q not found", schema)
		}
		t, ok := s.Tables[table]
		if !ok {
			return nil, fmt.Errorf(
				"table %q not found in schema %q", table, schema,
			)
		}

		fk, ok := t.ForeignKeys[constraint]
		if !ok {
			fk = &ForeignKeyState{
				Name:         constraint,
				ColsLocal:    []string{},
				ColsTarget:   []string{},
				TargetSchema: targetSchema,
				TargetTable:  targetTable,
				OnUpdate:     migrate.DatabaseForeignKeyAction(upRule),
				OnDelete:     migrate.DatabaseForeignKeyAction(delRule),
			}

			t.ForeignKeys[constraint] = fk
		}
		fk.ColsLocal = append(fk.ColsLocal, localCol)
		fk.ColsTarget = append(fk.ColsTarget, targetCol)
	}
	err = fkRows.Err()
	if err != nil {
		return nil, fmt.Errorf(
			"failed iterating foreign keys -> %w", err,
		)
	}

	return state, nil
}

// Clone performs a deep copy of the DatabaseState, ensuring all nested maps
// and slices are independently allocated. This is crucial for state
// simulation engines that mutate the state iteratively.
func (l *DatabaseState) Clone() *DatabaseState {
	if l == nil {
		return nil
	}

	liveClone := &DatabaseState{
		Schemas: make(map[string]*SchemaState, len(l.Schemas)),
	}

	for sName, s := range l.Schemas {
		sClone := &SchemaState{
			Name:         s.Name,
			NamePrevious: s.NamePrevious,
			Tables:       make(map[string]*TableState, len(s.Tables)),
			Enums:        make(map[string]*EnumState, len(s.Enums)),
			Composites:   make(map[string]*CompositeState, len(s.Composites)),
			Domains:      make(map[string]*DomainState, len(s.Domains)),
		}

		for tName, t := range s.Tables {
			tClone := &TableState{
				Name:         t.Name,
				NamePrevious: t.NamePrevious,
				Columns:      make(map[string]*ColumnState, len(t.Columns)),
				Indexes:      make(map[string]*IndexState, len(t.Indexes)),
				ForeignKeys:  make(map[string]*ForeignKeyState, len(t.ForeignKeys)),
			}

			if t.PrimaryKey != nil {
				pkCols := make([]string, len(t.PrimaryKey.Columns))
				copy(pkCols, t.PrimaryKey.Columns)
				tClone.PrimaryKey = &PrimaryKeyState{
					Name:    t.PrimaryKey.Name,
					Columns: pkCols,
				}
			}

			for cName, c := range t.Columns {
				cClone := &ColumnState{
					Name:            c.Name,
					NamePrevious:    c.NamePrevious,
					DataType:        c.DataType,
					IsNullable:      c.IsNullable,
					IsAutoIncrement: c.IsAutoIncrement,
				}
				if c.ColumnDefault != nil {
					cClone.ColumnDefault = new(*c.ColumnDefault)
				}
				tClone.Columns[cName] = cClone
			}

			for idxName, idx := range t.Indexes {
				idxCols := make([]string, len(idx.Columns))
				copy(idxCols, idx.Columns)
				tClone.Indexes[idxName] = &IndexState{
					Name:         idx.Name,
					NamePrevious: idx.NamePrevious,
					Columns:      idxCols,
					IsUnique:     idx.IsUnique,
				}
			}

			for fkName, fk := range t.ForeignKeys {
				lCols := make([]string, len(fk.ColsLocal))
				copy(lCols, fk.ColsLocal)
				tCols := make([]string, len(fk.ColsTarget))
				copy(tCols, fk.ColsTarget)

				tClone.ForeignKeys[fkName] = &ForeignKeyState{
					Name:         fk.Name,
					NamePrevious: fk.NamePrevious,
					ColsLocal:    lCols,
					ColsTarget:   tCols,
					TargetSchema: fk.TargetSchema,
					TargetTable:  fk.TargetTable,
					OnUpdate:     fk.OnUpdate,
					OnDelete:     fk.OnDelete,
				}
			}
			sClone.Tables[tName] = tClone
		}

		for eName, e := range s.Enums {
			vals := make([]string, len(e.Values))
			copy(vals, e.Values)
			sClone.Enums[eName] = &EnumState{
				Name:         e.Name,
				NamePrevious: e.NamePrevious,
				Values:       vals,
			}
		}

		for cName, c := range s.Composites {
			cClone := &CompositeState{
				Name: c.Name,

				NamePrevious: c.NamePrevious,
				Fields:       make(map[string]*CompositeFieldState, len(c.Fields)),
			}
			for fName, f := range c.Fields {
				cClone.Fields[fName] = &CompositeFieldState{
					Name:     f.Name,
					DataType: f.DataType,
					Position: f.Position,
				}
			}
			sClone.Composites[cName] = cClone
		}

		for dName, d := range s.Domains {
			sClone.Domains[dName] = &DomainState{
				Name:         d.Name,
				NamePrevious: d.NamePrevious,
				DataType:     d.DataType,
			}
		}

		liveClone.Schemas[sName] = sClone
	}

	return liveClone
}
