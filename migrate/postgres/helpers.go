package postgres

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/uthereal/scheme/genproto/postgres"
	"github.com/uthereal/scheme/migrate"
)

// formatQualifiedName safely schema-qualifies identifiers.
func formatQualifiedName(schema string, name string) migrate.DatabaseDataType {
	if schema == "" {
		return migrate.DatabaseDataType(name)
	}

	return migrate.DatabaseDataType(schema + "." + name)
}

// formatNumeric formats a numeric data type with optional precision and scale.
func formatNumeric(precision *int32, scale *int32) migrate.DatabaseDataType {
	if precision == nil {
		return "numeric"
	}

	if scale == nil {
		return migrate.DatabaseDataType(
			"numeric(" + strconv.Itoa(int(*precision)) + ")",
		)
	}

	return migrate.DatabaseDataType(
		"numeric(" + strconv.Itoa(int(*precision)) + "," +
			strconv.Itoa(int(*scale)) + ")",
	)
}

// formatLength formats a data type with an optional length or precision.
func formatLength(baseType string, length *int32) migrate.DatabaseDataType {
	if length == nil {
		return migrate.DatabaseDataType(baseType)
	}

	return migrate.DatabaseDataType(
		baseType + "(" + strconv.Itoa(int(*length)) + ")",
	)
}

// formatTimePrecision formats a time-related data type with optional precision.
func formatTimePrecision(
	baseType string, precision *int32,
) migrate.DatabaseDataType {
	if precision == nil {
		return migrate.DatabaseDataType(baseType)
	}

	parts := strings.SplitN(baseType, " ", 2)
	precisionStr := "(" + strconv.Itoa(int(*precision)) + ")"

	if len(parts) == 2 {
		return migrate.DatabaseDataType(
			parts[0] + precisionStr + " " + parts[1],
		)
	}

	return migrate.DatabaseDataType(baseType + precisionStr)
}

// formatInterval formats an interval data type with optional fields.
func formatInterval(
	fields string, precision *int32,
) migrate.DatabaseDataType {
	base := "interval"
	if fields != "" {
		base += " " + fields
	}

	if precision == nil {
		return migrate.DatabaseDataType(base)
	}

	return migrate.DatabaseDataType(
		base + "(" + strconv.Itoa(int(*precision)) + ")",
	)
}

// ToDatabaseDataType maps a Protobuf DataType definition into its equivalent
// PostgreSQL data type string representation.
func ToDatabaseDataType(
	dt *postgres.DataType,
) (migrate.DatabaseDataType, error) {
	if dt == nil {
		return "", errors.New("data type cannot be nil")
	}

	switch t := dt.GetType().(type) {
	// 1. Fixed-name types
	case *postgres.DataType_SmallintType:
		return "smallint", nil
	case *postgres.DataType_IntegerType:
		return "integer", nil
	case *postgres.DataType_BigintType:
		return "bigint", nil
	case *postgres.DataType_RealType:
		return "real", nil
	case *postgres.DataType_DoublePrecisionType:
		return "double precision", nil
	case *postgres.DataType_SmallserialType:
		return "smallint", nil
	case *postgres.DataType_SerialType:
		return "integer", nil
	case *postgres.DataType_BigserialType:
		return "bigint", nil
	case *postgres.DataType_MoneyType:
		return "money", nil
	case *postgres.DataType_TextType:
		return "text", nil
	case *postgres.DataType_ByteaType:
		return "bytea", nil
	case *postgres.DataType_DateType:
		return "date", nil
	case *postgres.DataType_BooleanType:
		return "boolean", nil
	case *postgres.DataType_PointType:
		return "point", nil
	case *postgres.DataType_LineType:
		return "line", nil
	case *postgres.DataType_LsegType:
		return "lseg", nil
	case *postgres.DataType_BoxType:
		return "box", nil
	case *postgres.DataType_PathType:
		return "path", nil
	case *postgres.DataType_PolygonType:
		return "polygon", nil
	case *postgres.DataType_CircleType:
		return "circle", nil
	case *postgres.DataType_InetType:
		return "inet", nil
	case *postgres.DataType_CidrType:
		return "cidr", nil
	case *postgres.DataType_MacaddrType:
		return "macaddr", nil
	case *postgres.DataType_Macaddr8Type:
		return "macaddr8", nil
	case *postgres.DataType_TsvectorType:
		return "tsvector", nil
	case *postgres.DataType_TsqueryType:
		return "tsquery", nil
	case *postgres.DataType_UuidType:
		return "uuid", nil
	case *postgres.DataType_XmlType:
		return "xml", nil
	case *postgres.DataType_JsonType:
		return "json", nil
	case *postgres.DataType_JsonbType:
		return "jsonb", nil
	case *postgres.DataType_JsonpathType:
		return "jsonpath", nil
	case *postgres.DataType_Int4RangeType:
		return "int4range", nil
	case *postgres.DataType_Int8RangeType:
		return "int8range", nil
	case *postgres.DataType_NumRangeType:
		return "numrange", nil
	case *postgres.DataType_TsRangeType:
		return "tsrange", nil
	case *postgres.DataType_TstzRangeType:
		return "tstzrange", nil
	case *postgres.DataType_DateRangeType:
		return "daterange", nil
	case *postgres.DataType_OidType:
		return "oid", nil
	case *postgres.DataType_PgLsnType:
		return "pg_lsn", nil

	// 2. Types with Precision/Scale (Numeric & Decimal)
	case *postgres.DataType_DecimalType:
		return formatNumeric(
			t.DecimalType.Precision, t.DecimalType.Scale,
		), nil
	case *postgres.DataType_NumericType:
		return formatNumeric(
			t.NumericType.Precision, t.NumericType.Scale,
		), nil

	// 3. Character Types
	case *postgres.DataType_VarcharType:
		return formatLength("character varying", t.VarcharType.Length), nil
	case *postgres.DataType_CharType:
		return formatLength("character", t.CharType.Length), nil

	// 4. Bit String Types
	case *postgres.DataType_BitType:
		return formatLength("bit", t.BitType.Length), nil
	case *postgres.DataType_BitVaryingType:
		return formatLength("bit varying", t.BitVaryingType.Length), nil

	// 5. Date/Time Types (with precision)
	case *postgres.DataType_TimestampType:
		return formatTimePrecision(
			"timestamp without time zone", t.TimestampType.Precision,
		), nil
	case *postgres.DataType_TimestamptzType:
		return formatTimePrecision(
			"timestamp with time zone", t.TimestamptzType.Precision,
		), nil
	case *postgres.DataType_TimeType:
		return formatTimePrecision(
			"time without time zone", t.TimeType.Precision,
		), nil
	case *postgres.DataType_TimetzType:
		return formatTimePrecision(
			"time with time zone", t.TimetzType.Precision,
		), nil
	case *postgres.DataType_IntervalType:
		return formatInterval(
			t.IntervalType.GetFields(), t.IntervalType.Precision,
		), nil

	// 6. Schema-Qualified Types (Enum, Composite, Domain)
	case *postgres.DataType_EnumType:
		return formatQualifiedName(
			t.EnumType.GetSchema(), t.EnumType.GetName(),
		), nil
	case *postgres.DataType_CompositeType:
		return formatQualifiedName(
			t.CompositeType.GetSchema(), t.CompositeType.GetName(),
		), nil
	case *postgres.DataType_DomainType:
		return formatQualifiedName(
			t.DomainType.GetSchema(), t.DomainType.GetName(),
		), nil
	case *postgres.DataType_CustomRangeType:
		return formatQualifiedName("", t.CustomRangeType.GetName()), nil

	// 7. Recursive Types (Arrays)
	case *postgres.DataType_ArrayType:
		inner, err := ToDatabaseDataType(t.ArrayType.GetElementType())
		if err != nil {
			return "", err
		}
		return migrate.DatabaseDataType(inner.String() + "[]"), nil

	default:
		return "", fmt.Errorf("unsupported data type mapping: %T", t)
	}
}
