package postgres

import (
	"testing"

	"github.com/gotidy/ptr"
	"github.com/uthereal/scheme/genproto/postgres"
)

func TestResolvePGType(t *testing.T) {
	tests := []struct {
		name    string
		dt      *postgres.DataType
		want    string
		wantErr bool
	}{
		// 1. Fixed-name types
		{"smallint", &postgres.DataType{Type: &postgres.DataType_SmallintType{}}, "smallint", false},
		{"integer", &postgres.DataType{Type: &postgres.DataType_IntegerType{}}, "integer", false},
		{"bigint", &postgres.DataType{Type: &postgres.DataType_BigintType{}}, "bigint", false},
		{"real", &postgres.DataType{Type: &postgres.DataType_RealType{}}, "real", false},
		{"double precision", &postgres.DataType{Type: &postgres.DataType_DoublePrecisionType{}}, "double precision", false},
		{"smallserial", &postgres.DataType{Type: &postgres.DataType_SmallserialType{}}, "smallint", false},
		{"serial", &postgres.DataType{Type: &postgres.DataType_SerialType{}}, "integer", false},
		{"bigserial", &postgres.DataType{Type: &postgres.DataType_BigserialType{}}, "bigint", false},
		{"money", &postgres.DataType{Type: &postgres.DataType_MoneyType{}}, "money", false},
		{"text", &postgres.DataType{Type: &postgres.DataType_TextType{}}, "text", false},
		{"bytea", &postgres.DataType{Type: &postgres.DataType_ByteaType{}}, "bytea", false},
		{"date", &postgres.DataType{Type: &postgres.DataType_DateType{}}, "date", false},
		{"boolean", &postgres.DataType{Type: &postgres.DataType_BooleanType{}}, "boolean", false},
		{"point", &postgres.DataType{Type: &postgres.DataType_PointType{}}, "point", false},
		{"line", &postgres.DataType{Type: &postgres.DataType_LineType{}}, "line", false},
		{"lseg", &postgres.DataType{Type: &postgres.DataType_LsegType{}}, "lseg", false},
		{"box", &postgres.DataType{Type: &postgres.DataType_BoxType{}}, "box", false},
		{"path", &postgres.DataType{Type: &postgres.DataType_PathType{}}, "path", false},
		{"polygon", &postgres.DataType{Type: &postgres.DataType_PolygonType{}}, "polygon", false},
		{"circle", &postgres.DataType{Type: &postgres.DataType_CircleType{}}, "circle", false},
		{"inet", &postgres.DataType{Type: &postgres.DataType_InetType{}}, "inet", false},
		{"cidr", &postgres.DataType{Type: &postgres.DataType_CidrType{}}, "cidr", false},
		{"macaddr", &postgres.DataType{Type: &postgres.DataType_MacaddrType{}}, "macaddr", false},
		{"macaddr8", &postgres.DataType{Type: &postgres.DataType_Macaddr8Type{}}, "macaddr8", false},
		{"tsvector", &postgres.DataType{Type: &postgres.DataType_TsvectorType{}}, "tsvector", false},
		{"tsquery", &postgres.DataType{Type: &postgres.DataType_TsqueryType{}}, "tsquery", false},
		{"uuid", &postgres.DataType{Type: &postgres.DataType_UuidType{}}, "uuid", false},
		{"xml", &postgres.DataType{Type: &postgres.DataType_XmlType{}}, "xml", false},
		{"json", &postgres.DataType{Type: &postgres.DataType_JsonType{}}, "json", false},
		{"jsonb", &postgres.DataType{Type: &postgres.DataType_JsonbType{}}, "jsonb", false},
		{"jsonpath", &postgres.DataType{Type: &postgres.DataType_JsonpathType{}}, "jsonpath", false},
		{"int4range", &postgres.DataType{Type: &postgres.DataType_Int4RangeType{}}, "int4range", false},
		{"int8range", &postgres.DataType{Type: &postgres.DataType_Int8RangeType{}}, "int8range", false},
		{"numrange", &postgres.DataType{Type: &postgres.DataType_NumRangeType{}}, "numrange", false},
		{"tsrange", &postgres.DataType{Type: &postgres.DataType_TsRangeType{}}, "tsrange", false},
		{"tstzrange", &postgres.DataType{Type: &postgres.DataType_TstzRangeType{}}, "tstzrange", false},
		{"daterange", &postgres.DataType{Type: &postgres.DataType_DateRangeType{}}, "daterange", false},
		{"oid", &postgres.DataType{Type: &postgres.DataType_OidType{}}, "oid", false},
		{"pg_lsn", &postgres.DataType{Type: &postgres.DataType_PgLsnType{}}, "pg_lsn", false},

		// 2. Types with Precision/Scale
		{"decimal no prec", &postgres.DataType{Type: &postgres.DataType_DecimalType{DecimalType: &postgres.DecimalType{}}}, "numeric", false},
		{"decimal prec", &postgres.DataType{Type: &postgres.DataType_DecimalType{DecimalType: &postgres.DecimalType{Precision: ptr.Int32(10)}}}, "numeric(10)", false},
		{"decimal prec scale", &postgres.DataType{Type: &postgres.DataType_DecimalType{DecimalType: &postgres.DecimalType{Precision: ptr.Int32(10), Scale: ptr.Int32(2)}}}, "numeric(10,2)", false},
		{"numeric no prec", &postgres.DataType{Type: &postgres.DataType_NumericType{NumericType: &postgres.NumericType{}}}, "numeric", false},
		{"numeric prec", &postgres.DataType{Type: &postgres.DataType_NumericType{NumericType: &postgres.NumericType{Precision: ptr.Int32(10)}}}, "numeric(10)", false},
		{"numeric prec scale", &postgres.DataType{Type: &postgres.DataType_NumericType{NumericType: &postgres.NumericType{Precision: ptr.Int32(10), Scale: ptr.Int32(2)}}}, "numeric(10,2)", false},

		// 3. Character Types
		{"varchar no len", &postgres.DataType{Type: &postgres.DataType_VarcharType{VarcharType: &postgres.VarcharType{}}}, "character varying", false},
		{"varchar len", &postgres.DataType{Type: &postgres.DataType_VarcharType{VarcharType: &postgres.VarcharType{Length: ptr.Int32(255)}}}, "character varying(255)", false},
		{"char no len", &postgres.DataType{Type: &postgres.DataType_CharType{CharType: &postgres.CharType{}}}, "character", false},
		{"char len", &postgres.DataType{Type: &postgres.DataType_CharType{CharType: &postgres.CharType{Length: ptr.Int32(10)}}}, "character(10)", false},

		// 4. Bit String Types
		{"bit no len", &postgres.DataType{Type: &postgres.DataType_BitType{BitType: &postgres.BitType{}}}, "bit", false},
		{"bit len", &postgres.DataType{Type: &postgres.DataType_BitType{BitType: &postgres.BitType{Length: ptr.Int32(8)}}}, "bit(8)", false},
		{"bit varying no len", &postgres.DataType{Type: &postgres.DataType_BitVaryingType{BitVaryingType: &postgres.BitVaryingType{}}}, "bit varying", false},
		{"bit varying len", &postgres.DataType{Type: &postgres.DataType_BitVaryingType{BitVaryingType: &postgres.BitVaryingType{Length: ptr.Int32(8)}}}, "bit varying(8)", false},

		// 5. Date/Time Types
		{"timestamp no prec", &postgres.DataType{Type: &postgres.DataType_TimestampType{TimestampType: &postgres.TimestampType{}}}, "timestamp without time zone", false},
		{"timestamp prec", &postgres.DataType{Type: &postgres.DataType_TimestampType{TimestampType: &postgres.TimestampType{Precision: ptr.Int32(6)}}}, "timestamp(6) without time zone", false},
		{"timestamptz no prec", &postgres.DataType{Type: &postgres.DataType_TimestamptzType{TimestamptzType: &postgres.TimestampTzType{}}}, "timestamp with time zone", false},
		{"timestamptz prec", &postgres.DataType{Type: &postgres.DataType_TimestamptzType{TimestamptzType: &postgres.TimestampTzType{Precision: ptr.Int32(6)}}}, "timestamp(6) with time zone", false},
		{"time no prec", &postgres.DataType{Type: &postgres.DataType_TimeType{TimeType: &postgres.TimeType{}}}, "time without time zone", false},
		{"time prec", &postgres.DataType{Type: &postgres.DataType_TimeType{TimeType: &postgres.TimeType{Precision: ptr.Int32(3)}}}, "time(3) without time zone", false},
		{"timetz no prec", &postgres.DataType{Type: &postgres.DataType_TimetzType{TimetzType: &postgres.TimeTzType{}}}, "time with time zone", false},
		{"timetz prec", &postgres.DataType{Type: &postgres.DataType_TimetzType{TimetzType: &postgres.TimeTzType{Precision: ptr.Int32(3)}}}, "time(3) with time zone", false},
		{"interval empty", &postgres.DataType{Type: &postgres.DataType_IntervalType{IntervalType: &postgres.IntervalType{}}}, "interval", false},
		{"interval fields", &postgres.DataType{Type: &postgres.DataType_IntervalType{IntervalType: &postgres.IntervalType{Fields: ptr.String("DAY")}}}, "interval DAY", false},
		{"interval prec", &postgres.DataType{Type: &postgres.DataType_IntervalType{IntervalType: &postgres.IntervalType{Precision: ptr.Int32(3)}}}, "interval(3)", false},
		{"interval fields prec", &postgres.DataType{Type: &postgres.DataType_IntervalType{IntervalType: &postgres.IntervalType{Fields: ptr.String("DAY"), Precision: ptr.Int32(3)}}}, "interval DAY(3)", false},

		// 6. Schema-Qualified Types
		{"enum public", &postgres.DataType{Type: &postgres.DataType_EnumType{EnumType: &postgres.EnumReference{Schema: "public", Name: "status"}}}, "public.status", false},
		{"enum no schema", &postgres.DataType{Type: &postgres.DataType_EnumType{EnumType: &postgres.EnumReference{Name: "status"}}}, "status", false},
		{"composite public", &postgres.DataType{Type: &postgres.DataType_CompositeType{CompositeType: &postgres.CompositeReference{Schema: "public", Name: "address"}}}, "public.address", false},
		{"composite no schema", &postgres.DataType{Type: &postgres.DataType_CompositeType{CompositeType: &postgres.CompositeReference{Name: "address"}}}, "address", false},
		{"domain public", &postgres.DataType{Type: &postgres.DataType_DomainType{DomainType: &postgres.DomainReference{Schema: "public", Name: "email"}}}, "public.email", false},
		{"domain no schema", &postgres.DataType{Type: &postgres.DataType_DomainType{DomainType: &postgres.DomainReference{Name: "email"}}}, "email", false},
		{"custom range", &postgres.DataType{Type: &postgres.DataType_CustomRangeType{CustomRangeType: &postgres.CustomRangeType{Name: "myrange"}}}, "myrange", false},

		// 7. Arrays
		{"array text", &postgres.DataType{Type: &postgres.DataType_ArrayType{ArrayType: &postgres.ArrayType{ElementType: &postgres.DataType{Type: &postgres.DataType_TextType{}}}}}, "text[]", false},
		{"array invalid", &postgres.DataType{Type: &postgres.DataType_ArrayType{ArrayType: &postgres.ArrayType{ElementType: nil}}}, "", true},

		// Errors
		{"nil data type", nil, "", true},
		{"unsupported type", &postgres.DataType{Type: nil}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToDatabaseDataType(tt.dt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToDatabaseDataType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if string(got) != tt.want {
				t.Errorf("ToDatabaseDataType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatTimePrecision(t *testing.T) {
	got := formatTimePrecision("time", ptr.Int32(3))
	if got != "time(3)" {
		t.Errorf("got %v", got)
	}
}
