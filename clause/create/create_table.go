package create

import (
	"errors"

	"gorm.io/gorm/clause"
)

const (
	STableType = iota + 1
	CommonTableType
)

const (
	TimestampType        = "TIMESTAMP"
	IntType              = "INT"
	IntUnsignedType      = "INT UNSIGNED"
	BigIntType           = "BIGINT"
	BigUnsignedIntType   = "BIGINT UNSIGNED"
	FloatType            = "FLOAT"
	DoubleType           = "DOUBLE"
	BinaryType           = "BINARY"
	SmallIntType         = "SMALLINT"
	SmallIntUnsignedType = "SMALLINT UNSIGNED"
	TinyIntType          = "TINYINT"
	TinyIntUnsignedType  = "TINYINT UNSIGNED"
	BoolType             = "BOOL"
	NCharType            = "NCHAR"
	JSONType             = "JSON"
	VarCharType          = "VARCHAR"
	GeometryType         = "GEOMETRY" // TODO: not support yet.
	VarBinaryType        = "VARBINARY"
)

type CreateTable struct {
	tables []*Table
}

// NewCreateTableClause Create table clause
func NewCreateTableClause(tables []*Table) *CreateTable {
	return &CreateTable{tables: tables}
}

// AddTables Add tables to clause
func (c *CreateTable) AddTables(tables ...*Table) *CreateTable {
	c.tables = append(c.tables, tables...)
	return c
}

func (CreateTable) Name() string {
	return "CREATE TABLE"
}

func (c CreateTable) Build(builder clause.Builder) {
	for _, table := range c.tables {
		switch table.TableType {
		case CommonTableType:
			_, _ = builder.WriteString("CREATE TABLE ")
		case STableType:
			_, _ = builder.WriteString("CREATE STABLE ")
		default:
			_ = builder.AddError(errors.New("Unsupported table type"))
			return
		}
		if table.IfNotExists {
			_, _ = builder.WriteString("IF NOT EXISTS ")
		}
		builder.WriteQuoted(table.Table)
		if table.TableType == CommonTableType && table.STable != "" {
			_, _ = builder.WriteString(" USING ")
			builder.WriteQuoted(table.STable)

			tagValueList := make([]any, 0, len(table.Tags))
			index := 0
			_ = builder.WriteByte('(')
			for tag, tagValue := range table.Tags {
				if index > 0 {
					_ = builder.WriteByte(',')
				}
				builder.WriteQuoted(tag)

				tagValueList = append(tagValueList, tagValue)
				index += 1
			}
			_, _ = builder.WriteString(") TAGS ")
			builder.AddVar(builder, tagValueList)
		} else {
			_, _ = builder.WriteString(" (")
			for i, column := range table.Column {
				if i > 0 {
					_ = builder.WriteByte(',')
				}
				_, _ = builder.WriteString(column.ToSql())
			}
			_ = builder.WriteByte(')')
		}
		if table.TableType == STableType {
			_, _ = builder.WriteString(" TAGS(")
			for i, tags := range table.TagColumn {
				if i > 0 {
					_ = builder.WriteByte(',')
				}
				_, _ = builder.WriteString(tags.ToSql())

			}
			_ = builder.WriteByte(')')
		}
	}
}

// MergeClause merge CREATE TABLE by clauses
func (c CreateTable) MergeClause(clause *clause.Clause) {
	clause.Name = ""
	clause.Expression = c
}
