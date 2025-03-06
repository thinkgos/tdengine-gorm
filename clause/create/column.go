package create

import (
	"strconv"
	"strings"
)

type Column struct {
	Name       string
	ColumnType string
	Length     uint64
}

func (c *Column) ToSql() string {
	b := strings.Builder{}
	b.WriteByte('`')
	b.WriteString(c.Name)
	b.WriteByte('`')
	b.WriteByte(' ')
	b.WriteString(c.ColumnType)
	if c.ColumnType == NCharType ||
		c.ColumnType == BinaryType ||
		c.ColumnType == VarCharType ||
		c.ColumnType == VarBinaryType {
		b.WriteByte('(')
		b.WriteString(strconv.FormatUint(c.Length, 10))
		b.WriteByte(')')
	}
	return b.String()
}
