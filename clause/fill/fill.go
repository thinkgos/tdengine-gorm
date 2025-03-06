package fill

import (
	"strconv"

	"gorm.io/gorm/clause"
)

type Type string

const (
	FillNone   Type = "NONE"
	FillValue  Type = "VALUE"
	FillPrev   Type = "PREV"
	FillNull   Type = "NULL"
	FillLinear Type = "LINEAR"
	FillNext   Type = "NEXT"
)

type Fill struct {
	fillType Type
	value    float64
}

// SetFill Fill clause
func SetFill(fillType Type) Fill {
	return Fill{
		fillType: fillType,
	}
}

// SetValue Set fill value
func (f Fill) SetValue(value float64) Fill {
	f.value = value
	return f
}

// Build [FILL(fill_mod_and_val)]
func (f Fill) Build(builder clause.Builder) {
	builder.WriteString("(")
	builder.WriteString(string(f.fillType))
	if f.fillType == FillValue {
		builder.WriteByte(',')
		builder.WriteString(strconv.FormatFloat(f.value, 'g', -1, 64))
	}
	builder.WriteByte(')')
}

func (f Fill) Name() string {
	return "FILL"
}

func (f Fill) MergeClause(c *clause.Clause) {
	c.Expression = f
}
