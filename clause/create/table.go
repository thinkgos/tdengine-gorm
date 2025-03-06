package create


type Table struct {
	TableType   int
	Table       string
	IfNotExists bool
	STable      string
	Tags        map[string]any
	Column      []*Column
	TagColumn   []*Column
}

// NewTable Create new common table
func NewTable(name string, ifNotExist bool, column []*Column, Stable string, tags map[string]any) *Table {
	return &Table{
		TableType:   CommonTableType,
		Table:       name,
		IfNotExists: ifNotExist,
		STable:      Stable,
		Tags:        tags,
		Column:      column,
	}
}

// NewSTable Create new sTable
func NewSTable(name string, ifNotExists bool, column []*Column, tagColumn []*Column) *Table {
	return &Table{
		TableType:   STableType,
		Table:       name,
		IfNotExists: ifNotExists,
		Column:      column,
		TagColumn:   tagColumn,
	}
}
