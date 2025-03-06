package create_test

import (
	"fmt"
	"testing"

	"github.com/thinkgos/TDengine-gorm/clause/create"
	"github.com/thinkgos/TDengine-gorm/clause/tests"

	"gorm.io/gorm/clause"
)

func Test_CreateTable(t *testing.T) {
	var testCases = []struct {
		Name    string
		Clauses []clause.Interface
		Result  []string
		Vars    [][][]any
	}{
		{
			"create table use stable",
			[]clause.Interface{
				create.NewCreateTableClause([]*create.Table{
					{
						TableType:   create.CommonTableType,
						Table:       "t_1",
						IfNotExists: true,
						STable:      "st_1",
						Tags: map[string]any{
							"tag_int":    1,
							"tag_string": "string",
						},
						Column: []*create.Column{
							{
								Name:       "ts",
								ColumnType: create.TimestampType,
								Length:     0,
							},
							{
								Name:       "c_int",
								ColumnType: create.IntType,
								Length:     0,
							},
							{
								Name:       "c_bigint",
								ColumnType: create.BigIntType,
								Length:     0,
							},
							{
								Name:       "c_float",
								ColumnType: create.FloatType,
								Length:     0,
							},
							{
								Name:       "c_double",
								ColumnType: create.DoubleType,
								Length:     0,
							},
							{
								Name:       "c_binary",
								ColumnType: create.BinaryType,
								Length:     128,
							},
							{
								Name:       "c_smallint",
								ColumnType: create.SmallIntType,
								Length:     0,
							},
							{
								Name:       "c_tinyint",
								ColumnType: create.TinyIntType,
								Length:     0,
							},
							{
								Name:       "c_bool",
								ColumnType: create.BoolType,
								Length:     0,
							},
							{
								Name:       "c_nchar",
								ColumnType: create.NCharType,
								Length:     128,
							},
						},
					},
				})},
			[]string{
				"CREATE TABLE IF NOT EXISTS `t_1` USING `st_1`(`tag_int`,`tag_string`) TAGS (?,?)",
				"CREATE TABLE IF NOT EXISTS `t_1` USING `st_1`(`tag_string`,`tag_int`) TAGS (?,?)",
			},
			[][][]any{{{1, "string"}}, {{"string", 1}}},
		},
		{
			"create table without stable",
			[]clause.Interface{
				create.NewCreateTableClause(nil).
					AddTables(&create.Table{
						TableType:   create.CommonTableType,
						Table:       "t_1",
						IfNotExists: true,
						STable:      "",
						Tags: map[string]any{
							"tag_int":    1,
							"tag_string": "string",
						},
						Column: []*create.Column{
							{
								Name:       "ts",
								ColumnType: create.TimestampType,
								Length:     0,
							},
							{
								Name:       "c_int",
								ColumnType: create.IntType,
								Length:     0,
							},
							{
								Name:       "c_bigint",
								ColumnType: create.BigIntType,
								Length:     0,
							},
							{
								Name:       "c_float",
								ColumnType: create.FloatType,
								Length:     0,
							},
							{
								Name:       "c_double",
								ColumnType: create.DoubleType,
								Length:     0,
							},
							{
								Name:       "c_binary",
								ColumnType: create.BinaryType,
								Length:     128,
							},
							{
								Name:       "c_smallint",
								ColumnType: create.SmallIntType,
								Length:     0,
							},
							{
								Name:       "c_tinyint",
								ColumnType: create.TinyIntType,
								Length:     0,
							},
							{
								Name:       "c_bool",
								ColumnType: create.BoolType,
								Length:     0,
							},
							{
								Name:       "c_nchar",
								ColumnType: create.NCharType,
								Length:     128,
							},
						},
					})},
			[]string{
				"CREATE TABLE IF NOT EXISTS `t_1` (`ts` TIMESTAMP,`c_int` INT,`c_bigint` BIGINT,`c_float` FLOAT,`c_double` DOUBLE,`c_binary` BINARY(128),`c_smallint` SMALLINT,`c_tinyint` TINYINT,`c_bool` BOOL,`c_nchar` NCHAR(128))",
			},
			nil,
		},
		{
			"create stable",
			[]clause.Interface{
				create.NewCreateTableClause([]*create.Table{
					{
						TableType:   create.STableType,
						Table:       "st_1",
						IfNotExists: true,
						Column: []*create.Column{
							{
								Name:       "ts",
								ColumnType: create.TimestampType,
								Length:     0,
							},
							{
								Name:       "c_int",
								ColumnType: create.IntType,
								Length:     0,
							},
							{
								Name:       "c_bigint",
								ColumnType: create.BigIntType,
								Length:     0,
							},
							{
								Name:       "c_float",
								ColumnType: create.FloatType,
								Length:     0,
							},
							{
								Name:       "c_double",
								ColumnType: create.DoubleType,
								Length:     0,
							},
							{
								Name:       "c_binary",
								ColumnType: create.BinaryType,
								Length:     128,
							},
							{
								Name:       "c_smallint",
								ColumnType: create.SmallIntType,
								Length:     0,
							},
							{
								Name:       "c_tinyint",
								ColumnType: create.TinyIntType,
								Length:     0,
							},
							{
								Name:       "c_bool",
								ColumnType: create.BoolType,
								Length:     0,
							},
							{
								Name:       "c_nchar",
								ColumnType: create.NCharType,
								Length:     128,
							},
						},
						TagColumn: []*create.Column{
							{
								Name:       "t_int",
								ColumnType: create.IntType,
								Length:     0,
							},
						},
					},
				})},
			[]string{
				"CREATE STABLE IF NOT EXISTS `st_1` (`ts` TIMESTAMP,`c_int` INT,`c_bigint` BIGINT,`c_float` FLOAT,`c_double` DOUBLE,`c_binary` BINARY(128),`c_smallint` SMALLINT,`c_tinyint` TINYINT,`c_bool` BOOL,`c_nchar` NCHAR(128)) TAGS(`t_int` INT)",
				"CREATE STABLE IF NOT EXISTS `st_1` (`ts` TIMESTAMP,`c_int` INT,`c_bigint` BIGINT,`c_float` FLOAT,`c_double` DOUBLE,`c_binary` BINARY(128),`c_smallint` SMALLINT,`c_tinyint` TINYINT,`c_bool` BOOL,`c_nchar` NCHAR(128)) TAGS(`t_int` INT)",
			},
			nil,
		},
		{
			"unsupported table type",
			[]clause.Interface{
				create.NewCreateTableClause([]*create.Table{
					{
						Table:       "st_1",
						IfNotExists: true,
						Column: []*create.Column{
							{
								Name:       "ts",
								ColumnType: create.TimestampType,
								Length:     0,
							},
						},
						TagColumn: []*create.Column{
							{
								Name:       "t_int",
								ColumnType: create.IntType,
								Length:     0,
							},
						},
					},
				}),
			},
			[]string{""},
			nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			tests.CheckBuildClauses(t, tc.Clauses, tc.Result, tc.Vars)
		})
	}
}

func Test_NewTable(t *testing.T) {
	table := create.NewTable(
		"t_1",
		true,
		[]*create.Column{
			{
				Name:       "ts",
				ColumnType: create.TimestampType,
				Length:     0,
			},
			{
				Name:       "c_int",
				ColumnType: create.IntType,
				Length:     0,
			},
			{
				Name:       "c_bigint",
				ColumnType: create.BigIntType,
				Length:     0,
			},
			{
				Name:       "c_float",
				ColumnType: create.FloatType,
				Length:     0,
			},
			{
				Name:       "c_double",
				ColumnType: create.DoubleType,
				Length:     0,
			},
			{
				Name:       "c_binary",
				ColumnType: create.BinaryType,
				Length:     128,
			},
			{
				Name:       "c_smallint",
				ColumnType: create.SmallIntType,
				Length:     0,
			},
			{
				Name:       "c_tinyint",
				ColumnType: create.TinyIntType,
				Length:     0,
			},
			{
				Name:       "c_bool",
				ColumnType: create.BoolType,
				Length:     0,
			},
			{
				Name:       "c_nchar",
				ColumnType: create.NCharType,
				Length:     128,
			},
		},
		"st_1",
		map[string]any{
			"tag_int":    1,
			"tag_string": "string",
		})
	sTable := create.NewSTable(
		"st_1",
		true,
		[]*create.Column{
			{
				Name:       "ts",
				ColumnType: create.TimestampType,
				Length:     0,
			},
			{
				Name:       "c_int",
				ColumnType: create.IntType,
				Length:     0,
			},
			{
				Name:       "c_bigint",
				ColumnType: create.BigIntType,
				Length:     0,
			},
			{
				Name:       "c_float",
				ColumnType: create.FloatType,
				Length:     0,
			},
			{
				Name:       "c_double",
				ColumnType: create.DoubleType,
				Length:     0,
			},
			{
				Name:       "c_binary",
				ColumnType: create.BinaryType,
				Length:     128,
			},
			{
				Name:       "c_smallint",
				ColumnType: create.SmallIntType,
				Length:     0,
			},
			{
				Name:       "c_tinyint",
				ColumnType: create.TinyIntType,
				Length:     0,
			},
			{
				Name:       "c_bool",
				ColumnType: create.BoolType,
				Length:     0,
			},
			{
				Name:       "c_nchar",
				ColumnType: create.NCharType,
				Length:     128,
			},
		},
		[]*create.Column{
			{
				Name:       "tag1",
				ColumnType: create.BinaryType,
				Length:     256,
			},
			{
				Name:       "tag2",
				ColumnType: create.DoubleType,
				Length:     0,
			},
		})
	var testCases = []struct {
		Clauses []clause.Interface
		Result  []string
		Vars    [][][]any
	}{
		{
			[]clause.Interface{
				create.NewCreateTableClause([]*create.Table{
					table,
				})},
			[]string{
				"CREATE TABLE IF NOT EXISTS `t_1` USING `st_1`(`tag_int`,`tag_string`) TAGS (?,?)",
				"CREATE TABLE IF NOT EXISTS `t_1` USING `st_1`(`tag_string`,`tag_int`) TAGS (?,?)",
			},
			[][][]any{{{1, "string"}}, {{"string", 1}}},
		},
		{
			[]clause.Interface{create.NewCreateTableClause([]*create.Table{
				sTable,
			})},
			[]string{
				"CREATE STABLE IF NOT EXISTS `st_1` (`ts` TIMESTAMP,`c_int` INT,`c_bigint` BIGINT,`c_float` FLOAT,`c_double` DOUBLE,`c_binary` BINARY(128),`c_smallint` SMALLINT,`c_tinyint` TINYINT,`c_bool` BOOL,`c_nchar` NCHAR(128)) TAGS(`tag1` BINARY(256),`tag2` DOUBLE)",
			},
			nil,
		},
	}

	for idx, result := range testCases {
		t.Run(fmt.Sprintf("case #%v", idx), func(t *testing.T) {
			tests.CheckBuildClauses(t, result.Clauses, result.Result, result.Vars)
		})
	}
}
