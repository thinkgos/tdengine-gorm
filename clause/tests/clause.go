package tests

import (
	"reflect"
	"strings"
	"sync"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils/tests"
)

var db, _ = gorm.Open(DummyDialector{}, nil)

func CheckBuildClauses(t *testing.T, clauses []clause.Interface, results []string, vars [][][]any) {
	var (
		buildNames    []string
		buildNamesMap = map[string]bool{}
		user, _       = schema.Parse(&tests.User{}, &sync.Map{}, db.NamingStrategy)
		stmt          = gorm.Statement{DB: db, Table: user.Table, Schema: user, Clauses: map[string]clause.Clause{}}
	)

	for _, c := range clauses {
		if _, ok := buildNamesMap[c.Name()]; !ok {
			buildNames = append(buildNames, c.Name())
			buildNamesMap[c.Name()] = true
		}

		stmt.AddClause(c)
	}

	stmt.Build(buildNames...)
	sql := strings.TrimSpace(stmt.SQL.String())
	matched := false
	for i, result := range results {
		if sql == result {
			matched = true
			matchVars := false
			if len(stmt.Vars) > 0 {
				if len(vars) > i {
					for _, varItem := range vars[i] {
						if reflect.DeepEqual(stmt.Vars, varItem) {
							matchVars = true
							break
						}
					}
				}
			} else {
				matchVars = true
			}
			if !matchVars {
				t.Errorf("Vars \nexpects:\n\t%+v\ngot:\n\t%v\n", stmt.Vars, vars[i])
			}
			break
		}
	}
	if !matched {
		t.Errorf("SQL \nexpects:\n\t%v\ngot:\n\t%v\n", results, sql)
	}
}
