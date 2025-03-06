package tdengine_gorm

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/thinkgos/TDengine-gorm/clause/create"
	"github.com/thinkgos/TDengine-gorm/clause/fill"
	"github.com/thinkgos/TDengine-gorm/clause/using"
	"github.com/thinkgos/TDengine-gorm/clause/window"
	"gorm.io/gorm"
)

const (
	testDb    = "gorm_test"
	dsn       = "root:taosdata@tcp(10.110.18.131:6030)/"
	dsnWithDb = dsn + testDb
)

func Test_Dialect(t *testing.T) {
	testCases := []struct {
		name         string
		dialect      *Dialect
		openSuccess  bool
		query        string
		querySuccess bool
	}{
		{
			name: "Default driver",
			dialect: &Dialect{
				DSN: dsn,
			},
			openSuccess:  true,
			query:        "SELECT 1",
			querySuccess: true,
		},
		{
			name: "create db",
			dialect: &Dialect{
				DriverName: DefaultDriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "create database if not exists `gorm_test`",
			querySuccess: true,
		},
		{
			name: "create table",
			dialect: &Dialect{
				DriverName: DefaultDriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "create table if not exists `gorm_test`.`test` (`ts` timestamp, `value` double)",
			querySuccess: true,
		},
		{
			name: "insert data",
			dialect: &Dialect{
				DriverName: DefaultDriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "insert into `gorm_test`.`test` values (now,12)",
			querySuccess: true,
		},
		{
			name: "query data",
			dialect: &Dialect{
				DriverName: DefaultDriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "select * from `gorm_test`.`test` limit 1",
			querySuccess: true,
		},
		{
			name: "syntax error",
			dialect: &Dialect{
				DriverName: DefaultDriverName,
				DSN:        dsn,
			},
			openSuccess:  true,
			query:        "select * rfom `gorm_test`.`test` limit 1",
			querySuccess: false,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d/%s", i, tc.name), func(t *testing.T) {
			db, err := gorm.Open(tc.dialect, &gorm.Config{})
			if !tc.openSuccess {
				if err == nil {
					t.Errorf("Expected Open to fail.")
				}
				return
			}
			if err != nil {
				t.Errorf("Expected Open to succeed; got error: %v", err)
				return
			}
			if db == nil {
				t.Errorf("Expected db to be non-nil.")
				return
			}
			if tc.query != "" {
				err = db.Exec(tc.query).Error
				if !tc.querySuccess {
					if err == nil {
						t.Errorf("Expected query to fail.")
					}
					return
				}

				if err != nil {
					t.Errorf("Expected query to succeed; got error: %v", err)
				}
			}
		})
	}
}

func Test_Clause(t *testing.T) {
	dsnWithoutDB := dsn + "?loc=Local"
	nativeDB, err := sql.Open(DefaultDriverName, dsnWithoutDB)
	if err != nil {
		t.Errorf("connect db error:%v", err)
		return
	}
	_, err = nativeDB.Exec("create database if not exists `gorm_test`")
	if err != nil {
		t.Errorf("create database error %v", err)
		return
	}
	nativeDB.Close()
	dsn := dsnWithDb + "?loc=Local"
	db, err := gorm.Open(&Dialect{DSN: dsn})
	if err != nil {
		t.Errorf("unexpected error:%v", err)
		return
	}
	db = db.Debug()

	t.Run("create stable", func(t *testing.T) {
		stable := create.NewSTable(
			"stb_1",
			true,
			[]*create.Column{{
				Name:       "ts",
				ColumnType: create.TimestampType,
			},
				{
					Name:       "value",
					ColumnType: create.BigUnsignedIntType,
				}},
			[]*create.Column{
				{
					Name:       "tbn",
					ColumnType: create.BinaryType,
					Length:     64,
				},
			})
		err = db.Table("stb_1").
			Clauses(create.NewCreateTableClause([]*create.Table{stable})).
			Create(map[string]any{}).Error
		if err != nil {
			t.Errorf("create stable error %v", err)
			return
		}
	})

	t.Run("create table using stable", func(t *testing.T) {
		table := create.NewTable(
			"tb_1",
			true,
			nil,
			"stb_1",
			map[string]any{
				"tbn": "tb_1",
			},
		)
		err = db.Table("tb_1").
			Clauses(create.NewCreateTableClause([]*create.Table{table})).
			Create(map[string]any{}).Error
		if err != nil {
			t.Errorf("create table error %v", err)
			return
		}
	})
	type Data struct {
		TS    time.Time
		Value int64
	}
	now := time.Now()
	randValue := rand.Int63()
	t.Run("insert data", func(t *testing.T) {
		err = db.Table("tb_1").
			Create(&Data{
				TS:    now,
				Value: randValue,
			}).Error
		if err != nil {
			t.Errorf("insert data error %v", err)
			return
		}
	})

	t1 := now.Add(time.Second)
	tRandValue := rand.Int63()
	t.Run("create table when insert data", func(t *testing.T) {
		//create table when insert data
		err = db.Table("tb_2").
			Clauses(using.SetUsing("stb_1", map[string]any{
				"tbn": "tb_2",
			})).
			Create(map[string]any{
				"ts":    t1,
				"value": tRandValue,
			}).Error
		if err != nil {
			t.Errorf("create table when insert data error %v", err)
			return
		}
	})

	t.Run("find tb_1 data", func(t *testing.T) {
		//find tb_1 data
		var d Data
		err = db.Table("tb_1").Where("ts = ?", now).Find(&d).Error
		if err != nil {
			t.Errorf("find data error %v", err)
			return
		}
		if d.Value != randValue {
			t.Errorf("expect value %v got %v", randValue, d.Value)
			return
		}
	})

	t.Run("find tb_2 data", func(t *testing.T) {
		//find tb_2 data
		var d2 Data
		err = db.Table("tb_2").Where("ts = ?", t1).Find(&d2).Error
		if err != nil {
			t.Errorf("find data error %v", err)
			return
		}
		if d2.Value != tRandValue {
			t.Errorf("expect value %v got %v", tRandValue, d2.Value)
			return
		}
	})

	t.Run("find by stable", func(t *testing.T) {
		// find by stable
		var d3 Data
		err = db.Table("stb_1").Where("ts = ?", now).Find(&d3).Error
		if err != nil {
			t.Errorf("find data by stable error %v", err)
			return
		}
		if d3.Value != randValue {
			t.Errorf("expect value %v got %v", randValue, d3.Value)
			return
		}
	})
	t2 := now.Add(time.Second * 2)
	t3 := now.Add(time.Second * 3)
	v1 := 11
	v2 := 12
	v3 := 13
	//aggregate query
	t.Run("aggregate insert data", func(t *testing.T) {
		err = db.Table("tb_aggregate").
			Clauses(using.SetUsing("stb_1", map[string]any{
				"tbn": "tb_aggregate",
			})).
			Create([]map[string]any{
				{
					"ts":    t1,
					"value": v1,
				}, {
					"ts":    t2,
					"value": v2,
				}, {
					"ts":    t3,
					"value": v3,
				},
			}).Error
		if err != nil {
			t.Errorf("create table when insert data error %v", err)
			return
		}
	})

	t.Run("aggregate query: avg", func(t *testing.T) {
		var result []map[string]any
		err = db.Table("tb_aggregate").Select("avg(`value`) as v").Where("`ts` >= ? and `ts` <= ?", now.Add(time.Second), now.Add(time.Second*3)).Find(&result).Error
		if err != nil {
			t.Errorf("aggregate query error %v", err)
			return
		}
		expectR1 := []map[string]any{
			{
				"v": float64(12),
			},
		}
		if !resultMapEqual(expectR1, result) {
			t.Errorf("expect %v got %v", expectR1, result)
			return
		}
	})

	t.Run("aggregate query: time window", func(t *testing.T) {
		var result2 []map[string]any
		windowD, err := window.NewDuration(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		err = db.Table("tb_aggregate").
			Select("max(`value`) as v").
			Where("ts >= ? and ts <= ?", now.Add(time.Second), now.Add(time.Second*4)).
			Clauses(
				window.SetInterval(*windowD),
				fill.SetFill(fill.FillNull),
			).
			Find(&result2).Error
		if err != nil {
			t.Errorf("aggregate query error %v", err)
			return
		}
		expectR2 := []map[string]any{
			{
				"ts": now.Add(time.Second),
				"v":  float64(11),
			},
			{
				"ts": now.Add(time.Second * 2),
				"v":  float64(12),
			},
			{
				"ts": now.Add(time.Second * 3),
				"v":  float64(13),
			},
			{
				"ts": now.Add(time.Second * 4),
				"v":  nil,
			},
		}
		if !resultMapEqual(result2, expectR2) {
			t.Errorf("aggregate query expect %v got %v", result2, expectR2)
			return
		}
	})
}

func resultMapEqual(m1, m2 []map[string]any) bool {
	if len(m1) != len(m2) {
		return false
	}
	for i := range m1 {
		if len(m1[i]) != len(m2[i]) {
			return false
		}

	}
	for i, m := range m1 {
		for s, v := range m {
			_, ok := m2[i][s].(time.Time)
			if ok {
				continue
			}
			if m2[i][s] != v {
				return false
			}
		}
	}
	return true
}
