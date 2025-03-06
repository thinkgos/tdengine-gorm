module github.com/thinkgos/tdengine-gorm

go 1.22

replace github.com/taosdata/driver-go/v3 v3.6.0 => github.com/thinkgos/driver-go/v3 v3.6.1-0.20250225142719-44d496748bef

require (
	github.com/taosdata/driver-go/v3 v3.6.0
	gorm.io/gorm v1.25.12
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	golang.org/x/text v0.14.0 // indirect
)
