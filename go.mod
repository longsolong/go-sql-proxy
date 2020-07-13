module github.com/longsolong/go-sql-proxy

go 1.14

require (
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/siddontang/go-mysql v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.6.0
	google.golang.org/appengine v1.6.6 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
)

replace github.com/siddontang/go-mysql => github.com/longsolong/go-mysql v0.0.0-20200713105929-e1404cec05da
