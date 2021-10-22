module github.com/ydb-platform/ydb-go-sdk-metrics

go 1.16

require (
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0-release
	google.golang.org/grpc v1.38.0 // indirect
)

retract (
	v0.0.1
	v0.0.2
	v0.0.3
)
