module github.com/ydb-platform/ydb-go-sdk-sensors

go 1.16

require (
	github.com/ydb-platform/ydb-go-sdk/v3 v3.0.0-20211017214604-e10eb72a6ed6
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f // indirect
	golang.org/x/sys v0.0.0-20211015200801-69063c4bb744 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20211016002631-37fc39342514 // indirect
	google.golang.org/grpc v1.41.0 // indirect
)

//replace github.com/ydb-platform/ydb-go-sdk/v3 => ../ydb-go-sdk-private/
