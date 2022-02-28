package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/str"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	bytesBuckets = []float64{
		10,
		20,
		50,
		100,
		200,
		500,
		1000,
		2000,
		5000,
		10000,
		20000,
		50000,
		100000,
	}
)

// Driver makes Driver with New publishing
func Driver(c registry.Config) (t trace.Driver) {
	c = c.WithSystem("driver")
	if c.Details()&trace.DriverNetEvents != 0 {
		c := c.WithSystem("net")
		read := scope.New(c, "read", config.New(
			config.WithValue(config.ValueTypeHistogram),
			config.WithValueBuckets(bytesBuckets),
		), labels.TagAddress)
		write := scope.New(c, "write", config.New(
			config.WithValue(config.ValueTypeHistogram),
			config.WithValueBuckets(bytesBuckets),
		), labels.TagAddress)
		dial := scope.New(c, "dial", config.New(), labels.TagAddress)
		close := scope.New(c, "close", config.New(), labels.TagAddress)
		t.OnNetRead = func(info trace.NetReadStartInfo) func(trace.NetReadDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := read.Start(address)
			return func(info trace.NetReadDoneInfo) {
				start.SyncWithValue(info.Error, float64(info.Received), address)
			}
		}
		t.OnNetWrite = func(info trace.NetWriteStartInfo) func(trace.NetWriteDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := write.Start(address)
			return func(info trace.NetWriteDoneInfo) {
				start.SyncWithValue(info.Error, float64(info.Sent), address)
			}
		}
		t.OnNetDial = func(info trace.NetDialStartInfo) func(trace.NetDialDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := dial.Start(address)
			return func(info trace.NetDialDoneInfo) {
				start.Sync(info.Error, address)
			}
		}
		t.OnNetClose = func(info trace.NetCloseStartInfo) func(trace.NetCloseDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := close.Start(address)
			return func(info trace.NetCloseDoneInfo) {
				start.Sync(info.Error, address)
			}
		}
	}
	if c.Details()&trace.DriverCoreEvents != 0 {
		c := c.WithSystem("core")
		take := scope.New(c, "take", config.New(), labels.TagAddress, labels.TagDataCenter)
		usages := scope.New(c, "usages", config.New(
			config.WithoutCalls(),
			config.WithoutLatency(),
			config.WithoutError(),
			config.WithValue(config.ValueTypeGauge),
		), labels.TagAddress)
		states := scope.New(c, "state", config.New(), labels.TagAddress, labels.TagDataCenter, labels.TagState)
		invoke := scope.New(c, "invoke", config.New(), labels.TagAddress, labels.TagDataCenter, labels.TagMethod)
		stream := scope.New(c, "stream", config.New(), labels.TagAddress, labels.TagDataCenter, labels.TagMethod, labels.TagStage)
		t.OnConnTake = func(info trace.ConnTakeStartInfo) func(trace.ConnTakeDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := labels.Label{
				Tag:   labels.TagDataCenter,
				Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := take.Start(address, dataCenter)
			return func(info trace.ConnTakeDoneInfo) {
				start.Sync(info.Error, address, dataCenter)
			}
		}
		t.OnConnUsagesChange = func(info trace.ConnUsagesChangeInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			usages.Start(address).SyncValue(float64(info.Usages), address)
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := labels.Label{
				Tag:   labels.TagDataCenter,
				Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := states.Start(address, dataCenter, labels.Label{
				Tag:   labels.TagState,
				Value: info.State.String(),
			})
			return func(info trace.ConnStateChangeDoneInfo) {
				start.Sync(nil, address, dataCenter, labels.Label{
					Tag:   labels.TagState,
					Value: info.State.String(),
				})
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			method := labels.Label{
				Tag:   labels.TagMethod,
				Value: string(info.Method),
			}
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := labels.Label{
				Tag:   labels.TagDataCenter,
				Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := invoke.Start(address, dataCenter, method)
			return func(info trace.ConnInvokeDoneInfo) {
				start.Sync(info.Error, address, dataCenter, method)
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			method := labels.Label{
				Tag:   labels.TagMethod,
				Value: string(info.Method),
			}
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := labels.Label{
				Tag:   labels.TagDataCenter,
				Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := stream.Start(address, dataCenter, method, labels.Label{
				Tag:   labels.TagStage,
				Value: "init",
			})
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				start.Sync(info.Error, address, dataCenter, method, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.ConnNewStreamDoneInfo) {
					start.Sync(info.Error, address, dataCenter, method, labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
	}
	if c.Details()&trace.DriverClusterEvents != 0 {
		c := c.WithSystem("cluster")
		init := scope.New(c, "init", config.New(
			config.WithoutCalls(),
			config.WithoutError(),
		))
		close := scope.New(c, "close", config.New(
			config.WithoutCalls(),
		))
		get := scope.New(c, "get", config.New(), labels.TagAddress, labels.TagDataCenter)
		insert := scope.New(c, "insert", config.New(), labels.TagAddress, labels.TagDataCenter)
		remove := scope.New(c, "remove", config.New(), labels.TagAddress, labels.TagDataCenter)
		update := scope.New(c, "update", config.New(), labels.TagAddress, labels.TagDataCenter)
		pessimize := scope.New(c, "pessimize", config.New(), labels.TagAddress, labels.TagDataCenter)
		t.OnClusterInit = func(info trace.ClusterInitStartInfo) func(trace.ClusterInitDoneInfo) {
			start := init.Start()
			return func(info trace.ClusterInitDoneInfo) {
				start.Sync(nil)
			}
		}
		t.OnClusterClose = func(info trace.ClusterCloseStartInfo) func(trace.ClusterCloseDoneInfo) {
			start := close.Start()
			return func(info trace.ClusterCloseDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			start := get.Start(
				labels.Label{
					Tag:   labels.TagAddress,
					Value: "wip",
				},
				labels.Label{
					Tag:   labels.TagDataCenter,
					Value: "wip",
				},
			)
			return func(info trace.ClusterGetDoneInfo) {
				start.Sync(
					info.Error,
					labels.Label{
						Tag:   labels.TagAddress,
						Value: info.Endpoint.Address(),
					},
					labels.Label{
						Tag:   labels.TagDataCenter,
						Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
					},
				)
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := labels.Label{
				Tag:   labels.TagDataCenter,
				Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := insert.Start(address, dataCenter)
			return func(info trace.ClusterInsertDoneInfo) {
				start.SyncWithValue(nil, float64(info.State.Code()), address, dataCenter)
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := labels.Label{
				Tag:   labels.TagDataCenter,
				Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := remove.Start(address, dataCenter)
			return func(info trace.ClusterRemoveDoneInfo) {
				start.SyncWithValue(nil, float64(info.State.Code()), address, dataCenter)
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := labels.Label{
				Tag:   labels.TagDataCenter,
				Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := update.Start(address, dataCenter)
			return func(info trace.ClusterUpdateDoneInfo) {
				start.SyncWithValue(nil, float64(info.State.Code()), address, dataCenter)
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := labels.Label{
				Tag:   labels.TagDataCenter,
				Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := pessimize.Start(address, dataCenter)
			return func(info trace.PessimizeNodeDoneInfo) {
				// Sync cause instead pessimize result error
				start.Sync(nil, address, dataCenter)
			}
		}
	}
	if c.Details()&trace.DriverCredentialsEvents != 0 {
		c := c.WithSystem("credentials")
		get := scope.New(c, "get", config.New())
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			start := get.Start()
			return func(info trace.GetCredentialsDoneInfo) {
				start.Sync(info.Error)
			}
		}
	}
	return t
}
