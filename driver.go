package metrics

import (
	"runtime"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes Driver with metrics publishing
func Driver(c Config) trace.Driver {
	if c.Details()&trace.DriverSystemEvents != 0 {
		c := c.WithSystem("system")
		goroutines := c.GaugeVec("goroutines", TagVersion)
		memory := c.GaugeVec("memory", TagVersion)
		uptime := c.GaugeVec("uptime", TagVersion)
		go func() {
			var stats runtime.MemStats
			start := time.Now()
			for {
				time.Sleep(time.Second)
				uptime.With(labelsToKeyValue(version)).Set(time.Since(start).Seconds())
				goroutines.With(labelsToKeyValue(version)).Set(float64(runtime.NumGoroutine()))
				runtime.ReadMemStats(&stats)
				memory.With(labelsToKeyValue(version)).Set(float64(stats.Alloc))
			}
		}()
	}
	c = c.WithSystem("driver")
	t := trace.Driver{}
	if c.Details()&trace.DriverNetEvents != 0 {
		c := c.WithSystem("net")
		read := metrics(c, "read", TagAddress)
		write := metrics(c, "write", TagAddress)
		dial := metrics(c, "dial", TagAddress)
		close := metrics(c, "close", TagAddress)
		t.OnNetRead = func(info trace.NetReadStartInfo) func(trace.NetReadDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			start := read.start(address)
			return func(info trace.NetReadDoneInfo) {
				start.syncWithValue(info.Error, float64(info.Received), address)
			}
		}
		t.OnNetWrite = func(info trace.NetWriteStartInfo) func(trace.NetWriteDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			start := write.start(address)
			return func(info trace.NetWriteDoneInfo) {
				start.syncWithValue(info.Error, float64(info.Sent), address)
			}
		}
		t.OnNetDial = func(info trace.NetDialStartInfo) func(trace.NetDialDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			start := dial.start(address)
			return func(info trace.NetDialDoneInfo) {
				lables, _ := start.sync(info.Error, address)
				// publish empty close call metric for register metrics on metrics storage
				close.calls.With(labelsToKeyValue(lables...)).Add(0)
			}
		}
		t.OnNetClose = func(info trace.NetCloseStartInfo) func(trace.NetCloseDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			start := close.start(address)
			return func(info trace.NetCloseDoneInfo) {
				start.sync(info.Error, address)
			}
		}
	}
	if c.Details()&trace.DriverCoreEvents != 0 {
		c := c.WithSystem("core")
		take := metrics(c, "take", TagAddress, TagDataCenter)
		release := metrics(c, "release", TagAddress, TagDataCenter)
		states := metrics(c, "state", TagAddress, TagDataCenter, TagState)
		invoke := metrics(c, "invoke", TagAddress, TagDataCenter, TagMethod)
		stream := metrics(c, "stream", TagAddress, TagDataCenter, TagMethod, TagStage)
		t.OnConnTake = func(info trace.ConnTakeStartInfo) func(trace.ConnTakeDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := take.start(address, dataCenter)
			return func(info trace.ConnTakeDoneInfo) {
				start.sync(info.Error, address, dataCenter)
			}
		}
		t.OnConnRelease = func(info trace.ConnReleaseStartInfo) func(trace.ConnReleaseDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := release.start(address, dataCenter)
			return func(info trace.ConnReleaseDoneInfo) {
				start.sync(nil, address, dataCenter)
			}
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := states.start(address, dataCenter, Label{
				Tag:   TagState,
				Value: info.State.String(),
			})
			return func(info trace.ConnStateChangeDoneInfo) {
				start.sync(nil, address, dataCenter, Label{
					Tag:   TagState,
					Value: info.State.String(),
				})
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			method := Label{
				Tag:   TagMethod,
				Value: string(info.Method),
			}
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := invoke.start(address, dataCenter, method)
			return func(info trace.ConnInvokeDoneInfo) {
				start.sync(info.Error, address, dataCenter, method)
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			method := Label{
				Tag:   TagMethod,
				Value: string(info.Method),
			}
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := stream.start(address, dataCenter, method, Label{
				Tag:   TagStage,
				Value: "init",
			})
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				start.sync(info.Error, address, dataCenter, method, Label{
					Tag:   TagStage,
					Value: "intermediate",
				})
				return func(info trace.ConnNewStreamDoneInfo) {
					start.sync(info.Error, address, dataCenter, method, Label{
						Tag:   TagStage,
						Value: "finish",
					})
				}
			}
		}
	}
	if c.Details()&trace.DriverDiscoveryEvents != 0 {
		discovery := metrics(c, "discovery")
		t.OnDiscovery = func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			start := discovery.start()
			return func(info trace.DiscoveryDoneInfo) {
				start.syncWithValue(info.Error, float64(len(info.Endpoints)))
			}
		}
	}
	if c.Details()&trace.DriverClusterEvents != 0 {
		c := c.WithSystem("cluster")
		get := metrics(c, "get", TagAddress, TagDataCenter)
		insert := metrics(c, "insert", TagAddress, TagDataCenter)
		remove := metrics(c, "remove", TagAddress, TagDataCenter)
		update := metrics(c, "update", TagAddress, TagDataCenter)
		pessimize := metrics(c, "pessimize", TagAddress, TagDataCenter)
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			start := get.start(
				Label{
					Tag:   TagAddress,
					Value: "wip",
				},
				Label{
					Tag:   TagDataCenter,
					Value: "wip",
				},
			)
			return func(info trace.ClusterGetDoneInfo) {
				start.sync(
					info.Error,
					Label{
						Tag:   TagAddress,
						Value: info.Endpoint.Address(),
					},
					Label{
						Tag:   TagDataCenter,
						Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
					},
				)
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := insert.start(address, dataCenter)
			return func(info trace.ClusterInsertDoneInfo) {
				start.syncWithValue(nil, float64(info.State.Code()), address, dataCenter)
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := remove.start(address, dataCenter)
			return func(info trace.ClusterRemoveDoneInfo) {
				start.syncWithValue(nil, float64(info.State.Code()), address, dataCenter)
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := update.start(address, dataCenter)
			return func(info trace.ClusterUpdateDoneInfo) {
				start.syncWithValue(nil, float64(info.State.Code()), address, dataCenter)
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Endpoint.Address(),
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: ifStr(info.Endpoint.LocalDC(), "local", "remote"),
			}
			start := pessimize.start(address, dataCenter)
			return func(info trace.PessimizeNodeDoneInfo) {
				// sync cause instead pessimize result error
				start.sync(nil, address, dataCenter)
			}
		}
	}
	if c.Details()&trace.DriverCredentialsEvents != 0 {
		c := c.WithSystem("credentials")
		get := metrics(c, "get")
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			start := get.start()
			return func(info trace.GetCredentialsDoneInfo) {
				start.syncWithSuccess(info.TokenOk)
			}
		}
	}
	return t
}
