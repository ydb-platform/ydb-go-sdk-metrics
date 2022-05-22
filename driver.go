package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/str"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
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
		t.OnNetRead = func(info trace.DriverNetReadStartInfo) func(trace.DriverNetReadDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := read.Start(address)
			return func(info trace.DriverNetReadDoneInfo) {
				start.SyncWithValue(info.Error, float64(info.Received), address)
			}
		}
		t.OnNetWrite = func(info trace.DriverNetWriteStartInfo) func(trace.DriverNetWriteDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := write.Start(address)
			return func(info trace.DriverNetWriteDoneInfo) {
				start.SyncWithValue(info.Error, float64(info.Sent), address)
			}
		}
		t.OnNetDial = func(info trace.DriverNetDialStartInfo) func(trace.DriverNetDialDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := dial.Start(address)
			return func(info trace.DriverNetDialDoneInfo) {
				start.Sync(info.Error, address)
			}
		}
		t.OnNetClose = func(info trace.DriverNetCloseStartInfo) func(trace.DriverNetCloseDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := close.Start(address)
			return func(info trace.DriverNetCloseDoneInfo) {
				start.Sync(info.Error, address)
			}
		}
	}
	if c.Details()&trace.DriverRepeaterEvents != 0 {
		repeater := scope.New(c, "repeater", config.New(), labels.TagMethod, labels.TagName)
		t.OnRepeaterWakeUp = func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			name := labels.Label{
				Tag:   labels.TagName,
				Value: info.Name,
			}
			event := labels.Label{
				Tag:   labels.TagMethod,
				Value: info.Event,
			}
			start := repeater.Start(name, event)
			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				start.Sync(info.Error, name, event)
			}
		}
	}
	if c.Details()&trace.DriverConnEvents != 0 {
		c := c.WithSystem("conn")
		take := scope.New(c, "take", config.New(), labels.TagAddress)
		invoke := scope.New(c, "invoke", config.New(), labels.TagAddress, labels.TagMethod)
		stream := scope.New(c, "stream", config.New(), labels.TagAddress, labels.TagMethod, labels.TagStage)
		states := scope.New(c, "state", config.New(), labels.TagAddress, labels.TagState)
		park := scope.New(c, "park", config.New(), labels.TagAddress)
		close := scope.New(c, "close", config.New(), labels.TagAddress)
		t.OnConnTake = func(info trace.DriverConnTakeStartInfo) func(trace.DriverConnTakeDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			start := take.Start(address)
			return func(info trace.DriverConnTakeDoneInfo) {
				start.Sync(info.Error, address)
			}
		}
		t.OnConnStateChange = func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			start := states.Start(address, labels.Label{
				Tag:   labels.TagState,
				Value: info.State.String(),
			})
			return func(info trace.DriverConnStateChangeDoneInfo) {
				start.Sync(nil, address, labels.Label{
					Tag:   labels.TagState,
					Value: info.State.String(),
				})
			}
		}
		t.OnConnInvoke = func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			method := labels.Label{
				Tag:   labels.TagMethod,
				Value: string(info.Method),
			}
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			start := invoke.Start(address, method)
			return func(info trace.DriverConnInvokeDoneInfo) {
				start.Sync(info.Error, address, method)
			}
		}
		t.OnConnNewStream = func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
			method := labels.Label{
				Tag:   labels.TagMethod,
				Value: string(info.Method),
			}
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			start := stream.Start(address, method, labels.Label{
				Tag:   labels.TagStage,
				Value: "init",
			})
			return func(info trace.DriverConnNewStreamRecvInfo) func(trace.DriverConnNewStreamDoneInfo) {
				start.Sync(info.Error, address, method, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.DriverConnNewStreamDoneInfo) {
					start.Sync(info.Error, address, method, labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
		t.OnConnPark = func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			start := park.Start(address)
			return func(info trace.DriverConnParkDoneInfo) {
				start.Sync(info.Error, address)
			}
		}
		t.OnConnClose = func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Endpoint.Address(),
			}
			start := close.Start(address)
			return func(info trace.DriverConnCloseDoneInfo) {
				start.Sync(info.Error, address)
			}
		}
	}
	if c.Details()&trace.DriverBalancerEvents != 0 {
		c := c.WithSystem("balancer")
		init := scope.New(c, "init", config.New(
			config.WithoutCalls(),
			config.WithoutError(),
		))
		close := scope.New(c, "close", config.New(
			config.WithoutCalls(),
		))
		update := scope.New(c, "update",
			config.New(
				config.WithValue(config.ValueTypeGauge),
			),
			labels.TagDataCenter,
		)
		choose := scope.New(c, "chooseEndpoint", config.New(), labels.TagAddress, labels.TagDataCenter)
		t.OnBalancerInit = func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			start := init.Start()
			return func(info trace.DriverBalancerInitDoneInfo) {
				start.Sync(nil)
			}
		}
		t.OnBalancerClose = func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
			start := close.Start()
			return func(info trace.DriverBalancerCloseDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnBalancerUpdate = func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
			start := update.Start(
				labels.Label{
					Tag:   labels.TagDataCenter,
					Value: "wip",
				},
			)
			return func(info trace.DriverBalancerUpdateDoneInfo) {
				start.SyncWithValue(info.Error, float64(len(info.Endpoints)),
					labels.Label{
						Tag:   labels.TagDataCenter,
						Value: info.LocalDC,
					},
				)
			}
		}
		t.OnBalancerChooseEndpoint = func(info trace.DriverBalancerChooseEndpointStartInfo) func(trace.DriverBalancerChooseEndpointDoneInfo) {
			start := choose.Start(
				labels.Label{
					Tag:   labels.TagAddress,
					Value: "wip",
				},
				labels.Label{
					Tag:   labels.TagDataCenter,
					Value: "wip",
				},
			)
			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error == nil {
					start.Sync(
						nil,
						labels.Label{
							Tag:   labels.TagAddress,
							Value: info.Endpoint.Address(),
						},
						labels.Label{
							Tag:   labels.TagDataCenter,
							Value: str.If(info.Endpoint.LocalDC(), "local", "remote"),
						},
					)
				} else {
					start.Sync(
						nil,
						labels.Label{
							Tag: labels.TagAddress,
						},
						labels.Label{
							Tag: labels.TagDataCenter,
						},
					)
				}
			}
		}
	}
	if c.Details()&trace.DriverCredentialsEvents != 0 {
		c := c.WithSystem("credentials")
		get := scope.New(c, "get", config.New())
		t.OnGetCredentials = func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			start := get.Start()
			return func(info trace.DriverGetCredentialsDoneInfo) {
				start.Sync(info.Error)
			}
		}
	}
	return t
}
