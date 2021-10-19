package sensors

import (
	"runtime"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes Driver with metrics publishing
func Driver(c Config) trace.Driver {
	if c.Details()&DriverSystemEvents != 0 {
		c := c.WithSystem("system")
		goroutines := c.GaugeVec("goroutines", "active goroutines", TagVersion)
		memory := c.GaugeVec("memory", "memory in bytes", TagVersion)
		uptime := c.GaugeVec("uptime", "total uptime in seconds", TagVersion)
		go func() {
			var stats runtime.MemStats
			start := time.Now()
			for {
				time.Sleep(time.Second)
				uptime.With(version).Set(time.Since(start).Seconds())
				goroutines.With(version).Set(float64(runtime.NumGoroutine()))
				runtime.ReadMemStats(&stats)
				memory.With(version).Set(float64(stats.Alloc))
			}
		}()
	}
	c = c.WithSystem("driver")
	t := trace.Driver{}
	errs := c.GaugeVec("errors", "error", TagAddress, TagDataCenter, TagCall, TagMethod, TagState, TagError, TagErrCode, TagVersion)
	latency := c.GaugeVec("latency", "latency of call", TagAddress, TagDataCenter, TagCall, TagMethod, TagSuccess, TagVersion)
	invoke := c.GaugeVec("invoke", "invoke calls in flight", TagSuccess, TagMethod, TagVersion)
	newStream := c.GaugeVec("new_stream", "new_stream calls in flight", TagSuccess, TagMethod, TagVersion)
	if c.Details()&DriverConnEvents != 0 {
		c := c.WithSystem("conn")
		created := c.GaugeVec("created", "balance between created and closed connection wrappers", TagAddress, TagDataCenter, TagVersion)
		states := c.GaugeVec("states", "states of connection", TagAddress, TagDataCenter, TagState, TagVersion)
		connected := c.GaugeVec("connected", "actual connections", TagAddress, TagDataCenter, TagCall, TagMethod, TagVersion)
		inflight := c.GaugeVec("inflight", "connections in flight", TagAddress, TagDataCenter, TagVersion)
		receives := c.GaugeVec("stream_receives", "number of receives per single stream", TagAddress, TagDataCenter, TagCall, TagMethod, TagSuccess, TagVersion)
		t.OnConnTake = func(info trace.ConnTakeStartInfo) func(trace.ConnTakeDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			return func(info trace.ConnTakeDoneInfo) {
				if info.Error == nil {
					inflight.With(
						version,
						address,
						dataCenter,
					).Add(1)
				}
			}
		}
		t.OnConnRelease = func(info trace.ConnReleaseStartInfo) func(trace.ConnReleaseDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			return func(info trace.ConnReleaseDoneInfo) {
				inflight.With(
					version,
					address,
					dataCenter,
				).Add(-1)
			}
		}
		t.OnConnNew = func(info trace.ConnNewStartInfo) func(trace.ConnNewDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			return func(info trace.ConnNewDoneInfo) {
				created.With(version, address, dataCenter).Add(1)
				states.With(version, address, dataCenter, Label{
					Tag:   TagState,
					Value: info.State.String(),
				}).Add(1)
			}
		}
		t.OnConnClose = func(info trace.ConnCloseStartInfo) func(trace.ConnCloseDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			states.With(version, address, dataCenter, Label{
				Tag:   TagState,
				Value: info.State.String(),
			}).Add(-1)
			return func(info trace.ConnCloseDoneInfo) {
				created.With(version, address, dataCenter).Add(-1)
			}
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			states.With(version, address, dataCenter, Label{
				Tag:   TagState,
				Value: info.State.String(),
			}).Add(-1)
			return func(info trace.ConnStateChangeDoneInfo) {
				states.With(version, address, dataCenter, Label{
					Tag:   TagState,
					Value: info.State.String(),
				}).Add(1)
			}
		}
		t.OnConnDial = func(info trace.ConnDialStartInfo) func(trace.ConnDialDoneInfo) {
			callLabel := Label{
				Tag:   TagCall,
				Value: "dial",
			}
			methodLabel := Label{
				Tag: TagMethod,
			}
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			start := time.Now()
			return func(info trace.ConnDialDoneInfo) {
				latency.With(
					version,
					address,
					dataCenter,
					callLabel,
					methodLabel,
					Label{
						Tag: TagSuccess,
						Value: func() string {
							if info.Error == nil {
								return "true"
							}
							return "false"
						}(),
					},
				).Set(float64(time.Since(start).Nanoseconds()))
				if info.Error != nil {
					errs.With(
						err(
							info.Error,
							version,
							address,
							dataCenter,
							callLabel,
							methodLabel,
							Label{
								Tag: TagState,
							},
						)...,
					).Add(1)
				} else {
					connected.With(version, address, dataCenter, callLabel, methodLabel).Add(1)
				}
			}
		}
		t.OnConnDisconnect = func(info trace.ConnDisconnectStartInfo) func(trace.ConnDisconnectDoneInfo) {
			callLabel := Label{
				Tag:   TagCall,
				Value: "disconnect",
			}
			methodLabel := Label{
				Tag: TagMethod,
			}
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			start := time.Now()
			return func(info trace.ConnDisconnectDoneInfo) {
				latency.With(
					version,
					address,
					dataCenter,
					callLabel,
					methodLabel,
					Label{
						Tag: TagSuccess,
						Value: func() string {
							if info.Error == nil {
								return "true"
							}
							return "false"
						}(),
					},
				).Set(float64(time.Since(start).Nanoseconds()))
				if info.Error != nil {
					errs.With(
						err(
							info.Error,
							version,
							address,
							dataCenter,
							callLabel,
							methodLabel,
							Label{
								Tag:   TagState,
								Value: info.State.String(),
							},
						)...,
					).Add(1)
				} else {
					connected.With(version, address, dataCenter, callLabel, methodLabel).Add(-1)
				}
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			callLabel := Label{
				Tag:   TagCall,
				Value: "invoke",
			}
			methodLabel := Label{
				Tag:   TagMethod,
				Value: string(info.Method),
			}
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			invoke.With(version, Label{Tag: TagSuccess, Value: "wip"}, methodLabel).Add(1)
			start := time.Now()
			return func(info trace.ConnInvokeDoneInfo) {
				success := Label{
					Tag: TagSuccess,
					Value: func() string {
						if info.Error == nil {
							return "true"
						}
						return "false"
					}(),
				}
				invoke.With(version, success, methodLabel).Add(-1)
				latency.With(
					version,
					address,
					dataCenter,
					callLabel,
					methodLabel,
					success,
				).Set(float64(time.Since(start).Nanoseconds()))
				if info.Error != nil {
					errs.With(
						err(
							info.Error,
							version,
							address,
							dataCenter,
							callLabel,
							methodLabel,
							Label{
								Tag:   TagState,
								Value: info.State.String(),
							},
						)...,
					).Add(1)
				}
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			callLabel := Label{
				Tag:   TagCall,
				Value: "new_stream",
			}
			methodLabel := Label{
				Tag:   TagMethod,
				Value: string(info.Method),
			}
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			newStream.With(version, Label{Tag: TagSuccess, Value: "wip"}, methodLabel).Add(1)
			start := time.Now()
			received := 0.0
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				received++
				return func(info trace.ConnNewStreamDoneInfo) {
					success := Label{
						Tag: TagSuccess,
						Value: func() string {
							if info.Error == nil {
								return "true"
							}
							return "false"
						}(),
					}
					newStream.With(version, success, methodLabel).Add(-1)
					receives.With(
						version,
						address,
						dataCenter,
						callLabel,
						methodLabel,
						success,
					).Set(received)
					latency.With(
						version,
						address,
						dataCenter, callLabel,
						methodLabel,
						success,
					).Set(float64(time.Since(start).Nanoseconds()))
					if info.Error != nil {
						errs.With(
							err(
								info.Error,
								version,
								address,
								dataCenter,
								callLabel,
								methodLabel,
								Label{
									Tag:   TagState,
									Value: info.State.String(),
								},
							)...,
						).Add(1)
					}
				}
			}
		}
	}
	if c.Details()&DriverDiscoveryEvents != 0 {
		c := c.WithSystem("discovery")
		requests := c.GaugeVec("requests", "discovery requests counter", TagVersion)
		endpoints := c.GaugeVec("endpoints", "endpoints count received on discovery call", TagVersion)
		latency := c.GaugeVec("latency", "latency of discovery call", TagSuccess, TagVersion)
		errs := c.GaugeVec("errors", "error", TagError, TagErrCode, TagVersion)
		t.OnDiscovery = func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			start := time.Now()
			return func(info trace.DiscoveryDoneInfo) {
				requests.With(version).Add(1)
				latency.With(
					version,
					Label{
						Tag: TagSuccess,
						Value: func() string {
							if info.Error == nil {
								return "true"
							}
							return "false"
						}(),
					},
				).Set(float64(time.Since(start).Nanoseconds()))
				if info.Error != nil {
					errs.With(err(info.Error, version)...).Add(1)
				} else {
					endpoints.With(version).Set(float64(len(info.Endpoints)))
				}
			}
		}
	}
	if c.Details()&DriverClusterEvents != 0 {
		c := c.WithSystem("cluster")
		endpoints := c.GaugeVec("endpoints", "balance between inserted and removed endpoints", TagAddress, TagDataCenter, TagVersion)
		pessimized := c.GaugeVec("pessimized", "pessimized endpoints counter", TagAddress, TagDataCenter, TagError, TagErrCode, TagSuccess, TagVersion)
		get := c.GaugeVec("get", "cluster get conn counter", TagAddress, TagDataCenter, TagSuccess, TagVersion)
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			start := time.Now()
			get.With(
				version,
				Label{
					Tag: TagAddress,
				},
				Label{
					Tag: TagDataCenter,
				},
				Label{
					Tag:   TagSuccess,
					Value: "wip",
				},
			).Add(1)
			return func(info trace.ClusterGetDoneInfo) {
				address := Label{
					Tag:   TagAddress,
					Value: info.Address,
				}
				dataCenter := Label{
					Tag:   TagDataCenter,
					Value: info.Location.String(),
				}
				latency.With(
					version,
					address,
					dataCenter,
					Label{
						Tag:   TagCall,
						Value: "get",
					},
					Label{
						Tag: TagMethod,
					},
					Label{
						Tag: TagSuccess,
						Value: func() string {
							if info.Error == nil {
								return "true"
							}
							return "false"
						}(),
					},
				).Set(float64(time.Since(start).Nanoseconds()))
				get.With(
					version,
					address,
					dataCenter,
					Label{
						Tag: TagSuccess,
						Value: func() string {
							if info.Error == nil {
								return "true"
							}
							return "false"
						}(),
					},
				).Add(-1)
				if info.Error != nil {
					errs.With(
						err(
							info.Error,
							version,
							address,
							dataCenter,
							Label{
								Tag:   TagCall,
								Value: "get",
							},
							Label{
								Tag: TagMethod,
							},
							Label{
								Tag: TagState,
							},
						)...,
					).Add(1)
				}
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			return func(info trace.ClusterInsertDoneInfo) {
				endpoints.With(version, address, dataCenter).Add(1)
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			return func(info trace.ClusterRemoveDoneInfo) {
				endpoints.With(version, address, dataCenter).Add(-1)
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			return func(info trace.ClusterUpdateDoneInfo) {
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			dataCenter := Label{
				Tag:   TagDataCenter,
				Value: info.Location.String(),
			}
			cause := info.Cause
			return func(info trace.PessimizeNodeDoneInfo) {
				pessimized.With(
					err(
						cause,
						version,
						address,
						dataCenter,
						Label{
							Tag: TagSuccess,
							Value: func() string {
								if info.Error == nil {
									return "true"
								}
								return "false"
							}(),
						},
					)...,
				).Add(1)
				if info.Error != nil {
					errs.With(
						err(
							info.Error,
							version,
							address,
							dataCenter,
							Label{
								Tag:   TagCall,
								Value: "pessimize",
							},
							Label{
								Tag: TagMethod,
							},
							Label{
								Tag:   TagState,
								Value: info.State.String(),
							},
						)...,
					).Add(1)
				}
			}
		}
	}
	if c.Details()&DriverCredentialsEvents != 0 {
		c := c.WithSystem("credentials")
		get := c.GaugeVec("get", "credentials get counter", TagSuccess, TagVersion)
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			start := time.Now()
			get.With(version, Label{Tag: TagSuccess, Value: "wip"}).Add(1)
			return func(info trace.GetCredentialsDoneInfo) {
				success := Label{
					Tag: TagSuccess,
					Value: func() string {
						if info.TokenOk {
							return "true"
						}
						return "false"
					}(),
				}

				latency.With(
					version,
					Label{
						Tag: TagAddress,
					},
					Label{
						Tag: TagDataCenter,
					},
					Label{
						Tag:   TagCall,
						Value: "get",
					},
					Label{
						Tag: TagMethod,
					},
					success,
				).Set(float64(time.Since(start).Nanoseconds()))
				get.With(version, success).Add(-1)
				if info.Error != nil {
					errs.With(
						err(
							info.Error,
							version,
							Label{
								Tag: TagAddress,
							},
							Label{
								Tag: TagDataCenter,
							},
							Label{
								Tag:   TagCall,
								Value: "credentials",
							},
							Label{
								Tag: TagMethod,
							},
							Label{
								Tag: TagState,
							},
						)...,
					).Add(1)
				}
			}
		}
	}
	return t
}
