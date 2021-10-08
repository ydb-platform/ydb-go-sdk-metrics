package metrics

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	DriverClusterEvents = 1 << iota
	DriverConnEvents
	DriverCredentialsEvents
	DriverDiscoveryEvents
)

// Driver makes Driver with metrics publishing
func Driver(c Config) trace.Driver {
	t := trace.Driver{}
	gauge, name, errName := parseConfig(c, DriverName)
	if c.Details()&DriverConnEvents != 0 {
		t.OnConnNew = func(info trace.ConnNewInfo) {
			gauge(
				name(DriverNameConn),
				name(NameBalance),
			).Inc()
			gauge(
				name(DriverNameConn),
				name(NameStatus),
				Name(info.State.String()),
			).Inc()
			gauge(
				name(DriverNameConn),
				Name(info.Endpoint.Address()),
				name(NameLocal),
			).Set(func() float64 {
				if info.Endpoint.LocalDC() {
					return 1
				}
				return 0
			}())
		}
		t.OnConnClose = func(info trace.ConnCloseInfo) {
			gauge(
				name(DriverNameConn),
				name(NameBalance),
			).Dec()
			gauge(
				name(DriverNameConn),
				name(NameStatus),
				Name(info.State.String()),
			).Dec()
		}
		t.OnConnStateChange = func(info trace.ConnStateChangeStartInfo) func(trace.ConnStateChangeDoneInfo) {
			gauge(
				name(DriverNameConn),
				name(NameStatus),
				Name(info.State.String()),
			).Dec()
			endpoint := info.Endpoint.Address()
			return func(info trace.ConnStateChangeDoneInfo) {
				gauge(
					name(DriverNameConn),
					name(NameStatus),
					Name(info.State.String()),
				).Inc()
				gauge(
					name(DriverNameConn),
					Name(endpoint),
					name(NameStatus),
				).Set(float64(info.State.Code()))
			}
		}
		t.OnConnDial = func(info trace.ConnDialStartInfo) func(trace.ConnDialDoneInfo) {
			endpoint := info.Endpoint
			start := time.Now()
			return func(info trace.ConnDialDoneInfo) {
				if info.Error != nil {
					gauge(
						name(DriverNameConn),
						Name(endpoint.Address()),
						name(NameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(DriverNameConn),
						name(NameInFlight),
					).Inc()
					gauge(
						name(DriverNameConn),
						name(DriverNameConnDial),
						name(NameTotal),
					).Inc()
					gauge(
						name(DriverNameConn),
						name(DriverNameConnDial),
						name(NameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
					gauge(
						name(DriverNameConn),
						name(DriverNameConnDial),
						Name(endpoint.Address()),
						name(NameTotal),
					).Inc()
					gauge(
						name(DriverNameConn),
						name(DriverNameConnDial),
						Name(endpoint.Address()),
						name(NameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
				}
			}
		}
		t.OnConnDisconnect = func(info trace.ConnDisconnectStartInfo) func(trace.ConnDisconnectDoneInfo) {
			return func(info trace.ConnDisconnectDoneInfo) {
				gauge(
					name(DriverNameConn),
					name(NameInFlight),
				).Dec()
			}
		}
		t.OnConnInvoke = func(info trace.ConnInvokeStartInfo) func(trace.ConnInvokeDoneInfo) {
			gauge(
				name(DriverNameConn),
				name(DriverNameConnInvoke),
				name(NameTotal),
			).Inc()
			method := info.Method.Name()
			gauge(
				name(DriverNameConn),
				name(DriverNameConnInvoke),
				Name(method),
				name(NameTotal),
			).Inc()
			endpoint := info.Endpoint
			start := time.Now()
			return func(info trace.ConnInvokeDoneInfo) {
				if info.Error != nil {
					gauge(
						name(DriverNameConn),
						name(DriverNameConnInvoke),
						Name(method),
						name(NameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(DriverNameConn),
						name(DriverNameConnInvoke),
						Name(method),
						name(NameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
					gauge(
						name(DriverNameConn),
						name(DriverNameConnInvoke),
						Name(endpoint.Address()),
						name(NameTotal),
					).Inc()
					gauge(
						name(DriverNameConn),
						name(DriverNameConnInvoke),
						Name(endpoint.Address()),
						name(NameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
					gauge(
						name(DriverNameConn),
						name(DriverNameConnInvoke),
						Name(endpoint.Address()),
						Name(method),
						name(NameTotal),
					).Inc()
					gauge(
						name(DriverNameConn),
						name(DriverNameConnInvoke),
						Name(endpoint.Address()),
						Name(method),
						name(NameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
				}
			}
		}
		t.OnConnNewStream = func(info trace.ConnNewStreamStartInfo) func(trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
			gauge(
				name(DriverNameConn),
				name(DriverNameConnStream),
				name(NameTotal),
			).Inc()
			method := info.Method.Name()
			gauge(
				name(DriverNameConn),
				name(DriverNameConnStream),
				Name(method),
				name(NameTotal),
			).Inc()
			endpoint := info.Endpoint
			start := time.Now()
			counter := 0
			return func(info trace.ConnNewStreamRecvInfo) func(trace.ConnNewStreamDoneInfo) {
				counter++
				gauge(
					name(DriverNameConn),
					name(DriverNameConnStream),
					name(DriverNameConnStreamRecv),
					Name(method),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverNameConn),
						name(DriverNameConnStream),
						name(DriverNameConnStreamRecv),
						Name(method),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
				return func(info trace.ConnNewStreamDoneInfo) {
					gauge(
						name(DriverNameConn),
						name(DriverNameConnStream),
						Name(method),
						name(NameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
					gauge(
						name(DriverNameConn),
						name(DriverNameConnStream),
						name(DriverNameConnStreamRecv),
						Name(method),
						name(NameSet),
						name(NameTotal),
					).Set(float64(counter))
					gauge(
						name(DriverNameConn),
						name(DriverNameConnStream),
						Name(endpoint.Address()),
						name(NameTotal),
					).Inc()
					gauge(
						name(DriverNameConn),
						name(DriverNameConnStream),
						Name(endpoint.Address()),
						name(NameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
					gauge(
						name(DriverNameConn),
						name(DriverNameConnStream),
						Name(endpoint.Address()),
						Name(method),
						name(NameTotal),
					).Inc()
					gauge(
						name(DriverNameConn),
						name(DriverNameConnStream),
						Name(endpoint.Address()),
						Name(method),
						name(NameLatency),
					).Set(float64(time.Since(start).Microseconds()) / 1000.)
					if info.Error != nil {
						gauge(
							name(DriverNameConn),
							name(DriverNameConnStream),
							Name(method),
							name(NameError),
							errName(info.Error),
						).Inc()
					}
				}
			}
		}
	}
	if c.Details()&DriverDiscoveryEvents != 0 {
		t.OnDiscovery = func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
			start := time.Now()
			return func(info trace.DiscoveryDoneInfo) {
				gauge(
					name(DriverNameDiscovery),
					name(NameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
				gauge(
					name(DriverNameDiscovery),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverNameDiscovery),
						name(NameError),
						errName(info.Error),
					).Inc()
				} else {
					gauge(
						name(DriverNameDiscovery),
						name(DriverNameDiscoveryEndpoints),
					).Set(float64(len(info.Endpoints)))
				}
			}
		}
	}
	if c.Details()&DriverClusterEvents != 0 {
		t.OnClusterGet = func(info trace.ClusterGetStartInfo) func(trace.ClusterGetDoneInfo) {
			return func(info trace.ClusterGetDoneInfo) {
				gauge(
					name(DriverNameCluster),
					name(DriverNameGet),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverNameCluster),
						name(DriverNameGet),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnClusterInsert = func(info trace.ClusterInsertStartInfo) func(trace.ClusterInsertDoneInfo) {
			return func(info trace.ClusterInsertDoneInfo) {
				gauge(
					name(DriverNameCluster),
					name(NameBalance),
				).Inc()
				gauge(
					name(DriverNameCluster),
					name(DriverNameInsert),
					name(NameTotal),
				).Inc()
			}
		}
		t.OnClusterRemove = func(info trace.ClusterRemoveStartInfo) func(trace.ClusterRemoveDoneInfo) {
			return func(info trace.ClusterRemoveDoneInfo) {
				gauge(
					name(DriverNameCluster),
					name(NameBalance),
				).Dec()
				gauge(
					name(DriverNameCluster),
					name(DriverNameRemove),
					name(NameTotal),
				).Inc()
			}
		}
		t.OnClusterUpdate = func(info trace.ClusterUpdateStartInfo) func(trace.ClusterUpdateDoneInfo) {
			return func(info trace.ClusterUpdateDoneInfo) {
				gauge(
					name(DriverNameCluster),
					name(DriverNameUpdate),
					name(NameTotal),
				).Inc()
			}
		}
		t.OnPessimizeNode = func(info trace.PessimizeNodeStartInfo) func(trace.PessimizeNodeDoneInfo) {
			return func(info trace.PessimizeNodeDoneInfo) {
				gauge(
					name(DriverNameCluster),
					name(DriverNamePessimize),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(DriverNameCluster),
						name(DriverNamePessimize),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&DriverCredentialsEvents != 0 {
		t.OnGetCredentials = func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
			gauge(
				name(DriverNameGetCredentials),
				name(NameTotal),
			).Inc()
			start := time.Now()
			return func(info trace.GetCredentialsDoneInfo) {
				gauge(
					name(DriverNameGetCredentials),
					name(NameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.)
				if info.Error != nil {
					gauge(
						name(DriverNameGetCredentials),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}

	//return trace.Driver{
	//	OnPessimization: func(info trace.PessimizationStartInfo) func(trace.PessimizationDoneInfo) {
	//		start := time.Now()
	//		before := info.State
	//		return func(info trace.PessimizationDoneInfo) {
	//			gauge(
	//				name(DriverNamePessimize),
	//				name(NameLatency),
	//			).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverNamePessimize),
	//					name(NameError),
	//					errName(info.Error),
	//				).Inc()
	//			} else {
	//				gauge(
	//					name(DriverNamePessimize),
	//					name(NameTotal),
	//				).Inc()
	//			}
	//			if before != info.State {
	//				gauge(
	//					name(DriverNameCluster),
	//					Name(before.String()),
	//				).Dec()
	//				gauge(
	//					name(DriverNameCluster),
	//					Name(info.State.String()),
	//				).Inc()
	//			}
	//		}
	//	},
	//	OnGetCredentials: func(info trace.GetCredentialsStartInfo) func(trace.GetCredentialsDoneInfo) {
	//		start := time.Now()
	//		return func(info trace.GetCredentialsDoneInfo) {
	//			gauge(
	//				name(DriverNameGetCredentials),
	//				name(NameLatency),
	//			).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//			gauge(
	//				name(DriverNameGetCredentials),
	//				name(NameTotal),
	//			).Inc()
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverNameGetCredentials),
	//					name(NameError),
	//					errName(info.Error),
	//				).Inc()
	//			}
	//		}
	//	},
	//	OnDiscovery: func(info trace.DiscoveryStartInfo) func(trace.DiscoveryDoneInfo) {
	//		start := time.Now()
	//		return func(info trace.DiscoveryDoneInfo) {
	//			gauge(
	//				name(DriverNameDiscovery),
	//				name(NameLatency),
	//			).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//			gauge(
	//				name(DriverNameDiscovery),
	//				name(NameTotal),
	//			).Inc()
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverNameDiscovery),
	//					name(NameError),
	//					errName(info.Error),
	//				).Inc()
	//			} else {
	//				gauge(
	//					name(DriverNameCluster),
	//					name(NameTotal),
	//				).Set(float64(len(info.Endpoints)))
	//				statesMtx.Lock()
	//				for state := range states {
	//					states[state] = 0
	//					gauge(
	//						name(DriverNameCluster),
	//						Name(state),
	//					).Set(0)
	//				}
	//				statesMtx.Unlock()
	//				for endpoint, state := range info.Endpoints {
	//					statesMtx.Lock()
	//					states[state.String()] += 1
	//					statesMtx.Unlock()
	//					gauge(
	//						name(DriverNameCluster),
	//						Name(state.String()),
	//					).Inc()
	//					gauge(
	//						name(DriverNameCluster),
	//						Name(endpoint.String()),
	//					).Set(float64(state.Code()))
	//				}
	//			}
	//		}
	//	},
	//	OnOperation: func(info trace.OperationStartInfo) func(trace.OperationDoneInfo) {
	//		start := time.Now()
	//		method := Name(strings.TrimLeft(string(info.Method), "/"))
	//		gauge(
	//			name(DriverNameOperation),
	//			method,
	//			name(NameInFlight),
	//		).Inc()
	//		return func(info trace.OperationDoneInfo) {
	//			gauge(
	//				name(DriverNameOperation),
	//				method,
	//				name(NameLatency),
	//			).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//			gauge(
	//				name(DriverNameOperation),
	//				method,
	//				name(NameTotal),
	//			).Inc()
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverNameOperation),
	//					method,
	//					name(NameError),
	//					errName(info.Error),
	//				).Inc()
	//			}
	//			gauge(
	//				name(DriverNameOperation),
	//				method,
	//				name(NameInFlight),
	//			).Dec()
	//		}
	//	},
	//	OnStream: func(info trace.StreamStartInfo) func(info trace.StreamRecvDoneInfo) func(info trace.StreamDoneInfo) {
	//		start := time.Now()
	//		gauge(
	//			name(DriverNameOperation),
	//			name(DriverNameStream),
	//			name(NameInFlight),
	//		).Inc()
	//		return func(info trace.StreamRecvDoneInfo) func(info trace.StreamDoneInfo) {
	//			gauge(
	//				name(DriverNameStreamRecv),
	//				name(NameTotal),
	//			).Inc()
	//			if info.Error != nil {
	//				gauge(
	//					name(DriverNameStreamRecv),
	//					name(NameError),
	//					errName(info.Error),
	//				).Inc()
	//			}
	//			return func(info trace.StreamDoneInfo) {
	//				gauge(
	//					name(DriverNameStream),
	//					name(NameLatency),
	//				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
	//				gauge(
	//					name(DriverNameStream),
	//					name(NameTotal),
	//				).Inc()
	//				if info.Error != nil {
	//					gauge(
	//						name(DriverNameStream),
	//						name(NameError),
	//						errName(info.Error),
	//					).Inc()
	//				}
	//				gauge(
	//					name(DriverNameOperation),
	//					name(DriverNameStream),
	//					name(NameInFlight),
	//				).Dec()
	//			}
	//		}
	//	},
	//}
	return t
}
