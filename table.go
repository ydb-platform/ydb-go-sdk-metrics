package metrics

import (
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	TableSessionEvents = 1 << iota
	tableSessionQueryEvents
	tableSessionQueryStreamEvents
	tableSessionTransactionEvents
	tablePoolLifeCycleEvents
	tablePoolRetryEvents
	tablePoolSessionLifeCycleEvents
	tablePoolCommonAPIEvents
	tablePoolNativeAPIEvents
	tablePoolYdbSqlAPIEvents

	TableQueryEvents       = TableSessionEvents | tableSessionQueryEvents
	TableStreamEvents      = TableSessionEvents | tableSessionQueryEvents | tableSessionQueryStreamEvents
	TableTransactionEvents = TableSessionEvents | tableSessionTransactionEvents
	TablePoolEvents        = tablePoolLifeCycleEvents | tablePoolRetryEvents | tablePoolSessionLifeCycleEvents | tablePoolCommonAPIEvents | tablePoolNativeAPIEvents | tablePoolYdbSqlAPIEvents
)

type sessionsUse struct {
	m   sync.Mutex
	use map[string]int
}

func (s *sessionsUse) Inc(id string) {
	s.m.Lock()
	s.use[id]++
	s.m.Unlock()
}

func (s *sessionsUse) Pop(id string) int {
	s.m.Lock()
	use := s.use[id]
	delete(s.use, id)
	s.m.Unlock()
	return use
}

// Table makes trace.ClientTrace with metrics publishing
func Table(c Config) trace.Table {
	t := trace.Table{}
	gauge, name, errName := parseConfig(c, TableName)
	if c.Details()&TableSessionEvents != 0 {
		t.OnSessionNew = func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
			return func(info trace.SessionNewDoneInfo) {
				gauge(
					name(TableNameSession),
					name(NameNew),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameSession),
						name(NameNew),
						name(NameError),
						errName(info.Error),
					).Inc()
					gauge(
						name(TableNameSession),
						name(NameNew),
						name(NameError),
					).Inc()
				} else {
					gauge(
						name(TableNameSession),
						name(NameBalance),
					).Inc()
					balance := gauge(
						name(TableNamePool),
						name(NameBalance),
					).Value()
					max := gauge(
						name(TableNamePool),
						name(NameMax),
					).Value()
					if balance > max {
						panic("balance > max")
					}
				}
			}
		}
		t.OnSessionDelete = func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
			gauge(
				name(TableNameSession),
				name(NameBalance),
			).Dec()
			start := time.Now()
			return func(info trace.SessionDeleteDoneInfo) {
				gauge(
					name(TableNameSession),
					name(NameDelete),
					name(NameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
				gauge(
					name(TableNameSession),
					name(NameDelete),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameSession),
						name(NameDelete),
						name(NameError),
						errName(info.Error),
					).Inc()
					gauge(
						name(TableNameSession),
						name(NameDelete),
						name(NameError),
					).Inc()
				}
			}
		}
		t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
			return func(info trace.KeepAliveDoneInfo) {
				gauge(
					name(TableNameSession),
					name(TableNameKeepAlive),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameSession),
						name(TableNameKeepAlive),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&tableSessionQueryEvents != 0 {
		t.OnSessionQueryPrepare = func(info trace.SessionQueryPrepareStartInfo) func(trace.PrepareDataQueryDoneInfo) {
			return func(info trace.PrepareDataQueryDoneInfo) {
				gauge(
					name(TableNameQuery),
					name(TableNamePrepareData),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameQuery),
						name(TableNamePrepareData),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnSessionQueryExecute = func(info trace.ExecuteDataQueryStartInfo) func(trace.SessionQueryPrepareDoneInfo) {
			return func(info trace.SessionQueryPrepareDoneInfo) {
				gauge(
					name(TableNameQuery),
					name(TableNameExecuteData),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameQuery),
						name(TableNameExecuteData),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&tableSessionQueryStreamEvents != 0 {
		t.OnSessionQueryStreamExecute = func(info trace.SessionQueryStreamExecuteStartInfo) func(trace.SessionQueryStreamExecuteDoneInfo) {
			return func(info trace.SessionQueryStreamExecuteDoneInfo) {
				gauge(
					name(TableNameStream),
					name(TableNameStreamExecuteScan),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameStream),
						name(TableNameStreamExecuteScan),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnSessionQueryStreamRead = func(info trace.SessionQueryStreamReadStartInfo) func(trace.SessionQueryStreamReadDoneInfo) {
			return func(info trace.SessionQueryStreamReadDoneInfo) {
				gauge(
					name(TableNameStream),
					name(TableNameStreamReadTable),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameStream),
						name(TableNameStreamReadTable),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&tableSessionTransactionEvents != 0 {
		t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
			return func(info trace.SessionTransactionBeginDoneInfo) {
				gauge(
					name(TableNameTransaction),
					name(TableNameBeginTransaction),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameTransaction),
						name(TableNameBeginTransaction),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
			return func(info trace.SessionTransactionCommitDoneInfo) {
				gauge(
					name(TableNameTransaction),
					name(TableNameCommitTransaction),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameTransaction),
						name(TableNameCommitTransaction),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
			return func(info trace.SessionTransactionRollbackDoneInfo) {
				gauge(
					name(TableNameTransaction),
					name(TableNameRollbackTransaction),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNameTransaction),
						name(TableNameRollbackTransaction),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&tablePoolLifeCycleEvents != 0 {
		t.OnPoolInit = func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
			return func(info trace.PoolInitDoneInfo) {
				gauge(
					name(TableNamePool),
					name(NameMax),
				).Set(float64(info.Limit))
				gauge(
					name(TableNamePool),
					name(NameMin),
				).Set(float64(info.KeepAliveMinSize))
			}
		}
		t.OnPoolClose = func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
			return func(info trace.PoolCloseDoneInfo) {
				gauge(
					name(TableNamePool),
					name(NameMax),
				).Set(0)
				gauge(
					name(TableNamePool),
					name(NameMin),
				).Set(0)
			}
		}
	}
	if c.Details()&tablePoolSessionLifeCycleEvents != 0 {
		t.OnPoolSessionNew = func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
			gauge(
				name(TableNamePool),
				name(TableNameSession),
				name(NameNew),
				name(NameInProgress),
			).Inc()
			return func(info trace.PoolSessionNewDoneInfo) {
				gauge(
					name(TableNamePool),
					name(TableNameSession),
					name(NameNew),
					name(NameInProgress),
				).Dec()
				if info.Error != nil {
					gauge(
						name(TableNamePool),
						name(TableNameSession),
						name(NameNew),
						name(NameError),
						errName(info.Error),
					).Inc()
					gauge(
						name(TableNamePool),
						name(TableNameSession),
						name(NameNew),
						name(NameError),
					).Inc()
				} else {
					gauge(
						name(TableNamePool),
						name(TableNameSession),
						name(NameNew),
						name(NameTotal),
					).Inc()
					gauge(
						name(TableNamePool),
						name(NameBalance),
					).Inc()
					balance := gauge(
						name(TableNamePool),
						name(NameBalance),
					).Value()
					max := gauge(
						name(TableNamePool),
						name(NameMax),
					).Value()
					if balance > max {
						panic("balance > max")
					}
				}
			}
		}
		t.OnPoolSessionClose = func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
			return func(info trace.PoolSessionCloseDoneInfo) {
				gauge(
					name(TableNamePool),
					name(TableNameSession),
					name(NameClose),
					name(NameTotal),
				).Inc()
				gauge(
					name(TableNamePool),
					name(NameBalance),
				).Dec()
			}
		}
	}
	if c.Details()&tablePoolRetryEvents != 0 {
		t.OnPoolRetry = func(info trace.PoolRetryStartInfo) func(trace.PoolRetryDoneInfo) {
			start := time.Now()
			idempotent := func() Name {
				if info.Idempotent {
					return name(NameIdempotent)
				}
				return name(NameNonIdempotent)
			}()
			return func(info trace.PoolRetryDoneInfo) {
				gauge(
					name(TableNamePool),
					name(TableNamePoolRetry),
					name(NameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
				gauge(
					name(TableNamePool),
					name(TableNamePoolRetry),
					name(NameAttempts),
				).Set(float64(info.Attempts))
				gauge(
					name(TableNamePool),
					name(TableNamePoolRetry),
					idempotent,
					name(NameLatency),
				).Set(float64(time.Since(start).Microseconds()) / 1000.0)
				gauge(
					name(TableNamePool),
					name(TableNamePoolRetry),
					idempotent,
					name(NameAttempts),
				).Set(float64(info.Attempts))
				if info.Error != nil {
					gauge(
						name(TableNamePool),
						name(TableNamePoolRetry),
						name(NameError),
						errName(info.Error),
					).Inc()
					gauge(
						name(TableNamePool),
						name(TableNamePoolRetry),
						idempotent,
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
	}
	if c.Details()&tablePoolCommonAPIEvents != 0 {
		t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
			return func(info trace.PoolPutDoneInfo) {
				gauge(
					name(TableNamePool),
					name(TableNamePoolPut),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNamePool),
						name(TableNamePoolPut),
						name(NameError),
						errName(info.Error),
					).Inc()
					gauge(
						name(TableNamePool),
						name(TableNamePoolPut),
						name(NameError),
					).Inc()
				}
			}
		}
	}
	if c.Details()&tablePoolNativeAPIEvents != 0 {
		t.OnPoolGet = func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
			return func(info trace.PoolGetDoneInfo) {
				gauge(
					name(TableNamePool),
					name(TableNamePoolGet),
					name(NameTotal),
				).Inc()
				if info.Error != nil {
					gauge(
						name(TableNamePool),
						name(TableNamePoolGet),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
			gauge(
				name(TableNamePool),
				name(TableNamePoolWait),
				name(NameBalance),
			).Inc()
			return func(info trace.PoolWaitDoneInfo) {
				gauge(
					name(TableNamePool),
					name(TableNamePoolWait),
					name(NameBalance),
				).Dec()
				if info.Error != nil {
					gauge(
						name(TableNamePool),
						name(TableNamePoolWait),
						name(NameError),
						errName(info.Error),
					).Inc()
				}
			}
		}
		//t.OnPoolTake = func(info trace.PoolTakeStartInfo) func(info trace.PoolTakeWaitInfo) func(info trace.PoolTakeDoneInfo) {
		//	return func(info trace.PoolTakeWaitInfo) func(info trace.PoolTakeDoneInfo) {
		//		return func(info trace.PoolTakeDoneInfo) {
		//			gauge(
		//				name(TableNamePool),
		//				name(TableNamePoolTake),
		//				name(NameTotal),
		//			).Inc()
		//			gauge(
		//				name(TableNamePool),
		//				name(TableNamePoolTake),
		//				name(NameBalance),
		//			).Dec()
		//			if info.Error != nil {
		//				gauge(
		//					name(TableNamePool),
		//					name(TableNamePoolTake),
		//					name(NameError),
		//					errName(info.Error),
		//				).Inc()
		//			}
		//		}
		//	}
		//}
	}
	return t
}
