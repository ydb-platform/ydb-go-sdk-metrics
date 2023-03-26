package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
)

func Table(c registry.Config) (t trace.Table) {
	versionLabel := labels.GenerateVersionLabel()

	c = c.WithSystem("table")
	if c.Details()&trace.TableEvents != 0 {
		buckets := []float64{
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 50, 100, 200,
		}
		doTxCounterVec := c.CounterVec("tx_calls_new", labels.TagStage, labels.TagIdempotent)
		doTxErrorsVec := c.CounterVec("tx_errors_new", labels.TagError, labels.TagErrCode, labels.TagVersion)
		doTxAttemptsHist := c.HistogramVec("tx_attempts_new", buckets, labels.TagStage, labels.TagIdempotent)
		doTxLatencyTimeVec := c.TimerVec("tx_latency_new", labels.TagStage, labels.TagVersion)

		t.OnDoTx = func(info trace.TableDoTxStartInfo) func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
			start := time.Now()
			initLabel := labels.Label{Tag: labels.TagStage, Value: "init"}
			idempotentLabel := labels.GenerateIdempotentLabel(info.Idempotent)
			doTxCounterVec.With(labels.KeyValue(initLabel, idempotentLabel)).Inc()
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				midLabel := labels.Label{Tag: labels.TagStage, Value: "mid"}
				if err := info.Error; err != nil {
					errorLabels := labels.GenerateErrorLabels(err)
					doTxErrorsVec.With(labels.KeyValue(errorLabels...)).Inc()
					doTxCounterVec.With(labels.KeyValue(midLabel, idempotentLabel)).Inc()
					doTxLatencyTimeVec.With(labels.KeyValue(midLabel, versionLabel)).Record(time.Since(start))
				} else {
					doTxCounterVec.With(labels.KeyValue(midLabel, idempotentLabel)).Inc()
					doTxLatencyTimeVec.With(labels.KeyValue(midLabel, versionLabel)).Record(time.Since(start))
				}

				return func(info trace.TableDoTxDoneInfo) {
					finalLabel := labels.Label{Tag: labels.TagStage, Value: "final"}
					if err := info.Error; err != nil {
						errorLabels := labels.GenerateErrorLabels(err)
						doTxErrorsVec.With(labels.KeyValue(errorLabels...)).Inc()
						doTxCounterVec.With(labels.KeyValue(finalLabel, idempotentLabel)).Inc()
					} else {
						doTxCounterVec.With(labels.KeyValue(idempotentLabel, finalLabel)).Inc()
						doTxAttemptsHist.With(labels.KeyValue(idempotentLabel, finalLabel)).Record(float64(info.Attempts))
					}
				}
			}
		}
	}

	if c.Details()&trace.TablePoolEvents != 0 {
		c := c.WithSystem("pool")
		if c.Details()&trace.TablePoolLifeCycleEvents != 0 {
			size := scope.New(c, "size", config.New(
				config.WithValueOnly(config.ValueTypeGauge),
			))
			t.OnPoolStateChange = func(info trace.TablePoolStateChangeInfo) {
				size.Start().SyncValue(float64(info.Size))
			}
		}
		if c.Details()&trace.TablePoolSessionLifeCycleEvents != 0 {
			c := c.WithSystem("session")
			add := scope.New(c, "add", config.New(config.WithoutError(), config.WithoutLatency()))
			remove := scope.New(c, "remove", config.New(config.WithoutError(), config.WithoutLatency()))
			t.OnPoolSessionAdd = func(info trace.TablePoolSessionAddInfo) {
				add.AddCall(nil)
			}
			t.OnPoolSessionRemove = func(info trace.TablePoolSessionRemoveInfo) {
				remove.AddCall(nil)
			}
		}
		if c.Details()&trace.TablePoolAPIEvents != 0 {
			//poolIdleGauge := c.GaugeVec("pool_idle_new")

			// Pool limit
			poolLimitGauge := c.GaugeVec("pool_limit_new")
			t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
				return func(info trace.TableInitDoneInfo) {
					poolLimitGauge.With(labels.KeyValue()).Set(float64(info.Limit))
				}
			}
			t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
				return func(info trace.TableCloseDoneInfo) {
					poolLimitGauge.With(labels.KeyValue()).Set(0)
				}
			}

			//Pool Size
			poolSizeGauge := c.GaugeVec("pool_size_new", labels.TagNodeID, labels.TagVersion)
			poolSizeMutex := sync.Mutex{}
			t.OnPoolSessionAdd = func(info trace.TablePoolSessionAddInfo) {
				poolSizeMutex.Lock()
				defer poolSizeMutex.Unlock()
				nodeLabel := labels.GenerateNodeLabel(info.Session.ID())
				poolSizeGauge.With(labels.KeyValue(nodeLabel, versionLabel)).Add(1)
			}
			t.OnPoolSessionRemove = func(info trace.TablePoolSessionRemoveInfo) {
				poolSizeMutex.Lock()
				defer poolSizeMutex.Unlock()
				nodeLabel := labels.GenerateNodeLabel(info.Session.ID())
				poolSizeGauge.With(labels.KeyValue(nodeLabel, versionLabel)).Add(-1)
			}

			//Inflight sessions
			poolInflightGauge := c.GaugeVec("pool_inflight_new", labels.TagNodeID)
			poolInflightErrorGauge := c.CounterVec("pool_inflight_error_new", labels.TagNodeID, labels.TagError,
				labels.TagErrCode, labels.TagVersion)
			poolInflightMutex := sync.Mutex{}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				return func(info trace.TablePoolGetDoneInfo) {
					poolInflightMutex.Lock()
					defer poolInflightMutex.Unlock()
					nodeLabel := labels.GenerateNodeLabel(info.Session.ID())
					if err := info.Error; err != nil {
						poolInflightMutex.Lock()
						defer poolInflightMutex.Unlock()
						errLabels := labels.Err(err, nodeLabel, versionLabel)
						poolInflightErrorGauge.With(labels.KeyValue(errLabels...)).Inc()
					} else {
						poolInflightGauge.With(labels.KeyValue(nodeLabel)).Add(1)
					}
				}
			}
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				nodeLabel := labels.GenerateNodeLabel(info.Session.ID())
				return func(info trace.TablePoolPutDoneInfo) {
					poolInflightMutex.Lock()
					defer poolInflightMutex.Unlock()
					if err := info.Error; err != nil {
						poolInflightMutex.Lock()
						defer poolInflightMutex.Unlock()
						errLabels := labels.Err(err, nodeLabel, versionLabel)
						poolInflightErrorGauge.With(labels.KeyValue(errLabels...)).Inc()
					} else {
						poolInflightGauge.With(labels.KeyValue(nodeLabel)).Add(-1)
					}
				}
			}

			//Queue sessions
			poolQueueGauge := c.GaugeVec("pool_queue_new")
			poolQueueErrorCounter := c.CounterVec("pool_queue_error_new", labels.TagError, labels.TagErrCode,
				labels.TagVersion)
			poolQueueMutex := sync.Mutex{}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				poolQueueMutex.Lock()
				defer poolQueueMutex.Unlock()
				poolQueueGauge.With(labels.KeyValue()).Add(1)
				return func(info trace.TablePoolWaitDoneInfo) {
					poolQueueMutex.Lock()
					defer poolQueueMutex.Unlock()
					if err := info.Error; err != nil {
						errLabels := labels.Err(err, versionLabel)
						poolQueueErrorCounter.With(labels.KeyValue(errLabels...)).Inc()
					}
					poolQueueGauge.With(labels.KeyValue()).Add(-1)
				}
			}
		}
	}
	return t
}
