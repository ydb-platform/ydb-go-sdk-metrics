package sensors

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type Label struct {
	Tag   string
	Value string
}

const (
	TagVersion    = "sdk"
	TagCall       = "call"
	TagMethod     = "method"
	TagError      = "error"
	TagErrCode    = "errCode"
	TagAddress    = "address"
	TagDataCenter = "destination"
	TagState      = "state"
	TagIdempotent = "idempotent"
	TagSuccess    = "success"
	TagInternal   = "internal"
)

type Name string

const (
	NameDriver  = Name("driver")
	NameNew     = Name("new")
	NameBalance = Name("balance")
)

func err(err error, labels ...Label) []Label {
	if errors.Is(err, context.DeadlineExceeded) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "context/DeadlineExceeded",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, context.Canceled) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "context/Canceled",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if ok, code, text := ydb.IsTransportError(err); ok {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "transport/" + text,
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", code),
			},
		)
	}
	if ok, code, text := ydb.IsOperationError(err); ok {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "operation/" + text,
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", code),
			},
		)
	}
	return append(
		labels,
		Label{
			Tag:   TagError,
			Value: "unknown/" + strings.ReplaceAll(err.Error(), " ", "_"),
		},
		Label{
			Tag:   TagErrCode,
			Value: "-1",
		},
	)
}

// Gauge tracks single float64 value.
type Gauge interface {
	Add(delta float64)
	Set(value float64)
}

// GaugeVec
type GaugeVec interface {
	With(labels ...Label) Gauge
}

type Details int

const (
	DriverSystemEvents = Details(1 << iota)
	DriverClusterEvents
	DriverConnEvents
	DriverCredentialsEvents
	DriverDiscoveryEvents

	TableSessionEvents
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

var (
	version = Label{
		Tag: TagVersion,
		Value: func() string {
			_, version := path.Split(ydb.Version)
			return version
		}(),
	}
)

type Config interface {
	// Details returns bitmask for customize details of metrics
	// If zero - use full set of driver metrics
	Details() Details

	// Gauge returns Gauge by name, subsystem and labels
	// If gauge by args already created - return gauge from cache
	// If gauge by args nothing - create and return newest gauge
	GaugeVec(name string, description string, labelNames ...string) GaugeVec

	// WithSubsystem returns new Config with subsystem scope
	WithSystem(subsystem string) Config
}
