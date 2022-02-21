package labels

import (
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"io"
	"net"
	"strings"
)

type Label struct {
	Tag   string
	Value string
}

const (
	TagVersion    = "sdk"
	TagSource     = "source"
	TagName       = "name"
	TagMethod     = "method"
	TagError      = "error"
	TagErrCode    = "errCode"
	TagAddress    = "address"
	TagID         = "ID"
	TagNodeID     = "nodeID"
	TagDataCenter = "destination"
	TagState      = "state"
	TagIdempotent = "idempotent"
	TagSuccess    = "success"
	TagStage      = "stage"
)

func KeyValue(labels ...Label) map[string]string {
	kv := make(map[string]string, len(labels))
	for _, l := range labels {
		kv[l.Tag] = l.Value
	}
	return kv
}

func Err(err error, lbls ...Label) []Label {
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "network/" + netErr.Op + " -> " + netErr.Err.Error(),
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, io.EOF) {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "io/EOF",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return append(
			lbls,
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
			lbls,
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
	if d := ydb.TransportErrorDescription(err); d != nil {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "transport/" + d.Name(),
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", d.Code()),
			},
		)
	}
	if d := ydb.OperationErrorDescription(err); d != nil {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "operation/" + d.Name(),
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", d.Code()),
			},
		)
	}
	return append(
		lbls,
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
