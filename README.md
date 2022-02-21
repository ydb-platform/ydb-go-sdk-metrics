# metrics

metrics package helps to create ydb-go-sdk traces with monitoring internal state of driver

## Usage
```go
import (
    "fmt"
    "sync/mutex"
    "time"
	
    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk-metrics"
)

// define custom gauge
type gauge struct {
    name string
    v    float64
    m    sync.Mutex
}

func (g *gauge) Inc() {
    g.m.Lock
    defer g.m.Unlock()
    g.v += 1
}

func (g *gauge) Dec() {
    g.m.Lock
    defer g.m.Unlock()
    g.v -= 1
}

func (g *gauge) Set(value float64) {
    g.m.Lock
    defer g.m.Unlock()
    g.v = value
}

func (g *gauge) Value() float64 {
    g.m.Lock
    defer g.m.Unlock()
    return g.v	
}

// define custom registry
type config struct {
    gauges []gauge
    m      sync.Mutex
}

func (c *config) Details() Details {
    return metrics.DriverClusterEvents |
        metrics.DriverConnEvents |
        metrics.DriverCredentialsEvents |
        metrics.DriverDiscoveryEvents
}

func (c *config) Gauge(name string) Gauge {
    g := &gauge{
        name: name,		
    }
    c.m.Lock()
    defer c.m.Unlock()
    c.gauges = append(c.gauges, g)
    return g
}

func (c *config) Delimiter() *string {
    // use default delimiter
    return nil
}

func (c *config) Prefix() *string {
    // nothing prefix
    return nil
}

func (c *config) Name(Type) *string {
    // use default metric names
    return nil
}

func (c *config) Join(...Name) *string {
    // use default join
    return nil
}

func (c *config) ErrName(err error) *string {
    // use default error name func
    return nil
}

func main() {
    registry := &config{
        gauges: make([]gauge, 0),
    }
    // print registry values 
    go func() {
        for {
            time.Sleep(time.Minute)
            registry.m.Lock()
            fmt.Printf("[%s] registry state:\n", time.Now().String())
            for _, g := range registry.gauges {
                fmt.Printf(" - %s = %f\n", g.name, g.Value())
            }
            registry.m.Lock()
        }
    }
    db, err := ydb.New(
        context.Background(),
		ydb.MustConnectionString(connection),
		ydb.WithTraceDriver(metrics.Driver(registry))
	)
    // work with db
}
```
