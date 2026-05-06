package openapi

import (
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	internal "github.com/wk-y/rama-swap/server/openapi/internal/generated/go"
	"github.com/wk-y/rama-swap/tracker"
)

func NewRouter() *gin.Engine {
	return internal.NewRouter(internal.ApiHandleFunctions{
		DefaultAPI: OpenAPIRoutes{},
	})
}

type OpenAPIRoutes struct{}

// TrackerServersGet implements [openapi.DefaultAPI].
func (o OpenAPIRoutes) TrackerServersGet(c *gin.Context) {
	nodes := tracker.DefaultTracker.GetServers()
	response := internal.TrackerServersGet200Response{
		Servers: make([]internal.TrackerServersGet200ResponseServersInner, 0, len(nodes)),
	}
	for _, node := range nodes {
		response.Servers = append(response.Servers, internal.TrackerServersGet200ResponseServersInner{
			Ip:            node.Ip,
			Port:          int32(node.Port),
			StoragePort:   int32(node.StoragePort),
			LastSeen:      node.LastSeen.Format(time.RFC3339),
			HardwareModel: node.HardwareModel,
			MaxSize:       int32(node.MaxSize),
			Battery:       float32(nanToZero(node.Battery)),
			Temperature:   float32(nanToZero(node.Temperature)),
		})
	}
	c.JSON(http.StatusOK, response)
}

// TrackerAnnounceGet implements [openapi.DefaultAPI].
func (o OpenAPIRoutes) TrackerAnnounceGet(c *gin.Context) {
	tracker.DefaultTracker.Announce(c)
}

var _ internal.DefaultAPI = OpenAPIRoutes{}

func nanToZero(val float64) float64 {
	if math.IsNaN(val) {
		return 0.0
	}
	return val
}
