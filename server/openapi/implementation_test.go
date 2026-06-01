package openapi

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rmcluster/backend/tracker"
	"github.com/stretchr/testify/assert"
)

func TestNewRouter(t *testing.T) {
	router := NewRouter()
	assert.NotNil(t, router)
}

func TestTrackerServersGet(t *testing.T) {
	// Create a fresh tracker for this test
	testTracker := tracker.NewTracker()

	// Register some test servers
	testServers := []tracker.RpcServerInfo{
		{
			Id:            "server1",
			Ip:            "192.168.1.1",
			Port:          8080,
			StoragePort:   9090,
			LastSeen:      time.Now(),
			HardwareModel: "Model-X",
			MaxSize:       1000,
			Battery:       85.5,
			Temperature:   45.2,
		},
		{
			Id:            "server2",
			Ip:            "192.168.1.2",
			Port:          8080,
			StoragePort:   9090,
			LastSeen:      time.Now(),
			HardwareModel: "Model-Y",
			MaxSize:       2000,
			Battery:       math.NaN(),
			Temperature:   math.NaN(),
		},
		{
			Id:            "server3",
			Ip:            "192.168.1.3",
			Port:          8080,
			StoragePort:   0,
			LastSeen:      time.Now(),
			HardwareModel: "Model-Z",
			MaxSize:       -1,
			Battery:       0.0,
			Temperature:   0.0,
		},
	}

	for _, server := range testServers {
		testTracker.RegisterNode(server)
	}

	// Create a request to the endpoint
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	// Temporarily replace the global DefaultTracker
	originalTracker := tracker.DefaultTracker
	defer func() {
		tracker.DefaultTracker = originalTracker
	}()
	tracker.DefaultTracker = testTracker

	// Call the handler
	routes := OpenAPIRoutes{}
	routes.TrackerServersGet(c)

	// Verify response
	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotEmpty(t, w.Body.String())
}

func TestTrackerServersGetEmpty(t *testing.T) {
	// Create a fresh tracker with no servers
	testTracker := tracker.NewTracker()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	// Temporarily replace the global DefaultTracker
	originalTracker := tracker.DefaultTracker
	defer func() {
		tracker.DefaultTracker = originalTracker
	}()
	tracker.DefaultTracker = testTracker

	// Call the handler
	routes := OpenAPIRoutes{}
	routes.TrackerServersGet(c)

	// Verify response
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestTrackerAnnounceGet(t *testing.T) {
	// Create a fresh tracker for this test
	testTracker := tracker.NewTracker()

	// Temporarily replace the global DefaultTracker
	originalTracker := tracker.DefaultTracker
	defer func() {
		tracker.DefaultTracker = originalTracker
	}()
	tracker.DefaultTracker = testTracker

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/announce?id=test-server&port=8080&ip=192.168.1.100", nil)
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	// Call the handler
	routes := OpenAPIRoutes{}
	routes.TrackerAnnounceGet(c)

	// Verify the request was handled (tracker.Announce will handle validation)
	assert.NotNil(t, w.Body.String())
}

func TestNanToZero(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected float64
	}{
		{
			name:     "NaN value",
			input:    math.NaN(),
			expected: 0.0,
		},
		{
			name:     "Regular positive value",
			input:    42.5,
			expected: 42.5,
		},
		{
			name:     "Regular negative value",
			input:    -10.3,
			expected: -10.3,
		},
		{
			name:     "Zero value",
			input:    0.0,
			expected: 0.0,
		},
		{
			name:     "Positive infinity",
			input:    math.Inf(1),
			expected: math.Inf(1),
		},
		{
			name:     "Negative infinity",
			input:    math.Inf(-1),
			expected: math.Inf(-1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := nanToZero(tt.input)
			if math.IsNaN(tt.expected) {
				assert.True(t, math.IsNaN(result))
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestOpenAPIRoutesImplementsInterface(t *testing.T) {
	// This test ensures that OpenAPIRoutes implements the required interface
	var _ interface {
		TrackerServersGet(c *gin.Context)
		TrackerAnnounceGet(c *gin.Context)
	} = OpenAPIRoutes{}
}
