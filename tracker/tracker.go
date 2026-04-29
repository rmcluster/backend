package tracker

import (
	"encoding/json"
	"log"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// the number of seconds after which an RPC server is removed from the list
const expiryDuration = time.Second * 30

// the number of seconds to wait between announces
const interval = time.Second * 10

type TrackerSubscriber interface {
	OnNodeAdded(node RpcServerInfo)
	OnNodeRemoved(node RpcServerInfo)
	OnNodeUpdated(node RpcServerInfo)
}

type Tracker struct {
	sync.RWMutex
	subscribers map[TrackerSubscriber]struct{}

	RpcServers map[string]clientInfo
}

type clientInfo struct {
	RpcServerInfo
	expiryTimer *time.Timer
}

type RpcServerInfo struct {
	Id            string    `json:"id"`
	Ip            string    `json:"ip"`
	Port          int       `json:"port"`
	StoragePort   int       `json:"storage_port"`
	LastSeen      time.Time `json:"last_seen"`
	HardwareModel string    `json:"hardware_model"` // the hardware's model name
	MaxSize       int64     `json:"max_size"`
	Battery       float64   `json:"battery"`
	Temperature   float64   `json:"temperature"`
}

func NewTracker() *Tracker {
	return &Tracker{
		RpcServers:  make(map[string]clientInfo),
		subscribers: make(map[TrackerSubscriber]struct{}),
	}
}

func (t *Tracker) Announce(c *gin.Context) {
	type response struct {
		Interval int `json:"interval"`
	}

	id, ok := c.GetQuery("id")
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id"})
		return
	}

	port, ok := c.GetQuery("port")
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing port"})
		return
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid port"})
		return
	}

	var storagePort int
	if storagePortStr, ok := c.GetQuery("storage_port"); ok {
		storagePort, err = strconv.Atoi(storagePortStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid storage_port"})
			return
		}
	}

	// use the request's source address if ip is not specified
	ip, ok := c.GetQuery("ip")
	if !ok {
		ip = c.RemoteIP()
	}

	hardwareModel := c.Query("model")

	var maxSize int64 = -1
	if maxSizeStr, ok := c.GetQuery("max_size"); ok {
		maxSize, err = strconv.ParseInt(maxSizeStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid max_size"})
			return
		}
	}

	var battery float64 = math.NaN()
	if batteryStr, ok := c.GetQuery("battery"); ok {
		battery, err = strconv.ParseFloat(batteryStr, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid battery"})
			return
		}
	}

	var temperature float64 = math.NaN()
	if temperatureStr, ok := c.GetQuery("temperature"); ok {
		temperature, err = strconv.ParseFloat(temperatureStr, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid temperature"})
			return
		}
	}

	// validate ip
	if net.ParseIP(ip) == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ip"})
		return
	}

	clientId := id

	func() {
		t.Lock()
		defer t.Unlock()
		notifyNew := false

		// avoid duplicate timers
		if existingTimer := t.RpcServers[clientId].expiryTimer; existingTimer != nil {
			existingTimer.Stop()
			log.Printf("Reannounce from %s", clientId)
		} else {
			notifyNew = true
			log.Printf("New announce from %s", clientId)
		}

		announceTime := time.Now()

		serverInfo := RpcServerInfo{
			Id:            id,
			LastSeen:      announceTime,
			Ip:            ip,
			Port:          portNum,
			StoragePort:   storagePort,
			HardwareModel: hardwareModel,
			MaxSize:       maxSize,
			Battery:       battery,
			Temperature:   temperature,
		}

		t.RpcServers[clientId] = clientInfo{
			RpcServerInfo: serverInfo,
			expiryTimer: time.AfterFunc(expiryDuration, func() {
				t.Lock()
				defer t.Unlock()

				// there's a possible race condition if the client announces just as the timer expires,
				// preventing the timer from being stopped. To prevent that, we verify that the last seen time
				// has not been changed.
				if t.RpcServers[clientId].LastSeen.Equal(announceTime) {
					serverInfo := t.RpcServers[clientId].RpcServerInfo
					delete(t.RpcServers, clientId)
					t.notifyNodeRemoved(serverInfo)
					log.Printf("Removed %s from tracker", clientId)
				}
			}),
		}

		if notifyNew {
			t.notifyNodeAdded(serverInfo)
		} else {
			t.notifyNodeUpdated(serverInfo)
		}
	}()

	// respond
	c.JSON(http.StatusOK, gin.H{"interval": interval.Seconds()})
}

func (t *Tracker) ListServers(w http.ResponseWriter, r *http.Request) {
	type response struct {
		Servers []RpcServerInfo `json:"servers"`
	}

	servers := t.GetServers()

	w.Header().Add("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(response{
		Servers: servers,
	})

	if err != nil {
		log.Printf("Failed to respond to list servers: %v", err)
	}
}

func (t *Tracker) GetServers() []RpcServerInfo {
	t.RLock()
	defer t.RUnlock()

	servers := make([]RpcServerInfo, 0, len(t.RpcServers))
	for _, server := range t.RpcServers {
		servers = append(servers, server.RpcServerInfo)
	}

	sort.Slice(servers, func(i, j int) bool {
		if servers[i].Ip < servers[j].Ip {
			return true
		}
		if servers[i].Ip > servers[j].Ip {
			return false
		}
		return servers[i].Port < servers[j].Port
	})

	return servers
}

func (t *Tracker) Subscribe(subscriber TrackerSubscriber) {
	t.Lock()
	defer t.Unlock()
	t.subscribers[subscriber] = struct{}{}
}

func (t *Tracker) Unsubscribe(subscriber TrackerSubscriber) {
	t.Lock()
	defer t.Unlock()
	delete(t.subscribers, subscriber)
}

func (t *Tracker) notifyNodeAdded(node RpcServerInfo) {
	for subscriber := range t.subscribers {
		subscriber.OnNodeAdded(node)
	}
}

func (t *Tracker) notifyNodeRemoved(node RpcServerInfo) {
	for subscriber := range t.subscribers {
		subscriber.OnNodeRemoved(node)
	}
}

func (t *Tracker) notifyNodeUpdated(node RpcServerInfo) {
	for subscriber := range t.subscribers {
		subscriber.OnNodeUpdated(node)
	}
}
