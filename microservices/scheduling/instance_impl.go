package scheduling

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/option"
)

type instanceImpl struct {
	process    *os.Process
	dead       chan struct{}
	port       int
	model      string
	startupLog *startupLogBuffer
	rpcNodes   []string
}

type startupLogBuffer struct {
	mu    sync.Mutex
	lines []string
}

func (b *startupLogBuffer) Add(stream string, line string) {
	if b == nil {
		return
	}
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines = append(b.lines, fmt.Sprintf("%s: %s", stream, trimmed))
	if len(b.lines) > 16 {
		b.lines = b.lines[len(b.lines)-16:]
	}
}

func (b *startupLogBuffer) FailureSummary() string {
	if b == nil {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.lines) == 0 {
		return ""
	}

	var important []string
	for _, line := range b.lines {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "error") ||
			strings.Contains(lower, "failed") ||
			strings.Contains(lower, "mismatch") ||
			strings.Contains(lower, "warning") ||
			strings.Contains(lower, "exiting") {
			important = append(important, line)
		}
	}
	if len(important) == 0 {
		important = b.lines
	}
	if len(important) > 4 {
		important = important[len(important)-4:]
	}
	return strings.Join(important, " | ")
}

// Model implements [Instance].
func (i *instanceImpl) Model() string {
	return i.model
}

// WaitReady implements [Instance].
func (i *instanceImpl) WaitReady() error {
	// send a request to the instance's /v1/models endpoint until it returns a 200 response
	for {
		// poll
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%v/v1/models", i.port))
		if err == nil {
			resp.Body.Close()
		}

		// if dead, return an error
		// the check is put here to minimize the window for a race condition
		select {
		case _, ok := <-i.dead:
			if !ok {
				if summary := i.startupLog.FailureSummary(); summary != "" {
					return fmt.Errorf("instance died during startup on rpc nodes [%s]: %s", strings.Join(i.rpcNodes, ", "), summary)
				}
				return fmt.Errorf("instance is dead on rpc nodes [%s]", strings.Join(i.rpcNodes, ", "))
			}
		default:
		}

		if err == nil {
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		time.Sleep(100 * time.Millisecond) // arbitrarily chosen polling interval
	}
}

// ReverseProxy implements [Instance].
func (i *instanceImpl) ReverseProxy() *httputil.ReverseProxy {
	// construct proxy base url
	baseUrl, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%v", i.port))
	if err != nil { // should hopefully never panic, as only the port can change
		panic(err)
	}

	proxy := httputil.NewSingleHostReverseProxy(baseUrl)
	// Flush immediately after every write so SSE tokens reach the client in real-time
	// instead of being buffered until the stream ends.
	proxy.FlushInterval = -1
	return proxy
}

// AwaitTermination implements [Instance].
func (i *instanceImpl) AwaitTermination() {
	i.process.Wait()
}

// GetOpenAIClient implements [Instance].
func (i *instanceImpl) GetOpenAIClient() openai.Client {
	return openai.NewClient(
		option.WithAPIKey(""),
		option.WithOrganization(""),
		option.WithProject(""),
		option.WithWebhookSecret(""),
		option.WithBaseURL(fmt.Sprintf("http://127.0.0.1:%v", i.port)),
	)
}

// Kill implements [Instance].
func (i *instanceImpl) Kill() {
	i.process.Kill()
}

// Stop implements [Instance].
func (i *instanceImpl) Stop() {
	if runtime.GOOS == "windows" {
		// Windows doesn't support interrupt, so kill instead
		i.process.Kill()
	} else {
		i.process.Signal(os.Interrupt)
	}
}

var _ Instance = (*instanceImpl)(nil)
