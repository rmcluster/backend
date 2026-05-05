package scheduling

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"runtime"
	"time"

	"github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/option"
)

type instanceImpl struct {
	process *os.Process
	dead    chan struct{}
	port    int
	model   string
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
				return fmt.Errorf("instance is dead")
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

	return httputil.NewSingleHostReverseProxy(baseUrl)
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
