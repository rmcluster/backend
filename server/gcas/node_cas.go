package gcas

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func NewRemoteCAS(name string, ip string, port int) NamedCAS {
	return &remoteCAS{
		name:   name,
		ip:     ip,
		port:   port,
		client: &http.Client{},
	}
}

type remoteCAS struct {
	name   string
	ip     string
	port   int
	client *http.Client
}

// Name implements [NamedCAS].
func (n *remoteCAS) Name() string {
	return n.name
}

func (n *remoteCAS) url(path string) string {
	return fmt.Sprintf("http://%s:%d%s", n.ip, n.port, path)
}

// Delete implements [CAS].
func (n *remoteCAS) Delete(ctx context.Context, hash Hash) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, n.url(fmt.Sprintf("/chunk/%s", hex.EncodeToString(hash[:]))), nil)
	if err != nil {
		return err
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return HashNotFoundError{}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

// FreeSpace implements [CAS].
func (n *remoteCAS) FreeSpace(ctx context.Context) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.url("/storage_info"), nil)
	if err != nil {
		return 0, err
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var info struct {
		AvailableSpace int64 `json:"available_space"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return 0, err
	}
	return info.AvailableSpace, nil
}

// Get implements [CAS].
func (n *remoteCAS) Get(ctx context.Context, hash Hash) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.url(fmt.Sprintf("/chunk/%s", hex.EncodeToString(hash[:]))), nil)
	if err != nil {
		return nil, err
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		var errResp struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			if errResp.Error == "corrupted_chunk" {
				return nil, DataCorruptError{}
			}
		}
		return nil, HashNotFoundError{}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// List implements [CAS].
func (n *remoteCAS) List(ctx context.Context) (<-chan Hash, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.url("/chunks/list"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var chunks []string
	if err := json.NewDecoder(resp.Body).Decode(&chunks); err != nil {
		return nil, err
	}

	ch := make(chan Hash, len(chunks))
	for _, chunkStr := range chunks {
		var h Hash
		b, err := hex.DecodeString(chunkStr)
		if err == nil && len(b) == len(h) {
			copy(h[:], b)
			ch <- h
		}
	}
	close(ch)
	return ch, nil
}

// Put implements [CAS].
func (n *remoteCAS) Put(ctx context.Context, hash Hash, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, n.url(fmt.Sprintf("/chunk/%s", hex.EncodeToString(hash[:]))), bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			if errResp.Error == "checksum_incorrect" {
				return DataCorruptError{}
			}
		}
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

var _ NamedCAS = (*remoteCAS)(nil)
