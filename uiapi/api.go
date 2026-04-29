package uiapi

import (
	"encoding/json"
	"crypto/rand"
	"encoding/json"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type apiModel struct {
	Model        string `json:"model"`
	DisplayName  string `json:"display_name"`
	Parameters   string `json:"parameters,omitempty"`
	Architecture string `json:"architecture,omitempty"`
	Quantization string `json:"quantization,omitempty"`
	Source       string `json:"source"`
	LinkHref     string `json:"link_href"`
	LinkLabel    string `json:"link_label"`
}

type apiModelsResponse struct {
	Models []apiModel `json:"models"`
}

type apiSearchModel struct {
	Model       string `json:"model"`
	DisplayName string `json:"display_name"`
	Downloads   int    `json:"downloads"`
	LinkHref    string `json:"link_href"`
}

type apiSearchResponse struct {
	Results []apiSearchModel `json:"results"`
}

type apiErrorResponse struct {
	Error string `json:"error"`
}

type apiAddModelRequest struct {
	Model string `json:"model"`
}

type dashboardServerSnapshot struct {
	Ip            string   `json:"ip"`
	Port          int      `json:"port"`
	HardwareModel string   `json:"hardware_model"`
	MaxSize       *int64   `json:"max_size"`
	Battery       *float64 `json:"battery"`
	Temperature   *float64 `json:"temperature"`
}

type dashboardDataResponse struct {
	Servers []dashboardServerSnapshot `json:"servers"`
}

type connectInfoResponse struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	Token string `json:"token"`
}

type deviceRegisterRequest struct {
	DeviceID string `json:"device_id"`
	Label    string `json:"label"`
	IP       string `json:"ip"`
	RPCPort  int    `json:"rpc_port"`
	Token    string `json:"token"`
}

func (s *UIApi) handleAPIRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeAPIJSON(w, http.StatusOK, map[string]any{
		"models":    "/api/ui/models",
		"search":    "/api/ui/models/search",
		"dashboard": "/api/ui/dashboard",
	})
}

		"connect":   "/api/ui/connect-info",
	})
}

func (s *UIApi) handleAPIConnectInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	host, port := preferredConnectHostPort(r)
	token, err := s.issueConnectToken()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "failed to mint connect token")
		return
	}

	writeAPIJSON(w, http.StatusOK, connectInfoResponse{Host: host, Port: port, Token: token})
}

func (s *UIApi) handleAPIDeviceRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req deviceRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if strings.TrimSpace(req.DeviceID) == "" || strings.TrimSpace(req.IP) == "" || req.RPCPort <= 0 {
		writeAPIError(w, http.StatusBadRequest, "missing device_id, ip, or rpc_port")
		return
	}

	if net.ParseIP(strings.TrimSpace(req.IP)) == nil {
		writeAPIError(w, http.StatusBadRequest, "invalid ip")
		return
	}

	if !s.consumeConnectToken(strings.TrimSpace(req.Token)) {
		writeAPIError(w, http.StatusUnauthorized, "invalid or expired token")
		return
	}

	now := time.Now()
	rec := registeredDevice{
		DeviceID: strings.TrimSpace(req.DeviceID),
		Label:    strings.TrimSpace(req.Label),
		IP:       strings.TrimSpace(req.IP),
		RPCPort:  req.RPCPort,
		Token:    strings.TrimSpace(req.Token),
		LastSeen: now,
	}

	s.deviceLock.Lock()
	s.deviceRegistry[rec.DeviceID] = rec
	s.deviceLock.Unlock()

	announceURL := fmt.Sprintf("/announce?ip=%s&port=%d&model=%s", rec.IP, rec.RPCPort, rec.Label)
	r2, _ := http.NewRequest(http.MethodGet, announceURL, nil)
	s.tracker.Announce(noopResponseWriter{}, r2)

	writeAPIJSON(w, http.StatusCreated, map[string]any{
		"status":   "registered",
		"endpoint": fmt.Sprintf("%s:%d", rec.IP, rec.RPCPort),
	})
}

func (s *UIApi) handleAPIDeviceAction(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/v1/devices/")
	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) != 2 || parts[1] != "keepalive" {
		writeAPIError(w, http.StatusNotFound, "not found")
		return
	}
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	deviceID := parts[0]
	s.deviceLock.Lock()
	rec, ok := s.deviceRegistry[deviceID]
	if ok {
		rec.LastSeen = time.Now()
		s.deviceRegistry[deviceID] = rec
	}
	s.deviceLock.Unlock()

	if !ok {
		writeAPIError(w, http.StatusNotFound, "device not found")
		return
	}

	if auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); auth == "" || auth != rec.Token {
		writeAPIError(w, http.StatusUnauthorized, "invalid token")
		return
	}

	announceURL := fmt.Sprintf("/announce?ip=%s&port=%d&model=%s", rec.IP, rec.RPCPort, rec.Label)
	r2, _ := http.NewRequest(http.MethodGet, announceURL, nil)
	s.tracker.Announce(noopResponseWriter{}, r2)

	writeAPIJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

type noopResponseWriter struct{}

func (noopResponseWriter) Header() http.Header { return make(http.Header) }
func (noopResponseWriter) Write([]byte) (int, error) { return 0, nil }
func (noopResponseWriter) WriteHeader(statusCode int) {}

func (s *UIApi) issueConnectToken() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)

	now := time.Now()
	expires := now.Add(2 * time.Minute)

	s.connectLock.Lock()
	for k, v := range s.connectTokens {
		if now.After(v) {
			delete(s.connectTokens, k)
		}
	}
	s.connectTokens[token] = expires
	s.connectLock.Unlock()

	return token, nil
}

func (s *UIApi) consumeConnectToken(token string) bool {
	if token == "" {
		return false
	}
	now := time.Now()

	s.connectLock.Lock()
	defer s.connectLock.Unlock()

	expires, ok := s.connectTokens[token]
	if !ok || now.After(expires) {
		delete(s.connectTokens, token)
		return false
	}

	delete(s.connectTokens, token)
	return true
}

func preferredConnectHostPort(r *http.Request) (string, int) {
	host := strings.TrimSpace(r.Host)
	port := 4917

	if parsedHost, parsedPort, err := net.SplitHostPort(host); err == nil {
		host = strings.Trim(parsedHost, "[]")
		if p, err := strconv.Atoi(parsedPort); err == nil && p > 0 {
			port = p
		}
	} else {
		host = strings.Trim(host, "[]")
	}

	if host == "" {
		host = "localhost"
	}

	if !isLoopbackHost(host) {
		return host, port
	}

	if lanIP, ok := firstNonLoopbackIPv4(); ok {
		return lanIP, port
	}

	return host, port
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func firstNonLoopbackIPv4() (string, bool) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", false
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			if v4 := ip.To4(); v4 != nil {
				return v4.String(), true
			}
		}
	}

	return "", false
}

func (s *UIApi) handleAPIModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	entries := s.listModelEntries()
	models := make([]apiModel, 0, len(entries))
	for _, entry := range entries {
		params := entry.Parameters
		arch := entry.Architecture
		quant := entry.Quantization

		if strings.HasPrefix(entry.Model, "hf:") && (params == "" || arch == "" || quant == "") {
			repo, variant, ok := parseHFModelRef(entry.Model)
			if ok {
				meta := fetchHFMetadata(repo, variant)
				if params == "" {
					params = meta.Parameters
				}
				if arch == "" {
					arch = meta.Architecture
				}
				if quant == "" {
					quant = meta.Quantization
				}
			}
		}

		models = append(models, apiModel{
			Model:        entry.Model,
			DisplayName:  entry.DisplayName,
			Parameters:   params,
			Architecture: arch,
			Quantization: quant,
			Source:       entry.Source,
			LinkHref:     entry.LinkHref,
			LinkLabel:    entry.LinkLabel,
		})
	}

	writeAPIJSON(w, http.StatusOK, apiModelsResponse{Models: models})
}

func (s *UIApi) handleAPISearchModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeAPIJSON(w, http.StatusOK, apiSearchResponse{Results: []apiSearchModel{}})
		return
	}

	results, err := searchHFModels(query, 12)
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	items := make([]apiSearchModel, 0, len(results))
	for _, result := range results {
		hfRef := "hf:" + result.ID
		items = append(items, apiSearchModel{
			Model:       hfRef,
			DisplayName: simplifyModelDisplayName(hfRef),
			Downloads:   result.Downloads,
			LinkHref:    "https://huggingface.co/" + result.ID,
		})
	}

	writeAPIJSON(w, http.StatusOK, apiSearchResponse{Results: items})
}

func (s *UIApi) handleAPIAddHFModel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req apiAddModelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	entry, err := parseHFModelAddInput(req.Model)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	if hfStore != nil {
		_ = hfStore.AddCustomModel(entry)
	}

	writeAPIJSON(w, http.StatusCreated, apiModel{
		Model:       entry.Model,
		DisplayName: entry.DisplayName,
		Source:      entry.Source,
		LinkHref:    entry.LinkHref,
		LinkLabel:   entry.LinkLabel,
	})
}

func (s *UIApi) handleAPILocalModelUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if err := r.ParseMultipartForm(64 << 20); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	file, header, err := r.FormFile("model_file")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "model_file is required")
		return
	}
	defer file.Close()

	entry, err := uploadLocalModel(r, file, header)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	if hfStore != nil {
		_ = hfStore.AddCustomModel(entry)
	}

	writeAPIJSON(w, http.StatusCreated, apiModel{
		Model:        entry.Model,
		DisplayName:  entry.DisplayName,
		Parameters:   entry.Parameters,
		Architecture: entry.Architecture,
		Quantization: entry.Quantization,
		Source:       entry.Source,
		LinkHref:     entry.LinkHref,
		LinkLabel:    entry.LinkLabel,
	})
}

func (s *UIApi) handleAPIDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	servers := s.tracker.GetServers()
	payload := dashboardDataResponse{Servers: make([]dashboardServerSnapshot, 0, len(servers))}
	for _, server := range servers {
		snapshot := dashboardServerSnapshot{
			Ip:            server.Ip,
			Port:          server.Port,
			HardwareModel: server.HardwareModel,
		}
		if server.MaxSize >= 0 {
			value := server.MaxSize
			snapshot.MaxSize = &value
		}
		if !math.IsNaN(server.Battery) {
			value := server.Battery
			snapshot.Battery = &value
		}
		if !math.IsNaN(server.Temperature) {
			value := server.Temperature
			snapshot.Temperature = &value
		}
		payload.Servers = append(payload.Servers, snapshot)
	}

	writeAPIJSON(w, http.StatusOK, payload)
}

func writeAPIJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeAPIJSON(w, status, apiErrorResponse{Error: message})
}

func uploadLocalModel(r *http.Request, file multipart.File, header *multipart.FileHeader) (customModelEntry, error) {
	if !strings.EqualFold(filepath.Ext(header.Filename), ".gguf") {
		return customModelEntry{}, fmt.Errorf("only .gguf models are allowed")
	}

	storageDir := localModelStorageDir()
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return customModelEntry{}, err
	}

	destinationPath := uniqueStoragePath(storageDir, filepath.Base(header.Filename))
	destination, err := os.Create(destinationPath)
	if err != nil {
		return customModelEntry{}, err
	}

	if _, err := io.Copy(destination, file); err != nil {
		_ = destination.Close()
		_ = os.Remove(destinationPath)
		return customModelEntry{}, err
	}
	if err := destination.Close(); err != nil {
		return customModelEntry{}, err
	}

	name := strings.TrimSpace(r.FormValue("name"))
	parameters := strings.TrimSpace(r.FormValue("parameters"))
	quantization := strings.TrimSpace(r.FormValue("quantization"))
	entry, err := parseLocalModelInput(name, destinationPath, parameters, quantization)
	if err != nil {
		_ = os.Remove(destinationPath)
		return customModelEntry{}, err
	}
	return entry, nil
}
