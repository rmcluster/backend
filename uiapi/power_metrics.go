package uiapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPowerMetricQuery   = "mqtt_power"
	defaultVoltageMetricQuery = "mqtt_voltage"
	defaultCurrentMetricQuery = "mqtt_current"
)

type mqttMetricsSnapshot struct {
	PowerWatts *float64
	Voltage    *float64
	Current    *float64
}

type powerMetricProvider interface {
	CurrentMetrics(ctx context.Context) (mqttMetricsSnapshot, error)
}

type prometheusPowerMetricProvider struct {
	client     *http.Client
	queryURL   string
	queryExprs mqttMetricQueries
}

type mqttMetricQueries struct {
	PowerWatts string
	Voltage    string
	Current    string
}

func newPowerMetricProviderFromEnv() powerMetricProvider {
	baseURL := strings.TrimSpace(envOrDefault("RMD_POWER_PROMETHEUS_URL", "http://127.0.0.1:9090"))

	queryURL, err := buildPrometheusQueryURL(baseURL)
	if err != nil {
		return nil
	}

	return &prometheusPowerMetricProvider{
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
		queryURL: queryURL,
		queryExprs: mqttMetricQueries{
			PowerWatts: envOrDefault("RMD_POWER_PROMETHEUS_QUERY", defaultPowerMetricQuery),
			Voltage:    envOrDefault("RMD_VOLTAGE_PROMETHEUS_QUERY", defaultVoltageMetricQuery),
			Current:    envOrDefault("RMD_CURRENT_PROMETHEUS_QUERY", defaultCurrentMetricQuery),
		},
	}
}

func envOrDefault(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func buildPrometheusQueryURL(baseURL string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("prometheus URL must include scheme and host")
	}

	if strings.HasSuffix(parsed.Path, "/api/v1/query") {
		return parsed.String(), nil
	}

	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/v1/query"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func (p *prometheusPowerMetricProvider) CurrentMetrics(ctx context.Context) (mqttMetricsSnapshot, error) {
	powerWatts, err := p.queryMetricValue(ctx, p.queryExprs.PowerWatts)
	if err != nil {
		return mqttMetricsSnapshot{}, err
	}
	voltage, err := p.queryMetricValue(ctx, p.queryExprs.Voltage)
	if err != nil {
		return mqttMetricsSnapshot{}, err
	}
	current, err := p.queryMetricValue(ctx, p.queryExprs.Current)
	if err != nil {
		return mqttMetricsSnapshot{}, err
	}

	return mqttMetricsSnapshot{
		PowerWatts: powerWatts,
		Voltage:    voltage,
		Current:    current,
	}, nil
}

func (p *prometheusPowerMetricProvider) queryMetricValue(ctx context.Context, queryExpr string) (*float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.queryURL, nil)
	if err != nil {
		return nil, err
	}

	query := req.URL.Query()
	query.Set("query", queryExpr)
	req.URL.RawQuery = query.Encode()

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus returned %s", resp.Status)
	}

	var payload struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	if payload.Status != "success" || len(payload.Data.Result) == 0 || len(payload.Data.Result[0].Value) < 2 {
		return nil, nil
	}

	valueText, ok := payload.Data.Result[0].Value[1].(string)
	if !ok {
		return nil, fmt.Errorf("unexpected prometheus value shape")
	}

	value, err := strconv.ParseFloat(valueText, 64)
	if err != nil {
		return nil, err
	}

	return &value, nil
}
