package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

type datadogSender struct {
	api              *datadogV2.MetricsApi
	site, key, meter string
}

func (s *datadogSender) context(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, datadog.ContextServerVariables, map[string]string{"site": s.site})
	return context.WithValue(ctx, datadog.ContextAPIKeys, map[string]datadog.APIKey{"apiKeyAuth": {Key: s.key}})
}

func newDatadogSender(client *http.Client, site, key, meter string) (*datadogSender, error) {
	if site == "" {
		return nil, errors.New("--dd-site is required")
	}
	cfg := datadog.NewConfiguration()
	cfg.HTTPClient = client
	cfg.RetryConfiguration.EnableRetry = true
	cfg.Debug = false
	s := &datadogSender{api: datadogV2.NewMetricsApi(datadog.NewAPIClient(cfg)), site: site, key: key, meter: meter}
	if _, err := cfg.ServerURLWithContext(s.context(context.Background()), "v2.MetricsApi.SubmitMetrics"); err != nil {
		return nil, errors.New("set a supported Datadog site with --dd-site")
	}
	return s, nil
}

func metricPayload(batch []measurement, meter string) datadogV2.MetricPayload {
	result := datadogV2.MetricPayload{Series: make([]datadogV2.MetricSeries, 0, len(batch))}
	for _, item := range batch {
		metric := datadogV2.MetricSeries{Metric: item.Name, Type: datadogV2.METRICINTAKETYPE_GAUGE.Ptr(), Tags: []string{"service:cube-j1-datadog", "meter_uuid:" + meter}}
		metric.Unit = datadog.PtrString(item.Unit)
		metric.Points = []datadogV2.MetricPoint{{Timestamp: datadog.PtrInt64(item.ReceivedAt.Unix()), Value: datadog.PtrFloat64(item.Value)}}
		result.Series = append(result.Series, metric)
	}
	return result
}

func (s *datadogSender) submit(ctx context.Context, batch []measurement) (int, error) {
	accepted, resp, err := s.api.SubmitMetrics(s.context(ctx), metricPayload(batch, s.meter))
	if resp == nil {
		if err != nil {
			return 0, fmt.Errorf("Datadog API request failed: %w", err)
		}
		return 0, errors.New("Datadog API request failed")
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	// SDK errors may contain response bodies. Log status only, never credentials.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("Datadog returned HTTP %d", resp.StatusCode)
	}
	if err != nil || len(accepted.Errors) > 0 {
		return resp.StatusCode, errors.New("Datadog API response contained errors")
	}
	return resp.StatusCode, nil
}
