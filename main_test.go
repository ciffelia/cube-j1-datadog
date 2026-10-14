package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestSubscriptionGranted(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := packets.ReadPacket(conn); err != nil {
			return
		}
		ack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
		ack.ReturnCode = packets.Accepted
		if err := ack.Write(conn); err != nil {
			return
		}
		packet, err := packets.ReadPacket(conn)
		if err != nil {
			return
		}
		sub, ok := packet.(*packets.SubscribePacket)
		if !ok {
			return
		}
		result := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
		result.MessageID = sub.MessageID
		for _, topic := range sub.Topics {
			code := byte(0)
			if topic == "rejected/#" {
				code = 0x80
			}
			result.ReturnCodes = append(result.ReturnCodes, code)
		}
		if err := result.Write(conn); err != nil {
			return
		}
		<-done
	}()
	client := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + listener.Addr().String()).SetClientID("test"))
	defer client.Disconnect(100)
	token := client.Connect()
	if !token.WaitTimeout(2*time.Second) || token.Error() != nil {
		t.Fatal("test connection failed")
	}
	token = client.SubscribeMultiple(map[string]byte{"accepted/#": 0, "rejected/#": 0}, nil)
	if !token.WaitTimeout(2*time.Second) || token.Error() != nil {
		t.Fatal("test subscription failed")
	}
	if !subscriptionGranted(token, "accepted/#") || subscriptionGranted(token, "rejected/#") {
		t.Fatal("incorrect subscription result")
	}
}

type errorTransport struct{ err error }

func (transport errorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, transport.err
}

func TestSubmitPreservesTransportError(t *testing.T) {
	want := errors.New("network unavailable")
	client := &http.Client{Transport: errorTransport{want}}
	sender, err := newDatadogSender(client, "datadoghq.com", "test-key", "meter")
	if err != nil {
		t.Fatal(err)
	}
	status, err := sender.submit(context.Background(), []measurement{{Name: "cube_j1.power_watts", Unit: "watt", Value: 1, ReceivedAt: time.Unix(123, 0)}})
	if status != 0 || !errors.Is(err, want) {
		t.Fatalf("status=%d err=%v", status, err)
	}
}

func TestReadMeasurements(t *testing.T) {
	t.Run("notifications", func(t *testing.T) {
		incoming := make(chan measurement, 10)
		r, tt := "cube_j1.current_r_amperes", "cube_j1.current_t_amperes"
		incoming <- measurement{Name: r, Value: 999} // stale push
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		items, err := readMeasurements(ctx, readGroups[1], incoming, func() error {
			incoming <- measurement{Name: "cube_j1.power_watts"}
			incoming <- measurement{Name: tt, Value: 1.5}
			incoming <- measurement{Name: tt, Value: 2.5} // duplicate
			incoming <- measurement{Name: r, Value: 0.8}
			return nil
		})
		if err != nil || len(items) != 2 {
			t.Fatalf("items=%+v, err=%v", items, err)
		}
		got := map[string]float64{}
		for _, item := range items {
			got[item.Name] = item.Value
		}
		if got[r] != 0.8 || got[tt] != 1.5 {
			t.Fatalf("accepted stale, unrelated or duplicate value: %+v", items)
		}
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		items, err := readMeasurements(ctx, readGroups[1], make(chan measurement), func() error { return nil })
		if len(items) != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("items=%+v, err=%v", items, err)
		}
	})
}

func TestNextPollSkipsMissedDeadlines(t *testing.T) {
	start := time.Unix(100, 0)
	for _, tc := range []struct{ elapsed, want time.Duration }{
		{3 * time.Second, 10 * time.Second},
		{10 * time.Second, 20 * time.Second},
		{25 * time.Second, 30 * time.Second},
	} {
		if got := nextPoll(start, start.Add(tc.elapsed), 10*time.Second); !got.Equal(start.Add(tc.want)) {
			t.Fatalf("elapsed=%s next=%s", tc.elapsed, got)
		}
	}
}

func TestDecode(t *testing.T) {
	now := time.Unix(1791936000, 0)
	for _, tc := range []struct {
		name, topic, body string
		retained, want    bool
	}{
		{"zero", "meter/42502375", `{"value":0}`, false, true},
		{"retained", "meter/42502375", `{"value":358}`, true, false},
		{"request", "meter/42502375", `{}`, false, false},
		{"wrong meter", "other/42502375", `{"value":358}`, false, false},
		{"unknown", "meter/42502387", `{"value":105}`, false, false},
		{"broken", "meter/42502375", `{`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ok := decode(tc.topic, []byte(tc.body), tc.retained, "meter", now)
			if ok != tc.want {
				t.Fatalf("accepted=%t, want %t", ok, tc.want)
			}
			if ok && s.Value != 0 {
				t.Fatalf("incorrect measurement: %+v", s)
			}
		})
	}
}

func TestSubmit(t *testing.T) {
	now := time.Unix(123, 0)
	expected := []struct {
		iid, name, unit, body string
		value                 float64
	}{
		{"42502375", "cube_j1.power_watts", "watt", `{"value":358,"timestamp":1}`, 358},
		{"42502376", "cube_j1.current_r_amperes", "ampere", `{"value":0.8}`, 0.8},
		{"42502392", "cube_j1.current_t_amperes", "ampere", `{"value":1.8}`, 1.8},
		{"42502368", "cube_j1.energy_forward_kwh", "kilowatt-hour", `{"value":136.5}`, 136.5},
		{"42502371", "cube_j1.energy_reverse_kwh", "kilowatt-hour", `{"value":0.625}`, 0.625},
	}
	var batch []measurement
	for _, want := range expected {
		item, ok := decode("meter/"+want.iid, []byte(want.body), false, "meter", now)
		if !ok {
			t.Fatalf("response %s was rejected", want.iid)
		}
		batch = append(batch, item)
	}
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"measurements", 202}, {"retry", 202}, {"rejected", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if tc.name == "measurements" {
					if r.Method != "POST" || r.URL.Path != "/api/v2/series" || r.Header.Get("DD-API-KEY") != "test-key" || r.Header.Get("DD-APPLICATION-KEY") != "" {
						t.Error("incorrect method or authentication")
					}
					var payload datadogV2.MetricPayload
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Error(err)
						return
					}
					got := map[string]datadogV2.MetricSeries{}
					for _, series := range payload.Series {
						got[series.Metric] = series
					}
					if len(payload.Series) != len(expected) || len(got) != len(expected) {
						t.Error("missing or duplicate measurements")
						return
					}
					for _, want := range expected {
						series, ok := got[want.name]
						if !ok || series.GetUnit() != want.unit || series.GetType() != datadogV2.METRICINTAKETYPE_GAUGE || len(series.Points) != 1 {
							t.Errorf("incorrect metric %s: %+v", want.name, series)
							continue
						}
						if series.Points[0].GetValue() != want.value || series.Points[0].GetTimestamp() != now.Unix() {
							t.Errorf("incorrect value or receive time: %s", want.name)
						}
					}
					tags := payload.Series[0].Tags
					if len(tags) != 2 || !slices.Contains(tags, "service:cube-j1-datadog") || !slices.Contains(tags, "meter_uuid:meter") {
						t.Error("incorrect tags")
					}
				}
				status := tc.status
				if tc.name == "retry" && requests == 1 {
					status = 503
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"errors":[]}`))
			}))
			defer server.Close()
			sender, err := newDatadogSender(server.Client(), "datadoghq.com", "test-key", "meter")
			if err != nil {
				t.Fatal(err)
			}
			sender.api.Client.Cfg.Servers = datadog.ServerConfigurations{{URL: server.URL}}
			sender.api.Client.Cfg.OperationServers = nil
			sender.api.Client.Cfg.RetryConfiguration.BackOffBase = 0
			status, err := sender.submit(context.Background(), batch)
			if status != tc.status || (err == nil) != (tc.status == 202) {
				t.Fatalf("status=%d err=%v", status, err)
			}
		})
	}
}

func TestDatadogSiteSelection(t *testing.T) {
	s, err := newDatadogSender(http.DefaultClient, "us5.datadoghq.com", "test-key", "meter")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := s.api.Client.Cfg.ServerURLWithContext(s.context(context.Background()), "v2.MetricsApi.SubmitMetrics")
	if err != nil || endpoint != "https://api.us5.datadoghq.com" {
		t.Fatalf("endpoint=%q, err=%v", endpoint, err)
	}
}

func TestValidateBroker(t *testing.T) {
	if err := validateBroker("ssl://192.0.2.10:8883"); err != nil {
		t.Fatal(err)
	}
	for _, broker := range []string{"tcp://example.com:1883", "ssl://", "ssl://example.com", "ssl://user:password@example.com:8883"} {
		if err := validateBroker(broker); err == nil {
			t.Errorf("accepted unsafe broker %q", broker)
		}
	}
}

func TestPinnedTLS(t *testing.T) {
	// Fixed synthetic data and its SHA-256 fingerprint; no device certificate.
	certificate := &x509.Certificate{Raw: []byte("test certificate")}
	config, err := pinnedTLS("86ad9afb262bca8c5658105977d9035fd7371a429ea73c36f756e84770dc4356")
	if err != nil {
		t.Fatal(err)
	}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
	if err := config.VerifyConnection(state); err != nil {
		t.Fatalf("matching certificate rejected: %v", err)
	}
	certificate.Raw = []byte("different certificate")
	if err := config.VerifyConnection(state); err == nil {
		t.Fatal("accepted an unpinned certificate")
	}
}
