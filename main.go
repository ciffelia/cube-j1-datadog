package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type metricDefinition struct {
	iid, name, unit string
}

type measurement struct {
	Name       string
	Unit       string
	Value      float64
	ReceivedAt time.Time
}

type readGroup struct {
	iid       string
	responses []metricDefinition
}

// E8 supplies both R and T from a single request.
var readGroups = []readGroup{
	{"42502375", []metricDefinition{{"42502375", "cube_j1.power_watts", "watt"}}},
	{"42502376", []metricDefinition{
		{"42502376", "cube_j1.current_r_amperes", "ampere"},
		{"42502392", "cube_j1.current_t_amperes", "ampere"},
	}},
	{"42502368", []metricDefinition{{"42502368", "cube_j1.energy_forward_kwh", "kilowatt-hour"}}},
	{"42502371", []metricDefinition{{"42502371", "cube_j1.energy_reverse_kwh", "kilowatt-hour"}}},
}

func metricForIID(iid string) (metricDefinition, bool) {
	for _, group := range readGroups {
		for _, metric := range group.responses {
			if metric.iid == iid {
				return metric, true
			}
		}
	}
	return metricDefinition{}, false
}

// Cube responses have no usable request ID. Accept each expected measurement
// once in the request window; a simultaneous native push cannot be distinguished.
func readMeasurements(ctx context.Context, group readGroup, incoming <-chan measurement, publish func() error) ([]measurement, error) {
	// Discard notifications received before this request.
	for {
		select {
		case <-incoming:
		default:
			goto drained
		}
	}
drained:
	if err := publish(); err != nil {
		return nil, err
	}
	wanted := make(map[string]bool, len(group.responses))
	for _, metric := range group.responses {
		wanted[metric.name] = true
	}
	var result []measurement
	for len(wanted) > 0 {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case item := <-incoming:
			if !wanted[item.Name] {
				continue
			}
			delete(wanted, item.Name)
			result = append(result, item)
		}
	}
	return result, nil
}

func nextPoll(start, now time.Time, interval time.Duration) time.Time {
	next := start.Add(interval)
	if !next.After(now) {
		next = next.Add((now.Sub(next)/interval + 1) * interval)
	}
	return next
}

func readCycle(ctx context.Context, c mqtt.Client, ready *atomic.Bool, meter string, incoming <-chan measurement) []measurement {
	var batch []measurement
	for _, group := range readGroups {
		if !ready.Load() || !c.IsConnectionOpen() {
			break
		}
		requestCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		items, err := readMeasurements(requestCtx, group, incoming, func() error {
			// Empty object is a read; never send value (write) or retain it.
			token := c.Publish(meter+"/"+group.iid, 1, false, "{}")
			select {
			case <-requestCtx.Done():
				return requestCtx.Err()
			case <-token.Done():
				return token.Error()
			}
		})
		cancel()
		batch = append(batch, items...)
		if err != nil {
			log.Printf("Read %s failed: %v; ending cycle", group.iid, err)
			break
		}
	}
	return batch
}

func poll(ctx context.Context, c mqtt.Client, ready *atomic.Bool, meter string, interval time.Duration, incoming <-chan measurement, output chan<- []measurement) {
	next := time.Now()
	for {
		timer := time.NewTimer(max(time.Until(next), 0))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		start := next
		if ready.Load() && c.IsConnectionOpen() {
			log.Print("Starting measurement cycle")
			batch := readCycle(ctx, c, ready, meter, incoming)
			if ctx.Err() != nil {
				return
			}
			if len(batch) > 0 {
				select {
				case output <- batch:
				default:
					log.Printf("Datadog busy; dropping %d points", len(batch))
				}
			}
		}
		// Skip missed deadlines instead of building a backlog of requests.
		next = nextPoll(start, time.Now(), interval)
	}
}

func decode(topic string, raw []byte, retained bool, meter string, now time.Time) (measurement, bool) {
	if retained || !strings.HasPrefix(topic, meter+"/") {
		return measurement{}, false
	}
	metric, ok := metricForIID(strings.TrimPrefix(topic, meter+"/"))
	if !ok {
		return measurement{}, false
	}
	var message struct {
		Value *float64 `json:"value"`
	}
	if json.Unmarshal(bytes.TrimRight(raw, "\x00"), &message) != nil || message.Value == nil {
		return measurement{}, false
	}
	return measurement{Name: metric.name, Unit: metric.unit, Value: *message.Value, ReceivedAt: now}, true
}

func validateBroker(broker string) error {
	u, err := url.Parse(broker)
	if err != nil || u.Hostname() == "" || u.Port() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("MQTT broker must be a TLS URL with a host and port")
	}
	switch u.Scheme {
	case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps":
		return nil
	default:
		return errors.New("MQTT broker must use a TLS scheme (for example ssl://)")
	}
}

func secret(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", errors.New("credential file is empty")
	}
	return s, nil
}

func pinnedTLS(fingerprint string) (*tls.Config, error) {
	want, err := hex.DecodeString(fingerprint)
	if err != nil || len(want) != sha256.Size {
		return nil, errors.New("MQTT certificate fingerprint must be 64 hexadecimal characters")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// The Cube has a private certificate; verify the pinned leaf instead of public PKI.
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("MQTT server provided no certificate")
			}
			sum := sha256.Sum256(state.PeerCertificates[0].Raw)
			if !bytes.Equal(sum[:], want) {
				return errors.New("MQTT certificate fingerprint mismatch")
			}
			return nil
		},
	}, nil
}

func subscriptionGranted(token mqtt.Token, topic string) bool {
	qos, ok := token.(*mqtt.SubscribeToken).Result()[topic]
	return ok && qos <= 2
}

func run() error {
	broker := flag.String("mqtt", "", "MQTT broker (required)")
	meter := flag.String("meter", "", "Smart-meter UUID (required)")
	username := flag.String("mqtt-username", "", "MQTT username (required)")
	clientID := flag.String("mqtt-client-id", "", "MQTT client ID (required)")
	passwordPath := flag.String("mqtt-password-file", "", "MQTT password file")
	fingerprint := flag.String("mqtt-cert-sha256", "", "Pinned certificate SHA256")
	site := flag.String("dd-site", "", "Datadog site")
	keyPath := flag.String("dd-api-key-file", "", "Datadog API key file")
	dryRun := flag.Bool("dry-run", false, "Log measurements without sending to Datadog")
	interval := flag.Duration("poll-interval", 10*time.Second, "Measurement cycle interval")
	flag.Parse()
	for _, setting := range []struct{ name, value string }{
		{"mqtt", *broker}, {"meter", *meter}, {"mqtt-username", *username}, {"mqtt-client-id", *clientID},
	} {
		if strings.TrimSpace(setting.value) == "" {
			return fmt.Errorf("--%s is required", setting.name)
		}
	}
	if *interval < time.Second {
		return errors.New("poll interval must be at least one second")
	}
	if err := validateBroker(*broker); err != nil {
		return err
	}
	password, err := secret(*passwordPath)
	if err != nil {
		return fmt.Errorf("load MQTT password: %w", err)
	}
	tlsConfig, err := pinnedTLS(*fingerprint)
	if err != nil {
		return err
	}
	var sender *datadogSender
	if !*dryRun {
		key, err := secret(*keyPath)
		if err != nil {
			return fmt.Errorf("load Datadog API key: %w", err)
		}
		httpClient := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
		sender, err = newDatadogSender(httpClient, *site, key, *meter)
		if err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	queue := make(chan []measurement, 1)
	incoming := make(chan measurement, 64)
	var ready atomic.Bool
	opts := mqtt.NewClientOptions().AddBroker(*broker).SetClientID(*clientID).SetUsername(*username).SetPassword(password).SetTLSConfig(tlsConfig).SetCleanSession(true).SetAutoReconnect(true).SetMaxReconnectInterval(time.Minute).SetConnectTimeout(10 * time.Second).SetKeepAlive(30 * time.Second)
	opts.SetWriteTimeout(5 * time.Second)
	opts.SetConnectionLostHandler(func(_ mqtt.Client, _ error) { ready.Store(false); log.Print("MQTT connection lost; reconnecting") })
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		ready.Store(false)
		log.Print("MQTT connected")
		topic := *meter + "/#"
		token := c.Subscribe(topic, 0, func(_ mqtt.Client, message mqtt.Message) {
			item, ok := decode(message.Topic(), message.Payload(), message.Retained(), *meter, time.Now())
			if !ok {
				return
			}
			select {
			case incoming <- item:
			default:
				log.Print("Measurement queue full; dropping new point")
			}
		})
		// Paho already invokes OnConnect in its own goroutine.
		if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
			log.Print("MQTT subscription failed; restarting")
			stop()
			return
		}
		if !subscriptionGranted(token, topic) {
			log.Print("MQTT subscription rejected; restarting")
			stop()
			return
		}
		ready.Store(true)
		log.Print("MQTT subscription active")
	})
	c := mqtt.NewClient(opts)
	token := c.Connect()
	defer c.Disconnect(250)
	select {
	case <-ctx.Done():
		return nil
	case <-token.Done():
		if err := token.Error(); err != nil {
			return fmt.Errorf("MQTT connection failed: %w", err)
		}
	}
	log.Printf("Polling every %s; sequential reads, 20s response timeout", *interval)
	go poll(ctx, c, &ready, *meter, *interval, incoming, queue)
	for {
		select {
		case <-ctx.Done():
			return nil
		case batch := <-queue:
			for _, item := range batch {
				log.Printf("Received %s=%g", item.Name, item.Value)
			}
			if *dryRun {
				continue
			}
			status, err := sender.submit(ctx, batch)
			if err != nil {
				log.Printf("Datadog send failed; dropping %d points: %v", len(batch), err)
			} else {
				log.Printf("Datadog accepted %d points (HTTP %d)", len(batch), status)
			}
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
