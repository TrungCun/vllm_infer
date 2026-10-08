package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeBackend struct {
	mu sync.Mutex
	asleep bool
	wakes atomic.Int32
	sleeps atomic.Int32
	generations atomic.Int32
	badInference atomic.Int32
	checks atomic.Int32
	wakeStarted chan struct{}
	sleepStarted chan struct{}
	wakeGate chan struct{}
	sleepGate chan struct{}
	wakeStatus int
}

func (b *fakeBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/is_sleeping":
		b.checks.Add(1)
		b.mu.Lock()
		value := b.asleep
		b.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"is_sleeping\":%t}", value)
	case "/health":
		b.checks.Add(1)
		w.WriteHeader(200)
	case "/wake_up":
		b.wakes.Add(1)
		select { case b.wakeStarted <- struct{}{}: default: }
		if b.wakeGate != nil {
			select { case <-b.wakeGate: case <-r.Context().Done(): return }
		}
		if b.wakeStatus != 0 {
			w.WriteHeader(b.wakeStatus)
			return
		}
		b.mu.Lock()
		b.asleep = false
		b.mu.Unlock()
		w.WriteHeader(200)
	case "/sleep":
		b.sleeps.Add(1)
		if r.URL.Query().Get("level") != "1" { w.WriteHeader(400); return }
		select { case b.sleepStarted <- struct{}{}: default: }
		if b.sleepGate != nil {
			select { case <-b.sleepGate: case <-r.Context().Done(): return }
		}
		b.mu.Lock()
		b.asleep = true
		b.mu.Unlock()
		w.WriteHeader(200)
	default:
		b.generations.Add(1)
		b.mu.Lock()
		sleeping := b.asleep
		b.mu.Unlock()
		if sleeping { b.badInference.Add(1) }
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{\"ok\":true}"))
	}
}

func fixture(t *testing.T, b *fakeBackend) (*gateway, *httptest.Server) {
	t.Helper()
	backend := httptest.NewServer(b)
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, idle: time.Millisecond, controlTimeout: 2*time.Second})
	server := httptest.NewServer(g)
	t.Cleanup(func() {
		server.Close()
		g.control.CloseIdleConnections()
		backend.Close()
	})
	return g, server
}

func eventually(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3*time.Second)
	for time.Now().Before(deadline) {
		if predicate() { return }
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func phaseIs(g *gateway, p phase) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state == p
}

func activeIs(g *gateway, n int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active == n
}

func doPost(client *http.Client, base string) (int, error) {
	resp, err := client.Post(base+"/v1/chat/completions", "application/json", strings.NewReader("{}"))
	if err != nil { return 0, err }
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, err
}

func TestConcurrentRequestsShareWake(t *testing.T) {
	gate := make(chan struct{})
	b := &fakeBackend{asleep: true, wakeGate: gate, wakeStarted: make(chan struct{}, 1)}
	g, server := fixture(t, b)
	defer func() { select { case <-gate: default: close(gate) } }()
	const count = 24
	results := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			status, err := doPost(server.Client(), server.URL)
			if err == nil && status != 200 { err = fmt.Errorf("HTTP %d", status) }
			results <- err
		}()
	}
	eventually(t, func() bool { return activeIs(g, count) && b.wakes.Load() == 1 })
	if b.generations.Load() != 0 { t.Fatal("forwarded before wake completed") }
	close(gate)
	for i := 0; i < count; i++ {
		if err := <-results; err != nil { t.Fatal(err) }
	}
	if b.wakes.Load() != 1 || b.badInference.Load() != 0 { t.Fatal("unsafe or duplicate wake") }
	eventually(t, func() bool { return activeIs(g, 0) })
}

func TestRequestDuringSleepWaitsThenWakes(t *testing.T) {
	sleepGate, wakeGate := make(chan struct{}), make(chan struct{})
	b := &fakeBackend{sleepGate: sleepGate, wakeGate: wakeGate}
	g, server := fixture(t, b)
	defer func() {
		select { case <-sleepGate: default: close(sleepGate) }
		select { case <-wakeGate: default: close(wakeGate) }
	}()
	g.mu.Lock()
	g.state = awake
	g.idleSince = time.Now().Add(-time.Second)
	g.mu.Unlock()
	if !g.trySleep(time.Now()) { t.Fatal("sleep did not start") }
	eventually(t, func() bool { return b.sleeps.Load() == 1 })
	result := make(chan error, 1)
	go func() {
		status, err := doPost(server.Client(), server.URL)
		if err == nil && status != 200 { err = fmt.Errorf("HTTP %d", status) }
		result <- err
	}()
	eventually(t, func() bool { return activeIs(g, 1) })
	if b.wakes.Load() != 0 || b.generations.Load() != 0 { t.Fatal("request raced sleep") }
	close(sleepGate)
	eventually(t, func() bool { return b.wakes.Load() == 1 })
	if b.generations.Load() != 0 { t.Fatal("request forwarded during wake") }
	close(wakeGate)
	if err := <-result; err != nil { t.Fatal(err) }
	if b.badInference.Load() != 0 { t.Fatal("inference while asleep") }
}

func TestAwakeProxyPreservesHTTPAndDoesNotPoll(t *testing.T) {
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.URL.RawQuery != "example=1" ||
			r.Method != "POST" || r.Header.Get("Authorization") != "Bearer example" {
			t.Errorf("request changed: %s %s", r.Method, r.URL)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "{\"hello\":\"world\"}" { t.Error("body changed") }
		calls.Add(1)
		w.Header().Set("X-Upstream", "kept")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(422)
		_, _ = w.Write([]byte("{\"error\":\"example\"}"))
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, idle: time.Minute, controlTimeout: time.Second})
	g.state = awake
	server := httptest.NewServer(g)
	defer server.Close()
	defer g.control.CloseIdleConnections()
	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions?example=1",
			strings.NewReader("{\"hello\":\"world\"}"))
		req.Header.Set("Authorization", "Bearer example")
		resp, err := server.Client().Do(req)
		if err != nil { t.Fatal(err) }
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 422 || resp.Header.Get("X-Upstream") != "kept" ||
			string(body) != "{\"error\":\"example\"}" { t.Fatal("response changed") }
	}
	if calls.Load() != 3 { t.Fatal("unexpected readiness poll or retry") }
}

func TestStreamStaysActiveUntilEOF(t *testing.T) {
	gate := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-gate
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, idle: time.Millisecond, controlTimeout: time.Second})
	g.state = awake
	server := httptest.NewServer(g)
	defer backend.Close()
	defer server.Close()
	defer g.control.CloseIdleConnections()
	defer func() { select { case <-gate: default: close(gate) } }()
	client := &http.Client{Timeout: 2*time.Second}
	resp, err := client.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader("{}"))
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != "data: first\n" { t.Fatalf("stream buffered or changed: %q %v", line, err) }
	if !activeIs(g, 1) || g.trySleep(time.Now().Add(time.Hour)) { t.Fatal("slept during streaming") }
	close(gate)
	rest, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(rest), "[DONE]") { t.Fatal("stream truncated") }
	eventually(t, func() bool { return activeIs(g, 0) })
	g.mu.Lock()
	elapsed := time.Since(g.idleSince)
	g.mu.Unlock()
	if elapsed > time.Second { t.Fatal("idle was not reset at completion") }
}

func TestWakeFailureAndTimeoutNeverForward(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint("timeout=", timeout), func(t *testing.T) {
			b := &fakeBackend{asleep: true, wakeStatus: 500}
			if timeout { b.wakeGate = make(chan struct{}) }
			g, server := fixture(t, b)
			if timeout { g.cfg.controlTimeout = 30*time.Millisecond }
			status, err := doPost(server.Client(), server.URL)
			if err != nil || status != 503 { t.Fatalf("got %d %v", status, err) }
			eventually(t, func() bool { return phaseIs(g, faulted) })
			status, err = doPost(server.Client(), server.URL)
			if err != nil || status != 503 { t.Fatalf("got %d %v", status, err) }
			if b.wakes.Load() != 1 || b.generations.Load() != 0 { t.Fatal("retry or inference after failed wake") }
		})
	}
}

func TestCancelledWaiterDoesNotCancelSharedWake(t *testing.T) {
	gate := make(chan struct{})
	b := &fakeBackend{asleep: true, wakeGate: gate}
	g, server := fixture(t, b)
	defer func() { select { case <-gate: default: close(gate) } }()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/chat/completions", strings.NewReader("{}"))
	first := make(chan error, 1)
	go func() {
		resp, err := server.Client().Do(req)
		if resp != nil { resp.Body.Close() }
		first <- err
	}()
	eventually(t, func() bool { return b.wakes.Load() == 1 })
	cancel()
	if err := <-first; err == nil { t.Fatal("expected cancellation") }
	second := make(chan error, 1)
	go func() {
		status, err := doPost(server.Client(), server.URL)
		if err == nil && status != 200 { err = fmt.Errorf("HTTP %d", status) }
		second <- err
	}()
	eventually(t, func() bool { return activeIs(g, 1) })
	close(gate)
	if err := <-second; err != nil { t.Fatal(err) }
	if b.wakes.Load() != 1 { t.Fatal("shared wake was cancelled") }
}

func TestInitializationRetriesWhenBackendNotReady(t *testing.T) {
	var ready atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() { w.WriteHeader(503); return }
		if r.URL.Path == "/is_sleeping" { fmt.Fprint(w, "{\"is_sleeping\":false}"); return }
		w.WriteHeader(200)
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, idle: time.Minute, controlTimeout: time.Second})
	server := httptest.NewServer(g)
	defer server.Close()
	defer g.control.CloseIdleConnections()
	status, err := doPost(server.Client(), server.URL)
	if err != nil || status != 503 { t.Fatalf("got %d %v", status, err) }
	ready.Store(true)
	status, err = doPost(server.Client(), server.URL)
	if err != nil || status != 200 { t.Fatalf("got %d %v", status, err) }
}

func TestControlRoutesAndHealthDoNotWake(t *testing.T) {
	b := &fakeBackend{asleep: true}
	g, server := fixture(t, b)
	for _, path := range []string{"/sleep", "/wake_up", "/collective_rpc", "/is_sleeping", "/metrics"} {
		resp, err := server.Client().Get(server.URL+path)
		if err != nil { t.Fatal(err) }
		resp.Body.Close()
		if resp.StatusCode != 404 { t.Fatalf("%s exposed", path) }
	}
	for _, path := range []string{"/healthz", "/v1/models"} {
		resp, err := server.Client().Get(server.URL+path)
		if err != nil { t.Fatal(err) }
		resp.Body.Close()
		if resp.StatusCode != 200 { t.Fatal("health/discovery unavailable") }
	}
	if b.wakes.Load() != 0 || !phaseIs(g, unknown) { t.Fatal("health or discovery woke model") }
}

func TestIdleDisabledAndAdmissionPreventSleep(t *testing.T) {
	b := &fakeBackend{}
	g, _ := fixture(t, b)
	g.mu.Lock()
	g.state = awake
	g.idleSince = time.Now().Add(-time.Hour)
	g.active = 1
	g.mu.Unlock()
	if g.trySleep(time.Now()) { t.Fatal("slept with active request") }
	g.mu.Lock()
	g.active = 0
	g.cfg.idle = 0
	g.mu.Unlock()
	if g.trySleep(time.Now()) { t.Fatal("slept with timeout disabled") }
	g.mu.Lock()
	g.cfg.idle = time.Millisecond
	g.mu.Unlock()
	g.disableSleep("uncertain backend cancellation")
	if g.trySleep(time.Now()) { t.Fatal("slept after uncertain cancellation") }
}

func TestInterruptedStreamDisablesSleep(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		fmt.Fprint(w, "truncated")
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, idle: time.Millisecond, controlTimeout: time.Second})
	g.state = awake
	server := httptest.NewServer(g)
	defer server.Close()
	defer g.control.CloseIdleConnections()
	resp, err := server.Client().Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader("{}"))
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.sleepDisabled && g.active == 0
	})
	if g.trySleep(time.Now().Add(time.Hour)) { t.Fatal("slept after truncated response") }
}

func TestDurationConfiguration(t *testing.T) {
	for _, value := range []string{"-1s", "oops"} {
		t.Setenv("IDLE_TIMEOUT", value)
		if _, err := durationEnv("IDLE_TIMEOUT", "30m", true); err == nil { t.Fatal("accepted invalid duration") }
	}
	t.Setenv("IDLE_TIMEOUT", "0")
	if d, err := durationEnv("IDLE_TIMEOUT", "30m", true); err != nil || d != 0 { t.Fatal("zero did not disable idle") }
	t.Setenv("CONTROL_TIMEOUT", "0")
	if _, err := durationEnv("CONTROL_TIMEOUT", "60s", false); err == nil { t.Fatal("accepted unbounded control timeout") }
}

func BenchmarkProxyAwake(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "{\"ok\":true}")
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, controlTimeout: time.Second})
	g.state = awake
	proxy := httptest.NewServer(g)
	defer proxy.Close()
	defer g.control.CloseIdleConnections()
	client := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	defer client.CloseIdleConnections()
	for _, endpoint := range []struct{name, address string}{
		{"direct", backend.URL}, {"gateway", proxy.URL},
	} {
		b.Run(endpoint.name, func(b *testing.B) {
			b.ReportAllocs()
			samples := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				started := time.Now()
				status, err := doPost(client, endpoint.address)
				samples = append(samples, time.Since(started))
				if err != nil || status != 200 { b.Fatalf("%d %v", status, err) }
			}
			b.StopTimer()
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			b.ReportMetric(float64(samples[(len(samples)-1)/2].Nanoseconds()), "p50-ns")
			b.ReportMetric(float64(samples[(95*len(samples)+99)/100-1].Nanoseconds()), "p95-ns")
		})
	}
}

func TestWakeAcknowledgementWaitsForRestoredState(t *testing.T) {
	var restored atomic.Bool
	var wakeCalls atomic.Int32
	var generations atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/wake_up":
			wakeCalls.Add(1)
			w.WriteHeader(200) // Acknowledgement precedes completion.
		case "/is_sleeping":
			fmt.Fprintf(w, "{\"is_sleeping\":%t}", !restored.Load())
		case "/health":
			w.WriteHeader(200)
		default:
			generations.Add(1)
			w.WriteHeader(200)
		}
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, idle: time.Minute, controlTimeout: time.Second})
	g.state = asleep
	server := httptest.NewServer(g)
	defer server.Close()
	defer g.control.CloseIdleConnections()
	result := make(chan error, 1)
	go func() {
		status, err := doPost(server.Client(), server.URL)
		if err == nil && status != 200 { err = fmt.Errorf("HTTP %d", status) }
		result <- err
	}()
	eventually(t, func() bool { return wakeCalls.Load() == 1 })
	if generations.Load() != 0 { t.Fatal("forwarded before restored state") }
	restored.Store(true)
	if err := <-result; err != nil { t.Fatal(err) }
	if generations.Load() != 1 || wakeCalls.Load() != 1 { t.Fatal("unexpected request count") }
}

func TestSleepFailureDoesNotAttemptWake(t *testing.T) {
	var wakes atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/wake_up" { wakes.Add(1) }
		w.WriteHeader(500)
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, idle: time.Millisecond, controlTimeout: time.Second})
	g.state = awake
	g.idleSince = time.Now().Add(-time.Hour)
	server := httptest.NewServer(g)
	defer server.Close()
	defer g.control.CloseIdleConnections()
	if !g.trySleep(time.Now()) { t.Fatal("sleep did not start") }
	eventually(t, func() bool { return phaseIs(g, faulted) })
	status, err := doPost(server.Client(), server.URL)
	if err != nil || status != 503 || wakes.Load() != 0 { t.Fatal("unsafe recovery after failed sleep") }
	if g.trySleep(time.Now()) { t.Fatal("retried failed control operation") }
}

func TestBackgroundRoutesAreRejected(t *testing.T) {
	b := &fakeBackend{}
	_, server := fixture(t, b)
	for _, path := range []string{"/v1/responses", "/v1/responses/example", "/v1/batches"} {
		resp, err := server.Client().Post(server.URL+path, "application/json", strings.NewReader("{}"))
		if err != nil { t.Fatal(err) }
		resp.Body.Close()
		if resp.StatusCode != 501 { t.Fatalf("accepted untracked background endpoint %s", path) }
	}
	if b.wakes.Load() != 0 || b.generations.Load() != 0 { t.Fatal("background request reached backend") }
}

func TestIdleDeadlineAndSingleSleep(t *testing.T) {
	b := &fakeBackend{}
	g, _ := fixture(t, b)
	now := time.Now()
	g.mu.Lock()
	g.state = awake
	g.idleSince = now
	g.mu.Unlock()
	if g.trySleep(now.Add(g.cfg.idle / 2)) { t.Fatal("slept before deadline") }
	if !g.trySleep(now.Add(g.cfg.idle)) { t.Fatal("missed idle deadline") }
	if g.trySleep(now.Add(time.Hour)) { t.Fatal("started duplicate sleep") }
	eventually(t, func() bool { return phaseIs(g, asleep) })
	if b.sleeps.Load() != 1 { t.Fatal("unexpected sleep count") }
}

func TestAwakeInferenceIsConcurrent(t *testing.T) {
	gate := make(chan struct{})
	var entered atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered.Add(1)
		<-gate
		fmt.Fprint(w, "{\"ok\":true}")
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	g := newGateway(settings{upstream: u, idle: time.Millisecond, controlTimeout: time.Second})
	g.state = awake
	server := httptest.NewServer(g)
	defer server.Close()
	defer g.control.CloseIdleConnections()
	defer func() { select { case <-gate: default: close(gate) } }()
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			status, err := doPost(server.Client(), server.URL)
			if err == nil && status != 200 { err = fmt.Errorf("HTTP %d", status) }
			results <- err
		}()
	}
	eventually(t, func() bool { return entered.Load() == 2 && activeIs(g, 2) })
	if g.trySleep(time.Now().Add(time.Hour)) { t.Fatal("slept during concurrent inference") }
	close(gate)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil { t.Fatal(err) }
	}
}
