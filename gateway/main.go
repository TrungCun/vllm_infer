package main

import (
	"bytes"
	"io"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type phase string

const (
	unknown phase = "unknown"
	awake phase = "awake"
	sleeping phase = "sleeping"
	asleep phase = "asleep"
	waking phase = "waking"
	faulted phase = "faulted"
)

// Bound RAM used by each request queued during a sleep/wake transition.
const maxQueuedBody = 32 << 20

type settings struct {
	upstream *url.URL
	idle time.Duration
	controlTimeout time.Duration
}

type gateway struct {
	mu sync.Mutex
	state phase
	changed chan struct{}
	active int
	idleSince time.Time
	sleepDisabled bool
	failure error
	cfg settings
	control *http.Client
	proxy *httputil.ReverseProxy
}

func newGateway(cfg settings) *gateway {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 128
	transport.MaxConnsPerHost = 0
	transport.DisableCompression = true
	g := &gateway{
		state: unknown, idleSince: time.Now(), cfg: cfg,
		control: &http.Client{Transport: transport},
	}
	g.proxy = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) { r.SetURL(cfg.upstream) },
		Transport: transport,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if !isDiscovery(r) {
				g.disableSleep("upstream request did not complete: " + err.Error())
			}
			if r.Context().Err() == nil {
				writeError(w, http.StatusBadGateway, "upstream_error", "vLLM request failed")
			}
		},
	}
	return g
}

func writeError(w http.ResponseWriter, status int, kind, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": message, "type": kind, "param": nil, "code": kind},
	})
}

func (g *gateway) disableSleep(reason string) {
	g.mu.Lock()
	first := !g.sleepDisabled
	g.sleepDisabled = true
	g.mu.Unlock()
	if first {
		log.Printf("auto-sleep disabled: %s; restart vLLM and gateway to reset", reason)
	}
}

func isDiscovery(r *http.Request) bool {
	return r.Method == http.MethodGet && (r.URL.Path == "/v1/models" ||
		strings.HasPrefix(r.URL.Path, "/v1/models/"))
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		http.NotFound(w, r)
		return
	}
	// Background jobs cannot be tracked by HTTP response lifetime.
	if r.URL.Path == "/v1/batches" || strings.HasPrefix(r.URL.Path, "/v1/batches/") ||
		r.URL.Path == "/v1/responses" || strings.HasPrefix(r.URL.Path, "/v1/responses/") {
		writeError(w, http.StatusNotImplemented, "unsupported_endpoint",
			"Background-capable APIs are unsupported; use /v1/chat/completions or /v1/completions")
		return
	}
	// Discovery does not need the GPU and must not reset idle time.
	if isDiscovery(r) {
		g.proxy.ServeHTTP(w, r)
		return
	}
	// Reserve before checking state so a sleep decision cannot miss a request.
	g.mu.Lock()
	g.active++
	ready := g.state == awake
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.active--
		if g.active == 0 {
			g.idleSince = time.Now()
		}
		g.mu.Unlock()
	}()

	// Bound readiness waiting, not a long generation or stream.
	var err error
	if !ready {
		// net/http detects HTTP/1 client disconnects only after reading the
		// complete request body. Consume it before waiting for the shared wake.
		// The awake path keeps streaming uploads directly to the backend.
		if r.Body != nil && r.Body != http.NoBody {
			if r.ContentLength > maxQueuedBody {
				writeError(w, http.StatusRequestEntityTooLarge, "request_too_large",
					"Request body exceeds 32 MiB while waiting for vLLM readiness")
				return
			}
			body := http.MaxBytesReader(w, r.Body, maxQueuedBody)
			controller := http.NewResponseController(w)
			_ = controller.SetReadDeadline(time.Now().Add(g.cfg.controlTimeout))
			payload, readErr := io.ReadAll(body)
			_ = body.Close()
			_ = controller.SetReadDeadline(time.Time{})
			if readErr != nil {
				if r.Context().Err() == nil {
					var tooLarge *http.MaxBytesError
					if errors.As(readErr, &tooLarge) {
						writeError(w, http.StatusRequestEntityTooLarge, "request_too_large",
							"Request body exceeds 32 MiB while waiting for vLLM readiness")
					} else {
						writeError(w, http.StatusBadRequest, "request_body_error",
							"Could not read request body before waiting for vLLM readiness")
					}
				}
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(payload))
		}
		ctx, cancel := context.WithTimeout(r.Context(), g.cfg.controlTimeout)
		err = g.ensureAwake(ctx)
		cancel()
	}
	if err != nil {
		if r.Context().Err() == nil {
			writeError(w, http.StatusServiceUnavailable, "backend_unavailable", err.Error())
		}
		return
	}
	// Wake completion and client cancellation may arrive at the same time.
	if r.Context().Err() != nil {
		return
	}
	defer func() {
		if value := recover(); value != nil {
			g.disableSleep("proxy response interrupted")
			panic(value)
		}
		if r.Context().Err() != nil {
			g.disableSleep("client disconnected before backend completion was confirmed")
		}
	}()
	g.proxy.ServeHTTP(w, r)
}

func (g *gateway) ensureAwake(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("waiting for vLLM readiness: %w", err)
		}
		g.mu.Lock()
		switch g.state {
		case awake:
			g.mu.Unlock()
			return nil
		case faulted:
			err := g.failure
			g.mu.Unlock()
			return err
		case unknown, asleep:
			initial := g.state == unknown
			g.state = waking
			g.changed = make(chan struct{})
			ch := g.changed
			g.mu.Unlock()
			// A disconnected waiter must not cancel the shared wake.
			go g.wake(initial, ch)
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for vLLM readiness: %w", ctx.Err())
			case <-ch:
				g.mu.Lock()
				err := g.failure
				g.mu.Unlock()
				if err != nil { return err }
			}
		case sleeping, waking:
			ch := g.changed
			g.mu.Unlock()
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for vLLM readiness: %w", ctx.Err())
			case <-ch:
				g.mu.Lock()
				err := g.failure
				g.mu.Unlock()
				if err != nil { return err }
			}
		}
	}
}

func (g *gateway) request(ctx context.Context, method, path string) (*http.Response, error) {
	target := *g.cfg.upstream
	relative, err := url.Parse(path)
	if err != nil { return nil, err }
	target.Path = strings.TrimRight(target.Path, "/") + relative.Path
	target.RawQuery = relative.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil { return nil, err }
	resp, err := g.control.Do(req)
	if err != nil { return nil, err }
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("%s %s returned HTTP %d", method, path, resp.StatusCode)
	}
	return resp, nil
}

func (g *gateway) call(ctx context.Context, method, path string) error {
	resp, err := g.request(ctx, method, path)
	if err != nil { return err }
	return resp.Body.Close()
}

func (g *gateway) isSleeping(ctx context.Context) (bool, error) {
	resp, err := g.request(ctx, http.MethodGet, "/is_sleeping")
	if err != nil { return false, err }
	defer resp.Body.Close()
	var body map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil { return false, err }
	value, ok := body["is_sleeping"]
	if !ok || string(value) == "null" { return false, errors.New("missing is_sleeping boolean") }
	var result bool
	if err := json.Unmarshal(value, &result); err != nil { return false, err }
	return result, nil
}

func (g *gateway) waitForState(ctx context.Context, expected bool) error {
	for {
		value, err := g.isSleeping(ctx)
		if err != nil { return err }
		if value == expected { return nil }
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (g *gateway) wake(initial bool, ch chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), g.cfg.controlTimeout)
	defer cancel()
	needsWake := !initial
	var err error
	if initial { needsWake, err = g.isSleeping(ctx) }
	mutated := false
	if err == nil && needsWake {
		mutated = true
		err = g.call(ctx, http.MethodPost, "/wake_up")
		if err == nil { err = g.waitForState(ctx, false) }
	}
	if err == nil { err = g.call(ctx, http.MethodGet, "/health") }
	g.mu.Lock()
	if err == nil {
		g.state = awake
		g.failure = nil
		g.idleSince = time.Now()
	} else if initial && !mutated {
		// Loading may still be in progress. The next request can retry.
		g.state = unknown
	} else {
		g.state = faulted
	}
	if err != nil { g.failure = fmt.Errorf("vLLM readiness failed: %w", err) }
	close(ch)
	g.mu.Unlock()
	if err != nil {
		log.Printf("readiness failed: %v", err)
	} else if needsWake {
		log.Print("vLLM awake")
	}
}

func (g *gateway) trySleep(now time.Time) bool {
	g.mu.Lock()
	if g.cfg.idle <= 0 || g.state != awake || g.sleepDisabled ||
		g.active != 0 || now.Sub(g.idleSince) < g.cfg.idle {
		g.mu.Unlock()
		return false
	}
	g.state = sleeping
	g.failure = nil
	g.changed = make(chan struct{})
	ch := g.changed
	g.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), g.cfg.controlTimeout)
		defer cancel()
		err := g.call(ctx, http.MethodPost, "/sleep?level=1")
		if err == nil { err = g.waitForState(ctx, true) }
		g.mu.Lock()
		if err == nil {
			g.state = asleep
		} else {
			// A timed-out call may still run upstream. Never race another
			// control operation against one with an unknown outcome.
			g.state = faulted
			g.failure = fmt.Errorf("vLLM sleep failed; restart vLLM and gateway: %w", err)
		}
		close(ch)
		g.mu.Unlock()
		if err != nil { log.Printf("sleep failed: %v", err) } else { log.Print("vLLM asleep (level 1)") }
	}()
	return true
}

func (g *gateway) idleLoop(ctx context.Context) {
	if g.cfg.idle <= 0 { return }
	interval := g.cfg.idle / 10
	if interval > time.Second { interval = time.Second }
	if interval < time.Millisecond { interval = time.Millisecond }
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done(): return
		case now := <-ticker.C: g.trySleep(now)
		}
	}
}

func durationEnv(name, fallback string, allowZero bool) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" { value = fallback }
	if allowZero && value == "0" { return 0, nil }
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 || (!allowZero && d == 0) {
		return 0, fmt.Errorf("invalid %s=%q", name, value)
	}
	return d, nil
}

func main() {
	target := os.Getenv("UPSTREAM_URL")
	if target == "" { target = "http://vllm:8000" }
	upstream, err := url.Parse(target)
	if err != nil || upstream == nil || upstream.Host == "" ||
		(upstream.Scheme != "http" && upstream.Scheme != "https") ||
		upstream.User != nil || upstream.RawQuery != "" || upstream.Fragment != "" {
		log.Fatal("UPSTREAM_URL must be an HTTP(S) URL without credentials, query or fragment")
	}
	idle, err := durationEnv("IDLE_TIMEOUT", "30m", true)
	if err != nil { log.Fatal(err) }
	controlTimeout, err := durationEnv("CONTROL_TIMEOUT", "60s", false)
	if err != nil { log.Fatal(err) }
	listen := os.Getenv("LISTEN_ADDR")
	if listen == "" { listen = ":8000" }
	g := newGateway(settings{upstream: upstream, idle: idle, controlTimeout: controlTimeout})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Addr: listen, Handler: g,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout: 120 * time.Second,
	}
	go g.idleLoop(ctx)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("gateway listening on %s; upstream=%s; idle=%s", listen, upstream, idle)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { log.Fatal(err) }
}
