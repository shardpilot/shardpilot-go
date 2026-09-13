package shardpilot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type rejectionHTTP struct {
	mu        sync.Mutex
	batches   [][]eventEnvelope
	failNext  bool
	transform func(*batchResult)
}

func (f *rejectionHTTP) RoundTrip(r *http.Request) (*http.Response, error) {
	status, body := http.StatusAccepted, `{}`
	if r.URL.Path == "/v1/events:batch" {
		var request batchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			return nil, err
		}
		f.mu.Lock()
		f.batches = append(f.batches, request.Events)
		fail := f.failNext
		transform := f.transform
		f.failNext = false
		f.mu.Unlock()
		if fail {
			status = http.StatusServiceUnavailable
		} else {
			result := batchResult{}
			for _, event := range request.Events {
				outcome := batchEventStatusWire{EventID: event.EventID, Status: "accepted"}
				if strings.HasPrefix(event.EventName, "rejected") {
					outcome.Status, outcome.Code, outcome.Message = "rejected", "event_too_large", "configured size limit exceeded"
					result.Rejected++
				} else {
					result.Accepted++
				}
				result.Events = append(result.Events, outcome)
			}
			if transform != nil {
				transform(&result)
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				return nil, err
			}
			body = string(encoded)
		}
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

func (f *rejectionHTTP) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.batches) }

type rejectionLog struct {
	mu  sync.Mutex
	out bytes.Buffer
}

func (l *rejectionLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.out.Write(p)
}
func (l *rejectionLog) text() string { l.mu.Lock(); defer l.mu.Unlock(); return l.out.String() }
func (l *rejectionLog) Printf(format string, args ...any) {
	_, _ = fmt.Fprintf(l, format+"\n", args...)
}

func rejectionClient(t *testing.T, configure func(*Config)) (*Client, *rejectionHTTP) {
	t.Helper()
	fake := &rejectionHTTP{}
	cfg := Config{IngestURL: "https://ingest.example.test", Token: "test-token", WorkspaceID: "workspace-test", AppID: "app-test", EnvironmentID: "develop", Source: SourceBackend, BatchSize: 100, BufferSize: 256, FlushInterval: time.Hour, HTTPTimeout: time.Second, DisableRequestCompression: true, HTTPClient: &http.Client{Transport: fake}}
	if configure != nil {
		configure(&cfg)
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := client.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return client, fake
}

func rejectionContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func rejectionEntries(t *testing.T, c *Client) []BatchEventStatus {
	t.Helper()
	getter, ok := any(c).(interface{ Rejections() []BatchEventStatus })
	if !ok {
		t.Error("public Rejections accessor is missing")
		return nil
	}
	return getter.Rejections()
}
func rejectionCapacity(t *testing.T, cfg *Config, capacity int) {
	t.Helper()
	field := reflect.ValueOf(cfg).Elem().FieldByName("RejectionCapacity")
	if !field.IsValid() {
		t.Error("RejectionCapacity configuration is missing")
		return
	}
	field.SetInt(int64(capacity))
}
func enqueueRejections(t *testing.T, c *Client, rejected int) {
	t.Helper()
	for i := 0; i < rejected; i++ {
		if err := c.Enqueue(Event{ID: fmt.Sprintf("rejected-id-%d", i), Name: fmt.Sprintf("rejected_%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Enqueue(Event{ID: "accepted-sibling", Name: "accepted"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(c.queue.ch) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("worker did not receive the fixture batch")
		}
		runtime.Gosched()
	}
}
func captureRejectionLog(t *testing.T) *rejectionLog {
	t.Helper()
	sink := &rejectionLog{}
	before := log.Writer()
	log.SetOutput(sink)
	t.Cleanup(func() { log.SetOutput(before) })
	return sink
}

func TestRejectionMixedBatchKeepsNilFlushAndHistory(t *testing.T) {
	captureRejectionLog(t)
	client, fake := rejectionClient(t, nil)
	enqueueRejections(t, client, 1)
	if err := client.Flush(rejectionContext(t)); err != nil {
		t.Fatalf("parsed 202 Flush must remain nil: %v", err)
	}
	entries := rejectionEntries(t, client)
	want := BatchEventStatus{EventID: "rejected-id-0", Status: EventStatusRejected, Code: "event_too_large", Message: "configured size limit exceeded"}
	if !reflect.DeepEqual(entries, []BatchEventStatus{want}) {
		t.Errorf("history = %#v, want %#v", entries, want)
	}
	stats := client.Snapshot()
	if stats.Accepted != 1 || stats.Rejected != 1 || stats.FailedBatches != 0 {
		t.Errorf("wrong counters: %+v", stats)
	}
	for _, field := range []string{want.EventID, want.Code, want.Message} {
		if !strings.Contains(stats.LastError, field) {
			t.Errorf("LastError %q omits %q", stats.LastError, field)
		}
	}
	if fake.count() != 1 {
		t.Errorf("mixed fixture dispatched %d batches", fake.count())
	}
}

func TestRejectionDefaultWarning(t *testing.T) {
	sink := captureRejectionLog(t)
	client, _ := rejectionClient(t, nil)
	enqueueRejections(t, client, 1)
	if err := client.Flush(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	output := sink.text()
	if strings.Count(output, "shardpilot event rejected") != 1 {
		t.Errorf("expected one default warning, got %q", output)
	}
	for _, field := range []string{"rejected-id-0", "event_too_large", "configured size limit exceeded"} {
		if !strings.Contains(output, field) {
			t.Errorf("default warning omits %q", field)
		}
	}
}

func TestRejectionCapacityAndCopies(t *testing.T) {
	captureRejectionLog(t)
	client, _ := rejectionClient(t, func(cfg *Config) { rejectionCapacity(t, cfg, 2) })
	enqueueRejections(t, client, 4)
	if err := client.Flush(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	entries := rejectionEntries(t, client)
	if len(entries) != 2 {
		t.Fatalf("history length = %d, want 2", len(entries))
	}
	if entries[0].EventID != "rejected-id-2" || entries[1].EventID != "rejected-id-3" {
		t.Errorf("oldest-first entries = %#v", entries)
	}
	entries[0].EventID = "host mutation"
	if rejectionEntries(t, client)[0].EventID != "rejected-id-2" {
		t.Error("caller changed retained history")
	}
	if client.Snapshot().Rejected != 4 {
		t.Error("eviction changed cumulative rejected count")
	}
}

func TestRejectionTerminalSpoolSettlesOnce(t *testing.T) {
	captureRejectionLog(t)
	directory := t.TempDir()
	var deadMu sync.Mutex
	var dead []SpoolDeadLetter
	client, fake := rejectionClient(t, func(cfg *Config) {
		cfg.SpoolDir = directory
		cfg.OnSpoolDeadLetter = func(entry SpoolDeadLetter) { deadMu.Lock(); defer deadMu.Unlock(); dead = append(dead, entry) }
	})
	client.SetConsent(true)
	fake.mu.Lock()
	fake.failNext = true
	fake.mu.Unlock()
	enqueueRejections(t, client, 1)
	if err := client.Flush(rejectionContext(t)); err == nil {
		t.Fatal("fixture must first fail and spool the batch")
	}
	saved, err := os.ReadFile(filepath.Join(directory, "spool.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"rejected-id-0", "accepted-sibling"} {
		if !bytes.Contains(saved, []byte(id)) {
			t.Fatalf("persisted fixture lacks %s", id)
		}
	}
	if client.Snapshot().Spooled != 2 {
		t.Fatal("both mixed-batch events must actually enter the spool")
	}
	if err := client.Flush(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	if fake.count() != 2 {
		t.Fatalf("terminal outcome retried: %d requests", fake.count())
	}
	deadMu.Lock()
	count := len(dead)
	deadMu.Unlock()
	if count != 1 || client.Snapshot().Accepted != 1 || client.Snapshot().SpoolResent != 0 {
		t.Errorf("dead letters=%d accepted=%d resent=%d; live retry settles one rejection and one acceptance", count, client.Snapshot().Accepted, client.Snapshot().SpoolResent)
	}
	if count == 1 && (dead[0].Reason != SpoolDropTerminal || !containsEventID(t, dead[0].Envelopes, "rejected-id-0") || len(dead[0].Envelopes) != 1) {
		t.Fatal("wrong terminal dead-letter event")
	}
	if err := client.Close(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	restarted, restartedHTTP := rejectionClient(t, func(cfg *Config) { cfg.SpoolDir = directory })
	if err := restarted.Flush(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	if restartedHTTP.count() != 0 {
		t.Fatal("restart replayed terminal events")
	}
}

func (f *rejectionHTTP) setTransform(transform func(*batchResult)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transform = transform
}

type rejectionLoggerFunc func(string, ...any)

func (f rejectionLoggerFunc) Printf(format string, args ...any) { f(format, args...) }

func TestRejectionDefaultCapacityAndLifetime(t *testing.T) {
	captureRejectionLog(t)
	for _, capacity := range []int{0, -1} {
		client, _ := rejectionClient(t, func(cfg *Config) { cfg.RejectionCapacity = capacity; cfg.OnBatchResult = func(BatchResult) {} })
		if client.cfg.RejectionCapacity != 64 {
			t.Errorf("default capacity = %d", client.cfg.RejectionCapacity)
		}
		enqueueRejections(t, client, 65)
		if err := client.Flush(rejectionContext(t)); err != nil {
			t.Fatal(err)
		}
		entries := client.Rejections()
		if len(entries) != 64 {
			t.Fatalf("default history has %d entries, want 64", len(entries))
		}
		if entries[0].EventID != "rejected-id-1" || entries[63].EventID != "rejected-id-64" {
			t.Fatal("default ring order is wrong")
		}
		if client.Snapshot().Rejected != 65 {
			t.Fatal("default eviction reduced the counter")
		}
		if err := client.Close(rejectionContext(t)); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(client.Rejections(), entries) {
			t.Fatal("Close removed inspectable history")
		}
	}
	fresh, _ := rejectionClient(t, nil)
	if len(fresh.Rejections()) != 0 || fresh.Snapshot().Rejected != 0 {
		t.Fatal("history crossed client lifetimes")
	}
}

func TestRejectionWarningDedupKeepsHistory(t *testing.T) {
	sink := captureRejectionLog(t)
	client, _ := rejectionClient(t, nil)
	enqueueRejections(t, client, 12)
	if err := client.Flush(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(sink.text(), "shardpilot event rejected"); got != 10 {
		t.Errorf("same-code warnings=%d, want 10", got)
	}
	if len(client.Rejections()) != 12 || client.Snapshot().Rejected != 12 {
		t.Fatal("warning limit suppressed retention")
	}
}

func TestRejectionWarningCodeBudget(t *testing.T) {
	sink := captureRejectionLog(t)
	client, fake := rejectionClient(t, nil)
	fake.setTransform(func(result *batchResult) {
		for i := range result.Events {
			if i >= 12 {
				result.Events[i].Code = fmt.Sprintf("code-%d", i)
			}
		}
	})
	enqueueRejections(t, client, 90)
	if err := client.Flush(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(sink.text(), "shardpilot event rejected"); got != 73 {
		t.Errorf("warnings=%d, want bounded 73", got)
	}
	if len(client.rejections.warnedCodes) != 64 {
		t.Errorf("retained warning keys=%d", len(client.rejections.warnedCodes))
	}
	if len(client.Rejections()) != 64 || client.Snapshot().Rejected != 90 {
		t.Fatal("warning code limit changed history or counter")
	}
}

func TestRejectionObserverOwnsDiagnosticsAndCannotMutateHistory(t *testing.T) {
	sink := captureRejectionLog(t)
	for _, panics := range []bool{false, true} {
		custom := &rejectionLog{}
		var client *Client
		var seen atomic.Int32
		client, _ = rejectionClient(t, func(cfg *Config) {
			cfg.Logger = custom
			cfg.OnBatchResult = func(result BatchResult) {
				entries := client.Rejections()
				if len(entries) != 2 {
					t.Errorf("observer sees %d retained entries", len(entries))
				}
				if !strings.Contains(client.Snapshot().LastError, "event_too_large") {
					t.Error("observer did not see rejection diagnosis")
				}
				seen.Add(1)
				result.Events[0].EventID = "observer mutation"
				if panics {
					panic("fixture observer panic")
				}
			}
		})
		enqueueRejections(t, client, 2)
		if err := client.Flush(rejectionContext(t)); err != nil {
			t.Fatal(err)
		}
		if seen.Load() != 1 || client.Rejections()[0].EventID != "rejected-id-0" {
			t.Fatal("observer changed or missed retained server values")
		}
		if custom.text() != "" {
			t.Fatal("logger duplicated the configured observer")
		}
	}
	if sink.text() != "" {
		t.Fatal("default logger duplicated the configured observer")
	}
}

func TestRejectionLoggerReplacesDefaultAndCannotAbandonBatch(t *testing.T) {
	sink := captureRejectionLog(t)
	for _, panics := range []bool{false, true} {
		var client *Client
		var calls atomic.Int32
		custom := rejectionLoggerFunc(func(format string, args ...any) {
			if len(client.Rejections()) != 12 {
				t.Error("logger ran before the entire batch was retained")
			}
			if !strings.Contains(fmt.Sprintf(format, args...), "event_too_large") {
				t.Error("custom logger missed event code")
			}
			calls.Add(1)
			if panics {
				panic("fixture logger panic")
			}
		})
		client, _ = rejectionClient(t, func(cfg *Config) { cfg.Logger = custom })
		enqueueRejections(t, client, 12)
		if err := client.Flush(rejectionContext(t)); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 12 || len(client.Rejections()) != 12 {
			t.Fatal("custom diagnostics lost a rejection")
		}
	}
	if sink.text() != "" {
		t.Fatal("default logger duplicated the configured logger")
	}
}

func TestRejectionOtherStatusesStayOut(t *testing.T) {
	sink := captureRejectionLog(t)
	client, fake := rejectionClient(t, nil)
	for _, status := range []string{"accepted", "duplicate", "observed", "suppressed_no_consent", "suppressed_ad_revenue_consent", "unknown_future"} {
		fake.setTransform(func(result *batchResult) { result.Rejected = 0; result.Events[0].Status = status })
		if err := client.Track(rejectionContext(t), Event{ID: "status-" + status, Name: "rejected_status"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(client.Rejections()) != 0 || client.Snapshot().Rejected != 0 || client.Snapshot().LastError != "" || sink.text() != "" {
		t.Fatal("non-rejection entered the rejection channel")
	}
}

func TestRejectionDiagnosticsRedactConfiguredSecrets(t *testing.T) {
	sink := captureRejectionLog(t)
	client, fake := rejectionClient(t, func(cfg *Config) {
		cfg.Token = "synthetic-token-with-\"quote"
		cfg.APIKey = "synthetic-key-with-\\slash"
	})
	message := client.cfg.Token + " / " + client.cfg.APIKey
	fake.setTransform(func(result *batchResult) { result.Events[0].Message = message })
	if err := client.Track(rejectionContext(t), Event{ID: "redacted-id", Name: "rejected_secret_echo"}); err != nil {
		t.Fatal(err)
	}
	if client.Rejections()[0].Message != message {
		t.Fatal("diagnostic redaction modified retained server values")
	}
	for _, output := range []string{sink.text(), client.Snapshot().LastError} {
		if strings.Contains(output, "synthetic-") || strings.Count(output, "[REDACTED]") != 2 {
			t.Errorf("diagnostic did not redact both configured secrets: %q", output)
		}
	}
}

func TestRejectionHistorySurvivesLaterAcceptedResponse(t *testing.T) {
	captureRejectionLog(t)
	client, _ := rejectionClient(t, nil)
	if err := client.Track(rejectionContext(t), Event{Name: "rejected_once"}); err != nil {
		t.Fatal(err)
	}
	before := client.Snapshot().LastError
	if err := client.Track(rejectionContext(t), Event{Name: "accepted"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(rejectionContext(t)); err != nil {
		t.Fatal(err)
	}
	if len(client.Rejections()) != 1 || client.Snapshot().Rejected != 1 || client.Snapshot().LastError != before {
		t.Fatal("later transport success erased rejection history")
	}
}

func TestRejectionConcurrentPublishAndRead(t *testing.T) {
	captureRejectionLog(t)
	var client *Client
	var callbacks atomic.Int32
	client, _ = rejectionClient(t, func(cfg *Config) {
		cfg.RejectionCapacity = 17
		cfg.OnBatchResult = func(BatchResult) { client.Rejections(); client.Snapshot(); callbacks.Add(1) }
	})
	var workers sync.WaitGroup
	done := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-done:
				return
			default:
				entries := client.Rejections()
				if len(entries) > 17 {
					t.Error("concurrent reader exceeded the bound")
				}
				if len(entries) > 0 {
					entries[0].EventID = "reader mutation"
				}
				client.Snapshot()
			}
		}
	}()
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for i := 0; i < 12; i++ {
				if err := client.Track(context.Background(), Event{ID: fmt.Sprintf("concurrent-%d-%d", worker, i), Name: "rejected_concurrent"}); err != nil {
					t.Errorf("Track: %v", err)
				}
			}
		}(worker)
	}
	workers.Wait()
	close(done)
	<-readerDone
	entries := client.Rejections()
	if len(entries) != 17 || client.Snapshot().Rejected != 96 || callbacks.Load() != 96 {
		t.Fatal("concurrent publication lost rejection state")
	}
	seen := make(map[string]bool)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.EventID, "concurrent-") || seen[entry.EventID] {
			t.Fatal("concurrent history corrupt or aliased")
		}
		seen[entry.EventID] = true
	}
}

func TestRejectionDeadLetterOrdering(t *testing.T) {
	for _, path := range []string{"live", "resend"} {
		for _, mode := range []string{"read", "block", "panic"} {
			for _, channel := range []string{"default", "logger", "observer"} {
				t.Run(path+"/"+mode+"/"+channel, func(t *testing.T) {
					sink := captureRejectionLog(t)
					directory := t.TempDir()
					if path == "resend" {
						writeConsentRecordFile(t, directory, "granted")
						writeSpoolRecordFile(t, directory, 0, spoolTestEnvelope(t, "rejected-id-0", time.Now()), spoolTestEnvelope(t, "accepted-sibling", time.Now()))
					}
					type observation struct {
						entries             []BatchEventStatus
						stats               Stats
						warnings, observers int
						reason              SpoolDropReason
					}
					observed := make(chan observation, 1)
					ready := make(chan struct{})
					release := make(chan struct{})
					var releaseOnce sync.Once
					unblock := func() { releaseOnce.Do(func() { close(release) }) }
					var client *Client
					var customWarnings, observers atomic.Int32
					client, fake := rejectionClient(t, func(cfg *Config) {
						cfg.SpoolDir = directory
						cfg.AnonymousID = "anon-spool-1"
						cfg.HTTPClient.Transport.(*rejectionHTTP).setTransform(func(result *batchResult) {
							result.Accepted, result.Rejected = 0, 0
							for i := range result.Events {
								entry := &result.Events[i]
								if entry.EventID == "rejected-id-0" {
									entry.Status, entry.Code, entry.Message = "rejected", "event_too_large", "configured size limit exceeded"
									result.Rejected++
								} else {
									result.Accepted++
								}
							}
						})
						if channel == "logger" {
							cfg.Logger = rejectionLoggerFunc(func(format string, args ...any) {
								if strings.Contains(fmt.Sprintf(format, args...), "shardpilot event rejected") {
									customWarnings.Add(1)
								}
							})
						}
						if channel == "observer" {
							cfg.OnBatchResult = func(BatchResult) { observers.Add(1) }
						}
						cfg.OnSpoolDeadLetter = func(letter SpoolDeadLetter) {
							select {
							case <-ready:
							case <-time.After(time.Second):
								observed <- observation{}
								return
							}
							observed <- observation{client.Rejections(), client.Snapshot(), int(customWarnings.Load()) + strings.Count(sink.text(), "shardpilot event rejected"), int(observers.Load()), letter.Reason}
							if mode == "block" {
								<-release
							}
							if mode == "panic" {
								panic("fixture dead-letter panic")
							}
						}
					})
					close(ready)
					t.Cleanup(unblock)
					if path == "live" {
						client.SetConsent(true)
						fake.mu.Lock()
						fake.failNext = true
						fake.mu.Unlock()
						enqueueRejections(t, client, 1)
						if err := client.Flush(rejectionContext(t)); err == nil {
							t.Fatal("live fixture did not fail before retry")
						}
						if got := len(readSpoolRecordFile(t, directory).Events); got != 2 {
							t.Fatalf("persisted fixture has %d events, want 2", got)
						}
					}
					finished := make(chan error, 1)
					ctx := rejectionContext(t)
					go func() { finished <- client.Flush(ctx) }()
					var atHook observation
					select {
					case atHook = <-observed:
					case <-ctx.Done():
						t.Fatal("terminal dead-letter hook was not reached")
					}
					if atHook.reason != SpoolDropTerminal {
						t.Error("fixture did not reach a terminal settlement")
					}
					if len(atHook.entries) != 1 || atHook.entries[0].EventID != "rejected-id-0" {
						t.Errorf("dead-letter hook saw stale history: %#v", atHook.entries)
					}
					if atHook.stats.Rejected != 1 || atHook.stats.Accepted != 1 {
						t.Errorf("dead-letter hook saw stale counters: %+v", atHook.stats)
					}
					if atHook.warnings != 0 || atHook.observers != 0 {
						t.Error("logger or observer ran before dead-letter settlement finished")
					}
					if mode == "block" {
						if entries := client.Rejections(); len(entries) != 1 {
							t.Errorf("blocked hook prevented retention: %#v", entries)
						}
						if observers.Load() != 0 || customWarnings.Load() != 0 || strings.Contains(sink.text(), "shardpilot event rejected") {
							t.Error("diagnostics ran through a blocked settlement")
						}
						select {
						case <-finished:
							t.Error("Flush completed while the dead-letter hook was blocked")
						default:
						}
					}
					unblock()
					select {
					case err := <-finished:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal("Flush did not finish after releasing the hook")
					}
					entries := client.Rejections()
					if len(entries) != 1 || client.Snapshot().Rejected != 1 {
						t.Fatal("settlement lost or duplicated retention")
					}
					expectedWarnings, expectedObservers := 1, 0
					if channel == "observer" {
						expectedWarnings, expectedObservers = 0, 1
					}
					warnings := int(customWarnings.Load()) + strings.Count(sink.text(), "shardpilot event rejected")
					if warnings != expectedWarnings || int(observers.Load()) != expectedObservers {
						t.Errorf("post-settlement diagnostics warnings=%d observers=%d", warnings, observers.Load())
					}
					if got := len(readSpoolRecordFile(t, directory).Events); got != 0 {
						t.Errorf("settlement left %d spooled events", got)
					}
					expectedResent := uint64(0)
					if path == "resend" {
						expectedResent = 1
					}
					if client.Snapshot().SpoolResent != expectedResent {
						t.Error("fixture did not exercise its named live/resend path")
					}
				})
			}
		}
	}
}
