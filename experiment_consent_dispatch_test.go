package shardpilot

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPointConsentDropsAfterBlockingDispatch(t *testing.T) {
	for _, mode := range []string{"worker_receipt", "flush_receipt", "flush_spool", "worker_publish"} {
		t.Run(mode, func(t *testing.T) {
			script := &expScript{}
			script.push(http.StatusOK, ageGolden(t, "adult"))
			script.apply.assignmentKey, script.outcome.assignmentKey = ageGoldenFactKey(t), ageGoldenFactKey(t)
			script.outcome.outcome = true
			capture := &expWireCapture{}
			var armed, crossed atomic.Bool
			entered, gate := make(chan struct{}), make(chan struct{})
			canceled := make(chan error, 1)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate) }) }
			defer release()
			block := func(r *http.Request) {
				if armed.Load() && crossed.CompareAndSwap(false, true) {
					if mode == "worker_publish" {
						// The server observes disconnects after consuming the body.
						_, _ = io.Copy(io.Discard, r.Body)
					}
					close(entered)
					select {
					case <-gate:
					case <-r.Context().Done():
					}
					if mode == "worker_publish" {
						canceled <- r.Context().Err()
					}
				}
			}
			mux := http.NewServeMux()
			mux.HandleFunc(expAssignmentRoute, script.handler(t))
			mux.HandleFunc(expExposureApplyRoute, script.apply.handler(t))
			mux.HandleFunc(expOutcomeApplyRoute, script.outcome.handler(t))
			mux.HandleFunc("/v1/consent", func(w http.ResponseWriter, r *http.Request) {
				if mode == "worker_receipt" || mode == "flush_receipt" {
					block(r)
				}
				w.WriteHeader(http.StatusAccepted)
			})
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				if mode == "flush_spool" || mode == "worker_publish" {
					block(r)
				}
				if mode == "worker_publish" && armed.Load() && crossed.Load() {
					return // only return after observing cancellation; a live return would acknowledge with HTTP 200
				}
				capture.handler(t)(w, r)
			})
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)
			spool := ""
			if mode == "flush_spool" {
				spool = t.TempDir()
			}
			rig := &ageWithdrawRig{script: script, capture: capture, server: server, spool: spool}
			rig.client = rig.launch(t, func(cfg *Config) {
				cfg.BatchSize = 8
				cfg.HTTPTimeout = 5 * time.Second // the fixture deliberately blocks one request
				if mode != "flush_spool" {
					cfg.ConsentFloor = &ConsentFloorConfig{}
				}
			})
			client := rig.client
			if _, err := client.SetConsent(true); err != nil {
				t.Fatal(err)
			}
			flushOrFail(t, client)
			fetchAdultAssignment(t, client, ageGoldenExperiment)
			client.ApplyExperimentVariant(ageGoldenExperiment)
			client.experimentCycle(context.Background())
			flushOrFail(t, client)
			if len(deliveredExperimentFacts(capture, ageGoldenExperiment)) != 1 {
				t.Fatal("the automatic exposure control must really settle")
			}
			if err := client.TrackExperimentExposure(ageGoldenExperiment); err != nil {
				t.Fatal(err)
			}
			if err := client.TrackExperimentOutcome(ageGoldenExperiment, "score", 1); err != nil {
				t.Fatal(err)
			}
			client.experimentCycle(context.Background())
			waitFor(t, 5*time.Second, "both actual point facts held by the worker", func() bool { return len(client.queue.ch) == 0 && client.expPipelineLow.Load() == 2 })
			if client.owedExperimentExposureCount() != 0 {
				t.Fatal("both actual point facts must have left the owed owner")
			}
			if mode == "flush_spool" {
				// Evict the durable copies with an actual failed ordinary
				// publish. The worker then owns the point facts' only copies.
				ordinary, err := client.prepareEvent(Event{Name: "ordinary.dispatch.block"})
				if err != nil {
					t.Fatal(err)
				}
				request, err := client.buildBatch([]Event{ordinary})
				if err != nil {
					t.Fatal(err)
				}
				capture.setStatus(http.StatusInternalServerError)
				_, publishErr := client.publishRequestResult(context.Background(), request, 1)
				if publishErr == nil {
					t.Fatal("the ordinary fixture must really fail")
				}
				client.spool.mu.Lock()
				client.spool.maxEvents = 1
				client.spool.mu.Unlock()
				client.spoolFailedBatch(request, publishErr, false)
				if client.Snapshot().SpoolEvicted != 2 || !applicationTestHasSpoolID(client, ordinary.ID) || spooledExperimentFacts(client, ageGoldenExperiment) != 0 {
					t.Fatal("capacity must actually retire both durable point copies and keep the ordinary retry")
				}
				capture.setStatus(http.StatusAccepted)
			} else if mode != "worker_publish" {
				waitFor(t, 5*time.Second, "initial receipt wake consumed", func() bool { return len(client.consentWake) == 0 })
				receipt := testConsentReceipt("018fbc8e-7d40-7b41-9c17-55cb93fb2281", true)
				receipt.AppID, receipt.ActorIdentifier, receipt.AnonymousID = "exposure-app", "anon-test", "anon-test"
				receipt.DecidedAt = client.clock.Now().UTC().Format(time.RFC3339Nano)
				if client.consentOutbox.append(receipt) {
					t.Fatal("the in-scope blocking receipt must be retained")
				}
			}
			armed.Store(true)
			done := make(chan error, 1)
			if mode == "worker_receipt" || mode == "worker_publish" {
				client.wakeConsentDispatch()
			} else {
				go func() { done <- client.Flush(context.Background()) }()
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the actual dispatch leg did not reach its blocking endpoint")
			}
			if _, err := client.SetConsent(false); err != nil {
				t.Fatal(err)
			}
			if mode == "worker_publish" {
				select {
				case err := <-canceled:
					if err != context.Canceled {
						t.Fatalf("publish handler returned before cancellation: %v", err)
					}
				case <-time.After(3 * time.Second): // below HTTPTimeout; a timeout cannot stand in for denial
					t.Fatal("consent denial did not cancel the actual worker publish")
				}
			}
			release()
			if mode != "worker_receipt" && mode != "worker_publish" {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("the denied flush did not return")
				}
			}
			flushOrFail(t, client)
			stats := client.Snapshot()
			t.Logf("dispatch=%s dropped=%d exposure=%v outcome=%v", mode, stats.Dropped, stats.ExperimentExposureDrops, stats.ExperimentOutcomeDrops)
			if stats.Dropped != 2 || stats.ExperimentExposureDrops[expDropConsentWithdrawn] != 1 || stats.ExperimentOutcomeDrops[expDropConsentWithdrawn] != 1 {
				t.Error("post-dispatch consent loss missed a worker-held point application")
			}
			if len(deliveredExperimentFacts(capture, ageGoldenExperiment)) != 1 {
				t.Error("a denied point fact reached analytics beside the settled automatic control")
			}
			if mode == "worker_publish" && (stats.Published != 1 || stats.UnparsedBatchResponses != 0) {
				t.Errorf("the blocked publish was acknowledged: published=%d unparsed=%d", stats.Published, stats.UnparsedBatchResponses)
			}
		})
	}
}
