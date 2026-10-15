package shardpilot

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestConsentQueueDrainRetiresItsFactKey(t *testing.T) {
	for _, durable := range []bool{false, true} {
		for _, workerSettled := range []bool{false, true} {
			name := "consent_drain"
			if workerSettled {
				name = "worker_settlement_control"
			}
			if durable {
				name = "durable/" + name
			} else {
				name = "memory/" + name
			}
			t.Run(name, func(t *testing.T) {
				spool := ""
				if durable {
					spool = t.TempDir()
				}
				rig := newAgeWithdrawRig(t, spool, func(cfg *Config) {
					cfg.BatchSize, cfg.BufferSize = 1, 1
				}, ageGolden(t, "adult"), ageRefusalBody(t, "age_ineligible"))
				fetchAdultAssignment(t, rig.client, ageGoldenExperiment)
				parkWorkerWithFullQueue(t, rig.client, rig.capture)
				if n := rig.client.queue.drainAll(); n != 1 {
					t.Fatalf("setup must remove only the queued ordinary filler: %d", n)
				}
				if variant, _ := rig.client.ApplyExperimentVariant(ageGoldenExperiment); variant != "control" {
					t.Fatal("the actual host application must be served")
				}
				rig.client.experimentCycle(context.Background())
				seq := rig.client.expFactSeq.Load()
				if seq == 0 || len(rig.client.queue.ch) != 1 || len(rig.script.apply.requestsSoFar()) != 1 {
					t.Fatal("the actual sealed fact must wait in the queue behind the held worker batch")
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if workerSettled {
					rig.capture.setStatus(http.StatusAccepted)
					flushOrFail(t, rig.client)
					if len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 1 {
						t.Fatal("the passing control must actually deliver its fact through the worker")
					}
				}
				rig.refuse(t, "age_ineligible")
				if !workerSettled && (len(factKeyHistoryInMemory(rig.client, ageGoldenExperiment)) != 1 || len(keyWithdrawalKeys(rig.client, ageGoldenExperiment)) != 1) {
					t.Fatal("the queued fact must actually keep its retired history and withdrawal key before the drain")
				}
				rig.client.SetConsent(false)
				_ = rig.client.Flush(ctx)
				if len(rig.client.queue.ch) != 0 || rig.client.owedExperimentExposureCount() != 0 {
					t.Fatal("no queued or owed fact may remain after the real consent denial")
				}
				rig.client.pruneExperimentFactKeys()
				history := factKeyHistoryInMemory(rig.client, ageGoldenExperiment)
				withdrawals := keyWithdrawalKeys(rig.client, ageGoldenExperiment)
				e := rig.client.exp
				e.mu.Lock()
				live := e.queuedFactSeq[ageGoldenExperiment][ageGoldenFactKey(t)]
				e.mu.Unlock()
				t.Logf("sealed sequence=%d worker low=%d queue=0 owed=0 live sequence=%d history=%d withdrawal keys=%d delivered=%d", seq, rig.client.expPipelineLow.Load(), live, len(history), len(withdrawals), len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)))
				if len(history) != 0 || len(withdrawals) != 0 || live != 0 {
					t.Errorf("the consent drain retained fact-key bookkeeping after the last fact was gone")
				}
				if durable && len(persistedFactKeyHistory(t, spool, ageGoldenExperiment)) != 0 {
					t.Error("the retired key remained in the actual durable history after the drain")
				}
				if !workerSettled && len(deliveredExperimentFacts(rig.capture, ageGoldenExperiment)) != 0 {
					t.Fatal("the withdrawn subject fact reached analytics")
				}
			})
		}
	}
}

func TestQueueDrainMarkKeepsTheEarlierWorkerMember(t *testing.T) {
	queue := newBoundedQueue(2)
	queue.enqueue(Event{expFactSeq: 3})
	if queue.drainAll() != 1 {
		t.Fatal("the sequence must actually leave the queue")
	}
	client := &Client{queue: queue, exp: &experimentsState{}, workerLastFactSeq: 1}
	client.publishExpPipelineLow([]Event{{expFactSeq: 1}, {Name: "ordinary.held"}})
	if low := client.expPipelineLow.Load(); low != 1 {
		t.Errorf("a drained later fact hid the earlier worker member: low=%d", low)
	}
	client.publishExpPipelineLow(nil)
	if low := client.expPipelineLow.Load(); low != 4 {
		t.Errorf("an empty worker forgot the queue's retired sequence: low=%d", low)
	}
	client.workerLastFactSeq = 5
	client.publishExpPipelineLow([]Event{{expFactSeq: 5}})
	if low := client.expPipelineLow.Load(); low != 5 {
		t.Errorf("fresh worker-held work crossed the low-water mark: low=%d", low)
	}
	client.publishExpPipelineLow(nil)
	if low := client.expPipelineLow.Load(); low != 6 {
		t.Errorf("the newer settled worker sequence did not advance: low=%d", low)
	}
}

func TestOrdinaryQueueDrainKeepsTheRetiredFactMark(t *testing.T) {
	queue := newBoundedQueue(2)
	queue.enqueue(Event{expFactSeq: 3})
	queue.drainAll()
	queue.enqueue(Event{Name: "ordinary.after"})
	queue.drainAll()
	client := &Client{queue: queue, exp: &experimentsState{}}
	client.publishExpPipelineLow(nil)
	if low := client.expPipelineLow.Load(); low != 4 {
		t.Errorf("an ordinary drain reset the retired fact mark: low=%d", low)
	}
}
