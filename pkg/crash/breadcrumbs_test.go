package crash

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBreadcrumbRingFixedCapacity(t *testing.T) {
	ring := newBreadcrumbRing()
	var tick int64
	ring.now = func() time.Time {
		tick++
		return time.Unix(1700000000+tick, 0)
	}

	for i := 0; i < maxBreadcrumbs+10; i++ {
		ring.Record(fmt.Sprintf("event_%02d", i))
	}

	snapshot := ring.Snapshot()
	if len(snapshot) != maxBreadcrumbs {
		t.Fatalf("expected %d breadcrumbs, got %d", maxBreadcrumbs, len(snapshot))
	}
	if snapshot[0].Name != "event_10" {
		t.Fatalf("expected oldest retained breadcrumb event_10, got %q", snapshot[0].Name)
	}
	if snapshot[len(snapshot)-1].Name != "event_59" {
		t.Fatalf("expected newest retained breadcrumb event_59, got %q", snapshot[len(snapshot)-1].Name)
	}
	for _, breadcrumb := range snapshot {
		if breadcrumb.Timestamp.Location() != time.UTC {
			t.Fatalf("expected UTC timestamp, got %v", breadcrumb.Timestamp.Location())
		}
	}
}

func TestBreadcrumbRingRejectsUnsafeNames(t *testing.T) {
	ring := newBreadcrumbRing()
	ring.Record("screen_open")
	ring.Record("player_raw_identifier")
	ring.Record("sample@example.invalid")
	ring.Record("{\"payload\":true}")

	snapshot := ring.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected only one safe breadcrumb, got %#v", snapshot)
	}
	if snapshot[0].Name != "screen_open" {
		t.Fatalf("expected safe breadcrumb to remain, got %q", snapshot[0].Name)
	}
}

func TestBreadcrumbRingConcurrentRecordAndSnapshot(t *testing.T) {
	ring := newBreadcrumbRing()
	var wg sync.WaitGroup

	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				ring.Record(fmt.Sprintf("event_%02d_%03d", worker, i))
				if i%7 == 0 {
					snapshot := ring.Snapshot()
					if len(snapshot) > maxBreadcrumbs {
						t.Errorf("snapshot length exceeded capacity: %d", len(snapshot))
					}
					for _, breadcrumb := range snapshot {
						if _, ok := sanitizeBreadcrumbName(breadcrumb.Name); !ok {
							t.Errorf("unsafe breadcrumb in snapshot: %q", breadcrumb.Name)
						}
					}
				}
			}
		}(worker)
	}
	wg.Wait()

	snapshot := ring.Snapshot()
	if len(snapshot) != maxBreadcrumbs {
		t.Fatalf("expected full ring of %d breadcrumbs, got %d", maxBreadcrumbs, len(snapshot))
	}
}
