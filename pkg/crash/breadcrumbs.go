package crash

import (
	"sync"
	"time"
)

type breadcrumbRing struct {
	mu      sync.Mutex
	entries [maxBreadcrumbs]Breadcrumb
	next    int
	count   int
	now     func() time.Time
}

func newBreadcrumbRing() *breadcrumbRing {
	return &breadcrumbRing{now: time.Now}
}

func (r *breadcrumbRing) Record(name string) {
	name, ok := sanitizeBreadcrumbName(name)
	if !ok {
		return
	}

	timestamp := r.timestamp()
	r.mu.Lock()
	r.entries[r.next] = Breadcrumb{Name: name, Timestamp: timestamp}
	r.next = (r.next + 1) % len(r.entries)
	if r.count < len(r.entries) {
		r.count++
	}
	r.mu.Unlock()
}

func (r *breadcrumbRing) Snapshot() []Breadcrumb {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return nil
	}
	out := make([]Breadcrumb, r.count)
	start := 0
	if r.count == len(r.entries) {
		start = r.next
	}
	for i := 0; i < r.count; i++ {
		out[i] = r.entries[(start+i)%len(r.entries)]
	}
	return out
}

func (r *breadcrumbRing) timestamp() time.Time {
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	return now().UTC()
}
