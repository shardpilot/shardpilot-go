package shardpilot

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	cfg       Config
	clock     Clock
	queue     *boundedQueue
	transport transport
	stats     statsCollector

	flushRequests chan flushRequest
	stop          chan struct{}
	workerDone    chan struct{}
	lifecycleMu   sync.Mutex
	trackWG       sync.WaitGroup
	closeOnce     sync.Once
	closed        atomic.Bool
}

type flushRequest struct {
	ctx   context.Context
	reply chan error
}

func NewClient(cfg Config) (*Client, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	client := &Client{
		cfg:           normalized,
		clock:         realClock{},
		queue:         newBoundedQueue(normalized.BufferSize),
		transport:     newHTTPTransport(normalized),
		flushRequests: make(chan flushRequest),
		stop:          make(chan struct{}),
		workerDone:    make(chan struct{}),
	}

	go client.run()
	return client, nil
}

func (c *Client) Track(ctx context.Context, event Event) error {
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return ErrClosed
	}
	c.trackWG.Add(1)
	c.lifecycleMu.Unlock()
	defer c.trackWG.Done()

	return c.publish(ctx, []Event{cloneEvent(event)})
}

func (c *Client) Enqueue(event Event) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.closed.Load() {
		return ErrClosed
	}
	if !c.queue.enqueue(cloneEvent(event)) {
		c.stats.dropped.Add(1)
		return ErrQueueFull
	}
	c.stats.enqueued.Add(1)
	return nil
}

func (c *Client) Flush(ctx context.Context) error {
	reply := make(chan error, 1)
	request := flushRequest{ctx: ctx, reply: reply}

	select {
	case c.flushRequests <- request:
	case <-c.workerDone:
		return ErrClosed
	case <-contextDone(ctx):
		return contextCause(ctx)
	}

	select {
	case err := <-reply:
		return err
	case <-c.workerDone:
		return ErrClosed
	case <-contextDone(ctx):
		return contextCause(ctx)
	}
}

func (c *Client) Close(ctx context.Context) error {
	var flushErr error
	c.closeOnce.Do(func() {
		c.lifecycleMu.Lock()
		c.closed.Store(true)
		c.lifecycleMu.Unlock()

		c.trackWG.Wait()
		flushErr = c.Flush(ctx)
		close(c.stop)
	})

	select {
	case <-c.workerDone:
	case <-contextDone(ctx):
		return contextCause(ctx)
	}
	return flushErr
}

func (c *Client) Snapshot() Stats {
	return c.stats.snapshot()
}

func (c *Client) run() {
	defer close(c.workerDone)

	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]Event, 0, c.cfg.BatchSize)
	for {
		queueEvents := c.queue.ch
		if len(batch) >= c.cfg.BatchSize {
			queueEvents = nil
		}
		select {
		case event := <-queueEvents:
			batch = append(batch, event)
			if len(batch) >= c.cfg.BatchSize {
				batch = c.publishWorkerBatch(batch)
			}
		case <-ticker.C:
			batch = c.publishWorkerBatch(batch)
		case request := <-c.flushRequests:
			var err error
			batch, err = c.flushAvailable(request.ctx, batch)
			request.reply <- err
		case <-c.stop:
			_, _ = c.flushAvailable(context.Background(), batch)
			return
		}
	}
}

func (c *Client) flushAvailable(ctx context.Context, batch []Event) ([]Event, error) {
	for {
		if len(batch) > 0 {
			if err := c.publishBatchWithContext(ctx, batch); err != nil {
				return batch, err
			}
			batch = batch[:0]
		}
		batch = c.queue.drainInto(batch, c.cfg.BatchSize)
		if len(c.queue.ch) == 0 {
			if len(batch) == 0 {
				return batch, nil
			}
			continue
		}
	}
}

func (c *Client) publishWorkerBatch(batch []Event) []Event {
	if len(batch) == 0 {
		return batch
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.HTTPTimeout)
	defer cancel()
	if err := c.publishBatchWithContext(ctx, batch); err != nil {
		return batch
	}
	return batch[:0]
}

func (c *Client) publish(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	return c.publishBatchWithContext(ctx, events)
}

func (c *Client) publishBatchWithContext(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	ctx, cancel := contextWithDefaultTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()

	request, err := c.buildBatch(events)
	if err != nil {
		c.stats.recordFailure(err)
		return err
	}
	result, err := c.transport.Publish(ctx, request)
	if err != nil {
		c.stats.recordFailure(err)
		if c.cfg.Logger != nil {
			c.cfg.Logger.Printf("shardpilot batch publish failed: %v", err)
		}
		return err
	}
	c.stats.recordBatch(result, len(events))
	return nil
}

func contextDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func contextCause(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}

func cloneEvent(event Event) Event {
	event.Props = cloneMap(event.Props)
	event.Context = cloneMap(event.Context)
	return event
}
