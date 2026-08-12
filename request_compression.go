package shardpilot

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
)

// compressionMinimumBytes is the body size below which a publish is sent
// uncompressed.
//
// gzip costs a fixed 18 bytes of framing (10-byte header, 8-byte trailer)
// before the deflate stream's own block overhead, so a small body can come out
// LARGER compressed — and the CPU is spent either way. A single-event batch is
// around 400 bytes; a full 100-event batch is tens of kilobytes and compresses
// to a few percent of its size, because a batch is the same envelope keys
// repeated N times, which is close to the best case deflate has. The threshold
// sits above the single-event shape and well below anything the flush worker
// produces at a real event rate, so the SDK compresses exactly the batches
// where compression pays.
const compressionMinimumBytes = 1024

// gzipWriterPool reuses deflate windows across publishes. A gzip.Writer holds
// a ~256 KiB compression state; allocating one per flush would trade the
// network win for allocation churn in the caller's process, which for a game
// SDK is the cost that actually shows up.
var gzipWriterPool = sync.Pool{
	New: func() any {
		// BestSpeed, not DefaultCompression. Measured on real batch bodies the
		// two land within a couple of percent of each other (batch JSON is
		// dominated by repeated envelope keys, which deflate catches at any
		// level), while BestSpeed costs a fraction of the CPU. An SDK
		// embedded in a game frame budget takes that trade every time.
		writer, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return writer
	},
}

// compressRequestBody gzip-compresses payload and reports whether the result
// is worth sending. It returns ok=false — leaving the caller on the
// uncompressed path — when the body is below the threshold, or when
// compression did not actually shrink it. The second case is not theoretical:
// a body that is already high-entropy comes back larger, and sending it would
// cost bytes AND CPU to make the request worse.
func compressRequestBody(payload []byte) ([]byte, bool) {
	if len(payload) < compressionMinimumBytes {
		return nil, false
	}

	writer, _ := gzipWriterPool.Get().(*gzip.Writer)
	defer func() {
		// Point the writer at a sink that holds nothing BEFORE pooling it.
		// Close does not release the destination — gzip.Writer keeps it in
		// its own field and the flate compressor keeps it again underneath —
		// so returning the writer as-is keeps `buf` and its backing array
		// reachable for as long as the writer sits idle in the pool. The
		// buffer is pre-grown to half the payload, so one large publish would
		// pin megabytes per pooled writer, multiplied by concurrency.
		writer.Reset(io.Discard)
		gzipWriterPool.Put(writer)
	}()

	var buf bytes.Buffer
	buf.Grow(len(payload) / 2)
	writer.Reset(&buf)
	if _, err := writer.Write(payload); err != nil {
		return nil, false
	}
	if err := writer.Close(); err != nil {
		return nil, false
	}

	if buf.Len() >= len(payload) {
		return nil, false
	}

	return buf.Bytes(), true
}

// serverRefusedCompression reports whether a non-2xx response is the server
// saying it cannot read this request's content coding.
//
// The match is on the ingest contract's DETAIL CODES, never on the bare 400.
// That precision is the whole point: a generic status match would let any
// ordinary validation failure — a bad event name, a scope mismatch — silently
// switch compression off for the rest of the process and hide the real defect
// behind a transport change nobody asked for.
//
//   - unsupported_content_encoding: the deployment predates gzip acceptance.
//   - invalid_content_encoding: the bytes arrived unreadable — this SDK's
//     compressor or something between here and the server mangling the body.
//     Uncompressed is the one thing that reliably gets the events through.
func serverRefusedCompression(err error) bool {
	statusErr, ok := err.(*HTTPStatusError)
	if !ok {
		return false
	}
	for _, detail := range statusErr.Details {
		switch detail.Code {
		case "unsupported_content_encoding", "invalid_content_encoding":
			return true
		}
	}

	return false
}
