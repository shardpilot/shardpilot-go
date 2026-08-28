// Command experiments records one experiment-assignment exchange made by this
// SDK against a live endpoint: the request as the SDK built it, and the
// response as it came back.
//
// WHY THIS EXISTS, AND WHY curl DOES NOT. Reaching the endpoint with curl shows
// that the endpoint answers. It does not show that THIS SDK reaches it: the
// route it builds, the query it assembles, the Authorization header it sets and
// the transport it uses are the SDK's, and a fault in any of them is invisible
// to a hand-made request. So this program changes nothing in the SDK -- it
// supplies an http.Client whose RoundTripper records what the SDK actually sent
// and what came back, which makes the result an artifact rather than a claim
// that something worked.
//
// The Authorization header is redacted in the output. The key is read from the
// environment and never from a command line, because a command line is visible
// to every process on the host and lands in shell history.
//
//	SP_REMOTE_CONFIG_URL=https://app.shardpilot.com \
//	SP_API_KEY=sp_ingest_... \
//	SP_WORKSPACE_ID=... SP_APP_ID=... SP_ENVIRONMENT_ID=... \
//	SP_EXPERIMENT_KEY=... \
//	go run ./examples/experiments
//
// Exit codes: 0 the pair was captured and the assignment was served; 1 the pair
// was captured and the platform refused it; 2 no request was made at all.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"time"

	shardpilot "github.com/shardpilot/shardpilot-go"
)

// recorder captures the one exchange the SDK performs. It is deliberately not
// a general proxy: a capture that summarises is a capture that can omit the
// thing under test.
// exchange is one attempt. The SDK may make several -- a refusal can send it
// round again with a fresh subject -- and a recorder that keeps only the last
// one reports a multi-attempt sequence as a single request that never happened
// that way.
type exchange struct {
	req      []byte
	resp     []byte
	status   int
	transErr error // set when no response arrived at all
	truncErr error // set when the body could not be read whole
}

type recorder struct {
	inner     http.RoundTripper
	exchanges []exchange
}

// last is the attempt whose verdict the program reports. It is the last one
// BECAUSE that is the one the SDK acted on, not because the others did not
// happen -- every one of them is printed.
func (r *recorder) last() *exchange {
	if len(r.exchanges) == 0 {
		return nil
	}
	return &r.exchanges[len(r.exchanges)-1]
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	ex := exchange{}
	if dump, err := httputil.DumpRequestOut(req, true); err == nil {
		ex.req = redact(dump)
	}

	resp, err := r.inner.RoundTrip(req)
	if err != nil {
		// DNS, TLS, connection setup: the request was formed but nothing came
		// back. Recorded as an attempt WITH NO RESPONSE rather than dropped,
		// because a recorder that keeps the request and loses the failure lets
		// the program print a pair whose second half never existed.
		ex.transErr = err
		r.exchanges = append(r.exchanges, ex)
		return nil, err
	}

	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		// A body that arrived short is not a short body. Handing the SDK the
		// partial bytes as if they were whole would make a truncated response
		// indistinguishable from a complete one -- to the SDK and to this
		// record both. The error goes to the SDK, and the partial bytes are
		// kept in the record so a reader can see how far it got.
		ex.truncErr = readErr
		ex.status = resp.StatusCode
		if d, derr := httputil.DumpResponse(resp, false); derr == nil {
			ex.resp = append(d, body...)
		}
		r.exchanges = append(r.exchanges, ex)
		return nil, readErr
	}

	resp.Body = io.NopCloser(bytes.NewReader(body))
	if d, derr := httputil.DumpResponse(resp, false); derr == nil {
		ex.resp = append(d, body...)
	}
	ex.status = resp.StatusCode
	r.exchanges = append(r.exchanges, ex)
	return resp, nil
}

// redact removes identifying VALUES from the recorded request while keeping
// every NAME and every length.
//
// WHY BY PROPERTY AND NOT BY A LIST OF FIELDS. The first version of this
// program redacted the bearer here and left the query string whole, and the
// identifying query parameters were then shortened BY HAND when the pair was
// pasted into a record -- three of four. The fourth, `subject_key`, was missed,
// and a 38-character subject identifier was published.
//
// A hand-maintained list of fields to shorten does not survive a fifth
// parameter, and neither would a list inside this function. So the rule is a
// PROPERTY: every query-parameter value is treated as identifying and replaced
// by its length. Names and lengths survive, because what this capture proves is
// that the SDK built the right route with the right parameters -- never what any
// particular subject was.
func redactQuery(line string) string {
	i := strings.IndexByte(line, '?')
	if i < 0 {
		return line
	}
	head, rest := line[:i+1], line[i+1:]
	tail := ""
	if j := strings.IndexByte(rest, ' '); j >= 0 {
		rest, tail = rest[:j], rest[j:]
	}
	parts := strings.Split(rest, "&")
	for k, p := range parts {
		eq := strings.IndexByte(p, '=')
		if eq < 0 {
			continue
		}
		parts[k] = p[:eq+1] + fmt.Sprintf("<redacted, %d chars>", len(p[eq+1:]))
	}
	return head + strings.Join(parts, "&") + tail
}

func redact(dump []byte) []byte {
	out := make([]string, 0, 32)
	for _, line := range strings.Split(string(dump), "\n") {
		if strings.HasPrefix(line, "GET ") || strings.HasPrefix(line, "POST ") {
			line = redactQuery(line)
		}
		// The header's PRESENCE is kept -- an absent Authorization header is
		// itself a client-profile defect this capture exists to detect, so
		// replacing the line entirely would hide the failure it is meant to show.
		if strings.HasPrefix(strings.ToLower(line), "authorization:") {
			field := strings.SplitN(line, " ", 2)
			scheme := "<redacted>"
			if len(field) == 2 {
				if parts := strings.SplitN(strings.TrimSpace(field[1]), " ", 2); len(parts) == 2 {
					scheme = parts[0] + " <redacted, " + fmt.Sprint(len(parts[1])) + " chars>"
				}
			}
			line = "Authorization: " + scheme
		}
		out = append(out, line)
	}
	return []byte(strings.Join(out, "\n"))
}

// captureDeadline bounds the SDK, its HTTP client and this program's context
// alike, so a run cannot end on a limit none of them was given.
const captureDeadline = 30 * time.Second

func env(name string) string { return strings.TrimSpace(os.Getenv(name)) }

func main() {
	required := []string{
		"SP_REMOTE_CONFIG_URL", "SP_API_KEY", "SP_WORKSPACE_ID",
		"SP_APP_ID", "SP_ENVIRONMENT_ID", "SP_EXPERIMENT_KEY",
	}
	var missing []string
	for _, name := range required {
		if env(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr,
			"no request made: %s unset.\nThis harness needs a publishable client "+
				"ingest key (sp_ingest_...) scoped for experiment-assignment reads; "+
				"minting one is an owner action, not this program's.\n",
			strings.Join(missing, ", "))
		os.Exit(2)
	}

	rec := &recorder{inner: http.DefaultTransport}
	cfg := shardpilot.Config{
		// The ingest leg is required by the constructor and is NOT exercised
		// here: nothing is tracked and nothing is flushed. It points at the
		// same origin so a misconfiguration cannot silently send events
		// somewhere else.
		IngestURL:          env("SP_REMOTE_CONFIG_URL"),
		Token:              env("SP_API_KEY"),
		WorkspaceID:        env("SP_WORKSPACE_ID"),
		AppID:              env("SP_APP_ID"),
		EnvironmentID:      env("SP_ENVIRONMENT_ID"),
		Source:             shardpilot.SourceClient,
		APIKey:             env("SP_API_KEY"),
		RemoteConfigURL:    env("SP_REMOTE_CONFIG_URL"),
		ExperimentsEnabled: true,
		// Matched to the capture deadline on purpose: leaving the SDK's own
		// timeout at its default lets the two disagree, and a run that ends
		// on whichever fires first records a deadline nobody chose.
		HTTPTimeout: captureDeadline,
		HTTPClient:  &http.Client{Transport: rec, Timeout: captureDeadline},
	}

	client, err := shardpilot.NewClient(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "no request made: %v\n", err)
		os.Exit(2)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = client.Close(closeCtx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), captureDeadline)
	defer cancel()

	result, fetchErr := client.FetchExperimentAssignment(ctx, env("SP_EXPERIMENT_KEY"), nil)

	if len(rec.exchanges) == 0 {
		fmt.Fprintf(os.Stderr,
			"no request made: the SDK returned %v without issuing one, so this "+
				"run says nothing about the endpoint\n", fetchErr)
		os.Exit(2)
	}

	fmt.Printf("# assignment capture — %s\n\n", time.Now().UTC().Format(time.RFC3339))
	if len(rec.exchanges) > 1 {
		fmt.Printf("The SDK made **%d attempts**. All are below; the verdict is the "+
			"last, because that is the one it acted on.\n\n", len(rec.exchanges))
	}
	for i, ex := range rec.exchanges {
		label := ""
		if len(rec.exchanges) > 1 {
			label = fmt.Sprintf(" %d", i+1)
		}
		fmt.Printf("## Request%s, as the SDK sent it\n\n```\n%s\n```\n\n",
			label, strings.TrimRight(string(ex.req), "\r\n"))
		switch {
		case ex.transErr != nil:
			fmt.Printf("## Response%s\n\nNONE — the request was formed and no "+
				"response arrived: %v\n\n", label, ex.transErr)
		case ex.truncErr != nil:
			fmt.Printf("## Response%s — TRUNCATED, and the SDK was told so\n\n"+
				"The body could not be read whole (%v). What arrived is below; it "+
				"is NOT a complete response.\n\n```\n%s\n```\n\n",
				label, ex.truncErr, strings.TrimRight(string(ex.resp), "\r\n"))
		default:
			fmt.Printf("## Response%s\n\n```\n%s\n```\n\n",
				label, strings.TrimRight(string(ex.resp), "\r\n"))
		}
	}

	last := rec.last()
	fmt.Printf("## SDK verdict\n\n")
	fmt.Printf("    attempts: %d\n", len(rec.exchanges))
	fmt.Printf("    status:   %d\n", last.status)
	fmt.Printf("    assigned: %t\n", result.Assigned)
	fmt.Printf("    variant:  %q\n", result.VariantKey)
	fmt.Printf("    reason:   %q\n", result.Reason)
	fmt.Printf("    version:  %d\n", result.Version)
	if fetchErr != nil {
		fmt.Printf("    error:    %v\n", fetchErr)
	}

	switch {
	case last.transErr != nil:
		fmt.Printf("\nNO RESPONSE. The SDK formed the request and nothing came " +
			"back, so this run says nothing about what the endpoint would have " +
			"answered.\n")
		os.Exit(1)
	case last.truncErr != nil:
		fmt.Printf("\nRESPONSE TRUNCATED. The SDK was handed the error rather than " +
			"the partial body, so nothing downstream mistook it for a complete " +
			"answer.\n")
		os.Exit(1)
	case last.status == http.StatusOK && fetchErr == nil:
		fmt.Printf("\nSERVED. The pair above is the capture.\n")
		os.Exit(0)
	case fetchErr != nil && errors.Is(fetchErr, context.DeadlineExceeded):
		fmt.Printf("\nNOT captured — the request timed out.\n")
		os.Exit(1)
	default:
		fmt.Printf("\nNOT served. The SDK reached the endpoint and it answered "+
			"%d; the pair above is what it answered, and it is not a served "+
			"assignment.\n", last.status)
		os.Exit(1)
	}
}
