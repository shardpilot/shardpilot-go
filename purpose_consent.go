package shardpilot

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

// PurposeConsentRequest identifies an actor and purpose with a stable key per decision.
type PurposeConsentRequest struct {
	Purpose, ActorKind, ActorIdentifier, IdempotencyKey string
}

// PurposeConsentWitness identifies the decision state on which a grant is based.
type PurposeConsentWitness struct {
	AuthorizationID string `json:"authorization_id"`
	DecisionSeq     int64  `json:"decision_seq"`
	GraphVersion    int64  `json:"graph_version"`
}

// PurposeConsentGrant adds the evidence and expected state to a grant request.
// Use a zero witness for the first grant, then pass the previous receipt's witness.
type PurposeConsentGrant struct {
	PurposeConsentRequest
	PolicyVersion, EvidenceRef string
	Expected                   PurposeConsentWitness
}

// PurposeConsentReceipt acknowledges a decision and reports any pending withdrawal cleanup.
type PurposeConsentReceipt struct {
	PurposeConsentWitness
	Decision    ConsentDecision `json:"decision"`
	Replayed    bool            `json:"replayed"`
	PurgeStatus string          `json:"purge_status,omitempty"`
}

type purposeConsentWrite struct {
	Purpose         string                 `json:"purpose"`
	ActorKind       string                 `json:"actor_kind"`
	ActorIdentifier string                 `json:"actor_identifier"`
	Decision        ConsentDecision        `json:"decision"`
	IdempotencyKey  string                 `json:"idempotency_key"`
	PolicyVersion   string                 `json:"policy_version,omitempty"`
	EvidenceRef     string                 `json:"evidence_ref,omitempty"`
	Expected        *PurposeConsentWitness `json:"expected,omitempty"`
}

// GrantPurposeConsent records a named grant using a trusted backend credential.
// It waits for acknowledgement and retries using the original grant and key.
func (c *Client) GrantPurposeConsent(ctx context.Context, grant PurposeConsentGrant) (PurposeConsentReceipt, error) {
	return c.writePurposeConsent(ctx, purposeConsentWrite{
		Purpose: grant.Purpose, ActorKind: grant.ActorKind, ActorIdentifier: grant.ActorIdentifier,
		Decision: ConsentDecisionGranted, IdempotencyKey: grant.IdempotencyKey,
		PolicyVersion: grant.PolicyVersion, EvidenceRef: grant.EvidenceRef, Expected: &grant.Expected,
	})
}

// WithdrawPurposeConsent records a named withdrawal and waits for cleanup.
// A pending receipt can accompany a retry error. Resume with the same request and key.
func (c *Client) WithdrawPurposeConsent(ctx context.Context, request PurposeConsentRequest) (PurposeConsentReceipt, error) {
	return c.writePurposeConsent(ctx, purposeConsentWrite{
		Purpose: request.Purpose, ActorKind: request.ActorKind, ActorIdentifier: request.ActorIdentifier,
		Decision: ConsentDecisionDenied, IdempotencyKey: request.IdempotencyKey,
	})
}

func (c *Client) writePurposeConsent(ctx context.Context, command purposeConsentWrite) (PurposeConsentReceipt, error) {
	var pending PurposeConsentReceipt
	if c == nil || c.queue == nil {
		return pending, ErrNotInitialized
	}
	c.lifecycleMu.Lock()
	if c.closed.Load() {
		c.lifecycleMu.Unlock()
		return pending, ErrConsentShutdown
	}
	c.consentDecisionsWG.Add(1)
	c.lifecycleMu.Unlock()
	defer c.consentDecisionsWG.Done()
	if c.cfg.Source == SourceClient {
		return pending, errors.New("shardpilot purpose consent requires a trusted service credential")
	}
	if strings.TrimSpace(command.IdempotencyKey) == "" {
		return pending, errors.New("shardpilot purpose consent requires an idempotency key")
	}
	transport, ok := c.transport.(*httpTransport)
	if !ok {
		return pending, ErrNotInitialized
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return pending, &EncodeError{Err: err}
	}
	if len(payload) > 8192 {
		return pending, errors.New("shardpilot purpose consent request exceeds 8192 bytes")
	}
	ctx, cancel := contextWithDefaultTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	for attempt := 1; ; attempt++ {
		receipt, err := transport.postPurposeConsent(ctx, payload, command.Decision)
		if err == nil {
			return receipt, nil
		}
		if receipt.PurgeStatus == "pending" {
			pending = receipt
		}
		if ctx.Err() != nil {
			return pending, ctx.Err()
		}
		if attempt == 3 || !consentDeliveryRetryable(err) {
			return pending, err
		}
		delay := time.Duration(rand.Float64() * float64(backoffCeiling(attempt)))
		var statusErr *HTTPStatusError
		if errors.As(err, &statusErr) && statusErr.retryAfterPresent {
			delay = statusErr.RetryAfter
		}
		if delay >= time.Until(deadline) {
			return pending, err
		}
		select {
		case <-ctx.Done():
			return pending, ctx.Err()
		case <-time.After(delay):
		}
	}
}

const purposeConsentMediaType = "application/vnd.shardpilot.purpose-decision+json"

func (t *httpTransport) postPurposeConsent(ctx context.Context, payload []byte, decision ConsentDecision) (PurposeConsentReceipt, error) {
	var receipt PurposeConsentReceipt
	ctx, cancel := contextWithDefaultTimeout(ctx, t.attemptTimeout)
	defer cancel()
	request, err := t.newIngestRequest(ctx, t.consentEndpoint, purposeConsentMediaType, payload)
	if err != nil {
		return receipt, err
	}
	response, err := t.client.Do(request)
	if err != nil {
		return receipt, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		statusErr, body := readHTTPStatusError(response)
		if decision == ConsentDecisionDenied && statusErr.StatusCode == http.StatusServiceUnavailable && statusErr.ErrorCode == "withdrawal_purge_pending" {
			var wire struct {
				Receipt PurposeConsentReceipt `json:"receipt"`
			}
			if json.Unmarshal(body, &wire) == nil && wire.Receipt.valid(decision, "pending") {
				receipt = wire.Receipt
			}
		}
		return receipt, statusErr
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes+1))
	purgeStatus := ""
	if decision == ConsentDecisionDenied {
		purgeStatus = "completed"
	}
	if err != nil || len(body) > maxErrorBodyBytes || json.Unmarshal(body, &receipt) != nil || !receipt.valid(decision, purgeStatus) {
		return PurposeConsentReceipt{}, errors.New("shardpilot purpose consent acknowledgement is unreadable or inconsistent")
	}
	return receipt, nil
}

func (r PurposeConsentReceipt) valid(decision ConsentDecision, purgeStatus string) bool {
	return r.Decision == decision && r.PurgeStatus == purgeStatus && strings.TrimSpace(r.AuthorizationID) != "" && r.DecisionSeq > 0 && r.GraphVersion >= 0
}
