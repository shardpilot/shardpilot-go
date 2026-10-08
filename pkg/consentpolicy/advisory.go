package consentpolicy

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Advisory is the plan's optional advisory part: the region matrix's research
// estimate for the connection's jurisdiction, served BESIDE the plan.
//
// ⚠ IT IS NOT A REGIME, AND NOTHING IN THIS PACKAGE READS IT AS ONE. The
// resolver serves it only to a request that asked for it (`advisory: true`,
// sent by a client SDK, never by this package), only for a workspace whose
// owner accepted the advisory terms, and always beside a plan whose regime is
// unchanged. Its estimate comes verbatim from a matrix row that is an
// unreviewed draft pending counsel, and RowStatus and RowBasis say so on every
// copy. Regime, the purpose helpers and every Decision method ignore it; it
// is carried so the integrating studio, as controller, can see what the
// resolver said and decide for itself what, if anything, to do with it.
//
// An advisory that does not match the contract makes the WHOLE plan
// unreadable, by the same rule as every other member: a plan this package
// cannot fully read is a plan it cannot act on.
type Advisory struct {
	// Jurisdiction is an ISO 3166-1 alpha-2 code, or OTHER when the connection
	// did not resolve or has no row of its own.
	Jurisdiction string `json:"jurisdiction"`
	// Estimate is the row's estimate. NIL MEANS THE ROW CARRIES NONE, and it
	// is always nil for OTHER. A pointer because the wire sends a present
	// null for that case, which is a different fact from an absent key.
	Estimate *AdvisoryEstimate `json:"estimate"`
	// RowID names the matrix row the estimate came from.
	RowID     string            `json:"row_id"`
	RowStatus AdvisoryRowStatus `json:"row_status"`
	RowBasis  AdvisoryRowBasis  `json:"row_basis"`
	// AdvisoryBasis is the row's basis text (Markdown) in the matrix's own
	// words, markers such as `contested` included, with the source's internal
	// references removed. It is for a human to read.
	AdvisoryBasis string             `json:"advisory_basis"`
	Matrix        AdvisoryMatrix     `json:"matrix"`
	ResolvedBy    AdvisoryResolvedBy `json:"resolved_by"`
}

// AdvisoryMatrix names the matrix version the estimate came from.
type AdvisoryMatrix struct {
	DocsCommit string `json:"docs_commit"`
	FileSHA256 string `json:"file_sha256"`
	// Date is a calendar date, YYYY-MM-DD.
	Date string `json:"date"`
}

// AdvisoryEstimate is an estimate's value.
//
// ⚠ A TYPE OF ITS OWN, NOT Regime, although the two spellings coincide. An
// estimate assignable where a Regime is expected is one forgotten conversion
// away from deciding a regime, which is the one thing it must never do.
type AdvisoryEstimate string

const (
	AdvisoryEstimateSoftOptOut  AdvisoryEstimate = "SOFT_OPT_OUT"
	AdvisoryEstimateStrictOptIn AdvisoryEstimate = "STRICT_OPT_IN"
)

func (e AdvisoryEstimate) known() bool {
	return e == AdvisoryEstimateSoftOptOut || e == AdvisoryEstimateStrictOptIn
}

// AdvisoryRowStatus is the review state of the row. One value: every row is
// pending counsel.
type AdvisoryRowStatus string

const AdvisoryRowCounselPending AdvisoryRowStatus = "COUNSEL_PENDING"

func (s AdvisoryRowStatus) known() bool { return s == AdvisoryRowCounselPending }

// AdvisoryRowBasis is where the row came from. One value: every row is an AI
// draft.
type AdvisoryRowBasis string

const AdvisoryRowBasisAIDraft AdvisoryRowBasis = "ai_draft"

func (b AdvisoryRowBasis) known() bool { return b == AdvisoryRowBasisAIDraft }

// AdvisoryResolvedBy says how the jurisdiction was reached.
type AdvisoryResolvedBy string

const (
	// AdvisoryResolvedByServerCountry: the resolver located the connection.
	AdvisoryResolvedByServerCountry AdvisoryResolvedBy = "server_country"
	// AdvisoryResolvedByUnknown: it could not, and the jurisdiction is OTHER.
	AdvisoryResolvedByUnknown AdvisoryResolvedBy = "unknown"
)

func (r AdvisoryResolvedBy) known() bool {
	return r == AdvisoryResolvedByServerCountry || r == AdvisoryResolvedByUnknown
}

// AdvisoryJurisdictionOther is the jurisdiction of a connection that did not
// resolve, or resolved to a country without a row of its own.
const AdvisoryJurisdictionOther = "OTHER"

// The contract's own bounds and patterns.
const maxAdvisoryBasisBytes = 2048

var (
	jurisdictionPattern = regexp.MustCompile(`^(?:[A-Z]{2}|OTHER)$`)
	commitPattern       = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256Pattern       = regexp.MustCompile(`^[0-9a-f]{64}$`)
	controlPattern      = regexp.MustCompile(`[\x00-\x1f\x7f]`)
)

func (a Advisory) validate() error {
	if !jurisdictionPattern.MatchString(a.Jurisdiction) {
		return errors.New("consentpolicy: advisory.jurisdiction is neither a two-letter code nor OTHER")
	}
	if !jurisdictionPattern.MatchString(a.RowID) {
		return errors.New("consentpolicy: advisory.row_id is neither a two-letter code nor OTHER")
	}
	if a.Estimate != nil && !a.Estimate.known() {
		return fmt.Errorf("consentpolicy: unknown advisory.estimate %q", string(*a.Estimate))
	}
	// The contract states both of these in its own words: OTHER never carries
	// an estimate, and an unresolved connection is OTHER. A body that says
	// otherwise is not one the resolver sends.
	if a.Jurisdiction == AdvisoryJurisdictionOther && a.Estimate != nil {
		return errors.New("consentpolicy: advisory names OTHER and still carries an estimate")
	}
	if !a.ResolvedBy.known() {
		return fmt.Errorf("consentpolicy: unknown advisory.resolved_by %q", string(a.ResolvedBy))
	}
	if a.ResolvedBy == AdvisoryResolvedByUnknown && a.Jurisdiction != AdvisoryJurisdictionOther {
		return errors.New("consentpolicy: advisory resolved nothing and still names a jurisdiction")
	}
	if !a.RowStatus.known() {
		return fmt.Errorf("consentpolicy: unknown advisory.row_status %q", string(a.RowStatus))
	}
	if !a.RowBasis.known() {
		return fmt.Errorf("consentpolicy: unknown advisory.row_basis %q", string(a.RowBasis))
	}
	if a.AdvisoryBasis == "" || len(a.AdvisoryBasis) > maxAdvisoryBasisBytes {
		return fmt.Errorf("consentpolicy: advisory.advisory_basis is empty or over the %d-byte bound", maxAdvisoryBasisBytes)
	}
	// ⚠ NO CONTROL CHARACTERS. The text is a table cell, which holds none, and
	// it is the one free-text member a caller is likely to log or show: a
	// newline here could write a second line into a log that nothing
	// authorised, the reason the refusal vocabulary is closed.
	if controlPattern.MatchString(a.AdvisoryBasis) {
		return errors.New("consentpolicy: advisory.advisory_basis carries a control character")
	}
	if !commitPattern.MatchString(a.Matrix.DocsCommit) {
		return errors.New("consentpolicy: advisory.matrix.docs_commit is not a 40-character hex commit")
	}
	if !sha256Pattern.MatchString(a.Matrix.FileSHA256) {
		return errors.New("consentpolicy: advisory.matrix.file_sha256 is not a 64-character hex digest")
	}
	if _, err := time.Parse(time.DateOnly, a.Matrix.Date); err != nil {
		return fmt.Errorf("consentpolicy: advisory.matrix.date is not a calendar date: %w", err)
	}
	return nil
}

// cloneAdvisory copies the advisory and the estimate behind its pointer, so a
// caller holding a copy cannot reach back into the verdict through it.
func cloneAdvisory(a *Advisory) *Advisory {
	if a == nil {
		return nil
	}
	out := *a
	if a.Estimate != nil {
		estimate := *a.Estimate
		out.Estimate = &estimate
	}
	return &out
}
