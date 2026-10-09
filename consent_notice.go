package shardpilot

import "regexp"

// ConsentNotice identifies the notice presented by the host for one consent
// decision. It carries version identifiers and a locale, never notice text.
// Supply all three fields or omit the setter's optional argument entirely.
// Values are copied into the receipt and preserved through retry and reload.
type ConsentNotice struct {
	NoticeVersion string `json:"notice_version,omitempty"`
	NoticeLocale  string `json:"notice_locale,omitempty"`
	PolicyVersion string `json:"policy_version,omitempty"`
}

var consentNoticeVersion = regexp.MustCompile(`\A[A-Za-z0-9._+/-]{1,64}\z`)

// RFC 5646 section 2.1 syntax, not language-registry membership or
// canonicalization. The fixed irregular alternatives are part of that grammar;
// regular grandfathered tags already match langtag. Bounded before matching.
// https://www.rfc-editor.org/rfc/rfc5646.html#section-2.1
var consentNoticeLocale = regexp.MustCompile(`(?i)\A(?:` +
	`(?:[a-z]{2,3}(?:-[a-z]{3}){0,3}|[a-z]{4,8})` + // language, optional extlang
	`(?:-[a-z]{4})?(?:-(?:[a-z]{2}|[0-9]{3}))?` + // script, region
	`(?:-(?:[a-z0-9]{5,8}|[0-9][a-z0-9]{3}))*` + // variants
	`(?:-[0-9a-wy-z](?:-[a-z0-9]{2,8})+)*` + // extensions
	`(?:-x(?:-[a-z0-9]{1,8})+)?` + // trailing private use
	`|x(?:-[a-z0-9]{1,8})+` + // entirely private use
	`|en-GB-oed|i-(?:ami|bnn|default|enochian|hak|klingon|lux|mingo|navajo|pwn|tao|tay|tsu)|sgn-(?:BE-FR|BE-NL|CH-DE)` +
	`)\z`)

func (n ConsentNotice) valid() bool {
	if len(n.NoticeVersion) > 64 || len(n.PolicyVersion) > 64 || len(n.NoticeLocale) < 2 || len(n.NoticeLocale) > 35 {
		return false
	}
	// Case-insensitive regexp matching includes Unicode case-fold equivalents;
	// language tags permit ASCII only, so reject those before matching.
	for i := 0; i < len(n.NoticeLocale); i++ {
		if n.NoticeLocale[i] >= 128 {
			return false
		}
	}
	return consentNoticeVersion.MatchString(n.NoticeVersion) &&
		consentNoticeVersion.MatchString(n.PolicyVersion) &&
		consentNoticeLocale.MatchString(n.NoticeLocale)
}

func snapshotConsentNotice(notices []ConsentNotice) (ConsentNotice, bool) {
	if len(notices) == 0 {
		return ConsentNotice{}, true
	}
	if len(notices) != 1 || !notices[0].valid() {
		return ConsentNotice{}, false
	}
	return notices[0], true
}
