// Package redact masks a configured credential wherever an evidence stream
// could echo it back, including the DECODED views an endpoint or an error might
// use: JSON escapes (\u0073), percent-encoding in either hex case (%2f, %2F)
// and `+` for a space in a query.
//
// ⚠ ONE IMPLEMENTATION, DELIBERATELY. Both example senders print replies and
// errors that can quote what they were handed, and a second copy of this
// equivalence set is a second thing to forget: the canonical-only version of
// this (a plain strings.Replacer over the raw key) passed every test that
// echoed the key verbatim and leaked it the moment a body came back
// percent-encoded.
package redact

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
)

type Redactor struct{ patterns []string }

type sourceSpan struct{ start, end int }
type comparisonView struct {
	text  string
	spans []sourceSpan
}

func (v *comparisonView) appendBytes(decoded string, span sourceSpan, text *strings.Builder) {
	text.WriteString(decoded)
	for range len(decoded) {
		v.spans = append(v.spans, span)
	}
}

func (v comparisonView) percentDecoded(query bool) comparisonView {
	result := comparisonView{spans: make([]sourceSpan, 0, len(v.text))}
	var text strings.Builder
	text.Grow(len(v.text))
	for i := 0; i < len(v.text); {
		ch, width := v.text[i], 1
		if ch == '%' && i+2 < len(v.text) {
			if value, err := hex.DecodeString(v.text[i+1 : i+3]); err == nil {
				ch, width = value[0], 3
			}
		} else if query && ch == '+' {
			ch = ' '
		}
		result.appendBytes(string([]byte{ch}), sourceSpan{v.spans[i].start, v.spans[i+width-1].end}, &text)
		i += width
	}
	result.text = text.String()
	return result
}

func (v comparisonView) jsonUnescaped() comparisonView {
	result := comparisonView{spans: make([]sourceSpan, 0, len(v.text))}
	var text strings.Builder
	text.Grow(len(v.text))
	for i := 0; i < len(v.text); {
		decoded, width := v.text[i:i+1], 1
		if v.text[i] == '\\' && i+1 < len(v.text) {
			n := 2
			if v.text[i+1] == 'u' {
				n = 6
				// A surrogate pair is one code point and one original span.
				if i+12 <= len(v.text) && v.text[i+6:i+8] == `\u` {
					high, highErr := strconv.ParseUint(v.text[i+2:i+6], 16, 16)
					low, lowErr := strconv.ParseUint(v.text[i+8:i+12], 16, 16)
					if highErr == nil && lowErr == nil && high >= 0xd800 && high <= 0xdbff && low >= 0xdc00 && low <= 0xdfff {
						n = 12
					}
				}
			}
			if i+n <= len(v.text) {
				var value string
				if json.Unmarshal([]byte(`"`+v.text[i:i+n]+`"`), &value) == nil {
					decoded, width = value, n
				}
			}
		}
		result.appendBytes(decoded, sourceSpan{v.spans[i].start, v.spans[i+width-1].end}, &text)
		i += width
	}
	result.text = text.String()
	return result
}

func New(keys ...string) *Redactor {
	r := &Redactor{}
	for _, key := range keys {
		encoded, _ := json.Marshal(key)
		r.patterns = append(r.patterns, key, string(encoded[1:len(encoded)-1]))
	}
	return r
}

// Compare raw, JSON-unescaped and percent-decoded views. At most one JSON and
// one percent pass are composed, in either order; this is not a general decoder
// for arbitrary encodings such as base64. Matches map back to original spans.
func (r *Redactor) Replace(text string) string {
	masked := make([]bool, len(text))
	plain := comparisonView{text: text, spans: make([]sourceSpan, len(text))}
	for i := range len(text) {
		plain.spans[i] = sourceSpan{i, i + 1}
	}
	mark := func(view comparisonView) {
		for _, pattern := range r.patterns {
			if pattern == "" {
				continue
			}
			for offset := 0; offset < len(view.text); {
				at := strings.Index(view.text[offset:], pattern)
				if at < 0 {
					break
				}
				start := offset + at
				end := start + len(pattern)
				for i := view.spans[start].start; i < view.spans[end-1].end; i++ {
					masked[i] = true
				}
				offset = end
			}
		}
	}
	for baseIndex, base := range []comparisonView{plain, plain.jsonUnescaped()} {
		mark(base)
		for _, query := range []bool{false, true} {
			percent := base.percentDecoded(query)
			mark(percent)
			if baseIndex == 0 {
				mark(percent.jsonUnescaped())
			}
		}
	}
	var out strings.Builder
	for i := 0; i < len(text); {
		if !masked[i] {
			out.WriteByte(text[i])
			i++
			continue
		}
		out.WriteString("[REDACTED]")
		for i < len(text) && masked[i] {
			i++
		}
	}
	return out.String()
}
