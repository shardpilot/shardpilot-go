// Package uuidv7 generates and validates RFC 9562 UUIDv7 identifiers.
//
// It is the single UUIDv7 implementation shared by the analytics SDK
// (anonymous IDs, consent idempotency keys) and the crash SDK (crash IDs).
package uuidv7

import (
	"crypto/rand"
	"fmt"
	"time"
)

// New returns a fresh UUIDv7 using the current UTC time.
func New() (string, error) {
	return NewAt(time.Now().UTC())
}

// NewAt returns a UUIDv7 whose timestamp bits encode the given time.
func NewAt(now time.Time) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	millis := uint64(now.UTC().UnixMilli())
	b[0] = byte(millis >> 40)
	b[1] = byte(millis >> 32)
	b[2] = byte(millis >> 24)
	b[3] = byte(millis >> 16)
	b[4] = byte(millis >> 8)
	b[5] = byte(millis)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	), nil
}

// IsValid reports whether value is a canonically formatted UUIDv7 string.
func IsValid(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i := 0; i < len(value); i++ {
		switch i {
		case 8, 13, 18, 23:
			if value[i] != '-' {
				return false
			}
		default:
			if !isHex(value[i]) {
				return false
			}
		}
	}
	if value[14] != '7' {
		return false
	}
	switch value[19] {
	case '8', '9', 'a', 'A', 'b', 'B':
		return true
	default:
		return false
	}
}

func isHex(ch byte) bool {
	return ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' || ch >= 'A' && ch <= 'F'
}
