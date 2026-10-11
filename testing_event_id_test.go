package shardpilot

import (
	"crypto/sha1"
	"fmt"
)

// fixtureEventID gives dynamic test labels a stable UUIDv5 identity.
func fixtureEventID(label string) string {
	b := sha1.Sum(append([]byte{0xad, 0xe2, 0xd6, 0x21, 0x55, 0x44, 0x48, 0xd0, 0x80, 0x91, 0x89, 0xd5, 0x74, 0xbc, 0xaa, 0x7}, []byte(label)...))
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
