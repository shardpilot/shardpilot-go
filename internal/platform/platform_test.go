package platform

import (
	"strings"
	"testing"
)

func TestCanonicalPlatforms(t *testing.T) {
	for _, value := range strings.Fields("windows macos linux android ios tvos web ps4 ps5 xbox switch other") {
		for _, input := range []string{value, " " + strings.ToUpper(value) + " "} {
			if got := Normalize(input); got != value {
				t.Errorf("Normalize(%q) = %q, want %q", input, got, value)
			}
		}
	}
}

func TestRuntimePlatforms(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"android", "android"}, {"darwin", "macos"}, {"ios", "ios"}, {"js", "web"},
		{"linux", "linux"}, {"windows", "windows"},
		{"aix", "other"}, {"dragonfly", "other"}, {"freebsd", "other"}, {"illumos", "other"},
		{"netbsd", "other"}, {"openbsd", "other"}, {"plan9", "other"}, {"solaris", "other"},
		{"wasip1", "other"}, {"future-os", "other"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			if got := NormalizeRuntime(tc.input); got != tc.want {
				t.Errorf("runtime %s = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
