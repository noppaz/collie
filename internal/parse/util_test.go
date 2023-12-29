package parse

import "testing"

func Test_humanBytes(t *testing.T) {
	testCases := []struct {
		name     string
		input    int64
		expected string
	}{
		{"byte size", 541, "541 B"},
		{"megabyte size", 8790212, "  8.383 MiB"},
		{"gigabyte size", 1073741824, "  1.000 GiB"},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got := humanBytes(tt.input)
			if got != tt.expected {
				t.Errorf("byteCountBinary(%v) = '%s', want '%s'", tt.input, got, tt.expected)
			}
		})
	}
}
