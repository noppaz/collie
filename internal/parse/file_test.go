package parse

import (
	"slices"
	"testing"
)

func Test_FileColumnGetStringedRow(t *testing.T) {
	testCases := []struct {
		name     string
		input    FileColumn
		expected []string
	}{
		{
			"nullable false",
			FileColumn{0, "col1", false, "int", "int"},
			[]string{"0", "col1", "false", "int", "int"},
		},
		{
			"nullable true",
			FileColumn{1, "col2", true, "bytearray", "string"},
			[]string{"1", "col2", "true", "bytearray", "string"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.GetStringedRow()
			if !slices.Equal(got, tt.expected) {
				t.Errorf("FileColumn.GetStringedRow error, input=%v, got=%v, expected=%v", tt.input, got, tt.expected)
			}
		})
	}
}
