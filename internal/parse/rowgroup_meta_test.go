package parse

import (
	"slices"
	"testing"

	"github.com/apache/arrow/go/parquet/metadata"
)

func Test_ChunkStatsGetStringedRow(t *testing.T) {
	testCases := []struct {
		name     string
		input    ChunkStats
		expected []string
	}{
		{
			"int col",
			ChunkStats{"col1", "int", "int", "100 B", "100 B", 1.00, "RLE", 100, "0", "10"},
			[]string{"col1", "int", "int", "100 B", "100 B", "1.00", "RLE", "100", "0", "10"},
		},
		{
			"bytearray col",
			ChunkStats{"col2", "bytearray", "string", "329.784 KiB", "573.153 KiB", 1.74, "RLE", 0, "asdf", "qwerty"},
			[]string{"col2", "bytearray", "string", "329.784 KiB", "573.153 KiB", "1.74", "RLE", "0", "asdf", "qwerty"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.GetStringedRow()
			if !slices.Equal(got, tt.expected) {
				t.Errorf("ChunkStats.GetStringedRow error, input=%v, got=%v, expected=%v", tt.input, got, tt.expected)
			}
		})
	}
}

func Test_getChunkMaxMin(t *testing.T) {
	case1 := metadata.BooleanStatistics{}
	case1.SetMinMax(false, true)
	case2 := metadata.Float64Statistics{}
	case2.SetMinMax(11.11, 22.22)
	case3 := metadata.ByteArrayStatistics{}
	case3.SetMinMax([]byte("asdf"), []byte("qwerty"))

	testCases := []struct {
		name        string
		input       metadata.TypedStatistics
		expectedMin string
		expectedMax string
	}{
		{"boolean statistics", &case1, "false", "true"},
		{"float64 statistics", &case2, "11.11", "22.22"},
		{"bytearray statistics", &case3, "asdf", "qwerty"},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			gotMin, gotMax, _ := getChunkMaxMin(tt.input)
			if gotMin != tt.expectedMin {
				t.Errorf("getChunkMaxMin min error, got=%v, expected=%v", gotMin, tt.expectedMin)
			}
			if gotMax != tt.expectedMax {
				t.Errorf("getChunkMaxMin max error, got=%v, expected=%v", gotMax, tt.expectedMax)
			}
		})
	}
}
