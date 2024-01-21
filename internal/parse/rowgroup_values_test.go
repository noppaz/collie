package parse

import (
	"slices"
	"testing"
)

func Test_ReadRows(t *testing.T) {
	type Input struct {
		filename string
		amount   int
	}

	type Expected struct {
		rowLength int
		headers   []string
	}

	testCases := []struct {
		name     string
		input    Input
		expected Expected
	}{
		{
			"amount less than one row group",
			Input{"../../testdata/sample.parquet", 33},
			Expected{33, []string{"col_int64", "col_double", "col_string", "col_timestamp"}},
		},
		{
			"amount more than several row groups",
			Input{"../../testdata/sample.parquet", 646},
			Expected{646, []string{"col_int64", "col_double", "col_string", "col_timestamp"}},
		},
		{
			"amount more than file has",
			Input{"../../testdata/sample.parquet", 1400},
			Expected{1000, []string{"col_int64", "col_double", "col_string", "col_timestamp"}},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			headers, rows, err := ReadRows(tt.input.filename, tt.input.amount)
			if err != nil {
				t.Errorf("ReadRows returned err: %v", err)
			}
			if !slices.Equal(headers, tt.expected.headers) {
				t.Errorf("ReadRows error: header mismatch, got=%v, expected=%v", headers, tt.expected.headers)
			}
			if len(rows) != tt.expected.rowLength {
				t.Errorf(
					"ReadRows error: read row length mismatch, got=%v, expected=%v",
					len(rows),
					tt.expected.rowLength)
			}
		})
	}
}
