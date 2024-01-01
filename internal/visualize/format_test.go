package visualize

import (
	"testing"

	"github.com/noppaz/collie/internal/parse"
)

func Test_FormatFileStatistics(t *testing.T) {
	testCases := []struct {
		name     string
		input    *parse.FileStats
		expected string
	}{
		{
			"single column",
			&parse.FileStats{
				Name: "file1.parquet", Creator: "collie", Rows: 100_000, ColumnCount: 1, RowGroups: 1,
				Columns: []*parse.FileColumn{
					{
						Index:        0,
						Name:         "col 1",
						Nullable:     false,
						PhysicalType: "INT64",
						LogicalType:  "Int(bitWidth=64, isSigned=true)",
					},
				},
			},
			`File:       file1.parquet
Creator:    collie
Rows:       100000
Columns:    1
Row groups: 1
┌───┬───────┬──────────┬───────────────┬─────────────────────────────────┐
│   │ Name  │ Nullable │ Physical type │          Logical type           │
├───┼───────┼──────────┼───────────────┼─────────────────────────────────┤
│ 0 │ col 1 │ false    │ INT64         │ Int(bitWidth=64, isSigned=true) │
└───┴───────┴──────────┴───────────────┴─────────────────────────────────┘
`,
		},
		{
			"several columns",
			&parse.FileStats{
				Name: "file2.parquet", Creator: "collie", Rows: 1_000_000, ColumnCount: 2, RowGroups: 10,
				Columns: []*parse.FileColumn{
					{
						Index:        0,
						Name:         "col 1",
						Nullable:     false,
						PhysicalType: "INT64",
						LogicalType:  "Int(bitWidth=64, isSigned=true)",
					},
					{
						Index:        1,
						Name:         "col 2",
						Nullable:     true,
						PhysicalType: "BYTEARRAY",
						LogicalType:  "String",
					},
				},
			},
			`File:       file2.parquet
Creator:    collie
Rows:       1000000
Columns:    2
Row groups: 10
┌───┬───────┬──────────┬───────────────┬─────────────────────────────────┐
│   │ Name  │ Nullable │ Physical type │          Logical type           │
├───┼───────┼──────────┼───────────────┼─────────────────────────────────┤
│ 0 │ col 1 │ false    │ INT64         │ Int(bitWidth=64, isSigned=true) │
│ 1 │ col 2 │ true     │ BYTEARRAY     │ String                          │
└───┴───────┴──────────┴───────────────┴─────────────────────────────────┘
`,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatFileStatistics(tt.input); got != tt.expected {
				t.Errorf("FormatFileStatistics error.\ninput=%v\ngot=%v\nexpected=%v", tt.input, got, tt.expected)
			}
		})
	}
}
