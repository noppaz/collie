package visualize

import (
	"testing"
)

func Test_LipglossTable(t *testing.T) {
	headers := []string{"col 1", "col 2", "col 3 with long name"}
	rows := [][]string{{"row 1", "long value row 1", "1"}, {"row 2", "long value row 2", "2"}}

	got := LipglossTable(headers, rows)

	expectedTable := `┌───────┬──────────────────┬──────────────────────┐
│ col 1 │      col 2       │ col 3 with long name │
├───────┼──────────────────┼──────────────────────┤
│ row 1 │ long value row 1 │ 1                    │
│ row 2 │ long value row 2 │ 2                    │
└───────┴──────────────────┴──────────────────────┘`

	rendered := got.Render()
	if rendered != expectedTable {
		t.Errorf(
			"LipglossTable error, render version not equal to expected\nGot:\n%s\nExpected:\n%s\n",
			got.Render(),
			expectedTable,
		)
	}
}
