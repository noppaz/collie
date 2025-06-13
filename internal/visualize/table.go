package visualize

import (
	"os"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
)

const (
	orange = lipgloss.Color("#E69945")
)

func LipglossTable(headers []string, rows [][]string) *table.Table {
	columnWidths := make(map[int]int, 0)
	for i, header := range headers {
		rowLength := 0
		for _, row := range rows {
			rowLength = max(rowLength, len(row[i]))
		}
		columnWidths[i] = max(len(header), rowLength) + 2
	}

	re := lipgloss.NewRenderer(os.Stdout)
	headerStyle := re.NewStyle().Foreground(orange).Bold(true).Align(lipgloss.Center)
	rowStyle := re.NewStyle().Padding(0, 1)
	borderStyle := lipgloss.NewStyle().Foreground(orange)

	t := table.New().
		Border(lipgloss.NormalBorder()).
		BorderStyle(borderStyle).
		StyleFunc(func(row, col int) lipgloss.Style {
			var style lipgloss.Style
			switch {
			case row == table.HeaderRow:
				style = headerStyle
			default:
				style = rowStyle
			}
			return style.Width(columnWidths[col])
		}).
		Headers(headers...).
		Rows(rows...)

	return t
}
