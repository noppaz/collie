package visualize

import (
	"fmt"
	"os"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	"github.com/noppaz/collie/internal/parse"
)

const (
	orange = lipgloss.Color("#db592a")
)

func lipglossTable(headers []string, rows [][]string) *table.Table {
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
	cellStyle := re.NewStyle().Padding(0, 1).Width(20)
	rowStyle := cellStyle.Copy()
	borderStyle := lipgloss.NewStyle().Foreground(orange)

	t := table.New().
		Border(lipgloss.NormalBorder()).
		BorderStyle(borderStyle).
		StyleFunc(func(row, col int) lipgloss.Style {
			var style lipgloss.Style
			switch {
			case row == 0:
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

func PrintFileStatistics(fileStats *parse.FileStats) {
	var rows [][]string
	headers := fileStats.Columns[0].GetHeaders()
	for _, col := range fileStats.Columns {
		rows = append(rows, col.GetStringedRow())
	}
	t := lipglossTable(headers, rows)

	fmt.Printf(
		"File:       %s\nCreator:    %s\nRows:       %v\nColumns:    %v\nRow groups: %v\n",
		fileStats.Name,
		fileStats.Creator,
		fileStats.Rows,
		fileStats.ColumnCount,
		fileStats.RowGroups,
	)
	fmt.Println(t)
}

func PrintRowGroup(rowGroupStats *parse.RowGroupStats) {
	var rows [][]string
	headers := rowGroupStats.ChunkStats[0].GetHeaders()
	for _, col := range rowGroupStats.ChunkStats {
		rows = append(rows, col.GetStringedRow())
	}
	t := lipglossTable(headers, rows)

	fmt.Printf(
		"Row group: %v, Rows: %v, Compressed size: %s, Uncompressed size: %s, Compression ratio %.2f\n",
		rowGroupStats.Index,
		rowGroupStats.RowCount,
		rowGroupStats.SizeCompressed,
		rowGroupStats.SizeUncompressed,
		rowGroupStats.CompressionRatio,
	)
	fmt.Println(t)
}
