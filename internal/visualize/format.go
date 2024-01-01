package visualize

import (
	"fmt"
	"strings"

	"github.com/noppaz/collie/internal/parse"
)

func FormatFileStatistics(fileStats *parse.FileStats) string {
	var rows [][]string
	headers := fileStats.Columns[0].GetHeaders()
	for _, col := range fileStats.Columns {
		rows = append(rows, col.GetStringedRow())
	}
	t := LipglossTable(headers, rows)

	b := strings.Builder{}
	fmt.Fprintf(
		&b,
		"File:       %s\nCreator:    %s\nRows:       %v\nColumns:    %v\nRow groups: %v\n",
		fileStats.Name,
		fileStats.Creator,
		fileStats.Rows,
		fileStats.ColumnCount,
		fileStats.RowGroups,
	)
	fmt.Fprintf(&b, "%s\n", t.String())
	return b.String()
}

func FormatRowGroup(rowGroupStats *parse.RowGroupStats) string {
	var rows [][]string
	headers := rowGroupStats.ChunkStats[0].GetHeaders()
	for _, col := range rowGroupStats.ChunkStats {
		rows = append(rows, col.GetStringedRow())
	}
	t := LipglossTable(headers, rows)

	b := strings.Builder{}
	fmt.Fprintf(
		&b,
		"Row group: %v, Rows: %v, Compressed size: %s, Uncompressed size: %s, Compression ratio %.2f\n",
		rowGroupStats.Index,
		rowGroupStats.RowCount,
		rowGroupStats.SizeCompressed,
		rowGroupStats.SizeUncompressed,
		rowGroupStats.CompressionRatio,
	)
	fmt.Fprintf(&b, "%s\n", t.String())
	return b.String()
}
