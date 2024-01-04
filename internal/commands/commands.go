package commands

import (
	"fmt"

	"github.com/apache/arrow/go/parquet/file"
	"github.com/noppaz/collie/internal/parse"
	"github.com/noppaz/collie/internal/visualize"
)

func MetaCommand(filename string) error {
	reader, err := file.OpenParquetFile(filename, true)
	if err != nil {
		return fmt.Errorf("error opening parquet file: %w", err)
	}
	defer reader.Close()

	fileStats, err := parse.GetFileStats(filename, reader)
	if err != nil {
		return err
	}
	fmt.Println(visualize.FormatFileStatistics(fileStats))
	return nil
}

func RowGroupsCommand(filename string, perPage int) error {
	reader, err := file.OpenParquetFile(filename, true)
	if err != nil {
		return fmt.Errorf("error opening parquet file: %w", err)
	}
	defer reader.Close()

	var rowGroupOutput []string
	for i := range reader.MetaData().RowGroups {
		rowGroup := reader.RowGroup(i)
		rowGroupStats, err := parse.GetRowGroupStats(i, rowGroup)
		if err != nil {
			return err
		}
		rowGroupOutput = append(rowGroupOutput, visualize.FormatRowGroup(rowGroupStats))

	}
	return visualize.PaginatorCreator(rowGroupOutput, perPage)
}

func HeadCommand(filename string, amount int) error {
	reader, err := file.OpenParquetFile(filename, true)
	if err != nil {
		return fmt.Errorf("error opening parquet file: %w", err)
	}
	defer reader.Close()

	const rowGroupIndex = 0
	rowGroup := reader.RowGroup(rowGroupIndex)

	rowGroupStats, err := parse.GetRowGroupStats(rowGroupIndex, rowGroup)
	if err != nil {
		return err
	}
	for _, rowGroupColumn := range rowGroupStats.ChunkStats {
		if rowGroupColumn.HasUnsupportedCompressions() {
			return fmt.Errorf(
				"Row group %v, column '%s' has unsupported column compression: %s",
				rowGroupIndex,
				rowGroupColumn.GetColumnName(),
				rowGroupColumn.GetColumnCompression(),
			)
		}
	}

	rows, err := parse.ReadRowGroupValues(rowGroup, amount)
	if err != nil {
		return err
	}

	headers := make([]string, rowGroup.NumColumns())
	for i := 0; i < rowGroup.NumColumns(); i++ {
		headers[i] = rowGroup.Column(i).Descriptor().Name()
	}

	fmt.Printf("Rows: %v\n", amount)
	fmt.Printf("Columns: %v\n", len(headers))
	fmt.Println(visualize.LipglossTable(headers, rows))
	return nil
}
