package commands

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/parquet/file"
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
	headers, rows, err := parse.ReadRows(filename, amount)
	if err != nil {
		return err
	}

	fmt.Printf("Rows: %v\n", amount)
	fmt.Printf("Columns: %v\n", len(headers))
	fmt.Println(visualize.LipglossTable(headers, rows))
	return nil
}

func LessCommand(filename string, amount int) error {
	headers, rows, err := parse.ReadRows(filename, amount)
	if err != nil {
		return err
	}

	tableString := visualize.LipglossTable(headers, rows).String()
	header := strings.Join(strings.Split(tableString, "\n")[:3], "\n")
	content := strings.Join(strings.Split(tableString, "\n")[3:], "\n")

	return visualize.ViewportCreator(header, content)
}
