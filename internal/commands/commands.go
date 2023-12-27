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
	visualize.PrintFileStatistics(fileStats)
	return nil
}
