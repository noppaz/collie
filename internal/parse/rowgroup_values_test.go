package parse

import (
	"testing"

	"github.com/apache/arrow/go/parquet/file"
)

func BenchmarkReadRowGroupValues(b *testing.B) {
	filename := "/Users/noah/Downloads/yellow_tripdata_2023-01.parquet"
	reader, err := file.OpenParquetFile(filename, true)
	if err != nil {
		b.Errorf("opening parquet file")
	}
	defer reader.Close()
	rowGroup := reader.RowGroup(0)
	amount := 10
	for n := 0; n < b.N; n++ {
		_, err = ReadRowGroupValues(rowGroup, amount)
		if err != nil {
			b.Errorf("reading row group values")
		}
	}
}
