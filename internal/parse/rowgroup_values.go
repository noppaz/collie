package parse

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/apache/arrow/go/parquet"
	"github.com/apache/arrow/go/parquet/file"
)

func ReadRows(filename string, amount int) ([]string, [][]string, error) {
	reader, err := file.OpenParquetFile(filename, true)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening parquet file: %w", err)
	}
	defer reader.Close()

	const rowGroupIndex = 0
	firstRowGroup := reader.RowGroup(rowGroupIndex)

	rowGroupStats, err := GetRowGroupStats(rowGroupIndex, firstRowGroup)
	if err != nil {
		return nil, nil, err
	}
	for _, rowGroupColumn := range rowGroupStats.ChunkStats {
		if rowGroupColumn.HasUnsupportedCompressions() {
			return nil, nil, fmt.Errorf(
				"Row group %v, column '%s' has unsupported column compression: %s",
				rowGroupIndex,
				rowGroupColumn.GetColumnName(),
				rowGroupColumn.GetColumnCompression(),
			)
		}
	}

	fileSchema := reader.MetaData().Schema
	headers := make([]string, firstRowGroup.NumColumns())
	for i := 0; i < fileSchema.NumColumns(); i++ {
		headers[i] = fileSchema.Column(i).Name()
	}

	amountWithCeiling := min(int64(amount), reader.NumRows())
	rows := make([][]string, 0, amountWithCeiling)
	for i := range reader.MetaData().RowGroups {
		rowGroup := reader.RowGroup(i)
		amountFromRowGroup := min(rowGroup.NumRows(), amountWithCeiling-int64(len(rows)))

		if int64(len(rows)) >= min(int64(amount), reader.NumRows()) {
			break
		}

		readRows, err := readRowGroupValues(rowGroup, amountFromRowGroup)
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, readRows...)
	}

	return headers, rows, nil
}

type columnValueResult struct {
	index  int
	values []string
	err    error
}

func readRowGroupValues(rowGroup *file.RowGroupReader, amount int64) ([][]string, error) {
	resultChan := make(chan columnValueResult, rowGroup.NumColumns())
	var wg sync.WaitGroup
	for i := 0; i < rowGroup.NumColumns(); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			column := rowGroup.Column(idx)
			columnValues, err := readColumnValues(column, amount)
			resultChan <- columnValueResult{idx, columnValues, err}
		}(i)
	}
	wg.Wait()
	close(resultChan)

	valuesPerRow := make([][]string, amount)
	for i := int64(0); i < amount; i++ {
		valuesPerRow[i] = make([]string, rowGroup.NumColumns())
	}
	for res := range resultChan {
		if res.err != nil {
			return nil, res.err
		}
		for rowIdx, val := range res.values {
			valuesPerRow[rowIdx][res.index] = val
		}
	}

	return valuesPerRow, nil
}

func readColumnValues(columnReader file.ColumnChunkReader, amount int64) ([]string, error) {
	batchSize := amount
	stringVals := make([]string, 0, batchSize)

	for {
		switch rdr := columnReader.(type) {
		case *file.BooleanColumnChunkReader:
			byteArrayVals := make([]bool, batchSize)
			_, _, err := rdr.ReadBatch(batchSize, byteArrayVals, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("reading bytearray column chunk batch: %w", err)
			}
			for _, val := range byteArrayVals {
				stringVals = append(stringVals, strconv.FormatBool(val))
			}
		case *file.ByteArrayColumnChunkReader:
			byteArrayVals := make([]parquet.ByteArray, batchSize)
			_, _, err := rdr.ReadBatch(batchSize, byteArrayVals, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("reading bytearray column chunk batch: %w", err)
			}
			for _, val := range byteArrayVals {
				stringVals = append(stringVals, val.String())
			}
		case *file.FixedLenByteArrayColumnChunkReader:
			byteArrayVals := make([]parquet.FixedLenByteArray, batchSize)
			_, _, err := rdr.ReadBatch(batchSize, byteArrayVals, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("reading bytearray column chunk batch: %w", err)
			}
			for _, val := range byteArrayVals {
				stringVals = append(stringVals, val.String())
			}
		case *file.Float32ColumnChunkReader:
			floatVals := make([]float32, batchSize)
			_, _, err := rdr.ReadBatch(batchSize, floatVals, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("reading bytearray column chunk batch: %w", err)
			}
			for _, val := range floatVals {
				stringVals = append(stringVals, strconv.FormatFloat(float64(val), 'f', -1, 32))
			}
		case *file.Float64ColumnChunkReader:
			floatVals := make([]float64, batchSize)
			_, _, err := rdr.ReadBatch(batchSize, floatVals, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("reading bytearray column chunk batch: %w", err)
			}
			for _, val := range floatVals {
				stringVals = append(stringVals, strconv.FormatFloat(val, 'f', -1, 64))
			}
		case *file.Int32ColumnChunkReader:
			intVals := make([]int32, batchSize)
			_, _, err := rdr.ReadBatch(batchSize, intVals, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("reading int32 column chunk batch: %w", err)
			}
			for _, val := range intVals {
				stringVals = append(stringVals, strconv.FormatInt(int64(val), 10))
			}
		case *file.Int64ColumnChunkReader:
			intVals := make([]int64, batchSize)
			_, _, err := rdr.ReadBatch(batchSize, intVals, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("reading int64 column chunk batch: %w", err)
			}
			for _, val := range intVals {
				stringVals = append(stringVals, strconv.FormatInt(val, 10))
			}
		case *file.Int96ColumnChunkReader:
			intVals := make([]parquet.Int96, batchSize)
			_, _, err := rdr.ReadBatch(batchSize, intVals, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("reading int96 column chunk batch: %w", err)
			}
			for _, val := range intVals {
				stringVals = append(stringVals, val.String())
			}

		default:
			return nil, fmt.Errorf("unsupported column type: %v", rdr.Type())
		}

		if !columnReader.HasNext() || int64(len(stringVals)) >= amount {
			break
		}
	}
	return stringVals, columnReader.Err()
}
