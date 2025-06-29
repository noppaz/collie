package parse

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
)

type ChunkStats struct {
	path             string
	physicalType     string
	compression      string
	sizeCompressed   string
	sizeUncompressed string
	compressionRatio float64
	encodings        string
	nullCount        int64
	minValue         string
	maxValue         string
}

func (c *ChunkStats) GetHeaders() []string {
	return []string{
		"Column",
		"Type",
		"Compression",
		"Compressed",
		"Uncompressed",
		"Ratio",
		"Encodings",
		"Nulls",
		"Min",
		"Max",
	}
}

func (c *ChunkStats) GetColumnCompression() string {
	return c.compression
}

func (c *ChunkStats) GetColumnName() string {
	return c.path
}

func (c *ChunkStats) HasUnsupportedCompressions() bool {
	return slices.Contains([]string{"LZO", "LZ4", "LZ4_RAW"}, c.compression)
}

func (c *ChunkStats) GetStringedRow() []string {
	return []string{
		c.path,
		c.physicalType,
		c.compression,
		c.sizeCompressed,
		c.sizeUncompressed,
		fmt.Sprintf("%.2f", c.compressionRatio),
		c.encodings,
		strconv.FormatInt(c.nullCount, 10),
		c.minValue,
		c.maxValue,
	}
}

type RowGroupStats struct {
	Index            int
	RowCount         int64
	SizeCompressed   string
	SizeUncompressed string
	CompressionRatio float64
	ChunkStats       []*ChunkStats
}

func GetRowGroupStats(index int, rowGroup *file.RowGroupReader) (*RowGroupStats, error) {
	rowGroupMeta := rowGroup.MetaData()

	numColumns := rowGroupMeta.NumColumns()
	chunkStatsCollection := make([]*ChunkStats, 0, numColumns)

	var totalCompressedSize int64
	var totalUncompressedSize int64

	for i := 0; i < numColumns; i++ {
		chunk, err := rowGroupMeta.ColumnChunk(i)
		if err != nil {
			return nil, fmt.Errorf("getting column chunk %w", err)
		}

		encodings := chunk.Encodings()
		encodingsStrings := make([]string, 0, len(encodings))
		for _, e := range encodings {
			encodingsStrings = append(encodingsStrings, e.String())
		}

		compressedSize := chunk.TotalCompressedSize()
		uncompressedSize := chunk.TotalUncompressedSize()
		totalCompressedSize += compressedSize
		totalUncompressedSize += uncompressedSize

		chunkStats := ChunkStats{
			path:             chunk.PathInSchema().String(),
			physicalType:     chunk.Type().String(),
			compression:      chunk.Compression().String(),
			sizeCompressed:   humanBytes(compressedSize),
			sizeUncompressed: humanBytes(uncompressedSize),
			compressionRatio: float64(uncompressedSize) / float64(compressedSize),
			encodings:        strings.Join(encodingsStrings, ","),
		}
		statistics, err := chunk.Statistics()
		if err != nil {
			return nil, fmt.Errorf("getting column chunk statistics %w", err)
		}
		if statistics != nil {
			chunkStats.nullCount = statistics.NullCount()
			min, max, err := getChunkMaxMin(statistics)
			if err != nil {
				return nil, fmt.Errorf("reading chunk max/min: %w", err)
			}
			chunkStats.minValue = min
			chunkStats.maxValue = max
		}
		chunkStatsCollection = append(chunkStatsCollection, &chunkStats)
	}

	// Get sizes from chunk metadata if row group metadata is unavailable
	sizeCompressed := rowGroupMeta.TotalCompressedSize()
	sizeUncompressed := rowGroupMeta.TotalByteSize()
	if sizeCompressed == 0 {
		sizeCompressed = totalCompressedSize
	}
	if sizeUncompressed == 0 {
		sizeUncompressed = totalUncompressedSize
	}
	var compressionRatio float64
	if sizeCompressed > 0 {
		compressionRatio = float64(sizeUncompressed) / float64(sizeCompressed)
	} else {
		compressionRatio = 0
	}

	rowGroupStats := &RowGroupStats{
		Index:            index,
		RowCount:         rowGroupMeta.NumRows(),
		SizeCompressed:   humanBytes(sizeCompressed),
		SizeUncompressed: humanBytes(sizeUncompressed),
		CompressionRatio: compressionRatio,
		ChunkStats:       chunkStatsCollection,
	}

	return rowGroupStats, nil
}

func getChunkMaxMin(statistics metadata.TypedStatistics) (string, string, error) {
	var min, max string
	switch stat := statistics.(type) {
	case *metadata.BooleanStatistics:
		min = strconv.FormatBool(stat.Min())
		max = strconv.FormatBool(stat.Max())
	case *metadata.ByteArrayStatistics:
		min = stat.Min().String()
		max = stat.Max().String()
	case *metadata.FixedLenByteArrayStatistics:
		min = stat.Min().String()
		max = stat.Max().String()
	case *metadata.Float32Statistics:
		min = strconv.FormatFloat(float64(stat.Min()), 'f', -1, 64)
		max = strconv.FormatFloat(float64(stat.Max()), 'f', -1, 64)
	case *metadata.Float64Statistics:
		min = strconv.FormatFloat(stat.Min(), 'f', -1, 64)
		max = strconv.FormatFloat(stat.Max(), 'f', -1, 64)
	case *metadata.Int32Statistics:
		min = strconv.FormatInt(int64(stat.Min()), 10)
		max = strconv.FormatInt(int64(stat.Max()), 10)
	case *metadata.Int64Statistics:
		min = strconv.FormatInt(stat.Min(), 10)
		max = strconv.FormatInt(stat.Max(), 10)
	case *metadata.Int96Statistics:
		min = stat.Min().String()
		max = stat.Max().String()
	default:
		return "", "", fmt.Errorf("unsupported chunk statistics type %v", stat.Type().String())
	}
	return min, max, nil
}
