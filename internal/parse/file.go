package parse

import (
	"strconv"

	"github.com/apache/arrow/go/parquet/file"
)

type FileColumn struct {
	Index        int
	Name         string
	Nullable     bool
	PhysicalType string
	LogicalType  string
}

func (c *FileColumn) GetHeaders() []string {
	return []string{
		"",
		"Name",
		"Nullable",
		"Physical type",
		"Logical type",
	}
}

func (f *FileColumn) GetStringedRow() []string {
	return []string{
		strconv.FormatInt(int64(f.Index), 10),
		f.Name,
		strconv.FormatBool(f.Nullable),
		f.PhysicalType,
		f.LogicalType,
	}
}

type FileStats struct {
	Name        string
	Creator     string
	Rows        int64
	ColumnCount int
	RowGroups   int
	Columns     []*FileColumn
}

func GetFileStats(filename string, reader *file.Reader) (*FileStats, error) {
	fileMetadata := reader.MetaData()
	fileSchema := fileMetadata.Schema
	columnCount := fileSchema.NumColumns()

	columns := make([]*FileColumn, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		col := fileSchema.Column(i)

		nullable := true
		if col.SchemaNode().RepetitionType().String() == "required" {
			nullable = false
		}
		fileColumn := &FileColumn{
			Index:        i,
			Nullable:     nullable,
			Name:         col.Name(),
			PhysicalType: col.PhysicalType().String(),
		}
		logicalType := col.LogicalType().String()
		if logicalType != "None" {
			fileColumn.LogicalType = logicalType
		}
		columns = append(columns, fileColumn)
	}

	fileStats := &FileStats{
		Name:        filename,
		Creator:     fileMetadata.GetCreatedBy(),
		Rows:        reader.NumRows(),
		ColumnCount: columnCount,
		RowGroups:   reader.NumRowGroups(),
		Columns:     columns,
	}

	return fileStats, nil
}
