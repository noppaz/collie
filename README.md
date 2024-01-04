# collie

A parquet file CLI explorer with TUI elements.

## Caveats

Column compressions LZO and LZ4 are not supported by collie due to the go parquet package not supporting them. See more [here](https://github.com/apache/arrow/blob/main/go/parquet/compress/compress.go).
