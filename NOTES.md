## Ideas

Commands:

1. Schema
1. Head
1. Cat(?)
1. Paginator (less-like interaction) (https://github.com/charmbracelet/bubbletea/tree/master/examples#paginator)
1. Tables (also less-like interaction) (https://github.com/charmbracelet/bubbles#table)

## Notes:

https://clickhouse.com/blog/apache-parquet-clickhouse-local-querying-writing
https://posulliv.github.io/posts/parquet-cli/

## LZ4 unsupported

https://github.com/apache/arrow/blob/81e47b20b241df100f3a24194e97a0423adc0d5e/go/parquet/compress/compress.go#L50-L51

## Compressed size statistics

Use the sum of columns for compressed size?
https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/metadata/BlockMetaData.java#L128-L134
