import datetime
from dataclasses import make_dataclass

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SampleData = make_dataclass(
    "SampleData",
    [
        ("col_int64", int),
        ("col_double", float),
        ("col_string", str),
        ("col_timestamp", datetime.datetime),
    ],
)

rows = []

base_time = datetime.datetime(
    year=2000, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
)
length = 1000
for i in range(length):
    rows.append(
        SampleData(
            i,
            i + i / length,
            f"String {i}",
            base_time + datetime.timedelta(days=i, milliseconds=i),
        )
    )

df = pd.DataFrame(rows)
print(df)

table = pa.Table.from_pandas(df)

pq.write_table(
    table,
    where="testdata/sample.parquet",
    row_group_size=100,
    compression="GZIP",
    use_dictionary=True,
)
