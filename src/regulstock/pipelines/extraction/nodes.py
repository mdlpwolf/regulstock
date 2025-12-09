from typing import Iterable, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

def sql_chunks_to_parquet(
    chunks: Iterable[pd.DataFrame],
    parquet_path: str,
    compression: str = "snappy",
) -> str:
    """
    Écrit un flux de chunks pandas en un parquet unique (streaming).
    Entrée: chunks = itérable de DataFrames
    Sortie: chemin du parquet écrit (utile pour chaîner les nodes)
    """
    parquet_path = Path(parquet_path)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    writer: Optional[pq.ParquetWriter] = None

    try:
        for i, df in enumerate(chunks):
            if df is None or df.empty:
                continue

            table = pa.Table.from_pandas(df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(
                    parquet_path.as_posix(),
                    table.schema,
                    compression=compression,
                    use_dictionary=True,
                )

            writer.write_table(table)

        return parquet_path.as_posix()

    finally:
        if writer is not None:
            writer.close()
