from kedro.pipeline import node, pipeline  # noqa
from .nodes import sql_chunks_to_parquet

def create_pipeline(**kwargs) -> pipeline:
    return pipeline([
        node(
                func=sql_chunks_to_parquet,
                inputs=dict(
                    chunks="m3_stock_dataset",
                    parquet_path="params:m3_parquet_path",
                    compression="params:parquet_compression",
                ),
                outputs="m3_parquet_written_path",
                name="m3_sql_chunks_to_parquet",
            ),
            node(
                func=sql_chunks_to_parquet,
                inputs=dict(
                    chunks="reflex_stock_dataset",
                    parquet_path="params:reflex_parquet_path",
                    compression="params:parquet_compression",
                    # Kedro accepte les inputs non utilisés si on les met dans un dict,
                    # mais la fonction doit les recevoir -> on wrappe via un lambda :
                ),
                outputs="reflex_parquet_written_path",
                name="reflex_sql_chunks_to_parquet",
            ),
    ])
