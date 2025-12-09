from kedro.pipeline import node, pipeline  # noqa
from .nodes import (
    sql_chunks_to_parquet,
    standardize_m3,
    standardize_reflex,
    map_reflex_category,
    map_m3_category,
    build_reflex_m3_wide_with_lotless,
    compute_m3_reliquat
)

def create_pipeline(**kwargs) -> pipeline:
    return pipeline([
        node(
                func=sql_chunks_to_parquet,
                inputs=dict(
                    chunks="m3_stock_dataset",
                    parquet_path="params:m3_parquet_path",
                    compression="params:parquet_compression",
                ),
                outputs="m3_stock_parquet_written_path",
                name="m3_sql_chunks_to_parquet",
                tags= ['extraction'],
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
                tags= ['extraction'],
            ),

            node(standardize_m3, "m3_stock_parquet", "m3_std", name="standardize_m3"),
            node(standardize_reflex, "reflex_stock_parquet", "reflex_std", name="standardize_reflex"),

            node(
                map_reflex_category,
                inputs=dict(reflex_df="reflex_std", mapping="params:reflex_mapping_rules"),
                outputs="reflex_cat",
                name="map_reflex_category",
            ),
            node(
                map_m3_category,
                inputs=dict(m3_df="m3_std", rules="params:m3_mapping_rules"),
                outputs="m3_cat",
                name="map_m3_category",
            ),

            node(
                build_reflex_m3_wide_with_lotless,
                inputs=dict(
                    reflex_cat="reflex_cat",
                    m3_cat="m3_cat",
                    depots="params:m3_depots_columns",
                ),
                outputs="reflex_m3_wide",
                name="build_reflex_m3_wide",
            ),
            
            node(
                compute_m3_reliquat,
                inputs=dict(m3_cat="m3_cat", reflex_cat="reflex_cat"),
                outputs="m3_reliquat",
                name="compute_m3_reliquat_node",
            ),
    ])
