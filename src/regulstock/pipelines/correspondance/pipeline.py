from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    standardize_m3,
    standardize_reflex,
    map_reflex_category,
    map_m3_category,
    build_reflex_m3_wide_with_lotless,
    compute_m3_reliquat
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
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
        ]
    )
