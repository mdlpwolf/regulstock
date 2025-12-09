from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    standardize_m3,
    standardize_reflex,
    add_reflex_category,
    expand_reflex_to_m3_candidates,
    join_with_m3_real,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(standardize_m3, "m3_stock_parquet", "m3_std", name="standardize_m3"),
            node(standardize_reflex, "reflex_stock_parquet", "reflex_std", name="standardize_reflex"),

            node(
                add_reflex_category,
                inputs=dict(reflex_df="reflex_std", mapping="params:reflex_mapping_rules"),
                outputs="reflex_categorized",
                name="add_reflex_category",
            ),

            node(
                expand_reflex_to_m3_candidates,
                inputs=dict(reflex_df="reflex_categorized", rules="params:reflex_to_m3_rules"),
                outputs="reflex_m3_candidates",
                name="expand_reflex_to_m3_candidates",
            ),

            node(
                join_with_m3_real,
                inputs=dict(reflex_candidates="reflex_m3_candidates", m3_df="m3_std"),
                outputs="reflex_m3_correspondence",
                name="join_candidates_with_m3",
            ),
        ]
    )
