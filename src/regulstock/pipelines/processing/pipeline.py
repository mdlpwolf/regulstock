from kedro.pipeline import node, pipeline  # noqa

from .nodes import (
    map_reflex_category,
    map_m3_category,
    build_reflex_m3_wide_with_lotless,
    compute_m3_reliquat,
    process_web_pos,
)

def create_pipeline(**kwargs) -> pipeline:
    return pipeline(
        [
            node(
                map_reflex_category,
                inputs=dict(reflex_df="reflex_stock_parquet", mapping="params:reflex_mapping_rules"),
                outputs="reflex_cat",
                name="map_reflex_category",
            ),
            node(
                map_m3_category,
                inputs=dict(m3_df="m3_stock_parquet", rules="params:m3_mapping_rules"),
                outputs="m3_cat",
                name="map_m3_category",
            ),node(
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
        node(
            process_web_pos,
            inputs=dict(corr_df='reflex_m3_wide', pos_df='m3_po_dataset'),
            outputs='exclu_150_parquet',
            name="process_web_exclus",
        )],
        tags=['processing']
    )

