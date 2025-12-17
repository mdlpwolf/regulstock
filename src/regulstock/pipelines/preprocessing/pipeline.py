from kedro.pipeline import node, pipeline  # noqa

from .nodes import (
    map_reflex,
    map_m3,
    build_reflex_m3_wide_with_lotless,
    compute_m3_reliquat,
)

def create_pipeline(**kwargs) -> pipeline:
    return pipeline([
        node(
            map_reflex,
            inputs=dict(reflex_df="reflex_stock_parquet", mapping="params:reflex_mapping_rules"),
            outputs="reflex_map",
            name="map_reflex_category",
        ),
        node(
            map_m3,
            inputs=dict(
                m3_df="m3_stock_parquet", 
                rules="params:m3_mapping_rules",
                pos_df='m3_po_dataset'
            ),
            outputs="m3_map",
            name="map_m3",
        ),
        node(
        build_reflex_m3_wide_with_lotless,
        inputs=dict(
            reflex_map="reflex_map",
            m3_map="m3_map",
            depots="params:m3_depots_columns",
        ),
        outputs="corr_dataset",
        name="build_reflex_m3_wide",
        ),
        node(
            compute_m3_reliquat,
            inputs=dict(m3_map="m3_map", reflex_map="reflex_map"),
            outputs="m3_reliquat",
            name="compute_m3_reliquat_node",
        ),
    ],
        tags=['preprocessing']
    )
