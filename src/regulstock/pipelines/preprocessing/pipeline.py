from kedro.pipeline import node, pipeline  # noqa

from .nodes import (
    map_reflex,
    map_m3,
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
    ],
        tags=['preprocessing']
    )
