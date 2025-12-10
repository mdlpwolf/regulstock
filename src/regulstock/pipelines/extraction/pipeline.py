from kedro.pipeline import node, pipeline  # noqa
from .nodes import (
    standardize_m3,
    standardize_reflex,
)

def create_pipeline(**kwargs) -> pipeline:
    return pipeline(
        [
            node(
                standardize_m3, 
                "m3_stock_dataset", 
                "m3_stock_parquet", 
                name="standardize_m3",
            ),
            node(
                standardize_reflex, 
                "reflex_stock_dataset", 
                "reflex_stock_parquet", 
                name="standardize_reflex",
            ),
        ],
        tags= ['extraction']
    )
