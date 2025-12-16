from kedro.pipeline import node, pipeline  # noqa

from .nodes import (
    compute_m3_regul,
    generate_api_m3_rfx, 
)

def create_pipeline(**kwargs) -> pipeline:
    return pipeline([
        node(
            compute_m3_regul,
            inputs="reflex_m3_wide",
            outputs="reflex_m3_regul",
            name="compute_m3_regul",
            tags="regul",
        ),
        node(
            generate_api_m3_rfx,
            inputs=dict(
                reflex_m3_regul="reflex_m3_regul",
                m3_cat="m3_map",
            ),
            outputs="stock_m3_rfx",
            name="generate_stock_m3_rfx_node",
        ),
    ],
        tags=['processing']
    )
