from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_reflex_m3_wide_node, compute_m3_reliquat_node


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_reflex_m3_wide_node,
                inputs=dict(
                    reflex_map="reflex_map",
                    m3_map="m3_map",
                    params="params:stock_reconciliation",
                ),
                outputs="corr_dataset",
                name="build_reflex_m3_wide",
            ),
            node(
                func=compute_m3_reliquat_node,
                inputs=dict(
                    m3_map="m3_map",
                    reflex_map="reflex_map",
                    params="params:stock_reconciliation",
                ),
                outputs="m3_reliquat",
                name="compute_m3_reliquat",
            ),
        ]
    )
