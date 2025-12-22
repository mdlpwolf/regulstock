from typing import Any, Dict, List, Sequence
import logging
import pandas as pd


def _build_stock_cols(df: pd.DataFrame, depots: Sequence[str]) -> pd.DataFrame:
    for d in depots:
        col = f"stock_{d}"
        if col not in df.columns:
            df[col] = 0
    return df


def _pivot_m3_wide(
    m3: pd.DataFrame,
    depots: Sequence[str],
    group_cols: Sequence[str],
    pivot_index: Sequence[str],
    depot_col: str = "depot",
    value_col: str = "qty_m3",
) -> pd.DataFrame:
    m3_agg = (
        m3.groupby(list(group_cols), dropna=False)[value_col]
        .sum()
        .reset_index()
    )

    wide = (
        m3_agg.pivot_table(
            index=list(pivot_index),
            columns=[depot_col],
            values=value_col,
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    wide = wide.rename(columns={d: f"stock_{d}" for d in depots if d in wide.columns})
    return _build_stock_cols(wide, depots)


def _merge_fill_stock(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: Sequence[str],
    depots: Sequence[str],
) -> pd.DataFrame:
    out = left.merge(right, on=list(on), how="left")
    for d in depots:
        out[f"stock_{d}"] = out[f"stock_{d}"].fillna(0)
    return out


def _filter_by_lot_mode(df: pd.DataFrame, lot_mode: str) -> pd.DataFrame:
    if lot_mode == "with_lot":
        return df[df["lot"].notna()].copy()
    if lot_mode == "no_lot":
        return df[df["lot"].isna()].copy()
    raise ValueError(f"Unknown lot_mode={lot_mode!r}")


def _prepare_reflex(reflex_map: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    df = _filter_by_lot_mode(reflex_map, spec["lot_mode"])

    if spec.get("reflex_agg", False):
        df = (
            df.groupby(list(spec["reflex_group_cols"]), dropna=False)[spec.get("reflex_value_col", "qty_reflex")]
            .sum()
            .reset_index()
        )
        df["lot"] = pd.NA  # comportement identique à ton code

    return df


def _prepare_m3_wide(m3_filtered: pd.DataFrame, depots: Sequence[str], spec: Dict[str, Any]) -> pd.DataFrame:
    df = _filter_by_lot_mode(m3_filtered, spec["lot_mode"])
    return _pivot_m3_wide(
        m3=df,
        depots=depots,
        group_cols=spec["m3_group_cols"],
        pivot_index=spec["m3_pivot_index"],
        depot_col=spec.get("m3_depot_col", "depot"),
        value_col=spec.get("m3_value_col", "qty_m3"),
    )


def _build_flow(
    reflex_map: pd.DataFrame,
    m3_filtered: pd.DataFrame,
    depots: Sequence[str],
    spec: Dict[str, Any],
) -> pd.DataFrame:
    logging.info(spec.get("name", "Building flow"))

    reflex_part = _prepare_reflex(reflex_map, spec)
    m3_wide = _prepare_m3_wide(m3_filtered, depots, spec)

    return _merge_fill_stock(
        left=reflex_part,
        right=m3_wide,
        on=spec["merge_on"],
        depots=depots,
    )


def build_reflex_m3_wide_node(
    reflex_map: pd.DataFrame,
    m3_map: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """
    construit la table wide (réconciliation Reflex vs M3).
    Paramètres attendus:
      params["depots"]
      params["wide_flows"]
    """
    depots: List[str] = params["depots"]
    flows: List[Dict[str, Any]] = params["wide_flows"]

    m3_filtered = m3_map[m3_map["depot"].isin(depots)].copy()

    out = pd.concat(
        [_build_flow(reflex_map, m3_filtered, depots, spec) for spec in flows],
        ignore_index=True,
    )

    out["stock_total_m3"] = out[[f"stock_{d}" for d in depots]].sum(axis=1)
    out["ecart_rfx_m3"] = out["qty_reflex"] - out["stock_total_m3"]

    return out[
        [
            "sku",
            "lot",
            "qualite",
            "type",
            "category",
            "qty_reflex",
            *[f"stock_{d}" for d in depots],
            "stock_total_m3",
            "ecart_rfx_m3",
        ]
    ]


def _anti_merge_left_only(left: pd.DataFrame, right_keys: pd.DataFrame, on: Sequence[str]) -> pd.DataFrame:
    tmp = left.merge(
        right_keys.drop_duplicates(list(on)).assign(_in_reflex=True),
        on=list(on),
        how="left",
    )
    return tmp[tmp["_in_reflex"].isna()].drop(columns=["_in_reflex"])


def compute_m3_reliquat_node(
    m3_map: pd.DataFrame,
    reflex_map: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """
    Node Kedro : calcule le reliquat M3 (lignes sans match Reflex).
    Paramètres attendus:
      params["reliquat_flows"]
    """
    flows: List[Dict[str, Any]] = params["reliquat_flows"]

    m3 = m3_map.copy()
    rfx = reflex_map.copy()

    rfx_with_lot_keys = rfx[rfx["lot"].notna()][["sku", "lot", "category"]]
    rfx_no_lot_keys = rfx[rfx["lot"].isna()][["sku", "category"]]

    parts = []
    for spec in flows:
        logging.info(spec["name"])
        m3_part = _filter_by_lot_mode(m3, spec["lot_mode"])
        rfx_keys = rfx_with_lot_keys if spec["lot_mode"] == "with_lot" else rfx_no_lot_keys
        parts.append(_anti_merge_left_only(m3_part, rfx_keys, on=spec["key_cols"]))

    reliquat = pd.concat(parts, ignore_index=True)

    reliquat["reliquat_reason"] = reliquat["lot"].apply(
        lambda x: "NO_MATCH_WITH_LOT" if pd.notna(x) else "NO_MATCH_NO_LOT"
    )

    return reliquat[
        ["sku_m3", "sku", "lot", "depot", "category", "qty_m3", "reliquat_reason"]
    ]
