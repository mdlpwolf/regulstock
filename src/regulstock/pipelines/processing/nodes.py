"""
Fonctions : 
1. Extraction des lignes exclusivement dédiée aux PO 150
2. Création de la table des correctifs (champs : CONO,WHLO,ITNO,WHSL,BANO,STQI,STAG,BREM,RSCD)
"""
from typing import Any, Dict, List

import pandas as pd


def map_reflex_category(reflex_df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df = reflex_df.copy()
    df["category"] = df["qualite"].map(mapping).fillna("UNMAPPED_REFLEX")
    return df


def map_m3_category(m3_df: pd.DataFrame, rules: List[Dict[str, Any]]) -> pd.DataFrame:
    df = m3_df.copy()
    df["category"] = pd.NA

    for r in rules:
        depot_in = set(r["depot_in"])
        emplacement_eq = r["emplacement_eq"]
        cat = r["category"]
        mask = df["depot"].isin(depot_in) & (df["emplacement"] == emplacement_eq)
        df.loc[mask, "category"] = cat

    df["category"] = df["category"].fillna("UNMAPPED_M3")
    return df

def build_reflex_m3_wide_with_lotless(
    reflex_cat: pd.DataFrame,
    m3_cat: pd.DataFrame,
    depots: List[str],
) -> pd.DataFrame:
    """
    Fabrique la table finale Reflex pivot → colonnes stock_100/150/400,
    en gérant:
      - correspondance AVEC lot: join sur (sku, lot, category)
      - correspondance SANS lot: join sur (sku, category) après agrégation
    """

    # --- filtre M3 sur dépôts cibles ---
    m3_filtered = m3_cat[m3_cat["depot"].isin(depots)].copy()

    # =========================
    # A) FLUX AVEC LOT
    # =========================
    reflex_with_lot = reflex_cat[reflex_cat["lot"].notna()].copy()
    m3_with_lot = m3_filtered[m3_filtered["lot"].notna()].copy()

    # agrég M3 avec lot par dépôt
    m3_with_lot_agg = (
        m3_with_lot
        .groupby(["depot","category", "lot", "type", "sku"], dropna=False)["qty_m3"]
        .sum()
        .reset_index()
    )

    # pivot large
    m3_with_lot_wide = (
        m3_with_lot_agg
        .pivot_table(
            index=["category", "lot", "type", "sku"],
            columns=["depot"],
            values="qty_m3",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    m3_with_lot_wide = m3_with_lot_wide.rename(
        columns={d: f"stock_{d}" for d in depots if d in m3_with_lot_wide.columns}
    )
    for d in depots:
        col = f"stock_{d}"
        if col not in m3_with_lot_wide.columns:
            m3_with_lot_wide[col] = 0

    wide_with_lot = reflex_with_lot.merge(
        m3_with_lot_wide,
        on=["category", "lot", "sku"],
        how="left",
    )
    for d in depots:
        wide_with_lot[f"stock_{d}"] = wide_with_lot[f"stock_{d}"].fillna(0)

    # =========================
    # B) FLUX SANS LOT
    # =========================
    reflex_no_lot = reflex_cat[reflex_cat["lot"].isna()].copy()
    m3_no_lot = m3_filtered[m3_filtered["lot"].isna()].copy()

    # agrég Reflex sans lot (clé = sku + category)
    reflex_no_lot_agg = (
        reflex_no_lot
        .groupby([ "reflex_emplacement", "category", "sku"], dropna=False)["qty_reflex"]
        .sum()
        .reset_index()
    )
    reflex_no_lot_agg["lot"] = pd.NA  # clé explicitement sans lot

    # agrég M3 sans lot par dépôt (clé = sku + category)
    m3_no_lot_agg = (
        m3_no_lot
        .groupby(["depot","category", "type", "sku"], dropna=False)["qty_m3"]
        .sum()
        .reset_index()
    )

    # pivot large sans lot
    m3_no_lot_wide = (
        m3_no_lot_agg
        .pivot_table(
            index=["category", "type", "sku"],
            columns=["depot"],
            values="qty_m3",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    m3_no_lot_wide = m3_no_lot_wide.rename(
        columns={d: f"stock_{d}" for d in depots if d in m3_no_lot_wide.columns}
    )
    for d in depots:
        col = f"stock_{d}"
        if col not in m3_no_lot_wide.columns:
            m3_no_lot_wide[col] = 0

    wide_no_lot = reflex_no_lot_agg.merge(
        m3_no_lot_wide,
        on=["sku", "category"],
        how="left",
    )
    for d in depots:
        wide_no_lot[f"stock_{d}"] = wide_no_lot[f"stock_{d}"].fillna(0)

    # =========================
    # C) RECOMBINAISON
    # =========================
    out = pd.concat([wide_with_lot, wide_no_lot], ignore_index=True)

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
            "ecart_rfx_m3"
        ]
    ]

def compute_m3_reliquat(
    m3_cat: pd.DataFrame,
    reflex_cat: pd.DataFrame,
) -> pd.DataFrame:
    """
    Retourne les lignes M3 qui ne trouvent aucune correspondance Reflex.
    Règles:
      - si lot présent en M3 -> match requis sur (sku, lot, category)
      - si lot absent en M3 -> match sur (sku, category)
    """

    m3 = m3_cat.copy()
    rfx = reflex_cat.copy()

    # --- clés Reflex disponibles ---
    rfx_with_lot_keys = (
        rfx[rfx["lot"].notna()][["sku", "lot", "category"]]
        .drop_duplicates()
    )

    rfx_no_lot_keys = (
        rfx[rfx["lot"].isna()][["sku", "category"]]
        .drop_duplicates()
    )

    # =========================
    # A) reliquat M3 avec lot
    # =========================
    m3_with_lot = m3[m3["lot"].notna()].copy()

    m3_with_lot = m3_with_lot.merge(
        rfx_with_lot_keys.assign(_in_reflex=True),
        on=["sku", "lot", "category"],
        how="left",
    )

    reliquat_with_lot = m3_with_lot[m3_with_lot["_in_reflex"].isna()].drop(columns=["_in_reflex"])

    # =========================
    # B) reliquat M3 sans lot
    # =========================
    m3_no_lot = m3[m3["lot"].isna()].copy()

    m3_no_lot = m3_no_lot.merge(
        rfx_no_lot_keys.assign(_in_reflex=True),
        on=["sku", "category"],
        how="left",
    )

    reliquat_no_lot = m3_no_lot[m3_no_lot["_in_reflex"].isna()].drop(columns=["_in_reflex"])

    # =========================
    # C) concat final
    # =========================
    reliquat = pd.concat([reliquat_with_lot, reliquat_no_lot], ignore_index=True)

    # petit flag utile
    reliquat["reliquat_reason"] = reliquat["lot"].apply(
        lambda x: "NO_MATCH_WITH_LOT" if pd.notna(x) else "NO_MATCH_NO_LOT"
    )

    return reliquat[
        ["sku", "lot", "depot", "emplacement", "category", "qty_m3", "reliquat_reason"]
    ]

def process_web_pos(
    corr_df: pd.DataFrame,
    pos_df : pd.DataFrame,
) -> pd.DataFrame :
    
    pos = pos_df.PO.to_list()
    filtered_df = corr_df[corr_df['lot'].isin(pos)]

    return filtered_df
