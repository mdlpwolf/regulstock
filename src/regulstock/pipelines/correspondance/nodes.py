from __future__ import annotations
import pandas as pd
from typing import Dict, Any, List


def standardize_m3(m3_df: pd.DataFrame) -> pd.DataFrame:
    df = m3_df.rename(columns={
        "SKU_M3": "sku",
        "Depot": "whlo",
        "Emplacement": "whsl",
        "Lot": "lot",
        "Quantite": "qty_m3",
    }).copy()

    df["sku"] = df["sku"].astype(str).str.strip()
    df["whlo"] = df["whlo"].astype(str).str.strip()
    df["whsl"] = df["whsl"].astype(str).str.strip()
    df["lot"] = df["lot"].astype(str).str.strip()
    df["qty_m3"] = pd.to_numeric(df["qty_m3"], errors="coerce").fillna(0)

    df.loc[df["lot"].isin(["", "None", "nan", "NaN"]), "lot"] = pd.NA

    return df[["sku", "lot", "whlo", "whsl", "qty_m3"]]


def standardize_reflex(reflex_df: pd.DataFrame) -> pd.DataFrame:
    df = reflex_df.rename(columns={
        "SKU": "sku",
        "Qualite_Origine": "qualite",
        "Emplacement": "reflex_emplacement",
        "Lot_1": "lot",
        "Stock_en_VL": "qty_reflex",
    }).copy()

    # si "Emplacement" n’existe pas parce que tu le calcules via CASE,
    # alors reflex_emplacement == Emplacement (déjà dans le SELECT)
    if "reflex_emplacement" not in df.columns:
        df["reflex_emplacement"] = df["qualite"]

    df["sku"] = df["sku"].astype(str).str.strip()
    df["qualite"] = df["qualite"].astype(str).str.strip()
    df["reflex_emplacement"] = df["reflex_emplacement"].astype(str).str.strip()
    df["lot"] = df["lot"].astype(str).str.strip()
    df["qty_reflex"] = pd.to_numeric(df["qty_reflex"], errors="coerce").fillna(0)

    df.loc[df["lot"].isin(["", "None", "nan", "NaN"]), "lot"] = pd.NA

    return df[["sku", "lot", "qualite", "reflex_emplacement", "qty_reflex"]]


def add_reflex_category(reflex_df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df = reflex_df.copy()
    df["reflex_category"] = df["qualite"].map(mapping).fillna("UNMAPPED_REFLEX")
    return df


def expand_reflex_to_m3_candidates(
    reflex_df: pd.DataFrame,
    rules: Dict[str, Any]
) -> pd.DataFrame:
    """
    Pour chaque ligne Reflex (sku, lot, reflex_category),
    génère N lignes candidates M3 selon rules.
    """
    rows = []
    for _, r in reflex_df.iterrows():
        cat = r["reflex_category"]
        if cat not in rules:
            continue

        whlo_list = rules[cat]["whlo_in"]
        whsl_eq = rules[cat]["whsl_eq"]

        for whlo in whlo_list:
            rows.append({
                "sku": r["sku"],
                "lot": r["lot"],
                "reflex_emplacement": r["reflex_emplacement"],
                "reflex_category": cat,
                "qty_reflex": r["qty_reflex"],
                "whlo_candidate": whlo,
                "whsl_candidate": whsl_eq,
                "mapping_rule": f"{cat} -> WHLO {whlo_list} / WHSL {whsl_eq}",
            })

    return pd.DataFrame(rows)


def join_with_m3_real(
    reflex_candidates: pd.DataFrame,
    m3_df: pd.DataFrame
) -> pd.DataFrame:
    """
    On garde uniquement les correspondances qui existent en M3.
    Join sur (sku, lot, whlo, whsl).
    """
    merged = reflex_candidates.merge(
        m3_df,
        left_on=["sku", "lot", "whlo_candidate", "whsl_candidate"],
        right_on=["sku", "lot", "whlo", "whsl"],
        how="left",
    )

    # si tu veux garder aussi les candidats sans stock M3, commente cette ligne
    # merged = merged[merged["qty_m3"].notna()].copy()

    # mise en forme finale
    merged = merged.rename(columns={
        "whlo": "m3_depot",
        "whsl": "m3_emplacement",
    })

    merged["qty_m3"] = merged["qty_m3"].fillna(0)

    return merged[[
        "sku",
        "lot",
        "reflex_emplacement",
        "reflex_category",
        "m3_depot",
        "m3_emplacement",
        "mapping_rule",
        "qty_reflex",
        "qty_m3",
    ]]
