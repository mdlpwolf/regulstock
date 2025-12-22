"""
Fonctions : 
1. Extraction des lignes exclusivement dédiée aux PO 150
2. Création de la table des correctifs (champs : CONO,WHLO,ITNO,WHSL,BANO,STQI,STAG,BREM,RSCD)
"""
from typing import Any, Dict, List

import pandas as pd

# ========================================= Helpers =========================================

def _process_web_pos(
    corr_df: pd.DataFrame,
    pos_df : pd.DataFrame,
) -> pd.DataFrame :

    pos = pos_df.PO.to_list()
    corr_df["is_150"] = corr_df["lot"].isin(pos).astype(int)

    return corr_df

def _process_sms_sku(
    m3_df: pd.DataFrame,
) -> pd.DataFrame :

    m3_df['is_sms'] = m3_df['depot'].isin(["400"]).astype(int)

    return m3_df

# ========================================= Preprocessing =========================================

def map_m3(m3_df: pd.DataFrame, rules: List[Dict[str, Any]], pos_df : pd.DataFrame) -> pd.DataFrame:

    sms_df = _process_sms_sku(m3_df)

    mapped_df = _process_web_pos(sms_df, pos_df)

    return mapped_df

def map_reflex(reflex_df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df = reflex_df.copy()
    df["category"] = df["qualite"].map(mapping).fillna("UNMAPPED_REFLEX")
    return df