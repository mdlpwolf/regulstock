from typing import Dict, List, Any
import pandas as pd

def standardize_m3(m3_df: pd.DataFrame) -> pd.DataFrame:
    df = m3_df.rename(
        columns={
            "SKU": "sku_m3",
            "WMS": "sku_wms",
            "Depot": "depot",
            "Type": "type",
            "Emplacement": "emplacement",
            "Lot": "lot",
            "Quantite": "qty_m3",
        }
    ).copy()

    # normalisation des 2 colonnes SKU
    df["sku_m3"] = df["sku_m3"].astype(str).str.strip()
    df["sku_wms"] = df["sku_wms"].astype(str).str.strip()

    # considère comme "vide" : NaN, None, '', 'nan', 'NaN'
    wms_empty = df["sku_wms"].isin(["", "None", "nan", "NaN","N/A"]) | df["sku_wms"].isna()

    # priorité WMS sinon SKU_M3
    df["sku"] = df["sku_wms"].where(~wms_empty, df["sku_m3"])

    # autres colonnes
    df["depot"] = df["depot"].astype(str).str.strip()
    df["emplacement"] = df["emplacement"].astype(str).str.strip()
    df["lot"] = df["lot"].astype(str).str.strip()
    df["type"] = df["type"].astype(str).str.strip()
    df["qty_m3"] = pd.to_numeric(df["qty_m3"], errors="coerce").fillna(0)

    df.loc[df["lot"].isin(["", "None", "nan", "NaN", "N/A"]), "lot"] = pd.NA

    return df[["sku", "sku_m3", "lot", "depot", "emplacement", "type", "qty_m3"]]


def standardize_reflex(reflex_df: pd.DataFrame) -> pd.DataFrame:
    df = reflex_df.rename(
        columns={
            "SKU": "sku",
            "Qualite_Origine": "qualite",
            "Emplacement": "reflex_emplacement",
            "Lot_1": "lot",
            "Stock_en_VL": "qty_reflex",
        }
    ).copy()

    # si tu n'as pas "Emplacement" dans Reflex parce que tu le calcules en CASE,
    # cette colonne existe déjà (alias Emplacement). sinon fallback sur qualite
    if "reflex_emplacement" not in df.columns:
        df["reflex_emplacement"] = df["qualite"]

    df["sku"] = df["sku"].astype(str).str.strip()
    df["qualite"] = df["qualite"].astype(str).str.strip()
    df["reflex_emplacement"] = df["reflex_emplacement"].astype(str).str.strip()
    df["lot"] = df["lot"].astype(str).str.strip()
    df["qty_reflex"] = pd.to_numeric(df["qty_reflex"], errors="coerce").fillna(0)

    df.loc[df["lot"].isin(["", "None", "nan", "NaN", "N/A"]), "lot"] = pd.NA

    return df[["sku", "lot", "qualite", "reflex_emplacement", "qty_reflex"]]
