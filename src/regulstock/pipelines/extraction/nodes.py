import pandas as pd

def standardize_m3(m3_df: pd.DataFrame) -> pd.DataFrame:
    df = m3_df.rename(
        columns={
            "SKU": "sku_m3",
            "WMS": "sku_wms",
            "Depot": "depot",
            "Type": "type",
            "Emplacement": "category",
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
    df["category"] = df["category"].astype(str).str.strip()
    df["lot"] = df["lot"].astype(str).str.strip()
    df["type"] = df["type"].astype(str).str.strip()
    df["qty_m3"] = pd.to_numeric(df["qty_m3"], errors="coerce").fillna(0)

    df.loc[df["lot"].isin(["", "None", "nan", "NaN", "N/A"]), "lot"] = pd.NA

    return df[["sku", "sku_m3", "lot", "depot", "category", "type", "qty_m3"]]


def standardize_reflex(reflex_df: pd.DataFrame) -> pd.DataFrame:
    df = reflex_df.rename(
        columns={
            "SKU": "sku",
            "Qualite_Origine": "qualite",
            "Lot_1": "lot",
            "Stock_en_VL": "qty_reflex",
        }
    ).copy()

    df["sku"] = df["sku"].astype(str).str.strip()
    df["qualite"] = df["qualite"].astype(str).str.strip()
    df["lot"] = df["lot"].astype(str).str.strip()
    df["qty_reflex"] = pd.to_numeric(df["qty_reflex"], errors="coerce").fillna(0)

    df.loc[df["lot"].isin(["", "None", "nan", "NaN", "N/A"]), "lot"] = pd.NA

    return df[["sku", "lot", "qualite", "qty_reflex"]]
