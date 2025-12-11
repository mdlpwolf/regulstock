"""
Fonctions : 
1. Extraction des lignes exclusivement dédiée aux PO 150
2. Création de la table des correctifs (champs : CONO,WHLO,ITNO,WHSL,BANO,STQI,STAG,BREM,RSCD)
"""
from typing import Any, Dict, List

import pandas as pd
import logging


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
    logging.info('Processing SKUs included in lots')
    
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
    logging.info('Processing lotless SKUs')

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
    logging.info('Recombobulating')

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
    logging.info('Processing residual M3 SKUs included in lots')

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
    logging.info('Processing lotless residual M3 SKUs')

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
    logging.info('Recombobulating')
    
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
    corr_df["exclu_150"] = corr_df["lot"].isin(pos).astype(int)

    return corr_df

def compute_m3_regul(reflex_m3_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule, pour chaque ligne (sku/lot/category/type), la quantité à RETIRER
    de chaque dépôt M3 pour que le stock total se rapproche de qty_reflex,
    en respectant les règles métier :

    1. SI category ∈ {STOCK, NDISP}
       - SI type == 'A01' :
         - on retire d'abord sur le stock_100 (sans passer en dessous de 0)
         - puis le reliquat éventuel sur le stock_150
       - SI type == 'A06' :
         - l'ajustement se fait uniquement sur stock_400

    2. SI category ∈ {DES, DEF}
       - l'ajustement se fait uniquement sur stock_200

    On ne traite que les cas où le stock M3 est SUPÉRIEUR au réflex :
        ecart_rfx_m3 = max(stock_total_m3 - qty_reflex, 0)

    Colonnes ajoutées :
      - regul_100 / regul_150 / regul_200 / regul_400 : quantités à retirer
      - regul_total                         : somme des retraits
      - stock_total_m3_apres_regul          : stock total après régul
      - ecart_rfx_m3_apres_regul            : nouvel écart (réflex - stock_après)
    """
    df = reflex_m3_df.copy()

    # --- Sécurisation des colonnes numériques ---
    num_cols = [
        "qty_reflex",
        "stock_100",
        "stock_150",
        "stock_200",
        "stock_400",
        "stock_total_m3",
        "ecart_rfx_m3",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # --- Colonnes de régul initialisées à 0 ---
    #for col in ["regul_100", "regul_150", "regul_200", "regul_400"]:
    #    df[col] = 0.0

    # ============================================================
    # 1. category ∈ {STOCK, NDISP}
    # ============================================================

    # 1.a) type == A01 → 100 puis 150
    mask_a01 = df["category"].isin(["STOCK", "NDISP"]) & (df["type"] == "A01")
    surplus_a01 = df["ecart_rfx_m3"].where(mask_a01, 0)

    # on retire d'abord sur le 100
    rm_100 = surplus_a01.clip(upper=df["stock_100"])
    restant_apres_100 = surplus_a01 - rm_100

    # puis sur le 150 (en tenant compte de l'exclusion éventuelle)
    rm_150 = (
        restant_apres_100.clip(lower=0)
    )

    df.loc[mask_a01, "regul_100"] = rm_100[mask_a01]
    df.loc[mask_a01, "regul_150"] = rm_150[mask_a01]

    # 1.b) type == A06 → uniquement dépôt 400
    mask_a06 = df["category"].isin(["STOCK", "NDISP"]) & (df["type"] == "A06")
    surplus_a06 = df["ecart_rfx_m3"].where(mask_a06, 0)

    rm_400 = surplus_a06.clip(upper=df["stock_400"])
    df.loc[mask_a06, "regul_400"] = rm_400[mask_a06]

    # ============================================================
    # 2. category ∈ {DES, DEF} → dépôt 200
    # ============================================================
    mask_desdef = df["category"].isin(["DES", "DEF"])
    surplus_desdef = df["ecart_rfx_m3"].where(mask_desdef, 0)

    rm_200 = surplus_desdef.clip(upper=df["stock_200"])
    df.loc[mask_desdef, "regul_200"] = rm_200[mask_desdef]

    # ============================================================
    # Synthèse & contrôle
    # ============================================================
    df["regul_total"] = df[["regul_100", "regul_150", "regul_200", "regul_400"]].sum(axis=1)

    return df

def generate_api_m3_rfx(
    reflex_m3_regul: pd.DataFrame,
    m3_cat: pd.DataFrame,
) -> pd.DataFrame:
    """
    Génère un fichier d'updates M3 au format STOCK_M3_RFX, à partir :

      - reflex_m3_regul : table agrégée par sku/lot/category/type
        contenant les colonnes de régulation :
            regul_100, regul_150, regul_200, regul_400, ...

      - m3_cat : lignes M3 détaillées (standardize_m3 + map_m3_category),
        avec au minimum les colonnes :
            sku, lot, depot, emplacement, category, qty_m3

    Sortie : DataFrame avec les colonnes :
        CONO, WHLO, ITNO, WHSL, BANO, STQI, STAG, BREM, RSCD

    Règle d'allocation :
      - pour chaque (sku, lot, category, depot) où qty_regul > 0,
        on répartit la quantité à retirer sur les lignes M3 détaillées
        correspondantes, en commençant par les plus grosses lignes de stock
        (qty_m3 décroissant), sans jamais dépasser la quantité disponible.

      - une ligne de sortie = 1 retrait sur une ligne M3
        (donc 1 dépôt / 1 lot / 1 emplacement / 1 sku).
    """

    # Copie des DF pour éviter les effets de bord
    regul_df = reflex_m3_regul.copy()
    m3 = m3_cat.copy()

    # Sécurisation colonnes
    if "lot" not in regul_df.columns:
        raise ValueError("reflex_m3_regul doit contenir une colonne 'lot'")
    if "category" not in regul_df.columns:
        raise ValueError("reflex_m3_regul doit contenir une colonne 'category'")
    if "sku" not in regul_df.columns:
        raise ValueError("reflex_m3_regul doit contenir une colonne 'sku'")

    # Colonnes de régul disponibles, typiquement regul_100, regul_150, ...
    regul_cols: List[str] = [c for c in regul_df.columns if c.startswith("regul_")]
    if not regul_cols:
        raise ValueError("Aucune colonne de régulation trouvée (attendu: 'regul_100', 'regul_150', ...)")

    # Mise au format long : une ligne par (sku, lot, category, depot, qty_regul)
    regul_long = regul_df.melt(
        id_vars=["sku", "lot", "category"],
        value_vars=regul_cols,
        var_name="regul_depot",
        value_name="qty_regul",
    )

    # regul_depot = "regul_100" → depot = "100"
    regul_long["depot"] = regul_long["regul_depot"].str.replace("regul_", "", regex=False)
    regul_long["qty_regul"] = pd.to_numeric(regul_long["qty_regul"], errors="coerce").fillna(0)

    # On ne garde que les lignes avec une régulation strictement positive
    regul_long = regul_long[regul_long["qty_regul"] > 0].copy()

    # Normalisation M3 détaillée
    m3["depot"] = m3["depot"].astype(str).str.strip()
    m3["sku"] = m3["sku"].astype(str).str.strip()
    m3["emplacement"] = m3["emplacement"].astype(str).str.strip()
    m3["qty_m3"] = pd.to_numeric(m3["qty_m3"], errors="coerce").fillna(0)
    m3["category"] = m3["category"].astype(str).str.strip()

    # On autorise lot = NA dans m3
    m3["lot"] = m3["lot"].astype("string")

    actions = []

    # Loop sur chaque groupe (sku, lot, category, depot) à réguler
    for row in regul_long.itertuples():
        sku = str(row.sku)
        lot = row.lot  # peut être <NA>
        category = str(row.category)
        depot = str(row.depot)
        restant = float(row.qty_regul)

        if restant <= 0:
            continue

        # Sélection des lignes M3 correspondant à cette régul
        if pd.notna(lot):
            # Cas AVEC lot : match strict sur lot
            subset = m3[
                (m3["sku"] == sku)
                & (m3["lot"] == str(lot))
                & (m3["depot"] == depot)
                & (m3["category"] == category)
            ].copy()
        else:
            # Cas SANS lot : match sur sku + category + depot, lot NA en M3
            subset = m3[
                (m3["sku"] == sku)
                & (m3["lot"].isna())
                & (m3["depot"] == depot)
                & (m3["category"] == category)
            ].copy()

        if subset.empty:
            # Rien à réguler pour ce groupe → on pourrait logger un warning ici
            continue

        # On trie par quantité décroissante pour vider d'abord les plus gros stocks
        subset = subset.sort_values("qty_m3", ascending=False)

        for _, m3_row in subset.iterrows():
            if restant <= 0:
                break

            dispo = float(m3_row["qty_m3"])
            if dispo <= 0:
                continue

            a_retirer = min(dispo, restant)
            if a_retirer <= 0:
                continue

            restant -= a_retirer

            actions.append(
                {
                    # Company (fixe, comme dans ton Excel)
                    "CONO": 100,
                    # Dépôt = WHLO
                    "WHLO": m3_row["depot"],
                    # SKU = ITNO
                    "ITNO": m3_row["sku_m3"],
                    # Emplacement = WHSL (ici on garde l'emplacement réel M3)
                    "WHSL": m3_row["emplacement"],
                    # Lot = BANO (vide si NA)
                    "BANO": "" if pd.isna(m3_row["lot"]) else str(m3_row["lot"]),
                    # Quantité à retirer = STQI (positive, l'interface sait que c'est un retrait)
                    "STQI": a_retirer,
                    # Statut, motif, code raison comme dans ton fichier d'exemple
                    "STAG": 2,
                    "BREM": "ECART",
                    "RSCD": "X01",
                }
            )

        # Si `restant > 0` ici, cela veut dire qu'on n'avait pas assez de stock
        # pour appliquer toute la régulation sur ce groupe.
        # On pourrait créer une ligne de log séparée si tu veux suivre ça.

    if not actions:
        # On renvoie un DF vide mais avec la bonne structure
        return pd.DataFrame(
            columns=["CONO", "WHLO", "ITNO", "WHSL", "BANO", "STQI", "STAG", "BREM", "RSCD"]
        )

    actions_df = pd.DataFrame(actions)

    # Typage basique
    actions_df["CONO"] = actions_df["CONO"].astype(int)
    actions_df["WHLO"] = actions_df["WHLO"].astype(str)
    actions_df["ITNO"] = actions_df["ITNO"].astype(str)
    actions_df["WHSL"] = actions_df["WHSL"].astype(str)
    actions_df["BANO"] = actions_df["BANO"].astype(str)
    actions_df["STQI"] = pd.to_numeric(actions_df["STQI"], errors="coerce").fillna(0).astype(int)
    actions_df["STAG"] = actions_df["STAG"].astype(int)
    actions_df["BREM"] = actions_df["BREM"].astype(str)
    actions_df["RSCD"] = actions_df["RSCD"].astype(str)

    return actions_df
