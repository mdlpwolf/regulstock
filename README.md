# regulstock

Pipeline Kedro pour **réconcilier** les stocks entre **M3** (ERP) et **Reflex** (WMS), puis **générer un fichier d’updates M3** au format *STOCK_M3_RFX*.

Le projet est exécutable via la commande `regulstock` (script Kedro) ou via `python -m regulstock`.

---

## Objectif

1. Extraire les stocks M3 et Reflex depuis SQL  
2. Standardiser les données et les stocker en parquet (`data/01_raw/`)  
3. Catégoriser / mapper les données et construire une table de correspondance M3 ↔ Reflex  
4. Calculer les **quantités à retirer** dans M3 afin d’aligner le stock M3 sur Reflex  
5. Générer un fichier CSV d’update M3 : `API-MMS310MI.Update.csv`

---

## Prérequis

- Python `>= 3.10`
- Principales dépendances :
  - `kedro`
  - `pandas`
  - `sqlalchemy`
  - `pyodbc`
  - `kedro-datasets`
- Accès aux bases SQL M3 et Reflex

# regulstock

Pipeline Kedro pour **réconcilier** les stocks entre **M3** (ERP) et **Reflex** (WMS), puis **générer un fichier d’updates M3** au format *STOCK_M3_RFX*.

Le projet est exécutable via la commande `kedro run`.

---

## Objectif

1. Extraire les stocks M3 et Reflex depuis SQL  
2. Standardiser les données et les stocker en parquet (`data/01_raw/`)  
3. Catégoriser / mapper les données et construire une table de correspondance M3 ↔ Reflex  
4. Calculer les **quantités à retirer** dans M3 afin d’aligner le stock M3 sur Reflex  
5. Générer un fichier CSV d’update M3 : `API-MMS310MI.Update.csv`

---

## Prérequis

- Python `>= 3.10`
- Principales dépendances :
  - `kedro`
  - `pandas`
  - `sqlalchemy`
  - `pyodbc`
  - `kedro-datasets`
- Accès aux bases SQL M3 et Reflex

---

## Installation

En local (recommandé : environnement virtuel) :

```bash
uv venv
uv sync
source .venv/bin/activate

````

---

## Configuration

### Credentials SQL

Deux connexions SQL sont attendues :

* `wolfdb_M3_sql` : base M3
* `wolfdb_REFLEX_sql` : base Reflex

À définir dans `conf/local/credentials.yml` (non versionné) :

```yaml
wolfdb_M3_sql:
  con: "mssql+pyodbc://USER:PASSWORD@SERVER/M3?driver=ODBC+Driver+17+for+SQL+Server"

wolfdb_REFLEX_sql:
  con: "mssql+pyodbc://USER:PASSWORD@SERVER/REFLEX?driver=ODBC+Driver+17+for+SQL+Server"
```

---

### Paramètres métier

Les pipelines utilisent plusieurs paramètres de mapping :

* `reflex_mapping_rules`
* `m3_mapping_rules`
* `m3_depots_columns`

Ils sont définis dans `conf/base/parameters*.yml` (ou `conf/local/` selon l’environnement).

---

## Données & sorties

### Entrées (SQL)

* **Stock M3** : stock par article / dépôt / lot
* **PO M3** : informations complémentaires utilisées pour le mapping
* **Stock Reflex** : stock par SKU, qualité et lot

---

### Sorties principales

* `corr_dataset`
  Table réconciliée M3 / Reflex
  → `data/03_primary/rfx_m3_corr.parquet`

* `reflex_m3_regul`
  Table de régulation calculée
  → `data/03_primary/reflex_m3_regul.parquet`

* `stock_m3_rfx`
  Fichier CSV d’update M3
  → `data/05_model_input/API-MMS310MI.Update.csv`

---

## Pipelines Kedro

Les pipelines sont automatiquement enregistrés.
Le pipeline par défaut exécute l’ensemble de la chaîne.

---

### 1) Extraction

* Extraction et standardisation du stock M3
* Extraction et standardisation du stock Reflex

```bash
kedro run --pipeline extraction
```

---

### 2) Preprocessing

* Mapping et catégorisation Reflex
* Mapping et catégorisation M3 (avec PO)
* Construction de la table de correspondance M3 / Reflex
* Calcul du reliquat M3

```bash
kedro run --pipeline preprocessing
```

---

### 3) Processing

* Calcul des quantités à retirer dans M3
* Génération du fichier final d’update M3

```bash
kedro run --pipeline processing
```

---

### Exécution complète

```bash
kedro run
```

---

## Règles métier (régulation)

### Principe général

La régulation ne traite **que les cas où le stock M3 est supérieur au stock Reflex**.

```text
écart = max(stock_M3 - stock_Reflex, 0)
```

---

### Règles par catégorie

* **STOCK / NDISP**

  * Type `A01` : retrait prioritaire dépôt 100, puis 150
  * Type `A06` : retrait uniquement dépôt 400

* **DES / DEF**

  * Retrait uniquement dépôt 200

Les retraits sont calculés par dépôt (`regul_100`, `regul_150`, `regul_200`, `regul_400`) ainsi qu’un total.

---

### Génération du fichier M3

Le fichier CSV contient notamment :

```
CONO, WHLO, ITNO, WHSL, BANO, STQI, STAG, BREM, RSCD
```

Pour chaque article, la quantité à retirer est répartie sur les lignes M3 détaillées
en commençant par les plus gros stocks disponibles, sans jamais dépasser le stock réel.
    -> a terme, fonctionnement à changer pour imputer le stock 100 puis 150.

---

## Développement & debug

* Visualisation du graphe Kedro :

```bash
kedro viz run
```

* Les datasets intermédiaires sont stockés en parquet dans :

  * `data/02_intermediate/`
  * `data/03_primary/`

Ils permettent de relancer les pipelines sans réinterroger les bases SQL.

---

## Notes

* Toute la configuration des datasets est centralisée dans `conf/base/catalog.yml`
* Le projet est conçu pour être relancé partiellement (pipeline par pipeline)
* Le fichier CSV généré est prêt à être injecté dans l’API M3
