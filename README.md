# 🏗️ Datalake Regression Testing — Medallion Architecture

> **Big Data Testing Project** | PySpark · Pytest · Medallion Architecture (Bronze/Silver/Gold)

---

## 🇫🇷 Description

Suite de tests de non-régression automatisés pour une architecture Datalake **Bronze → Silver → Gold**.
Vérifie que chaque transformation ne dégrade pas la qualité des données entre les releases.
Inclut un comparateur DataFrame pour détecter les dérives de schéma, volumétrie et statistiques.

## 🇬🇧 Description

Automated regression test suite for a **Bronze → Silver → Gold** Medallion Datalake architecture.
Ensures that each pipeline release doesn't degrade data quality. Includes a DataFrame comparator
for detecting schema drift, row count anomalies, and statistical deviations between releases.

---

## 🗂️ Structure du projet

```
02_datalake_regression_tests/
├── src/
│   ├── datalake/
│   │   └── layers.py              # Transformations Bronze→Silver→Gold
│   └── comparators/
│       └── dataframe_comparator.py  # Comparaison release-over-release
├── tests/
│   ├── conftest.py                  # Fixtures Spark + datasets de test
│   ├── smoke/
│   │   └── test_smoke.py            # TC-SMOKE-001 → 004 (sanity checks rapides)
│   └── regression/
│       ├── test_bronze_to_silver.py # TC-REG-001 → 010
│       ├── test_silver_to_gold.py   # TC-GOLD-001 → 008
│       └── test_comparator.py       # TC-COMP-001 → 006
├── data/
│   ├── bronze/    # Données brutes (CSV/JSON)
│   ├── silver/    # Données nettoyées (Parquet)
│   ├── gold/      # Données agrégées (Parquet)
│   └── reference/ # Snapshots de référence pour comparaison release N-1
├── docs/
└── scripts/
    └── run_regression.sh
```

---

## 🏛️ Architecture Medallion

```
CSV/JSON → [BRONZE] → bronze_to_silver() → [SILVER] → silver_to_gold() → [GOLD]
  Raw        Stockage brut    Nettoyage             Agrégation business
```

| Couche | Description | Règles appliquées |
|--------|-------------|-------------------|
| **Bronze** | Données brutes, telles quelles | Aucune transformation |
| **Silver** | Données nettoyées et enrichies | Filtrage NULLs, cast types, dédup, statuts valides |
| **Gold** | Agrégats business | Transactions COMPLETED uniquement |

---

## 🧪 Stratégie de Test

### Pyramide de tests

```
         [Smoke]     ← 4 tests rapides, run first
        [Regression] ← 24 tests de non-régression
```

### Types de vérification

| Vérification | Outil | Description |
|---|---|---|
| Schema drift | `compare_schemas()` | Colonnes ajoutées/supprimées/typées |
| Volumétrie | `compare_row_counts()` | Delta % entre releases (tolérance configurable) |
| Contenu | `compare_content()` | Lignes disparues / nouvelles |
| Dérive statistique | `compare_numeric_stats()` | SUM/MEAN drift sur colonnes numériques |
| Règles métier | Tests regression | Nulls, doublons, formats, réconciliation |

---

## ⚙️ Installation & Exécution

```bash
pip install -r requirements.txt

# Smoke tests (rapides — à lancer en premier)
pytest tests/smoke/ -v

# Tests de régression complets
pytest tests/regression/ -v

# Tous les tests avec rapport HTML
pytest --html=reports/regression_report.html
```

---

## 📊 Anomalies volontaires dans les données de test

| Anomalie | Couche | Test qui la détecte |
|---|---|---|
| `customer_id = NULL` | Bronze | TC-REG-002 |
| `amount = -50.00` | Bronze | TC-REG-003 |
| `status = CANCELLED` | Bronze | TC-REG-004 |
| Doublon sur `event_id` | Bronze | TC-REG-005 |

---

## 👩‍💻 Auteure

**Imane Moussafir** — Ingénieure Data & BI  
*Projet réalisé dans le cadre d'une candidature Testeur Big Data / Datalake.*
