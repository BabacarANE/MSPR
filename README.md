# ElectioAnalytics

> Proof-of-concept de prédiction des tendances électorales à l'échelle communale — Département du Nord (59)

**Type :** Apprentissage supervisé — classification multiclasse  
**Données :** Élections présidentielles 2017 (train) et 2022 (test), couplées à 5 indicateurs socio-économiques  
**Cible :** `bloc_vainqueur` — DROITE / CENTRE / GAUCHE par commune  
**Projection :** Prédiction 2027 via modèle socio-économique pur (Version B)

---

## Résultats principaux

| Modèle | Version | Accuracy | F1 macro | AUC-ROC |
|--------|---------|----------|----------|---------|
| Random Forest | A (avec historique) | 99.5% | 0.996 | — |
| Random Forest | B (socio-éco pur) | 76.5% | 0.525 | — |
| Régression Logistique | B (socio-éco pur) | 88.7% | 0.787\* | 0.884 |

\* F1 binaire (DROITE vs reste)

**Feature la plus prédictive (Version B) :** `revenu_median` (importance RF 0.276), suivi de `taux_faible_diplome` (0.218).

---

## Structure du projet

```
ElectioAnalytics/
├── data/
│   ├── raw/                    ← données brutes (non modifiées)
│   │   ├── elections/
│   │   ├── demographie/
│   │   ├── emploi/
│   │   ├── securite/
│   │   ├── economie/
│   │   └── social/
│   ├── curated/                ← données nettoyées par source
│   │   ├── election/
│   │   ├── demographie/
│   │   ├── emploi/
│   │   ├── securite/
│   │   ├── economie/
│   │   └── social/
│   ├── final/                  ← datasets ML prêts à l'emploi
│   │   ├── dataset_ml_final.csv        (1296 × 37)
│   │   ├── ml_train_2017_vA.csv
│   │   ├── ml_test_2022_vA.csv
│   │   ├── ml_train_2017_vB.csv
│   │   └── ml_test_2022_vB.csv
│   └── models/                 ← modèles entraînés
│       ├── best_model_vB.pkl
│       ├── scaler.pkl
│       ├── comparison_results.csv
│       └── best_model_meta.json
│
├── src/
│   ├── cleaning/               ← scripts de nettoyage par source
│   │   ├── electoral_data_cleaner_2017.py
│   │   ├── electoral_data_cleaner_2022.py
│   │   ├── pivot_election.py
│   │   ├── demographic_data_cleaner_2016.py
│   │   ├── demographic_data_cleaner_2021.py
│   │   ├── unemployment_data_cleaner.py
│   │   ├── delinquency_data_cleaner.py
│   │   ├── economic_data_cleaner.py
│   │   └── education_data_cleaner.py
│   ├── pipeline/               ← construction dataset + preprocessing
│   │   ├── build_dataset.py
│   │   └── preprocessing.py
│   └── models/                 ← scripts d'entraînement
│       ├── model_binaire.py
│       └── model_binaire_benchmark.py
│
├── notebooks/                  ← analyses et résultats
│   ├── 01_exploration.ipynb    ← EDA + distributions + corrélations
│   ├── 02_model_binaire.ipynb  ← Régression Logistique binaire
│   ├── 03_model_multiclasse.ipynb  ← Random Forest multiclasse
│   ├── 04_model_comparison.ipynb   ← Comparaison 5 modèles
│   └── 05_projections_2027.ipynb   ← Prédictions et carte choroplèthe
│
├── scripts/                    ← automatisation
│   ├── run_pipeline.py         ← pipeline complet (nettoyage → dataset)
│   └── run_models.py           ← comparaison + sélection du meilleur modèle
│
├── reorganize.py               ← à exécuter une seule fois (migration)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Installation

```bash
# 1. Cloner le repository
git clone <url-du-repo>
cd ElectioAnalytics

# 2. Créer l'environnement virtuel
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/Mac

# 3. Installer les dépendances
pip install -r requirements.txt
```

---

## Utilisation

### Pipeline complet (première exécution)

```bash
# Nettoyage + construction dataset + preprocessing
python scripts/run_pipeline.py

# Comparaison des 5 modèles + sélection du meilleur
python scripts/run_models.py
```

### Notebooks (analyse et visualisation)

Lancer Jupyter puis ouvrir dans l'ordre :

```bash
jupyter notebook notebooks/
```

| Notebook | Contenu |
|----------|---------|
| `01_exploration.ipynb` | Distribution des blocs, corrélations, matrice de transition |
| `02_model_binaire.ipynb` | Régression Logistique DROITE vs reste, coefficients |
| `03_model_multiclasse.ipynb` | Random Forest 3 classes, feature importance |
| `04_model_comparison.ipynb` | Benchmark 5 modèles, graphiques comparatifs |
| `05_projections_2027.ipynb` | Prédictions 2027, carte choroplèthe du Nord |

---

## Pipeline de données

```
data/raw/
    elections/          ← fichiers XLS/XLSX du Ministère de l'Intérieur
    demographie/        ← recensement INSEE (RP 2016, 2021)
    emploi/             ← taux de chômage INSEE
    securite/           ← délinquance data.gouv
    economie/           ← revenus médians INSEE Filosofi
    social/             ← diplômes INSEE

         ↓  src/cleaning/  (9 scripts)

data/curated/           ← données nettoyées, 1 fichier par source/année

         ↓  src/pipeline/build_dataset.py

data/final/dataset_ml_final.csv    (1296 lignes × 37 colonnes, 0 NaN)

         ↓  src/pipeline/preprocessing.py

data/final/ml_train_2017_v[AB].csv + ml_test_2022_v[AB].csv
```

**Logique temporelle :** les indicateurs socio-économiques utilisent l'année N-1 par rapport à l'élection (2016 → élection 2017, 2021 → élection 2022) pour capturer les conditions de vie précédant le vote.

---

## Choix méthodologiques

### Deux versions de dataset

| Version | Features | Usage |
|---------|----------|-------|
| **A** | historique électoral + socio-éco (7 features) | Plafond théorique (99.5%) |
| **B** | socio-éco pur (5 features) | Prédiction 2027 ✅ |

La Version A n'est pas utilisable pour la projection 2027 (l'historique électoral 2022 n'existe pas encore pour prédire 2027).

### Déséquilibre de classes

La domination DROITE (72.5% des communes en 2022) est traitée avec `class_weight='balanced'`. Le F1 macro est la métrique principale — plus pénalisante que l'accuracy sur les classes minoritaires.

### Suppression de features

- `inscrits` supprimé (corrélation 0.99 avec `population_municipale`)
- `taux_chomage` supprimé (corrélation -0.69 avec `revenu_median`)

---

## Sources des données

| Indicateur | Source | Années |
|-----------|--------|--------|
| Résultats électoraux | Ministère de l'Intérieur | 2017, 2022 |
| Population municipale | INSEE Recensement | 2016, 2021 |
| Taux de pauvreté | INSEE | 2016, 2021 |
| Taux de chômage | INSEE RP | 2016, 2021 |
| Revenu médian | INSEE Filosofi | 2016, 2021 |
| Taux de délinquance | data.gouv.fr | 2016, 2021 |
| Niveau de diplôme | INSEE | 2016, 2022 |

---

## Dépendances principales

```
pandas >= 1.5
scikit-learn >= 1.2
numpy >= 1.23
matplotlib >= 3.6
seaborn >= 0.12
folium >= 0.14          # carte choroplèthe interactive
geopandas >= 0.12       # données géographiques communes
openpyxl >= 3.0         # lecture fichiers Excel
```

Voir `requirements.txt` pour la liste complète avec versions fixées.