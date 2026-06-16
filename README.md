# ElectioAnalytics

> Preuve de concept de prédiction des tendances électorales à l'échelle communale
> **Département du Nord (59) — Élection Présidentielle Tour 1 — Train 2017 / Test 2022**

**Type :** Apprentissage supervisé — classification binaire & multiclasse
**Données :** 648 communes × 2 années = 1 296 observations, 5 indicateurs socio-économiques
**Variable cible :** `bloc_vainqueur` — DROITE / CENTRE / GAUCHE
**Projection :** Prédiction 2027 via modèle socio-économique pur (Version B)
**Équipe :** Babacar Diallo, Marie Dupont, Thomas Bernard, Léa Martin — M1 EISI EPSI 2024-2025

---

## Résultats principaux

| Tâche | Meilleur modèle | Accuracy | F1 Macro | AUC-ROC |
|-------|----------------|----------|----------|---------|
| Binaire (DROITE vs EXCEPTION) | Régression Logistique | **88.7%** | **0.855** | **0.884** |
| Multiclasse (DROITE/CENTRE/GAUCHE) | SVM (kernel RBF) | **82.1%** | **0.659** | **0.916** |

**Feature la plus prédictive :** `taux_faible_diplome` (corrélation Pearson +0.66 avec la cible en 2022, coefficient LR +1.040)

---

## Table des matières

1. [Structure du projet](#structure)
2. [Données brutes — Google Drive](#donnees-brutes)
3. [Installation Python](#installation)
4. [Lancer le pipeline complet](#pipeline)
5. [Base de données PostgreSQL avec Docker](#docker)
6. [Notebooks](#notebooks)
7. [Choix méthodologiques](#methodo)
8. [Sources des données](#sources)
9. [Dépendances](#dependances)

---

## 1. Structure du projet {#structure}

```
ElectioAnalytics/
│
├── data/
│   ├── raw/                        ← ⚠️ DONNÉES BRUTES (voir section Google Drive)
│   │   ├── elections/              ← XLS/XLSX Ministère de l'Intérieur
│   │   ├── demographie/            ← XLS INSEE Recensement
│   │   ├── emploi/                 ← CSV INSEE taux de chômage
│   │   ├── securite/               ← CSV data.gouv.fr délinquance
│   │   ├── economie/               ← XLS/CSV INSEE Filosofi revenus
│   │   └── social/                 ← CSV INSEE diplômes
│   │
│   ├── curated/                    ← données nettoyées (générées par run_cleaning.py)
│   │   ├── election/
│   │   ├── demographie/
│   │   ├── emploi/
│   │   ├── securite/
│   │   ├── economie/
│   │   └── social/
│   │
│   ├── final/                      ← datasets ML prêts (générés par run_pipeline.py)
│   │   ├── dataset_ml_final.csv    ← 1 296 × 38 colonnes, 0 NaN
│   │   ├── ml_train_2017_vA.csv    ← Version A : 7 features (avec historique)
│   │   ├── ml_test_2022_vA.csv
│   │   ├── ml_train_2017_vB.csv    ← Version B : 5 features (socio-éco pur) ✅
│   │   ├── ml_test_2022_vB.csv
│   │   └── projections_2027.csv    ← généré par 05_projections_2027.ipynb
│   │
│   └── models/                     ← modèles entraînés (générés par run_models.py)
│       ├── best_model_binaire_vB.pkl
│       ├── best_model_multiclasse_vB.pkl
│       ├── scaler.pkl
│       ├── results_binaire.csv
│       ├── results_multiclasse.csv
│       ├── best_model_meta.json
│       └── figures/                ← 12 graphiques PNG
│
├── src/
│   ├── cleaning/                   ← 9 scripts de nettoyage
│   │   ├── electoral_data_cleaner_2017.py
│   │   ├── electoral_data_cleaner_2022.py
│   │   ├── pivot_election.py
│   │   ├── demographic_data_cleaner_2016.py
│   │   ├── demographic_data_cleaner_2021.py
│   │   ├── unemployment_data_cleaner.py
│   │   ├── delinquency_data_cleaner.py
│   │   ├── economic_data_cleaner.py
│   │   └── education_data_cleaner.py
│   │
│   ├── pipeline/
│   │   ├── build_dataset.py        ← jointure des 9 sources sur code_geo
│   │   └── preprocessing.py        ← log-transform, scaling, split vA/vB
│   │
│   └── models/
│       ├── model_binaire.py
│       └── model_binaire_benchmark.py
│
├── notebooks/
│   ├── 01_exploration.ipynb        ← EDA, distributions, corrélations, cartes
│   ├── 02_model_binaire.ipynb      ← Régression Logistique
│   ├── 03_model_multiclasse.ipynb  ← SVM multiclasse
│   ├── 04_model_comparison.ipynb   ← benchmark 6 modèles
│   └── 05_projections_2027.ipynb   ← projections 3 scénarios
│
├── scripts/
│   ├── run_cleaning.py             ← exécute les 9 scripts de nettoyage
│   ├── run_pipeline.py             ← nettoyage + build_dataset + preprocessing
│   └── run_models.py               ← 6 modèles, métriques, 9 figures
│
├── sql/
│   ├── init_schema.sql             ← DDL PostgreSQL (star schema)
│   ├── docker-compose.yml          ← PostgreSQL 16 conteneurisé
│   └── migrate.py                  ← ETL CSV → PostgreSQL
│
├── results/
│   └── figures/                    ← figures générées par run_models.py
│
├── requirements.txt
├── .gitignore
└── README.md

```

---

## 2. Données brutes — Google Drive {#donnees-brutes}

> ⚠️ Les données brutes ne sont **pas versionnées** dans ce repository (fichiers volumineux, licences).
> Elles sont disponibles sur Google Drive et doivent être téléchargées manuellement avant toute exécution.

### 🔗 Lien Google Drive

```
https://drive.google.com/drive/folders/1ASIAZMSivQ7KImAboJqM2m_ZHzaJetja?usp=drive_link
```

### Téléchargement et placement des fichiers

**Étape 1 — Ouvrir le lien** et télécharger chaque dossier

**Étape 2 — Placer les fichiers dans la structure suivante :**

```
data/raw/
├── elections/
│   ├── election_2017_Tour_1.xls          ← feuille "Feuil1", header=3
│   └── election_2022_Tour_1.xlsx         ← feuille "Tour1", header=0
│
├── demographie/
│   ├── dep59.xlsx                        ← population 2016 (feuille "communes", header=8)
│   └── dep59_2021.xlsx                   ← population 2021
│
├── emploi/
│   └── donnees_2011_2016_2022.csv        ← taux de chômage INSEE RP
│
├── securite/
│   └── delinquence.csv                   ← délinquance par commune
│
├── economie/
│   ├── eco_2016.xls                      ← revenus médians 2016 INSEE Filosofi
│   └── eco_2021.csv                      ← revenus médians 2021
│
└── social/
    └── DIPLOMES_2022_data.csv            ← niveaux de diplôme INSEE
```

**Étape 3 — Vérifier la structure :**

```bash
# Vérifier que tous les fichiers sont présents
python -c "
import os
fichiers = [
    'data/raw/elections/election_2017_Tour_1.xls',
    'data/raw/elections/election_2022_Tour_1.xlsx',
    'data/raw/demographie/dep59.xlsx',
    'data/raw/emploi/donnees_2011_2016_2022.csv',
    'data/raw/securite/delinquence.csv',
    'data/raw/economie/eco_2016.xls',
    'data/raw/social/DIPLOMES_2022_data.csv',
]
for f in fichiers:
    status = '✅' if os.path.exists(f) else '❌ MANQUANT'
    print(f'{status} {f}')
"
```

---

## 3. Installation Python {#installation}

### Prérequis

- Python 3.11+
- pip
- (Optionnel) Docker Desktop pour la base de données PostgreSQL

### Installation

```bash
# 1. Cloner le repository
git clone <url-du-repo>
cd ElectioAnalytics

# 2. Créer l'environnement virtuel
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux / Mac
source .venv/bin/activate

# 3. Installer les dépendances
pip install -r requirements.txt
```

### Vérification

```bash
python -c "import pandas, sklearn, numpy, matplotlib; print('✅ Dépendances OK')"
```

---

## 4. Lancer le pipeline complet {#pipeline}

> ⚠️ Les données brutes doivent être dans `data/raw/` avant de commencer (voir section 2).

### Ordre d'exécution

```
reorganize.py          (une seule fois — migration initiale)
    ↓
run_cleaning.py        (nettoyage des 9 sources)
    ↓
run_pipeline.py        (jointure + preprocessing)
    ↓
run_models.py          (6 modèles + figures)
    ↓
05_projections_2027.ipynb  (projections manuelles dans Jupyter)
```

### Commandes

```bash
# ── ÉTAPE 0 : Migration initiale (première fois seulement) ──────────────────
python reorganize.py
python scripts/fix_paths.py

# ── ÉTAPE 1 : Nettoyage des données brutes ──────────────────────────────────
# Lance les 9 scripts de src/cleaning/ séquentiellement
# Sortie : data/curated/ (9 fichiers CSV)
python scripts/run_cleaning.py

# ── ÉTAPE 2 : Construction du dataset ML ────────────────────────────────────
# Jointure sur code_geo + logique temporelle N-1 + preprocessing
# Sortie : data/final/ (dataset_ml_final.csv + splits vA/vB)
python scripts/run_pipeline.py

# ── ÉTAPE 3 : Entraînement et comparaison des modèles ───────────────────────
# 6 modèles × 2 classifications + 9 figures + best models .pkl
# Sortie : data/models/ + results/figures/
python scripts/run_models.py

# ── ÉTAPE 4 : Projections 2027 (Jupyter) ────────────────────────────────────
jupyter notebook notebooks/05_projections_2027.ipynb
```

### Durée estimée

| Étape | Durée |
|-------|-------|
| run_cleaning.py | ~2 minutes |
| run_pipeline.py | ~1 minute |
| run_models.py | ~5-10 minutes |
| 05_projections_2027.ipynb | ~3 minutes |

---

## 5. Base de données PostgreSQL avec Docker {#docker}

Le dataset final peut être chargé dans une base PostgreSQL 16 conteneurisée via Docker Compose.
Le schéma implémente un **star schema** (schéma en étoile) optimisé pour les requêtes analytiques.

### Architecture de la base

```
dim_commune ──────┐
dim_temps ────────┼──→ fait_electoral (table de faits centrale)
dim_socio ────────┘         │
                            └──→ dim_candidat (top 5 candidats par commune)
```

### Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installé et démarré
- Python avec `sqlalchemy` et `psycopg2-binary`

```bash
pip install sqlalchemy psycopg2-binary
```

### Étape 1 — Démarrer PostgreSQL

```bash
# Se placer dans le dossier sql/
cd sql/

# Démarrer le conteneur PostgreSQL 16
# (-d = mode détaché, le conteneur tourne en arrière-plan)
docker compose up -d
```

**Vérifier que le conteneur est prêt :**

```bash
# Attendre le message "healthy" (peut prendre 10-15 secondes)
docker compose ps

# Résultat attendu :
# NAME               STATUS
# electio_postgres   Up (healthy)
```

**Ce qui se passe automatiquement au premier démarrage :**
- PostgreSQL 16 démarre sur le port `5432`
- Le script `init_schema.sql` est exécuté automatiquement → crée les 5 tables
- Un volume Docker `postgres_data` est créé pour la persistance des données

### Étape 2 — Charger les données

```bash
# Depuis la racine du projet
python sql/migrate.py --csv data/final/dataset_ml_final.csv
```

**Sortie attendue :**

```
🚀 Migration Electio-Analytics

✅ CSV chargé : 1296 lignes
  ↳ dim_commune : 648 communes
  ↳ dim_temps : [2017, 2022]

✅ Terminé — 1296/1296 lignes importées
```

**Durée estimée : ~2-3 minutes**

### Étape 3 — Vérifier les données

```bash
# Se connecter à la base via psql dans le conteneur
docker exec -it electio_postgres psql -U electio_user -d electio_db

# Requêtes de vérification
SELECT COUNT(*) FROM dim_commune;         -- 648
SELECT COUNT(*) FROM fait_electoral;      -- 1296
SELECT COUNT(*) FROM dim_candidat;        -- ~6000
SELECT annee, COUNT(*) FROM fait_electoral
  JOIN dim_temps USING (id_temps)
  GROUP BY annee;                         -- 648 en 2017, 648 en 2022

# Quitter psql
\q
```

### Exemples de requêtes utiles

```sql
-- Top 10 communes avec le plus fort risque de bascule
-- (revenu médian élevé ET faible diplôme en baisse)
SELECT c.libelle_commune,
       s.revenu_median,
       s.taux_faible_diplome,
       f.bloc_vainqueur
FROM fait_electoral f
JOIN dim_commune c    ON f.id_commune = c.id_commune
JOIN dim_temps t      ON f.id_temps   = t.id_temps
JOIN dim_socio s      ON f.id_socio   = s.id_socio
WHERE t.annee = 2022
  AND f.bloc_vainqueur = 'DROITE'
ORDER BY s.revenu_median DESC, s.taux_faible_diplome ASC
LIMIT 10;

-- Évolution du nombre de communes par bloc 2017 → 2022
SELECT t.annee,
       f.bloc_vainqueur,
       COUNT(*) AS nb_communes,
       ROUND(COUNT(*) * 100.0 / 648, 1) AS pct
FROM fait_electoral f
JOIN dim_temps t ON f.id_temps = t.id_temps
GROUP BY t.annee, f.bloc_vainqueur
ORDER BY t.annee, nb_communes DESC;

-- Chômage moyen par bloc vainqueur en 2022
SELECT f.bloc_vainqueur,
       ROUND(AVG(s.taux_chomage)::NUMERIC, 1) AS chomage_moyen,
       ROUND(AVG(s.revenu_median)::NUMERIC, 0) AS revenu_median_moyen
FROM fait_electoral f
JOIN dim_socio s   ON f.id_socio = s.id_socio
JOIN dim_temps t   ON f.id_temps = t.id_temps
WHERE t.annee = 2022
GROUP BY f.bloc_vainqueur
ORDER BY chomage_moyen;

-- Top 5 candidats pour une commune donnée (ex: Lille)
SELECT c.libelle_commune,
       t.annee,
       cand.rang,
       cand.nom_candidat,
       cand.bloc_politique,
       cand.pct_voix
FROM dim_candidat cand
JOIN fait_electoral f ON cand.id_fait = f.id_fait
JOIN dim_commune c    ON f.id_commune = c.id_commune
JOIN dim_temps t      ON f.id_temps   = t.id_temps
WHERE c.libelle_commune ILIKE '%Lille%'
ORDER BY t.annee, cand.rang;
```

### Arrêter / Redémarrer le conteneur

```bash
# Arrêter (les données sont conservées dans le volume)
docker compose down

# Redémarrer (les données sont restaurées automatiquement)
docker compose up -d

# Supprimer complètement (données effacées)
docker compose down -v
```

### Connexion depuis Python (SQLAlchemy)

```python
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    "postgresql://electio_user:electio_pass@localhost:5432/electio_db"
)

# Exemple : charger tous les résultats 2022
df = pd.read_sql("""
    SELECT c.libelle_commune, f.bloc_vainqueur, f.pct_droite, f.pct_centre,
           s.revenu_median, s.taux_faible_diplome
    FROM fait_electoral f
    JOIN dim_commune c ON f.id_commune = c.id_commune
    JOIN dim_socio s   ON f.id_socio   = s.id_socio
    JOIN dim_temps t   ON f.id_temps   = t.id_temps
    WHERE t.annee = 2022
""", engine)

print(df.head())
```

### Paramètres de connexion

| Paramètre | Valeur |
|-----------|--------|
| Host | `localhost` |
| Port | `5432` |
| Base | `electio_db` |
| Utilisateur | `electio_user` |
| Mot de passe | `electio_pass` |

---

## 6. Notebooks {#notebooks}

```bash
# Lancer Jupyter depuis la racine du projet
jupyter notebook notebooks/
```

| Notebook | Contenu | Prérequis |
|----------|---------|-----------|
| `01_exploration.ipynb` | EDA, distributions, matrices de corrélation, cartes choroplèthes | `dataset_ml_final.csv` |
| `02_model_binaire.ipynb` | Régression Logistique, coefficients, seuil optimal | `ml_train/test_vB.csv` |
| `03_model_multiclasse.ipynb` | SVM RBF, matrice de confusion, courbes ROC | `ml_train/test_vB.csv` |
| `04_model_comparison.ipynb` | Benchmark 6 modèles, feature importance | `ml_train/test_vB.csv` |
| `05_projections_2027.ipynb` | 3 scénarios, communes à risque, visualisations | `dataset_ml_final.csv` |

---

## 7. Choix méthodologiques {#methodo}

### Deux versions du dataset

| Version | Features | Nb features | Accuracy binaire | Usage |
|---------|----------|-------------|-----------------|-------|
| **A** | historique électoral + socio-éco | 7 | 96.5% | Plafond théorique uniquement |
| **B** | socio-éco pur | 5 | **88.7%** | Prédiction prospective 2027 ✅ |

La Version A utilise les résultats électoraux 2017 pour prédire 2022 — elle ne peut pas être utilisée pour 2027 car l'historique 2022 n'existe pas encore.

### Features Version B

| Feature | Transformation | Justification |
|---------|---------------|---------------|
| `revenu_median` | StandardScaler | Discriminant DROITE/CENTRE |
| `taux_faible_diplome` | StandardScaler | Corrélation la plus forte (+0.66) |
| `pct_abstention` | StandardScaler | Signal de désengagement civic |
| `taux_delinquance` | Cap P95 + log1p + StandardScaler | Outliers extrêmes (798‰) |
| `population_municipale` | log1p + StandardScaler | Distribution très asymétrique |

### Features exclues

| Feature | Raison |
|---------|--------|
| `taux_chomage` | Multicolinéarité -0.69 avec revenu médian |
| `inscrits` | Corrélation 0.99 avec population_municipale |

### Validation et prévention du data leakage

- **StandardScaler** ajusté uniquement sur les données 2017, appliqué sans ré-ajustement sur 2022
- **StratifiedKFold k=5** sur les données 2017 uniquement
- **Données socio-éco N-1** : indicateurs 2016 → élection 2017, indicateurs 2021 → élection 2022
- **Séparation temporelle** train/test : 2017 vs 2022 (plus robuste qu'un split aléatoire)

---

## 8. Sources des données {#sources}

| Indicateur | Source | Lien | Années |
|-----------|--------|------|--------|
| Résultats électoraux | Ministère de l'Intérieur | [data.gouv.fr/elections](https://www.data.gouv.fr/fr/pages/donnees-des-elections/) | 2017, 2022 |
| Population municipale | INSEE Recensement | [insee.fr/RP](https://www.insee.fr/fr/statistiques) | 2016, 2021 |
| Taux de chômage | INSEE RP | [data.gouv.fr/emploi](https://www.data.gouv.fr/datasets/search?q=emploi) | 2016, 2022 |
| Revenu médian | INSEE Filosofi | [insee.fr/filosofi](https://www.insee.fr/fr/statistiques/6036907) | 2016, 2021 |
| Délinquance | data.gouv.fr | [data.gouv.fr/securite](https://www.data.gouv.fr/fr/pages/donnees-securite/) | 2016, 2022 |
| Niveau de diplôme | INSEE | [data.gouv.fr/insee](https://www.data.gouv.fr/fr/organizations/insee/) | 2016, 2022 |

Toutes ces sources sont en **open data** et librement téléchargeables.

---

## 9. Dépendances {#dependances}

### requirements.txt

```
pandas>=2.0.0
numpy>=1.25.0
scikit-learn>=1.5.0
matplotlib>=3.7.0
seaborn>=0.12.0
openpyxl>=3.1.0
xlrd>=2.0.1
jupyter>=1.0.0
ipykernel>=6.0.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0
geopandas>=0.13.0
folium>=0.14.0
```

### Installation complète

```bash
pip install -r requirements.txt
```

### Stack technique

| Composant | Technologie | Version |
|-----------|------------|---------|
| Langage | Python | 3.11+ |
| Machine Learning | scikit-learn | ≥ 1.5 |
| Data manipulation | pandas | ≥ 2.0 |
| Base de données | PostgreSQL | 16 |
| Conteneurisation | Docker Compose | 3.9 |
| ORM / Migration | SQLAlchemy | ≥ 2.0 |
| Visualisations | Matplotlib / Seaborn | ≥ 3.7 |
| Notebooks | Jupyter | ≥ 1.0 |

---

## Résolution des problèmes courants

### ❌ `UnpicklingError: STACK_GLOBAL requires str` (chargement .pkl)

Problème de compatibilité scikit-learn entre versions. Solution : re-créer le scaler et le modèle depuis les CSV.

```python
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
import pandas as pd, numpy as np

train = pd.read_csv('data/final/ml_train_2017_vB.csv')
df_full = pd.read_csv('data/final/dataset_ml_final.csv', dtype={'code_geo': str})

# Re-créer le scaler
df_2017 = df_full[df_full['annee'] == 2017].copy()
df_2017['log_population']  = np.log1p(df_2017['population_municipale'])
df_2017['log_delinquance'] = np.log1p(np.clip(
    df_2017['taux_delinquance'], 0,
    df_2017['taux_delinquance'].quantile(0.95)
))
scaler = StandardScaler()
scaler.fit(df_2017[['revenu_median','taux_faible_diplome','pct_abstention',
                     'log_delinquance','log_population']])

# Re-entraîner le modèle
FEATURES_B = ['revenu_median_scaled','taux_faible_diplome_scaled',
              'pct_abstention_scaled','log_delinquance_scaled','log_population_scaled']
model = LogisticRegression(C=0.1, class_weight='balanced',
                           max_iter=1000, random_state=42)
model.fit(train[FEATURES_B].values,
          (train['bloc_vainqueur'] != 'DROITE').astype(int).values)
```

### ❌ `KeyError: 'bloc_vainqueur'` dans build_dataset.py

Vérifier que `pivot_election.py` a bien été exécuté avant `build_dataset.py`. Lancer `run_cleaning.py` en entier.

### ❌ `Connection refused` pour PostgreSQL

```bash
# Vérifier que Docker tourne
docker compose ps

# Si le conteneur n'est pas "healthy", attendre 15 secondes et relancer
docker compose up -d

# Vérifier les logs en cas d'erreur
docker compose logs postgres
```

### ❌ `ON CONFLICT DO NOTHING` ne fonctionne pas dans migrate.py

Le schéma a peut-être été corrompu. Réinitialiser la base :

```bash
# Supprimer le volume et relancer (efface toutes les données)
docker compose down -v
docker compose up -d

# Attendre que le conteneur soit "healthy", puis relancer la migration
python sql/migrate.py --csv data/final/dataset_ml_final.csv
```

---

## Licence

Données sources sous licence open data (Licence Ouverte / Open Licence Etalab).
Code source sous licence MIT.

---

*MSPR TPRE813 — Big Data & Analyse de Données — EPSI 2024-2025*