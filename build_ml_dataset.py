"""
build_ml_dataset.py
-------------------
Jointure finale + nettoyage pour le Machine Learning
Projet : ElectioAnalytics – Prédiction tendances électorales Nord (59)

Logique temporelle :
  données socio-éco 2016  →  élection 2017  (train)
  données socio-éco 2021  →  élection 2022  (test)

Fichiers attendus en entrée :
  data/final/elections_2017_pivot.csv          ← pivot_election.py
  data/final/elections_2022_pivot.csv          ← pivot_election.py
  data/curated/emploi/chomage_2016_2022.csv    ← unemployment_data_cleaner.py
  data/curated/demographie/population_2016.csv ← demoghaphie_data_cleaner_2011_2016.py
  data/curated/demographie/demographie_2021.csv← demographic_data_cleaner_2021.py
  data/curated/securite/delinquance_2016_2022.csv ← delinquency_data_cleaner.py

Fichiers générés en sortie :
  data/final/ml_dataset_2017.csv   ← set d'entraînement
  data/final/ml_dataset_2022.csv   ← set de test
  data/final/ml_dataset_complet.csv← les deux concaténés
"""

import pandas as pd
import numpy as np
import os

# ============================================================
# 0. CHEMINS
# ============================================================
FINAL_PATH    = "data/final"
CURATED_BASE  = "data/curated"

os.makedirs(FINAL_PATH, exist_ok=True)

# ============================================================
# 1. CHARGEMENT DES FICHIERS ÉLECTORAUX (déjà pivotés)
# ============================================================
print("=" * 60)
print("1. CHARGEMENT DES DONNÉES ÉLECTORALES")
print("=" * 60)

df_elec_2017 = pd.read_csv(
    os.path.join(FINAL_PATH, "elections_2017_pivot.csv"),
    dtype={"code_commune": str, "code_geo": str},
    encoding="utf-8"
)
df_elec_2022 = pd.read_csv(
    os.path.join(FINAL_PATH, "elections_2022_pivot.csv"),
    dtype={"code_commune": str, "code_geo": str},
    encoding="utf-8"
)

print(f"✅ Élections 2017 : {df_elec_2017.shape[0]} communes, {df_elec_2017.shape[1]} colonnes")
print(f"✅ Élections 2022 : {df_elec_2022.shape[0]} communes, {df_elec_2022.shape[1]} colonnes")

# Normaliser la clé de jointure (code_geo sur 5 chiffres)
for df in [df_elec_2017, df_elec_2022]:
    df["code_geo"] = df["code_geo"].astype(str).str.strip().str.zfill(5)

# ============================================================
# 2. CHARGEMENT DES INDICATEURS SOCIO-ÉCONOMIQUES
# ============================================================
print("\n" + "=" * 60)
print("2. CHARGEMENT DES INDICATEURS SOCIO-ÉCONOMIQUES")
print("=" * 60)

# ---- 2a. Taux de chômage ----
df_chomage = pd.read_csv(
    os.path.join(CURATED_BASE, "emploi", "chomage_2016_2022.csv"),
    dtype={"code_geo": str},
    encoding="utf-8"
)
df_chomage["code_geo"] = df_chomage["code_geo"].astype(str).str.strip().str.zfill(5)
print(f"✅ Chômage       : {df_chomage.shape[0]} lignes | années : {sorted(df_chomage['annee'].unique())}")

# ---- 2b. Population 2016 ----
df_pop_2016 = pd.read_csv(
    os.path.join(CURATED_BASE, "demographie", "population_2016.csv"),
    dtype={"code_commune": str},
    encoding="utf-8"
)
df_pop_2016["code_geo"] = df_pop_2016["code_commune"].astype(str).str.strip().str.zfill(5)
df_pop_2016["annee_socio"] = 2016
print(f"✅ Population 2016: {df_pop_2016.shape[0]} communes")

# ---- 2c. Population 2021 ----
df_pop_2021 = pd.read_csv(
    os.path.join(CURATED_BASE, "demographie", "demographie_2021.csv"),
    dtype={"code_geo": str},
    encoding="utf-8"
)
df_pop_2021["code_geo"] = df_pop_2021["code_geo"].astype(str).str.strip().str.zfill(5)
df_pop_2021["annee_socio"] = 2021
print(f"✅ Population 2021: {df_pop_2021.shape[0]} communes")

# ---- 2d. Délinquance ----
df_delinquance = pd.read_csv(
    os.path.join(CURATED_BASE, "securite", "delinquance_2016_2022.csv"),
    dtype={"code_geo": str},
    encoding="utf-8"
)
df_delinquance["code_geo"] = df_delinquance["code_geo"].astype(str).str.strip().str.zfill(5)
print(f"✅ Délinquance    : {df_delinquance.shape[0]} lignes | années : {sorted(df_delinquance['annee'].unique())}")

# ============================================================
# 3. PRÉPARATION DES TABLES SOCIO-ÉCO PAR ANNÉE
# ============================================================
print("\n" + "=" * 60)
print("3. PRÉPARATION DES TABLES SOCIO-ÉCO PAR ANNÉE")
print("=" * 60)

def prepare_socio(annee_socio, df_pop):
    """
    Assemble les indicateurs socio-éco pour une année donnée.
    Retourne un DataFrame avec une ligne par commune (code_geo).
    """
    print(f"\n  → Assemblage socio {annee_socio}...")

    # Chômage
    chomage = df_chomage[df_chomage["annee"] == annee_socio][
        ["code_geo", "taux_chomage", "nb_chomeurs", "nb_actifs"]
    ].copy()
    print(f"     Chômage    : {len(chomage)} communes")

    # Population
    if annee_socio == 2016:
        pop = df_pop[["code_geo", "population_municipale", "population_totale"]].copy()
    else:  # 2021
        pop = df_pop[["code_geo", "population_municipale", "population_totale"]].copy()
    pop = pop.rename(columns={
        "population_municipale": "population_mun",
        "population_totale":     "population_tot"
    })
    print(f"     Population : {len(pop)} communes")

    # Délinquance
    delinq = df_delinquance[df_delinquance["annee"] == annee_socio][
        ["code_geo", "taux_delinquance", "nb_faits_total", "nb_indicateurs"]
    ].copy()
    print(f"     Délinquance: {len(delinq)} communes")

    # Fusion
    socio = (
        chomage
        .merge(pop,    on="code_geo", how="outer")
        .merge(delinq, on="code_geo", how="outer")
    )
    socio["annee_socio"] = annee_socio
    print(f"     Résultat   : {len(socio)} communes après fusion outer")
    return socio


socio_2016 = prepare_socio(2016, df_pop_2016)
socio_2021 = prepare_socio(2021, df_pop_2021)

# ============================================================
# 4. JOINTURE ÉLECTIONS + SOCIO-ÉCO
# ============================================================
print("\n" + "=" * 60)
print("4. JOINTURE ÉLECTIONS + SOCIO-ÉCO")
print("=" * 60)

def joindre(df_elec, socio, annee_election):
    """
    Joint les données électorales avec les indicateurs socio-éco.
    Utilise left join pour conserver toutes les communes électorales.
    """
    df = df_elec.merge(
        socio.drop(columns=["annee_socio"], errors="ignore"),
        on="code_geo",
        how="left"
    )
    print(f"  Élections {annee_election} : {len(df_elec)} communes")
    print(f"  Après jointure socio     : {len(df)} lignes")
    # Vérification des pertes
    manquants = df["taux_chomage"].isna().sum()
    print(f"  Communes sans chômage    : {manquants} ({manquants/len(df)*100:.1f}%)")
    manquants_pop = df["population_mun"].isna().sum()
    print(f"  Communes sans population : {manquants_pop} ({manquants_pop/len(df)*100:.1f}%)")
    return df


print("\n→ Dataset 2017 (élections) × socio 2016")
df_2017 = joindre(df_elec_2017, socio_2016, 2017)

print("\n→ Dataset 2022 (élections) × socio 2021")
df_2022 = joindre(df_elec_2022, socio_2021, 2022)

# ============================================================
# 5. NETTOYAGE FINAL
# ============================================================
print("\n" + "=" * 60)
print("5. NETTOYAGE FINAL")
print("=" * 60)

def nettoyer_dataset(df, annee):
    """
    Nettoyage final avant ML :
    - Suppression colonnes inutiles
    - Gestion des valeurs manquantes
    - Vérification de la variable cible
    - Encodage de la variable cible
    """
    print(f"\n  → Nettoyage dataset {annee}...")
    print(f"     Shape initial : {df.shape}")

    # ---- 5a. Supprimer les colonnes inutiles pour le ML ----
    cols_a_supprimer = [
        "libelle_departement", "libelle_circonscription",
        "code_departement", "code_circonscription",
        "tour",
    ]
    cols_existantes = [c for c in cols_a_supprimer if c in df.columns]
    df = df.drop(columns=cols_existantes, errors="ignore")

    # ---- 5b. Vérifier la variable cible ----
    print(f"\n     Distribution bloc_vainqueur :")
    print(df["bloc_vainqueur"].value_counts().to_string())

    # Supprimer les lignes sans variable cible
    nb_avant = len(df)
    df = df.dropna(subset=["bloc_vainqueur"])
    if len(df) < nb_avant:
        print(f"     ⚠️  Suppression de {nb_avant - len(df)} lignes sans bloc_vainqueur")

    # ---- 5c. Imputation des valeurs manquantes ----
    # Indicateurs numériques socio-éco : médiane du département (robuste aux outliers)
    cols_socio = ["taux_chomage", "population_mun", "population_tot", "taux_delinquance",
                  "nb_chomeurs", "nb_actifs", "nb_faits_total"]

    for col in cols_socio:
        if col in df.columns:
            nb_nan = df[col].isna().sum()
            if nb_nan > 0:
                mediane = df[col].median()
                df[col] = df[col].fillna(mediane)
                print(f"     Imputation {col:25s} : {nb_nan} NaN → médiane ({mediane:.2f})")

    # Colonnes de pourcentage candidats : 0 si absent (candidat non présent dans la commune)
    cols_pct_cand = [c for c in df.columns if c.startswith("pct_cand")]
    for col in cols_pct_cand:
        if df[col].isna().any():
            df[col] = df[col].fillna(0)

    # Colonnes de nom candidat : "ABSENT" si manquant
    cols_nom_cand = [c for c in df.columns if c.startswith("nom_cand")]
    for col in cols_nom_cand:
        if df[col].isna().any():
            df[col] = df[col].fillna("ABSENT")

    # Colonnes bloc candidat
    cols_bloc_cand = [c for c in df.columns if c.startswith("bloc_cand")]
    for col in cols_bloc_cand:
        if df[col].isna().any():
            df[col] = df[col].fillna("ABSENT")

    # ---- 5d. Encodage numérique de la variable cible ----
    # Utile pour certains algorithmes ML (Random Forest l'accepte en string, LR non)
    bloc_encoder = {"GAUCHE": 0, "CENTRE": 1, "DROITE": 2, "DIVERS": 3}
    df["bloc_vainqueur_encoded"] = df["bloc_vainqueur"].map(bloc_encoder)

    # ---- 5e. Rapport final ----
    print(f"\n     Shape final   : {df.shape}")
    print(f"     NaN restants  : {df.isnull().sum().sum()}")
    print(f"     Communes      : {df['code_commune'].nunique()}")

    return df


df_2017_clean = nettoyer_dataset(df_2017, 2017)
df_2022_clean = nettoyer_dataset(df_2022, 2022)

# ============================================================
# 6. DATASET COMPLET (TRAIN + TEST CONCATÉNÉS)
# ============================================================
print("\n" + "=" * 60)
print("6. CRÉATION DU DATASET COMPLET")
print("=" * 60)

df_complet = pd.concat([df_2017_clean, df_2022_clean], ignore_index=True)
print(f"✅ Dataset complet : {df_complet.shape[0]} lignes × {df_complet.shape[1]} colonnes")

# ============================================================
# 7. DIAGNOSTIC FINAL
# ============================================================
print("\n" + "=" * 60)
print("7. DIAGNOSTIC FINAL")
print("=" * 60)

for annee, df in [("2017 (TRAIN)", df_2017_clean), ("2022 (TEST)", df_2022_clean)]:
    print(f"\n  📊 {annee} :")
    print(f"     Communes          : {len(df)}")
    print(f"     Colonnes features : {df.shape[1] - 2}")  # -2 pour les 2 targets
    print(f"     NaN totaux        : {df.isnull().sum().sum()}")
    print(f"     Cible (blocs)     : {df['bloc_vainqueur'].value_counts().to_dict()}")
    if "taux_chomage" in df.columns:
        print(f"     Chômage médian    : {df['taux_chomage'].median():.1f}%")
    if "population_mun" in df.columns:
        print(f"     Population moy.   : {df['population_mun'].mean():,.0f}")

print(f"\n  📋 Colonnes du dataset final :")
for i, col in enumerate(df_complet.columns, 1):
    print(f"     {i:3d}. {col}")

# ============================================================
# 8. SAUVEGARDE
# ============================================================
print("\n" + "=" * 60)
print("8. SAUVEGARDE")
print("=" * 60)

out_2017    = os.path.join(FINAL_PATH, "ml_dataset_2017.csv")
out_2022    = os.path.join(FINAL_PATH, "ml_dataset_2022.csv")
out_complet = os.path.join(FINAL_PATH, "ml_dataset_complet.csv")

df_2017_clean.to_csv(out_2017,    index=False, encoding="utf-8")
df_2022_clean.to_csv(out_2022,    index=False, encoding="utf-8")
df_complet.to_csv(out_complet,    index=False, encoding="utf-8")

print(f"\n✅ ml_dataset_2017.csv    → {out_2017}    ({len(df_2017_clean)} lignes)")
print(f"✅ ml_dataset_2022.csv    → {out_2022}    ({len(df_2022_clean)} lignes)")
print(f"✅ ml_dataset_complet.csv → {out_complet} ({len(df_complet)} lignes)")
print(f"\n🎯 Dataset prêt pour le Machine Learning !")
print(f"   Train : ml_dataset_2017.csv  →  X_train / y_train")
print(f"   Test  : ml_dataset_2022.csv  →  X_test  / y_test")
print(f"   Cible : colonne 'bloc_vainqueur' (str) ou 'bloc_vainqueur_encoded' (int 0-3)")
