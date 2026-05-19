import pandas as pd
import os

# ============================================================
# build_dataset.py
# Jointure de toutes les sources curated → dataset_ml_final.csv
# Clé de jointure : code_geo (ex: "59001")
# Logique temporelle :
#   Élection 2017 ← socio-éco 2016
#   Élection 2022 ← socio-éco 2021 ou 2022 selon la source
# ============================================================

FINAL_PATH = "data/final"
os.makedirs(FINAL_PATH, exist_ok=True)

# ============================================================
# ÉTAPE 1 — CHARGEMENT DES FICHIERS ÉLECTORAUX (base)
# ============================================================
print("=" * 60)
print("📂 ÉTAPE 1 — Chargement des pivots électoraux")
print("=" * 60)

df_2017 = pd.read_csv(
    "data/final/elections_2017_pivot.csv",
    dtype={"code_geo": str, "code_commune": str}
)
df_2022 = pd.read_csv(
    "data/final/elections_2022_pivot.csv",
    dtype={"code_geo": str, "code_commune": str}
)

print(f"✅ 2017 : {len(df_2017)} communes, {df_2017.shape[1]} colonnes")
print(f"✅ 2022 : {len(df_2022)} communes, {df_2022.shape[1]} colonnes")

# Empiler les deux années
df_elections = pd.concat([df_2017, df_2022], ignore_index=True)
print(f"✅ Base électorale combinée : {len(df_elections)} lignes")

# Créer la colonne annee_socioeco pour la jointure temporelle
# 2017 → données socio-éco de 2016
# 2022 → données socio-éco de 2021/2022 (géré source par source)
df_elections["annee_socioeco_base"] = df_elections["annee"].map({2017: 2016, 2022: 2022})

# ============================================================
# ÉTAPE 2 — CHÔMAGE (2016 → élection 2017 / 2022 → élection 2022)
# ============================================================
print("\n" + "=" * 60)
print("📂 ÉTAPE 2 — Chômage")
print("=" * 60)

df_chomage = pd.read_csv(
    "data/curated/emploi/chomage_2016_2022.csv",
    dtype={"code_geo": str}
)

# Renommer l'année pour correspondre à la logique temporelle
df_chomage = df_chomage.rename(columns={"annee": "annee_socioeco"})
df_chomage = df_chomage[["code_geo", "annee_socioeco", "taux_chomage"]]

print(f"✅ Chômage chargé : {len(df_chomage)} lignes")
print(f"   Années : {sorted(df_chomage['annee_socioeco'].unique())}")
print(f"   NaN    : {df_chomage['taux_chomage'].isna().sum()}")

# ============================================================
# ÉTAPE 3 — REVENUS (2016 → élection 2017 / 2021 → élection 2022)
# ============================================================
print("\n" + "=" * 60)
print("📂 ÉTAPE 3 — Revenus médians")
print("=" * 60)

df_revenus = pd.read_csv(
    "data/curated/economie/revenu_median_2016_2021.csv",
    dtype={"code_geo": str}
)

df_revenus = df_revenus.rename(columns={"annee": "annee_socioeco"})

# ⚠️  La source revenus utilise 2021, mais l'élection cible est 2022
# → On remplace 2021 par 2022 pour que la jointure fonctionne
df_revenus["annee_socioeco"] = df_revenus["annee_socioeco"].replace({2021: 2022})
df_revenus = df_revenus[["code_geo", "annee_socioeco", "revenu_median"]]

print(f"✅ Revenus chargés : {len(df_revenus)} lignes")
print(f"   Années (après mapping) : {sorted(df_revenus['annee_socioeco'].unique())}")
print(f"   NaN    : {df_revenus['revenu_median'].isna().sum()}")

# ============================================================
# ÉTAPE 4 — DIPLÔMES (2016 → élection 2017 / 2022 → élection 2022)
# ============================================================
print("\n" + "=" * 60)
print("📂 ÉTAPE 4 — Diplômes")
print("=" * 60)

df_diplomes = pd.read_csv(
    "data/curated/social/diplomes_2016_2022.csv",
    dtype={"code_geo": str}
)

df_diplomes = df_diplomes.rename(columns={"annee": "annee_socioeco"})
df_diplomes = df_diplomes[["code_geo", "annee_socioeco", "taux_faible_diplome"]]

print(f"✅ Diplômes chargés : {len(df_diplomes)} lignes")
print(f"   Années : {sorted(df_diplomes['annee_socioeco'].unique())}")
print(f"   NaN    : {df_diplomes['taux_faible_diplome'].isna().sum()}")

# ============================================================
# ÉTAPE 5 — DÉLINQUANCE (2016 → élection 2017 / 2022 → élection 2022)
# ============================================================
print("\n" + "=" * 60)
print("📂 ÉTAPE 5 — Délinquance")
print("=" * 60)

df_delinquance = pd.read_csv(
    "data/curated/securite/delinquance_2016_2022.csv",
    dtype={"code_geo": str}
)

df_delinquance = df_delinquance.rename(columns={"annee": "annee_socioeco"})
df_delinquance = df_delinquance[["code_geo", "annee_socioeco", "taux_delinquance", "nb_faits_total"]]

print(f"✅ Délinquance chargée : {len(df_delinquance)} lignes")
print(f"   Années : {sorted(df_delinquance['annee_socioeco'].unique())}")
print(f"   NaN    : {df_delinquance['taux_delinquance'].isna().sum()}")

# ============================================================
# ÉTAPE 6 — DÉMOGRAPHIE
# 2016 → élection 2017 / 2021 → élection 2022
# ============================================================
print("\n" + "=" * 60)
print("📂 ÉTAPE 6 — Démographie")
print("=" * 60)

df_demo_2016 = pd.read_csv(
    "data/curated/demographie/population_2016.csv",
    dtype={"code_commune": str}
)
# code_commune vaut déjà "59001" (5 chiffres dept+commune) → c'est notre code_geo
df_demo_2016["code_geo"] = df_demo_2016["code_commune"].str.strip().str.zfill(5)
df_demo_2016["annee_socioeco"] = 2016
df_demo_2016 = df_demo_2016[["code_geo", "annee_socioeco", "population_municipale", "population_totale"]]

df_demo_2021 = pd.read_csv(
    "data/curated/demographie/demographie_2021.csv",
    dtype={"code_geo": str}
)
# Vérifier si code_geo existe, sinon le reconstruire depuis code_commune
if "code_geo" not in df_demo_2021.columns:
    df_demo_2021["code_geo"] = df_demo_2021["code_commune"].str.strip().str.zfill(5)
df_demo_2021["annee_socioeco"] = 2022  # mapping 2021 → élection 2022
df_demo_2021 = df_demo_2021[["code_geo", "annee_socioeco", "population_municipale", "population_totale"]]

df_demo = pd.concat([df_demo_2016, df_demo_2021], ignore_index=True)

print(f"✅ Démographie chargée : {len(df_demo)} lignes")
print(f"   Années (après mapping) : {sorted(df_demo['annee_socioeco'].unique())}")
print(f"   NaN    : {df_demo['population_municipale'].isna().sum()}")

# ============================================================
# ÉTAPE 7 — JOINTURES SUCCESSIVES
# Base : df_elections (code_geo + annee_socioeco_base)
# On joint chaque source sur (code_geo, annee_socioeco)
# ============================================================
print("\n" + "=" * 60)
print("🔗 ÉTAPE 7 — Jointures")
print("=" * 60)

# Renommer la colonne de jointure temporelle dans la base
df_base = df_elections.rename(columns={"annee_socioeco_base": "annee_socioeco"})

def join_source(df_base, df_source, nom_source):
    n_avant = len(df_base)
    df_result = df_base.merge(df_source, on=["code_geo", "annee_socioeco"], how="left")
    n_apres = len(df_result)
    cols_ajoutees = [c for c in df_source.columns if c not in ["code_geo", "annee_socioeco"]]
    nan_par_col = {col: df_result[col].isna().sum() for col in cols_ajoutees}
    print(f"   ✅ {nom_source:<20} : {n_avant} → {n_apres} lignes | NaN : {nan_par_col}")
    return df_result

df_final = df_base.copy()
df_final = join_source(df_final, df_chomage,    "Chômage")
df_final = join_source(df_final, df_revenus,    "Revenus")
df_final = join_source(df_final, df_diplomes,   "Diplômes")
df_final = join_source(df_final, df_delinquance,"Délinquance")
df_final = join_source(df_final, df_demo,       "Démographie")

# ============================================================
# ÉTAPE 8 — GESTION DES NaN
# Stratégie : imputation par médiane départementale par année
# ============================================================
print("\n" + "=" * 60)
print("🩹 ÉTAPE 8 — Imputation des NaN")
print("=" * 60)

features_numeriques = [
    "taux_chomage", "revenu_median", "taux_faible_diplome",
    "taux_delinquance", "nb_faits_total",
    "population_municipale", "population_totale"
]

nan_avant = df_final[features_numeriques].isna().sum().sum()
print(f"   NaN avant imputation : {nan_avant}")

for col in features_numeriques:
    nb_nan = df_final[col].isna().sum()
    if nb_nan > 0:
        # Imputation par médiane de l'année (département entier = Nord 59)
        df_final[col] = df_final.groupby("annee")[col].transform(
            lambda x: x.fillna(x.median())
        )
        print(f"   {col:<30} : {nb_nan} NaN imputés par médiane annuelle")

# Blocs candidats absents → "ABSENT" (NaN structurel)
cols_blocs_cand = [c for c in df_final.columns if c.startswith("bloc_cand")]
for col in cols_blocs_cand:
    df_final[col] = df_final[col].fillna("ABSENT")

nan_apres = df_final[features_numeriques].isna().sum().sum()
print(f"\n   NaN après imputation  : {nan_apres}")

# ============================================================
# ÉTAPE 9 — VARIABLE CIBLE ML
# cible_binaire : 1 si la commune échappe à la DROITE, 0 sinon
# Utile même avec déséquilibre car on s'intéresse aux exceptions
# ============================================================
print("\n" + "=" * 60)
print("🎯 ÉTAPE 9 — Variable cible")
print("=" * 60)

df_final["cible_binaire"] = (df_final["bloc_vainqueur"] != "DROITE").astype(int)

for annee in [2017, 2022]:
    sub = df_final[df_final["annee"] == annee]
    n_exception = sub["cible_binaire"].sum()
    print(f"\n   {annee} :")
    print(f"   DROITE     : {len(sub) - n_exception} communes ({(1 - n_exception/len(sub))*100:.1f}%)")
    print(f"   EXCEPTION  : {n_exception} communes ({n_exception/len(sub)*100:.1f}%)")

# ============================================================
# ÉTAPE 10 — NETTOYAGE FINAL ET ORDRE DES COLONNES
# ============================================================
print("\n" + "=" * 60)
print("🗂️  ÉTAPE 10 — Ordre des colonnes")
print("=" * 60)

# Supprimer la colonne de jointure temporelle (inutile pour le ML)
df_final = df_final.drop(columns=["annee_socioeco"], errors="ignore")

# Définir l'ordre logique des colonnes
cols_ids = ["code_geo", "code_commune", "libelle_commune", "annee"]

cols_participation = ["inscrits", "abstentions", "votants", "exprimes", "pct_abstention"]

cols_top5 = (
    [c for c in df_final.columns if c.startswith("nom_cand")] +
    [c for c in df_final.columns if c.startswith("pct_cand")] +
    [c for c in df_final.columns if c.startswith("bloc_cand")]
)

cols_blocs = sorted([c for c in df_final.columns if c.startswith("pct_") and
                     any(b in c for b in ["GAUCHE", "CENTRE", "DROITE", "DIVERS"])])

cols_socioeco = [
    "taux_chomage", "revenu_median", "taux_faible_diplome",
    "taux_delinquance", "nb_faits_total",
    "population_municipale", "population_totale"
]

cols_cible = ["bloc_vainqueur", "cible_binaire"]

# Ne garder que les colonnes existantes
def filter_existing(cols):
    return [c for c in cols if c in df_final.columns]

col_order = filter_existing(
    cols_ids + cols_participation + cols_top5 +
    cols_blocs + cols_socioeco + cols_cible
)

# Colonnes restantes non classées (sécurité)
cols_restantes = [c for c in df_final.columns if c not in col_order]
if cols_restantes:
    print(f"   ⚠️  Colonnes non classées ajoutées en fin : {cols_restantes}")

df_final = df_final[col_order + cols_restantes]

# ============================================================
# ÉTAPE 11 — VÉRIFICATION FINALE
# ============================================================
print("\n" + "=" * 60)
print("📊 ÉTAPE 11 — Vérification finale")
print("=" * 60)

print(f"\n   Lignes totales        : {len(df_final)}")
print(f"   Colonnes totales      : {df_final.shape[1]}")
print(f"   Communes uniques      : {df_final['code_geo'].nunique()}")
print(f"   Années                : {sorted(df_final['annee'].unique())}")
print(f"   NaN résiduels totaux  : {df_final.isnull().sum().sum()}")

print(f"\n   Couverture par année :")
for annee in [2017, 2022]:
    sub = df_final[df_final["annee"] == annee]
    completude = (sub[cols_socioeco].notna().all(axis=1).sum())
    print(f"   {annee} : {completude}/{len(sub)} communes avec toutes les features socio-éco")

print(f"\n   NaN par feature socio-éco :")
for col in cols_socioeco:
    if col in df_final.columns:
        print(f"   {col:<35} : {df_final[col].isna().sum()} NaN")

print(f"\n🔍 Aperçu (5 premières lignes, colonnes clés) :")
cols_apercu = ["code_geo", "libelle_commune", "annee",
               "bloc_vainqueur", "cible_binaire",
               "taux_chomage", "revenu_median", "population_municipale"]
cols_apercu = filter_existing(cols_apercu)
print(df_final[cols_apercu].head(5).to_string(index=False))

# ============================================================
# SAUVEGARDE
# ============================================================
fichier_final = os.path.join(FINAL_PATH, "dataset_ml_final.csv")
df_final.to_csv(fichier_final, index=False, encoding="utf-8")
print(f"\n✅ Dataset ML final sauvegardé → {fichier_final}")
print(f"   {len(df_final)} lignes × {df_final.shape[1]} colonnes")

# Sauvegarder aussi par année pour faciliter le train/test split
for annee in [2017, 2022]:
    sub = df_final[df_final["annee"] == annee]
    fichier = os.path.join(FINAL_PATH, f"dataset_ml_{annee}.csv")
    sub.to_csv(fichier, index=False, encoding="utf-8")
    print(f"✅ Dataset {annee} → {fichier} ({len(sub)} communes)")