import pandas as pd
import os

# ---- CHEMINS ----
RAW_PATH     = "data/raw/emploi"
CURATED_PATH = "data/curated/emploi"

os.makedirs(CURATED_PATH, exist_ok=True)

# ---- LECTURE ----
print("📂 Lecture des fichiers...")
df = pd.read_csv(
    os.path.join(RAW_PATH, "donnees_2011_2016_2022.csv"),
    sep=";",
    dtype={"GEO": str}
)

print(f"✅ Fichier chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")
print(f"📋 Colonnes : {list(df.columns)}")

# ---- FILTRAGE ----
# Nord (59), niveau commune, toutes catégories, pop. active 15-64 ans
# Années retenues : 2016 (→ élection 2017) et 2022 (→ élection 2022)
# Note: Pour 2022, les chômeurs sont calculés = actifs - employés
df["GEO"] = df["GEO"].astype(str).str.strip().str.zfill(5)
df["TIME_PERIOD"] = df["TIME_PERIOD"].astype(int)

df_nord = df[
    (df["GEO"].str.startswith("59")) &
    (df["GEO_OBJECT"] == "COM") &
    (df["PCS"] == "_T") &
    (df["AGE"] == "Y15T64") &
    (df["TIME_PERIOD"].isin([2016, 2022]))
].copy()

print(f"✅ Après filtrage Nord + années 2016/2022 : {df_nord.shape[0]} lignes")

# ---- SÉPARATION CHÔMEURS / ACTIFS / EMPLOYÉS ----
# 2016 : les chômeurs sont directement disponibles (EMPSTA_ENQ = "2")
# 2022 : pas de donnée chômeurs directes, on les calcule = actifs - employés

df_actifs = df_nord[df_nord["EMPSTA_ENQ"] == "1T2"][
    ["GEO", "TIME_PERIOD", "OBS_VALUE"]
].rename(columns={"OBS_VALUE": "nb_actifs"})

# ---- TRAITEMENT 2016 ----
df_chomeurs_2016 = df_nord[(df_nord["TIME_PERIOD"] == 2016) & (df_nord["EMPSTA_ENQ"] == "2")][
    ["GEO", "TIME_PERIOD", "OBS_VALUE"]
].rename(columns={"OBS_VALUE": "nb_chomeurs"})

df_actifs_2016 = df_actifs[df_actifs["TIME_PERIOD"] == 2016]

df_taux_2016 = pd.merge(
    df_chomeurs_2016,
    df_actifs_2016,
    on=["GEO", "TIME_PERIOD"],
    how="inner"
)

# ---- TRAITEMENT 2022 (calcul chômeurs = actifs - employés) ----
df_employes_2022 = df_nord[(df_nord["TIME_PERIOD"] == 2022) & (df_nord["EMPSTA_ENQ"] == "1")][
    ["GEO", "TIME_PERIOD", "OBS_VALUE"]
].rename(columns={"OBS_VALUE": "nb_employes"})

df_actifs_2022 = df_actifs[df_actifs["TIME_PERIOD"] == 2022]

df_merge_2022 = pd.merge(
    df_actifs_2022,
    df_employes_2022,
    on=["GEO", "TIME_PERIOD"],
    how="inner"
)

# Calculer les chômeurs comme différence
df_merge_2022["nb_chomeurs"] = (df_merge_2022["nb_actifs"] - df_merge_2022["nb_employes"]).abs()
df_taux_2022 = df_merge_2022[["GEO", "TIME_PERIOD", "nb_chomeurs", "nb_actifs"]]

# ---- FUSION 2016 + 2022 ----
df_taux = pd.concat([df_taux_2016, df_taux_2022], ignore_index=True)

print(f"📊 Données 2016  : {len(df_taux_2016)} communes")
print(f"📊 Données 2022  : {len(df_taux_2022)} communes")

df_taux["taux_chomage"] = (
    df_taux["nb_chomeurs"] / df_taux["nb_actifs"] * 100
).round(2)

# ---- RENOMMAGE ----
df_taux = df_taux.rename(columns={
    "GEO":         "code_geo",
    "TIME_PERIOD": "annee"
})

# ---- VÉRIFICATION ----
print(f"\n📊 Résumé :")
print(f"   Lignes totales   : {len(df_taux)}")
print(f"   Communes uniques : {df_taux['code_geo'].nunique()}")
print(f"   Années           : {sorted(df_taux['annee'].unique())}")
print(f"   Taux min/max     : {df_taux['taux_chomage'].min()}% / {df_taux['taux_chomage'].max()}%")
print(f"   Note: 2022 calculé comme (actifs - employés)")
print(f"\n🔍 Aperçu :")
print(df_taux.head(10).to_string(index=False))

# ---- SAUVEGARDE PAR ANNÉE ----
for annee in [2016, 2022]:
    df_annee = df_taux[df_taux["annee"] == annee]
    fichier  = os.path.join(CURATED_PATH, f"chomage_{annee}.csv")
    df_annee.to_csv(fichier, index=False, encoding="utf-8")
    print(f"✅ Sauvegardé → {fichier} ({len(df_annee)} communes)")

# ---- SAUVEGARDE FICHIER GLOBAL ----
fichier_global = os.path.join(CURATED_PATH, "chomage_2016_2022.csv")
df_taux.to_csv(fichier_global, index=False, encoding="utf-8")
print(f"✅ Fichier global → {fichier_global}")

