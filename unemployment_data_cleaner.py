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
df["GEO"] = df["GEO"].astype(str).str.strip().str.zfill(5)

df_nord = df[
    (df["GEO"].str.startswith("59")) &
    (df["GEO_OBJECT"] == "COM") &
    (df["PCS"] == "_T") &
    (df["AGE"] == "Y15T64") &
    (df["TIME_PERIOD"].isin([2016, 2022]))
].copy()

print(f"✅ Après filtrage Nord + années 2016/2022 : {df_nord.shape[0]} lignes")

# ---- SÉPARATION CHÔMEURS / ACTIFS ----
df_chomeurs = df_nord[df_nord["EMPSTA_ENQ"] == "2"][
    ["GEO", "TIME_PERIOD", "OBS_VALUE"]
].rename(columns={"OBS_VALUE": "nb_chomeurs"})

df_actifs = df_nord[df_nord["EMPSTA_ENQ"] == "1T2"][
    ["GEO", "TIME_PERIOD", "OBS_VALUE"]
].rename(columns={"OBS_VALUE": "nb_actifs"})

print(f"📊 Chômeurs     : {len(df_chomeurs)} lignes")
print(f"📊 Actifs total : {len(df_actifs)} lignes")

# ---- JOINTURE ET CALCUL DU TAUX ----
df_taux = pd.merge(
    df_chomeurs,
    df_actifs,
    on=["GEO", "TIME_PERIOD"],
    how="inner"
)

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