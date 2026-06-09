import pandas as pd
import os

from pathlib import Path as _Path
_BASE_DIR = _Path(__file__).resolve().parents[2]


# ---- CHEMINS ----
RAW_PATH     = str(_BASE_DIR / "data/raw/social")
CURATED_PATH = str(_BASE_DIR / "data/curated/social")

os.makedirs(CURATED_PATH, exist_ok=True)

fichier_data = os.path.join(RAW_PATH, "DIPLOMES_2022_data.csv")
print(f"📂 Lecture de {fichier_data}...")

# ---- STRATÉGIE PAR ANNÉE ----
# Le fichier ne fournit pas les mêmes codes selon l'année :
#
#   2016 : codes disponibles → _T, 350T351_RP (bac), 300_RP (CAP/BEP)
#   2022 : codes disponibles → tout + 001T100_RP, 001T200_RP, etc.
#
# Indicateur retenu : "taux_faible_diplome"
#   = Part de la population sans CAP/BEP ni bac ni supérieur
#   = (_T - bac - CAP_BEP) / _T  ← disponible pour 2016 ET 2022
#   ≈ "Aucun diplôme ou BEPC" (001T200_RP) — comparable à 2022 par construction
#
# Vérification croisée sur 2022 :
#   001T200_RP / _T  doit être proche de (_T - 350T351_RP - 300_RP) / _T
#   Si l'écart est < 2 points → les deux méthodes sont cohérentes

CODES_NECESSAIRES = ["_T", "350T351_RP", "300_RP", "001T200_RP"]

# ---- LECTURE FILTRÉE ----
chunks = []
for chunk in pd.read_csv(
    fichier_data,
    sep=";",
    encoding="utf-8",
    dtype={"GEO": str, "OBS_STATUS": str, "EDUC": str},
    chunksize=200_000
):
    mask = (
        (chunk["GEO_OBJECT"] == "COM") &
        (chunk["GEO"].str.startswith("59", na=False)) &
        (chunk["SEX"] == "_T") &
        (chunk["TIME_PERIOD"].isin([2016, 2022])) &
        (chunk["EDUC"].isin(CODES_NECESSAIRES))
    )
    chunks.append(chunk[mask])

df = pd.concat(chunks, ignore_index=True)
df = df[df["OBS_STATUS"] != "O"].copy()
df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
df["code_geo"]  = df["GEO"].str.strip().str.zfill(5)

print(f"✅ Données chargées : {len(df)} lignes")
print(f"📋 Codes présents par année :")
print(df.groupby(["TIME_PERIOD", "EDUC"]).size().to_string())

# ---- PIVOT : une ligne par commune × année ----
df_pivot = df.pivot_table(
    index=["code_geo", "TIME_PERIOD"],
    columns="EDUC",
    values="OBS_VALUE",
    aggfunc="first"
).reset_index()

df_pivot.columns.name = None
print(f"\n✅ Pivot : {len(df_pivot)} lignes, colonnes : {list(df_pivot.columns)}")

# ---- CALCUL DE L'INDICATEUR ----
# Méthode unifiée : (_T - bac - CAP_BEP) / _T
# Applicable 2016 ET 2022 car ces 3 codes sont disponibles les deux années
df_pivot["nb_bas_diplome"] = df_pivot["_T"] - df_pivot["350T351_RP"] - df_pivot["300_RP"]
df_pivot["taux_faible_diplome"] = (
    df_pivot["nb_bas_diplome"] / df_pivot["_T"] * 100
).round(2)

# ---- VÉRIFICATION CROISÉE SUR 2022 ----
# Comparer avec 001T200_RP / _T (disponible uniquement en 2022)
df_2022 = df_pivot[df_pivot["TIME_PERIOD"] == 2022].copy()
if "001T200_RP" in df_2022.columns:
    df_2022["taux_ref_2022"] = (df_2022["001T200_RP"] / df_2022["_T"] * 100).round(2)
    df_2022["ecart"] = (df_2022["taux_faible_diplome"] - df_2022["taux_ref_2022"]).abs()
    print(f"\n📊 Vérification croisée 2022 :")
    print(f"   Écart moyen méthode unifiée vs 001T200_RP : {df_2022['ecart'].mean():.2f} pts")
    print(f"   Écart max                                  : {df_2022['ecart'].max():.2f} pts")
    print(f"   → Cohérence {'✅ bonne' if df_2022['ecart'].mean() < 3 else '⚠️ à vérifier'}")

# ---- RÉSULTAT FINAL ----
df_final = df_pivot[["code_geo", "TIME_PERIOD", "taux_faible_diplome"]].rename(
    columns={"TIME_PERIOD": "annee"}
)

print(f"\n📊 Résumé par année :")
for annee in [2016, 2022]:
    sub = df_final[df_final["annee"] == annee]
    print(f"\n   {annee} :")
    print(f"   Communes  : {len(sub)}")
    print(f"   NaN       : {sub['taux_faible_diplome'].isna().sum()}")
    print(f"   Min / Max : {sub['taux_faible_diplome'].min():.1f}% / {sub['taux_faible_diplome'].max():.1f}%")
    print(f"   Médiane   : {sub['taux_faible_diplome'].median():.1f}%")

print(f"\n🔍 Aperçu :")
print(df_final.head(8).to_string(index=False))

# ---- SAUVEGARDE ----
df_final.to_csv(os.path.join(CURATED_PATH, "diplomes_2016_2022.csv"), index=False, encoding="utf-8")
print(f"\n✅ Fichier global → data/curated/social/diplomes_2016_2022.csv")

for annee in [2016, 2022]:
    sub = df_final[df_final["annee"] == annee]
    sub.to_csv(os.path.join(CURATED_PATH, f"diplomes_{annee}.csv"), index=False, encoding="utf-8")
    print(f"✅ Fichier {annee}   → {len(sub)} communes")