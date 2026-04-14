import pandas as pd
import os

# ---- CHEMINS ----
RAW_PATH     = "data/raw/demographie"
CURATED_PATH = "data/curated/demographie"

os.makedirs(CURATED_PATH, exist_ok=True)

fichier_raw = os.path.join(RAW_PATH, "dep59.xlsx")
print(f"📂 Lecture de {fichier_raw}...")

# ---- LECTURE ----
# header=7 : lignes 0-6 = métadonnées INSEE (titre, source, champ, lignes vides)
# ligne index 7 = vrais noms de colonnes, données à partir de l'index 8
df = pd.read_excel(
    fichier_raw,
    sheet_name="Communes",
    header=7,
    dtype=str
)

print(f"✅ Chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")
print(f"📋 Colonnes brutes : {list(df.columns)}")

# ---- NETTOYAGE DES NOMS DE COLONNES ----
df.columns = df.columns.str.strip().str.replace(r"\s+", " ", regex=True)
print(f"📋 Colonnes nettoyées : {list(df.columns)}")

# ---- DÉTECTION AUTOMATIQUE DES COLONNES ----
# Robuste si l'en-tête change légèrement entre versions du fichier
def find_col(cols, *keywords):
    for c in cols:
        if all(k.lower() in c.lower() for k in keywords):
            return c
    raise ValueError(f"Colonne introuvable avec mots-clés {keywords}. Dispo : {list(cols)}")

col_commune = find_col(df.columns, "code", "commune")
col_nom     = find_col(df.columns, "nom", "commune")
col_pop_mun = find_col(df.columns, "municipale")
col_pop_tot = find_col(df.columns, "totale")

print(f"📋 code commune    : '{col_commune}'")
print(f"📋 nom commune     : '{col_nom}'")
print(f"📋 pop municipale  : '{col_pop_mun}'")
print(f"📋 pop totale      : '{col_pop_tot}'")

# ---- SUPPRESSION DES LIGNES VIDES ----
df = df.dropna(subset=[col_commune])
print(f"✅ Après suppression lignes vides : {df.shape[0]} lignes")

# ---- NETTOYAGE DES VALEURS NUMÉRIQUES ----
# INSEE utilise l'espace (classique 0x20 ou insécable 0x00A0) comme séparateur de milliers
def clean_int(val):
    if pd.isna(val):
        return None
    cleaned = str(val).replace("\xa0", "").replace(" ", "").strip()
    try:
        return int(float(cleaned))
    except ValueError:
        return None

df[col_pop_mun] = df[col_pop_mun].apply(clean_int)
df[col_pop_tot] = df[col_pop_tot].apply(clean_int)

# ---- CODE GEO INSEE ----
# Code commune = "001" (3 chiffres) → code_geo = "59001"
df[col_commune] = df[col_commune].str.strip().str.zfill(3)
df["code_geo"]  = "59" + df[col_commune]

# ---- SÉLECTION ET RENOMMAGE ----
df_final = df[["code_geo", col_commune, col_nom, col_pop_mun, col_pop_tot]].copy()

df_final = df_final.rename(columns={
    col_commune: "code_commune",
    col_nom:     "libelle_commune",
    col_pop_mun: "population_municipale",
    col_pop_tot: "population_totale",
})

df_final["annee"] = 2021

df_final = df_final[[
    "code_geo", "code_commune", "libelle_commune",
    "population_municipale", "population_totale", "annee"
]]

# ---- VÉRIFICATION ----
print(f"\n📊 Résumé :")
print(f"   Communes          : {len(df_final)}")
print(f"   Valeurs NaN       : {df_final.isnull().sum().sum()}")
print(f"   Pop. min          : {df_final['population_municipale'].min():,}")
print(f"   Pop. max          : {df_final['population_municipale'].max():,}")
print(f"   Pop. médiane      : {df_final['population_municipale'].median():,.0f}")
print(f"   Pop. totale Nord  : {df_final['population_municipale'].sum():,}")
print(f"\n🔍 Aperçu :")
print(df_final.head(5).to_string(index=False))

# ---- SAUVEGARDE ----
fichier_curated = os.path.join(CURATED_PATH, "population_2016.csv")
df_final.to_csv(fichier_curated, index=False, encoding="utf-8")
print(f"\n✅ Sauvegardé → {fichier_curated}")