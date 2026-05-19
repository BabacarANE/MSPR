import pandas as pd
import os

# ---- CHEMINS ----
RAW_PATH     = "data/raw/economie"
CURATED_PATH = "data/curated/economie"

os.makedirs(CURATED_PATH, exist_ok=True)

# ---- UTILITAIRE ----
def clean_float(val):
    """Nettoie une valeur numérique : espaces insécables, virgule décimale, chaînes vides."""
    if pd.isna(val):
        return None
    cleaned = str(val).replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None

# ==============================================================
# PARTIE 1 — ECO 2016 (eco_2016.xls, feuille COM)
# ==============================================================
print("=" * 55)
print("📂 Lecture eco_2016.xls — feuille COM...")

# Structure du fichier :
#   Ligne 0-3 : métadonnées INSEE (titre, champ, géo, ©)
#   Ligne 4   : libellés longs des colonnes
#   Ligne 5   : codes courts → CODGEO, LIBGEO, MED16...  ← header=5
#   Ligne 6+  : données (une ligne = une commune)
df_2016 = pd.read_excel(
    os.path.join(RAW_PATH, "eco_2016.xls"),
    sheet_name="COM",
    header=5,
    dtype={"CODGEO": str}    # évite la conversion int qui perdrait le zéro initial
)

df_2016.columns = df_2016.columns.str.strip()
print(f"✅ Chargé : {df_2016.shape[0]} lignes")
print(f"📋 Colonnes clés : CODGEO={('CODGEO' in df_2016.columns)}, "
      f"MED16={('MED16' in df_2016.columns)}")

# Filtrage Nord (59)
df_2016["CODGEO"] = df_2016["CODGEO"].astype(str).str.strip().str.zfill(5)
df_2016 = df_2016[df_2016["CODGEO"].str.startswith("59")].copy()
print(f"✅ Communes Nord : {len(df_2016)}")

# MED16 est un entier brut dans le fichier (ex: 22679) — on force la conversion propre
# On passe par numeric directement sans clean_float pour éviter les décimales parasites
df_2016["MED16"] = pd.to_numeric(df_2016["MED16"], errors="coerce")

# Sélection finale
df_2016_final = df_2016[["CODGEO", "LIBGEO", "MED16"]].rename(columns={
    "CODGEO": "code_geo",
    "LIBGEO": "libelle_commune",
    "MED16":  "revenu_median"
})
df_2016_final["revenu_median"] = df_2016_final["revenu_median"].round(0).astype("Int64")
df_2016_final["annee"] = 2016

nb_nan = df_2016_final["revenu_median"].isna().sum()
print(f"\n📊 Résumé 2016 :")
print(f"   Communes     : {len(df_2016_final)}")
print(f"   NaN revenu   : {nb_nan}")
print(f"   Min / Max    : {df_2016_final['revenu_median'].min():,} € / {df_2016_final['revenu_median'].max():,} €")
print(f"   Moyenne      : {df_2016_final['revenu_median'].mean():,.0f} €")
print(f"\n🔍 Aperçu :")
print(df_2016_final.head(5).to_string(index=False))

# ==============================================================
# PARTIE 2 — ECO 2021 (eco_2021.csv, niveau commune)
# ==============================================================
print("\n" + "=" * 55)
print("📂 Lecture eco_2021.csv...")

# Structure :
#   Ligne 0 : header direct (pas de métadonnées)
#   Séparateur : point-virgule
#   Colonnes utiles :
#     "Code géographique"  → code commune (CODGEO)
#     "Libellé géographique" → nom commune
#     "[DISP] Médiane (€)"  → revenu médian disponible (données diffusées)
df_2021 = pd.read_csv(
    os.path.join(RAW_PATH, "eco_2021.csv"),
    sep=";",
    dtype={"Code géographique": str},
    encoding="utf-8"        # tester latin-1 si UnicodeDecodeError
)

print(f"✅ Chargé : {df_2021.shape[0]} lignes, {df_2021.shape[1]} colonnes")

# Nettoyer les noms de colonnes (espaces, caractères spéciaux résiduels)
df_2021.columns = df_2021.columns.str.strip()

# Identifier la colonne médiane [DISP]
col_med = [c for c in df_2021.columns if "disp" in c.lower() and "diane" in c.lower()]
col_geo = [c for c in df_2021.columns if "code" in c.lower() and "ographique" in c.lower()]
col_lib = [c for c in df_2021.columns if "lib" in c.lower() and "ographique" in c.lower()]

if not col_med:
    raise ValueError(f"Colonne médiane [DISP] introuvable. Colonnes : {list(df_2021.columns[:10])}")

col_med = col_med[0]
col_geo = col_geo[0]
col_lib = col_lib[0]

print(f"📋 Colonnes détectées :")
print(f"   code_geo   : '{col_geo}'")
print(f"   libelle    : '{col_lib}'")
print(f"   médiane    : '{col_med}'")

# Filtrage Nord (59)
df_2021[col_geo] = df_2021[col_geo].astype(str).str.strip().str.zfill(5)
df_2021 = df_2021[df_2021[col_geo].str.startswith("59")].copy()
print(f"✅ Communes Nord : {len(df_2021)}")

# Nettoyage de la médiane (peut avoir virgule décimale ou être vide)
df_2021[col_med] = df_2021[col_med].apply(clean_float)

# Sélection finale
df_2021_final = df_2021[[col_geo, col_lib, col_med]].rename(columns={
    col_geo: "code_geo",
    col_lib: "libelle_commune",
    col_med: "revenu_median"
})
df_2021_final["revenu_median"] = df_2021_final["revenu_median"].round(0).astype("Int64")
df_2021_final["annee"] = 2021

nb_nan = df_2021_final["revenu_median"].isna().sum()
print(f"\n📊 Résumé 2021 :")
print(f"   Communes     : {len(df_2021_final)}")
print(f"   NaN revenu   : {nb_nan} ({nb_nan/len(df_2021_final)*100:.1f}%)")
print(f"   Min / Max    : {df_2021_final['revenu_median'].min():,} € / {df_2021_final['revenu_median'].max():,} €")
print(f"   Moyenne      : {df_2021_final['revenu_median'].mean():,.0f} €")
print(f"\n🔍 Aperçu :")
print(df_2021_final.head(5).to_string(index=False))

# ==============================================================
# SAUVEGARDE
# ==============================================================
out_2016   = os.path.join(CURATED_PATH, "revenu_median_2016.csv")
out_2021   = os.path.join(CURATED_PATH, "revenu_median_2021.csv")
out_global = os.path.join(CURATED_PATH, "revenu_median_2016_2021.csv")

df_2016_final.to_csv(out_2016,   index=False, encoding="utf-8")
df_2021_final.to_csv(out_2021,   index=False, encoding="utf-8")

df_global = pd.concat([df_2016_final, df_2021_final], ignore_index=True)
df_global.to_csv(out_global, index=False, encoding="utf-8")

print(f"\n✅ {out_2016}   → {len(df_2016_final)} communes")
print(f"✅ {out_2021}   → {len(df_2021_final)} communes")
print(f"✅ {out_global} → {len(df_global)} lignes")