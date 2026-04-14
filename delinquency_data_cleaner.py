import pandas as pd
import os

# ---- CHEMINS ----
RAW_PATH     = "data/raw/securite"
CURATED_PATH = "data/curated/securite"

os.makedirs(CURATED_PATH, exist_ok=True)

fichier_raw = os.path.join(RAW_PATH, "delinquence.csv")
print(f"📂 Lecture de {fichier_raw}...")

# ---- DÉTECTION AUTOMATIQUE DES COLONNES ----
# On lit juste la première ligne pour connaître les vrais noms de colonnes
df_head = pd.read_csv(fichier_raw, sep=";", encoding="latin-1", quotechar='"', nrows=1)
print(f"📋 Colonnes détectées : {df_head.columns.tolist()}")

# Détection robuste de la colonne code géo (peut s'appeler CODGEO, CODGEO_2025, COD_GEO...)
col_codgeo = next(
    (c for c in df_head.columns if "codgeo" in c.lower() or "cod_geo" in c.lower()),
    None
)
if col_codgeo is None:
    raise ValueError("❌ Aucune colonne CODGEO trouvée. Colonnes disponibles : " + str(df_head.columns.tolist()))
print(f"✅ Colonne code géo utilisée : '{col_codgeo}'")

# ---- LECTURE PAR CHUNKS ----
# Fichier volumineux → on filtre à la lecture pour ne pas saturer la RAM
ANNEES_CIBLES = [2016, 2022]
chunks = []

for chunk in pd.read_csv(
    fichier_raw,
    sep=";",                                           # ✅ FIX 1 : séparateur point-virgule
    encoding="latin-1",                                # ✅ FIX 2 : encodage latin-1 (évite les Ã©, Ã¨...)
    quotechar='"',                                     # ✅ FIX 3 : guillemets autour des valeurs
    dtype={col_codgeo: str, "est_diffuse": str},
    chunksize=100_000
):
    # Filtrage immédiat : Nord (59) + années utiles
    mask = (
        (chunk[col_codgeo].str.startswith("59", na=False)) &
        (chunk["annee"].isin(ANNEES_CIBLES))
    )
    chunks.append(chunk[mask])

# Renommer la colonne détectée en nom standard pour la suite du script
df_temp = pd.concat(chunks, ignore_index=True)
if col_codgeo != "CODGEO_2025":
    df_temp = df_temp.rename(columns={col_codgeo: "CODGEO_2025"})

df = df_temp
print(f"✅ Données Nord 2016/2022 : {df.shape[0]} lignes, {df.shape[1]} colonnes")

# ---- DIAGNOSTIC ----
print(f"\n📋 Indicateurs disponibles ({df['indicateur'].nunique()}) :")
print(df["indicateur"].value_counts().to_string())
print(f"\n📋 est_diffuse : {df['est_diffuse'].value_counts().to_dict()}")
print(f"\n📋 unite_de_compte : {df['unite_de_compte'].value_counts().to_dict()}")
print(f"\n📋 Taux NaN : {df['taux_pour_mille'].isna().sum()} / {len(df)}")
print(f"📋 Nombre NaN : {df['nombre'].isna().sum()} / {len(df)}")

# ---- NETTOYAGE ----
# Garder uniquement les lignes avec des données réelles
# (exclure les lignes où nombre ET taux sont tous les deux manquants)
df = df[~(df["nombre"].isna() & df["taux_pour_mille"].isna())].copy()
print(f"✅ Après suppression lignes vides : {df.shape[0]} lignes")

# ✅ FIX 3 : la colonne est_diffuse contient "diff" / "ndiff" (pas "OUI"/"TRUE")
# On garde uniquement les lignes marquées "diff" (données diffusées publiquement)
if df["est_diffuse"].notna().any():
    df = df[df["est_diffuse"].str.lower() == "diff"].copy()
    print(f"✅ Après filtre est_diffuse='diff' : {df.shape[0]} lignes")

# Standardiser le code géo (5 chiffres)
df["code_geo"] = df["CODGEO_2025"].str.strip().str.zfill(5)

# ---- AGRÉGATION PAR COMMUNE ET ANNÉE ----
# Stratégie : sommer le nombre de faits par commune/année (tous indicateurs confondus)
# puis recalculer un taux global sur la population INSEE du fichier
print("\n🔄 Agrégation par commune et année...")

df_agg = (
    df.groupby(["code_geo", "annee"])
    .agg(
        nb_faits_total    = ("nombre",     "sum"),
        population_insee  = ("insee_pop",  "median"),  # identique sur toutes les lignes d'une commune
        nb_indicateurs    = ("indicateur", "nunique"),
    )
    .reset_index()
)

# Recalcul du taux global pour mille habitants
df_agg["taux_delinquance"] = (
    df_agg["nb_faits_total"] / df_agg["population_insee"] * 1000
).round(2)

print(f"✅ Format agrégé : {len(df_agg)} lignes")
print(f"   Communes uniques : {df_agg['code_geo'].nunique()}")
print(f"   Années           : {sorted(df_agg['annee'].unique())}")

# ---- PIVOT PAR INDICATEUR (optionnel — pour analyse détaillée) ----
# Créer aussi une version détaillée avec un taux par indicateur
print("\n🔄 Création version détaillée par indicateur...")

df_detail = df.groupby(["code_geo", "annee", "indicateur"]).agg(
    nb_faits = ("nombre",    "sum"),
    pop      = ("insee_pop", "median")
).reset_index()

df_detail["taux"] = (df_detail["nb_faits"] / df_detail["pop"] * 1000).round(2)

df_pivot = df_detail.pivot_table(
    index=["code_geo", "annee"],
    columns="indicateur",
    values="taux",
    aggfunc="first"
).reset_index()

# Nettoyer les noms de colonnes (espaces, caractères spéciaux)
df_pivot.columns = (
    df_pivot.columns
    .str.strip()
    .str.lower()
    .str.replace(r"[^a-z0-9_]", "_", regex=True)
    .str.replace(r"_+", "_", regex=True)
    .str.strip("_")
)

print(f"✅ Format pivot : {len(df_pivot)} lignes, {len(df_pivot.columns)} colonnes")

# ---- VÉRIFICATION ----
print(f"\n📊 Résumé agrégé :")
for annee in ANNEES_CIBLES:
    sub = df_agg[df_agg["annee"] == annee]
    print(f"\n   Année {annee} :")
    print(f"   Communes : {len(sub)}")
    print(f"   Taux min / max : {sub['taux_delinquance'].min()} / {sub['taux_delinquance'].max()} ‰")
    print(f"   Taux médian    : {sub['taux_delinquance'].median():.1f} ‰")

print(f"\n🔍 Aperçu agrégé :")
print(df_agg.head(6).to_string(index=False))

# ---- SAUVEGARDE ----
# Fichier agrégé global (utilisé pour la jointure ML)
fichier_global = os.path.join(CURATED_PATH, "delinquance_2016_2022.csv")
df_agg.to_csv(fichier_global, index=False, encoding="utf-8")
print(f"\n✅ Fichier global   → {fichier_global}")

# Fichiers par année
for annee in ANNEES_CIBLES:
    df_annee = df_agg[df_agg["annee"] == annee]
    fichier  = os.path.join(CURATED_PATH, f"delinquance_{annee}.csv")
    df_annee.to_csv(fichier, index=False, encoding="utf-8")
    print(f"✅ Fichier {annee}     → {fichier} ({len(df_annee)} communes)")

# Fichier pivot par indicateur (pour analyse et feature engineering avancé)
fichier_pivot = os.path.join(CURATED_PATH, "delinquance_pivot_indicateurs.csv")
df_pivot.to_csv(fichier_pivot, index=False, encoding="utf-8")
print(f"✅ Fichier pivot    → {fichier_pivot}")