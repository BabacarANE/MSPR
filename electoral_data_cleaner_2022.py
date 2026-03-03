import pandas as pd
import os

# ---- CHEMINS ----
RAW_PATH = "data/raw/elections"
CURATED_PATH = "data/curated/election"

os.makedirs(CURATED_PATH, exist_ok=True)

# ---- LECTURE DU FICHIER BRUT ----
fichier_raw = os.path.join(RAW_PATH, "election_2022_Tour_1.xlsx")
print(f"📂 Lecture de {fichier_raw}...")

df = pd.read_excel(
    fichier_raw,
    sheet_name="Tour1",
    header=0
)

print(f"✅ Fichier chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")

# ---- CORRECTION 1 : FILTRAGE ----
# Vérifier le type réel du code département
print(f"\n🔍 Type code département : {df['Code du département'].dtype}")
print(f"🔍 Valeurs uniques (5 premières) : {df['Code du département'].unique()[:5]}")

# Convertir en string pour comparaison sûre
df["Code du département"] = df["Code du département"].astype(str).str.strip()
df = df[df["Code du département"] == "59"].copy()
print(f"✅ Après filtrage Nord (59) : {df.shape[0]} lignes")

# ---- DÉFINITION DES COLONNES FIXES ----
cols_fixes = [
    "Code du département",
    "Libellé du département",
    "Code de la circonscription",
    "Libellé de la circonscription",
    "Code de la commune",
    "Libellé de la commune",
    "Code du b.vote",
    "Inscrits",
    "Abstentions",
    "% Abs/Ins",
    "Votants",
    "% Vot/Ins",
    "Blancs",
    "% Blancs/Ins",
    "% Blancs/Vot",
    "Nuls",
    "% Nuls/Ins",
    "% Nuls/Vot",
    "Exprimés",
    "% Exp/Ins",
    "% Exp/Vot"
]

# ---- CORRECTION 2 : RENOMMER LES COLONNES UNNAMED ----
# Le premier bloc candidat a des vrais noms, les suivants sont Unnamed
# On repère l'index de la première colonne candidat
idx_debut_candidats = len(cols_fixes)  # = 21

# Renommer toutes les colonnes candidats par position
colonnes = list(df.columns)
nb_candidats = (len(colonnes) - idx_debut_candidats) // 7

print(f"📋 Nombre de candidats max détectés : {nb_candidats}")

# Renommer les colonnes candidats de façon explicite
nouveaux_noms = colonnes[:idx_debut_candidats]  # garder les colonnes fixes
for i in range(nb_candidats):
    nouveaux_noms.extend([
        f"cand{i + 1}_panneau",
        f"cand{i + 1}_sexe",
        f"cand{i + 1}_nom",
        f"cand{i + 1}_prenom",
        f"cand{i + 1}_voix",
        f"cand{i + 1}_pct_ins",
        f"cand{i + 1}_pct_exp"
    ])

# Ajuster si colonnes restantes
while len(nouveaux_noms) < len(colonnes):
    nouveaux_noms.append(f"col_extra_{len(nouveaux_noms)}")

df.columns = nouveaux_noms[:len(colonnes)]

# ---- TRANSFORMATION FORMAT LARGE → FORMAT LONG ----
print("\n🔄 Transformation format large → long...")
resultats = []

for _, row in df.iterrows():
    commune_info = {col: row[col] for col in cols_fixes}

    for i in range(1, nb_candidats + 1):
        nom = row.get(f"cand{i}_nom")

        if pd.isna(nom):
            continue

        candidat = {
            "numero_panneau": row.get(f"cand{i}_panneau"),
            "sexe": row.get(f"cand{i}_sexe"),
            "nom": nom,
            "prenom": row.get(f"cand{i}_prenom"),
            "voix": row.get(f"cand{i}_voix"),
            "pct_voix_ins": row.get(f"cand{i}_pct_ins"),
            "pct_voix_exp": row.get(f"cand{i}_pct_exp")
        }

        resultats.append({**commune_info, **candidat})

df_long = pd.DataFrame(resultats)
print(f"✅ Format long : {len(df_long)} lignes")

# ---- AGRÉGATION PAR COMMUNE ----
print("\n🔄 Agrégation par commune...")

df_commune = df_long.groupby([
    "Code du département",
    "Libellé du département",
    "Code de la circonscription",
    "Libellé de la circonscription",
    "Code de la commune",
    "Libellé de la commune",
    "nom", "prenom", "sexe", "numero_panneau"
]).agg(
    inscrits=("Inscrits", "sum"),
    abstentions=("Abstentions", "sum"),
    votants=("Votants", "sum"),
    blancs=("Blancs", "sum"),
    nuls=("Nuls", "sum"),
    exprimes=("Exprimés", "sum"),
    voix=("voix", "sum")
).reset_index()

# ---- RECALCUL DES POURCENTAGES ----
df_commune["pct_abstention"] = (df_commune["abstentions"] / df_commune["inscrits"] * 100).round(2)
df_commune["pct_votants"] = (df_commune["votants"] / df_commune["inscrits"] * 100).round(2)
df_commune["pct_blancs_ins"] = (df_commune["blancs"] / df_commune["inscrits"] * 100).round(2)
df_commune["pct_blancs_vot"] = (df_commune["blancs"] / df_commune["votants"] * 100).round(2)
df_commune["pct_nuls_ins"] = (df_commune["nuls"] / df_commune["inscrits"] * 100).round(2)
df_commune["pct_nuls_vot"] = (df_commune["nuls"] / df_commune["votants"] * 100).round(2)
df_commune["pct_exprimes_ins"] = (df_commune["exprimes"] / df_commune["inscrits"] * 100).round(2)
df_commune["pct_exprimes_vot"] = (df_commune["exprimes"] / df_commune["votants"] * 100).round(2)
df_commune["pct_voix_ins"] = (df_commune["voix"] / df_commune["inscrits"] * 100).round(2)
df_commune["pct_voix_exp"] = (df_commune["voix"] / df_commune["exprimes"] * 100).round(2)

# ---- RENOMMAGE FINAL ----
df_commune = df_commune.rename(columns={
    "Code du département": "code_departement",
    "Libellé du département": "libelle_departement",
    "Code de la circonscription": "code_circonscription",
    "Libellé de la circonscription": "libelle_circonscription",
    "Code de la commune": "code_commune",
    "Libellé de la commune": "libelle_commune",
})

# ---- MÉTADONNÉES ----
df_commune["annee"] = 2022
df_commune["tour"] = 1

# ---- STANDARDISATION CODE COMMUNE ----
df_commune["code_commune"] = df_commune["code_commune"].astype(str).str.zfill(5)

# ---- VÉRIFICATION FINALE ----
print(f"\n📊 Résumé du fichier nettoyé :")
print(f"   Lignes totales     : {len(df_commune)}")
print(f"   Communes uniques   : {df_commune['code_commune'].nunique()}")
print(f"   Valeurs manquantes : {df_commune.isnull().sum().sum()}")
print(f"\n🔍 Aperçu :")
print(df_commune.head(5).to_string())

# ---- SAUVEGARDE ----
fichier_curated = os.path.join(CURATED_PATH, "elections_2022_tour1.csv")
df_commune.to_csv(fichier_curated, index=False, encoding="utf-8")
print(f"\n✅ Fichier sauvegardé → {fichier_curated}")