import pandas as pd
import os

# ---- CHEMINS ----
RAW_PATH = "data/raw/elections"
CURATED_PATH = "data/curated/election"

# Créer le dossier curated/election s'il n'existe pas
os.makedirs(CURATED_PATH, exist_ok=True)

# ---- LECTURE DU FICHIER BRUT ----
fichier_raw = os.path.join(RAW_PATH, "election_2012.xls")
print(f"📂 Lecture de {fichier_raw}...")

df = pd.read_excel(fichier_raw, sheet_name="Tour 1", header=0)

print(f"✅ Fichier chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")
print(f"📋 Colonnes détectées : {list(df.columns)}")

# ---- FILTRAGE SUR LE NORD (59) ----
df = df[df["Code du département"] == 59].copy()
print(f"✅ Après filtrage Nord (59) : {df.shape[0]} lignes")

# ---- DÉFINITION DES COLONNES FIXES ----
cols_fixes = [
    "Code du département",
    "Libellé du département",
    "Code de la commune",
    "Libellé de la commune",
    "Inscrits",
    "Abstentions",
    "% Abs/Ins",
    "Votants",
    "% Vot/Ins",
    "Blancs et nuls",
    "% BlNuls/Ins",
    "% BlNuls/Vot",
    "Exprimés",
    "% Exp/Ins",
    "% Exp/Vot"
]

# ---- COLONNES CANDIDATS (tout ce qui est après les colonnes fixes) ----
cols_candidats = [col for col in df.columns if col not in cols_fixes]
print(f"📋 Nombre de colonnes candidats détectées : {len(cols_candidats)}")
print(f"📋 Nombre de candidats max par commune : {len(cols_candidats) // 6}")

# ---- TRANSFORMATION FORMAT LARGE → FORMAT LONG ----
resultats = []

for _, row in df.iterrows():
    # Récupérer les infos fixes de la commune
    commune_info = {col: row[col] for col in cols_fixes}

    # Parcourir les candidats par blocs de 6 colonnes
    # (Sexe, Nom, Prénom, Voix, % Voix/Ins, % Voix/Exp)
    for i in range(0, len(cols_candidats), 6):
        bloc = cols_candidats[i:i + 6]

        # Ignorer si bloc incomplet
        if len(bloc) < 6:
            break

        # Ignorer les candidats vides
        if pd.isna(row[bloc[1]]):
            continue

        candidat = {
            "sexe": row[bloc[0]],
            "nom": row[bloc[1]],
            "prenom": row[bloc[2]],
            "voix": row[bloc[3]],
            "pct_voix_ins": row[bloc[4]],
            "pct_voix_exp": row[bloc[5]]
        }

        resultats.append({**commune_info, **candidat})

# ---- CRÉATION DU DATAFRAME FINAL ----
df_final = pd.DataFrame(resultats)

# ---- RENOMMAGE DES COLONNES ----
df_final = df_final.rename(columns={
    "Code du département": "code_departement",
    "Libellé du département": "libelle_departement",
    "Code de la commune": "code_commune",
    "Libellé de la commune": "libelle_commune",
    "Inscrits": "inscrits",
    "Abstentions": "abstentions",
    "% Abs/Ins": "pct_abstention",
    "Votants": "votants",
    "% Vot/Ins": "pct_votants",
    "Blancs et nuls": "blancs_nuls",
    "% BlNuls/Ins": "pct_blancs_nuls_ins",
    "% BlNuls/Vot": "pct_blancs_nuls_vot",
    "Exprimés": "exprimes",
    "% Exp/Ins": "pct_exprimes_ins",
    "% Exp/Vot": "pct_exprimes_vot"
})

# ---- AJOUT DES MÉTADONNÉES ----
df_final["annee"] = 2012
df_final["tour"] = 1

# ---- STANDARDISATION DU CODE COMMUNE (5 chiffres) ----
df_final["code_commune"] = df_final["code_commune"].astype(str).str.zfill(5)

# ---- VÉRIFICATION FINALE ----
print(f"\n📊 Résumé du fichier nettoyé :")
print(f"   Lignes totales     : {len(df_final)}")
print(f"   Communes uniques   : {df_final['code_commune'].nunique()}")
print(f"   Valeurs manquantes : {df_final.isnull().sum().sum()}")
print(f"\n🔍 Aperçu :")
print(df_final.head(5).to_string())

# ---- SAUVEGARDE ----
fichier_curated = os.path.join(CURATED_PATH, "elections_2012_tour1.csv")
df_final.to_csv(fichier_curated, index=False, encoding="utf-8")
print(f"\n✅ Fichier sauvegardé → {fichier_curated}")