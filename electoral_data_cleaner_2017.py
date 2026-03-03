import pandas as pd
import os

# ---- CHEMINS ----
RAW_PATH = "data/raw/elections"
CURATED_PATH = "data/curated/election"

os.makedirs(CURATED_PATH, exist_ok=True)

# ---- LECTURE DU FICHIER BRUT ----
# header=3 car il y a 3 lignes avant le vrai en-tête (titre + lignes vides)
fichier_raw = os.path.join(RAW_PATH, "election_2017_Tour_1.xls")
print(f"📂 Lecture de {fichier_raw}...")

df = pd.read_excel(fichier_raw, sheet_name="Feuil1", header=3)

print(f"✅ Fichier chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")
print(f"📋 Colonnes détectées : {list(df.columns)}")

# ---- FILTRAGE SUR LE NORD (59) ----
df = df[df["Code du département"] == 59].copy()
print(f"✅ Après filtrage Nord (59) : {df.shape[0]} lignes")

# ---- DÉFINITION DES COLONNES FIXES ----
# Attention : Blancs et Nuls sont maintenant séparés en 2017
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
    "Blancs",  # séparé en 2017
    "% Blancs/Ins",  # séparé en 2017
    "% Blancs/Vot",  # séparé en 2017
    "Nuls",  # séparé en 2017
    "% Nuls/Ins",  # séparé en 2017
    "% Nuls/Vot",  # séparé en 2017
    "Exprimés",
    "% Exp/Ins",
    "% Exp/Vot"
]

# ---- COLONNES CANDIDATS ----
cols_candidats = [col for col in df.columns if col not in cols_fixes]
print(f"📋 Colonnes candidats détectées : {len(cols_candidats)}")
print(f"📋 Nombre de candidats max par commune : {len(cols_candidats) // 7}")

# ---- TRANSFORMATION FORMAT LARGE → FORMAT LONG ----
resultats = []

for _, row in df.iterrows():
    commune_info = {col: row[col] for col in cols_fixes}

    # Blocs de 7 colonnes en 2017
    # (N°Panneau, Sexe, Nom, Prénom, Voix, % Voix/Ins, % Voix/Exp)
    for i in range(0, len(cols_candidats), 7):
        bloc = cols_candidats[i:i + 7]

        if len(bloc) < 7:
            break

        # Ignorer les candidats vides (vérifier sur le Nom = bloc[2])
        if pd.isna(row[bloc[2]]):
            continue

        candidat = {
            "numero_panneau": row[bloc[0]],
            "sexe": row[bloc[1]],
            "nom": row[bloc[2]],
            "prenom": row[bloc[3]],
            "voix": row[bloc[4]],
            "pct_voix_ins": row[bloc[5]],
            "pct_voix_exp": row[bloc[6]]
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
    "Blancs": "blancs",
    "% Blancs/Ins": "pct_blancs_ins",
    "% Blancs/Vot": "pct_blancs_vot",
    "Nuls": "nuls",
    "% Nuls/Ins": "pct_nuls_ins",
    "% Nuls/Vot": "pct_nuls_vot",
    "Exprimés": "exprimes",
    "% Exp/Ins": "pct_exprimes_ins",
    "% Exp/Vot": "pct_exprimes_vot"
})

# ---- AJOUT DES MÉTADONNÉES ----
df_final["annee"] = 2017
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
fichier_curated = os.path.join(CURATED_PATH, "elections_2017_tour1.csv")
df_final.to_csv(fichier_curated, index=False, encoding="utf-8")
print(f"\n✅ Fichier sauvegardé → {fichier_curated}")