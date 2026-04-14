import pandas as pd
import unicodedata
import os

# ---- CHEMINS ----
CURATED_PATH = "data/curated/election"
FINAL_PATH   = "data/final"

os.makedirs(FINAL_PATH, exist_ok=True)

# ---- MAPPING POLITIQUE (source : PROJET_NOTES.md) ----
mapping_candidat_parti = {
    # 2017
    "LE PEN":        "RN",
    "FILLON":        "LR",
    "MACRON":        "LREM",
    "MELENCHON":     "FI",    # version sans accent (fallback encodage)
    "MÉLENCHON":     "FI",
    "DUPONT-AIGNAN": "DLF",
    "HAMON":         "PS",
    "LASSALLE":      "DIV",
    "ARTHAUD":       "LO",
    "POUTOU":        "NPA",
    "ASSELINEAU":    "UPR",
    "CHEMINADE":     "SP",
    # 2022
    "ZEMMOUR":       "REC",
    "PECRESSE":      "LR",    # version sans accent
    "PÉCRESSE":      "LR",
    "HIDALGO":       "PS",
    "JADOT":         "EELV",
    "ROUSSEL":       "PCF",
}

mapping_parti_bloc = {
    "FI":   "GAUCHE",
    "PS":   "GAUCHE",
    "LO":   "GAUCHE",
    "NPA":  "GAUCHE",
    "EELV": "GAUCHE",
    "PCF":  "GAUCHE",
    "LREM": "CENTRE",
    "RN":   "DROITE",
    "LR":   "DROITE",
    "DLF":  "DROITE",
    "REC":  "DROITE",
    "DIV":  "DIVERS",
    "UPR":  "DIVERS",
    "SP":   "DIVERS",
}

# ---- NORMALISATION ACCENTS ----
# Permet de gérer les problèmes d'encodage (MÉLENCHON vs MELENCHON vs MÃ‰LENCHON)
def strip_accents(s: str) -> str:
    """Supprime les accents et met en majuscules."""
    return (
        unicodedata.normalize("NFD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .upper()
        .strip()
    )

# Construire un mapping normalisé (sans accents) pour la recherche
mapping_norm = {strip_accents(k): v for k, v in mapping_candidat_parti.items()}


def get_parti(nom):
    """Recherche le parti d'un candidat en normalisant les accents."""
    if pd.isna(nom):
        return "DIVERS"
    return mapping_norm.get(strip_accents(str(nom)), "DIVERS")


# ---- CONSTRUCTION DU CODE GEO INSEE ----
# code_commune est zero-paddé sur 5 chiffres (ex: "00001")
# INSEE : code_dept (2 chiffres) + code_commune (3 chiffres) → "59001"
def build_code_geo(code_dept, code_commune):
    dept = str(int(code_dept)).zfill(2)
    commune_3 = str(code_commune)[-3:]  # "00001" → "001"
    return dept + commune_3


# ---- FONCTION PRINCIPALE ----
def pivoter_top5(annee: int) -> pd.DataFrame:
    fichier = os.path.join(CURATED_PATH, f"elections_{annee}_tour1.csv")
    print(f"\n📂 Lecture {fichier}...")

    df = pd.read_csv(
        fichier,
        dtype={"code_commune": str, "code_departement": str},
        encoding="utf-8"
    )
    print(f"✅ Chargé : {df.shape[0]} lignes")

    # ---- PARTI + BLOC ----
    df["parti"] = df["nom"].apply(get_parti)
    df["bloc"]  = df["parti"].map(mapping_parti_bloc).fillna("DIVERS")

    # Diagnostic : candidats non reconnus
    inconnus = df[df["parti"] == "DIVERS"]["nom"].unique()
    if len(inconnus):
        print(f"⚠️  Candidats non mappés (DIVERS) : {list(inconnus)}")

    # ---- CODE GEO INSEE ----
    df["code_geo"] = df.apply(
        lambda r: build_code_geo(r["code_departement"], r["code_commune"]),
        axis=1
    )

    # ---- INFOS FIXES PAR COMMUNE ----
    # On garde uniquement les colonnes utiles pour le ML
    cols_commune = [
        "code_geo", "code_commune", "libelle_commune",
        "inscrits", "abstentions", "votants",
        "exprimes", "pct_abstention", "annee"
    ]
    df_commune = df[cols_commune].drop_duplicates(subset=["code_commune"])

    # ---- TOP 5 PAR COMMUNE ----
    df_top5 = (
        df.sort_values(["code_commune", "voix"], ascending=[True, False])
        .groupby("code_commune")
        .head(5)
        .copy()
    )
    print(f"📋 Candidats moyens / commune : "
          f"{df_top5.groupby('code_commune')['nom'].count().mean():.1f}")

    df_top5["rang"] = (
        df_top5.groupby("code_commune")["voix"]
        .rank(ascending=False, method="first")
        .astype(int)
    )

    # Pivot % voix
    df_pivot_pct = df_top5.pivot_table(
        index="code_commune", columns="rang",
        values="pct_voix_exp", aggfunc="first"
    ).reset_index()
    df_pivot_pct.columns = (
        ["code_commune"] +
        [f"pct_cand{i}" for i in range(1, len(df_pivot_pct.columns))]
    )

    # Pivot noms
    df_pivot_nom = df_top5.pivot_table(
        index="code_commune", columns="rang",
        values="nom", aggfunc="first"
    ).reset_index()
    df_pivot_nom.columns = (
        ["code_commune"] +
        [f"nom_cand{i}" for i in range(1, len(df_pivot_nom.columns))]
    )

    # Pivot bloc par candidat
    df_pivot_bloc_cand = df_top5.pivot_table(
        index="code_commune", columns="rang",
        values="bloc", aggfunc="first"
    ).reset_index()
    df_pivot_bloc_cand.columns = (
        ["code_commune"] +
        [f"bloc_cand{i}" for i in range(1, len(df_pivot_bloc_cand.columns))]
    )

    # ---- AGRÉGATION PAR BLOC ----
    df_blocs = (
        df.groupby(["code_commune", "bloc"])
        .agg(voix_bloc=("voix", "sum"))
        .reset_index()
    )
    df_pivot_bloc = df_blocs.pivot_table(
        index="code_commune", columns="bloc",
        values="voix_bloc", aggfunc="sum"
    ).reset_index()
    df_pivot_bloc.columns = (
        ["code_commune"] +
        [f"voix_{col}" for col in df_pivot_bloc.columns[1:]]
    )

    # ---- FUSION ----
    df_final = (
        df_commune
        .merge(df_pivot_nom,       on="code_commune", how="left")
        .merge(df_pivot_pct,       on="code_commune", how="left")
        .merge(df_pivot_bloc_cand, on="code_commune", how="left")
        .merge(df_pivot_bloc,      on="code_commune", how="left")
    )

    # ---- PCT PAR BLOC (recalcul) ----
    cols_voix_blocs = [c for c in df_final.columns if c.startswith("voix_")]
    for col in cols_voix_blocs:
        pct_col = col.replace("voix_", "pct_")
        df_final[pct_col] = (df_final[col] / df_final["exprimes"] * 100).round(2)

    # Supprimer les colonnes voix_ intermédiaires (inutiles pour le ML)
    df_final = df_final.drop(columns=cols_voix_blocs)

    # Remplir NaN des blocs absents par 0 (ex : commune sans vote CENTRE)
    cols_pct_blocs = [c for c in df_final.columns
                      if c in ["pct_GAUCHE", "pct_CENTRE", "pct_DROITE", "pct_DIVERS"]]
    df_final[cols_pct_blocs] = df_final[cols_pct_blocs].fillna(0)

    # ---- BLOC VAINQUEUR (variable cible ML) ----
    df_final["bloc_vainqueur"] = (
        df_final[cols_pct_blocs]
        .idxmax(axis=1)
        .str.replace("pct_", "", regex=False)
    )

    # ---- ORDRE FINAL DES COLONNES ----
    # Identifiants → participation → top5 → blocs → cible
    cols_ids          = ["code_geo", "code_commune", "libelle_commune", "annee"]
    cols_participation = ["inscrits", "abstentions", "votants", "exprimes", "pct_abstention"]
    cols_top5_nom     = [c for c in df_final.columns if c.startswith("nom_cand")]
    cols_top5_pct     = [c for c in df_final.columns if c.startswith("pct_cand")]
    cols_top5_bloc    = [c for c in df_final.columns if c.startswith("bloc_cand")]
    cols_blocs_pct    = sorted(cols_pct_blocs)
    cols_cible        = ["bloc_vainqueur"]

    col_order = (
        cols_ids + cols_participation +
        cols_top5_nom + cols_top5_pct + cols_top5_bloc +
        cols_blocs_pct + cols_cible
    )
    df_final = df_final[col_order]

    # ---- RÉSUMÉ ----
    print(f"\n📊 Résumé {annee} :")
    print(f"   Communes       : {len(df_final)}")
    print(f"   Colonnes       : {len(df_final.columns)}")
    print(f"   Valeurs NaN    : {df_final.isnull().sum().sum()}")
    print(f"\n🏆 Blocs vainqueurs :\n{df_final['bloc_vainqueur'].value_counts().to_string()}")

    return df_final


# ---- TRAITEMENT ----
df_2017 = pivoter_top5(2017)
df_2022 = pivoter_top5(2022)

# ---- SAUVEGARDE ----
out_2017 = os.path.join(FINAL_PATH, "elections_2017_pivot.csv")
out_2022 = os.path.join(FINAL_PATH, "elections_2022_pivot.csv")

df_2017.to_csv(out_2017, index=False, encoding="utf-8")
df_2022.to_csv(out_2022, index=False, encoding="utf-8")

print(f"\n✅ Fichiers sauvegardés :")
print(f"   → {out_2017}")
print(f"   → {out_2022}")