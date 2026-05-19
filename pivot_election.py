import pandas as pd
import unicodedata
import os

# ---- CHEMINS ----
CURATED_PATH = "data/curated/election"
FINAL_PATH   = "data/final"

os.makedirs(FINAL_PATH, exist_ok=True)

# ---- MAPPING POLITIQUE ----
mapping_candidat_parti = {
    # 2017
    "LE PEN":        "RN",
    "FILLON":        "LR",
    "MACRON":        "LREM",
    "MELENCHON":     "FI",    # sans accent (fallback encodage)
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
    "PECRESSE":      "LR",    # sans accent
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
def strip_accents(s: str) -> str:
    """Supprime les accents et met en majuscules."""
    return (
        unicodedata.normalize("NFD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .upper()
        .strip()
    )

# Mapping normalisé (sans accents) pour la recherche robuste
mapping_norm = {strip_accents(k): v for k, v in mapping_candidat_parti.items()}

def get_parti(nom):
    """Recherche le parti d'un candidat en normalisant les accents."""
    if pd.isna(nom):
        return "DIVERS"
    return mapping_norm.get(strip_accents(str(nom)), "DIVERS")

# ---- CONSTRUCTION DU CODE GEO INSEE ----
# code_commune zero-paddé sur 5 chiffres (ex: "00001")
# INSEE : code_dept (2 chiffres) + code_commune (3 derniers chiffres) → "59001"
def build_code_geo(code_dept, code_commune):
    dept      = str(int(float(str(code_dept)))).zfill(2)
    commune_3 = str(code_commune).zfill(5)[-3:]
    return dept + commune_3

# ============================================================
# FONCTION PRINCIPALE
# ============================================================
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
        print(f"⚠️  Candidats non mappés → DIVERS : {list(inconnus)}")

    # ---- CODE GEO INSEE ----
    df["code_geo"] = df.apply(
        lambda r: build_code_geo(r["code_departement"], r["code_commune"]),
        axis=1
    )

    # ---- INFOS FIXES PAR COMMUNE ----
    cols_commune = [
        "code_geo", "code_commune", "libelle_commune",
        "inscrits", "abstentions", "votants",
        "exprimes", "pct_abstention", "annee"
    ]
    df_commune = df[cols_commune].drop_duplicates(subset=["code_commune"])

    # ---- TOP 5 PAR COMMUNE (trié par voix décroissant) ----
    df_sorted = df.sort_values(["code_commune", "voix"], ascending=[True, False])

    df_top5 = (
        df_sorted
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

    # ---- AGRÉGATION PAR BLOC (pour pct_GAUCHE, pct_CENTRE, etc.) ----
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

    # ---- PCT PAR BLOC (recalcul sur les exprimés) ----
    cols_voix_blocs = [c for c in df_final.columns if c.startswith("voix_")]
    for col in cols_voix_blocs:
        pct_col = col.replace("voix_", "pct_")
        df_final[pct_col] = (df_final[col] / df_final["exprimes"] * 100).round(2)

    # Supprimer les colonnes voix_ (inutiles pour le ML)
    df_final = df_final.drop(columns=cols_voix_blocs)

    # Remplir NaN des blocs absents par 0 (commune sans vote CENTRE par ex.)
    cols_pct_blocs = [c for c in df_final.columns
                      if c in ["pct_GAUCHE", "pct_CENTRE", "pct_DROITE", "pct_DIVERS"]]
    df_final[cols_pct_blocs] = df_final[cols_pct_blocs].fillna(0)

    # ============================================================
    # ✅ CORRECTIF BLOC_VAINQUEUR
    # Ancienne logique (BUGGUÉE) :
    #   idxmax sur les pct par bloc cumulés → DROITE gagnait toujours
    #   car LE PEN + FILLON + DU PONT-AIGNAN > MACRON seul
    #
    # Nouvelle logique (CORRECTE) :
    #   Le bloc vainqueur = bloc du CANDIDAT arrivé 1er dans la commune
    #   C'est la définition électorale standard
    # ============================================================
    df_vainqueur = (
        df.sort_values("voix", ascending=False)   # trier par voix décroissant
        .groupby("code_commune")                  # 1 ligne par commune
        .first()                                  # → candidat avec le plus de voix
        .reset_index()[["code_commune", "nom", "bloc"]]
        .rename(columns={
            "bloc": "bloc_vainqueur",
            "nom":  "nom_vainqueur"
        })
    )

    # Supprimer l'ancienne colonne si elle existe, puis fusionner
    df_final = df_final.drop(columns=["bloc_vainqueur", "nom_vainqueur"], errors="ignore")
    df_final = df_final.merge(df_vainqueur, on="code_commune", how="left")

    # ---- ORDRE FINAL DES COLONNES ----
    cols_ids           = ["code_geo", "code_commune", "libelle_commune", "annee"]
    cols_participation = ["inscrits", "abstentions", "votants", "exprimes", "pct_abstention"]
    cols_top5_nom      = sorted([c for c in df_final.columns if c.startswith("nom_cand")])
    cols_top5_pct      = sorted([c for c in df_final.columns if c.startswith("pct_cand")])
    cols_top5_bloc     = sorted([c for c in df_final.columns if c.startswith("bloc_cand")])
    cols_blocs_pct     = sorted(cols_pct_blocs)
    cols_cible         = ["nom_vainqueur", "bloc_vainqueur"]

    col_order = (
        cols_ids + cols_participation +
        cols_top5_nom + cols_top5_pct + cols_top5_bloc +
        cols_blocs_pct + cols_cible
    )
    # Colonnes existantes uniquement
    col_order = [c for c in col_order if c in df_final.columns]
    df_final  = df_final[col_order]

    # ---- RÉSUMÉ ----
    print(f"\n📊 Résumé {annee} :")
    print(f"   Communes       : {len(df_final)}")
    print(f"   Colonnes       : {len(df_final.columns)}")
    print(f"   Valeurs NaN    : {df_final.isnull().sum().sum()}")
    print(f"\n🏆 Blocs vainqueurs :")
    counts = df_final["bloc_vainqueur"].value_counts()
    pcts   = df_final["bloc_vainqueur"].value_counts(normalize=True) * 100
    for bloc in counts.index:
        print(f"   {bloc:<10} : {counts[bloc]:>4} communes  ({pcts[bloc]:.1f}%)")

    # ---- VÉRIFICATION CROISÉE ----
    # Comparer communes gagnées vs cumul voix pour détecter tout résidu
    print(f"\n🔍 Top 5 candidats les plus souvent vainqueurs :")
    print(df_final["nom_vainqueur"].value_counts().head().to_string())

    return df_final


# ============================================================
# TRAITEMENT
# ============================================================
df_2017 = pivoter_top5(2017)
df_2022 = pivoter_top5(2022)

# ---- SAUVEGARDE ----
out_2017 = os.path.join(FINAL_PATH, "elections_2017_pivot.csv")
out_2022 = os.path.join(FINAL_PATH, "elections_2022_pivot.csv")

df_2017.to_csv(out_2017, index=False, encoding="utf-8")
df_2022.to_csv(out_2022, index=False, encoding="utf-8")

print(f"\n✅ Fichiers sauvegardés :")
print(f"   → {out_2017}  ({len(df_2017)} communes)")
print(f"   → {out_2022}  ({len(df_2022)} communes)")
print(f"\n⚠️  N'oublie pas de relancer build_dataset.py après ce correctif !")