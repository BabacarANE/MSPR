import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.feature_selection import mutual_info_classif
import matplotlib.pyplot as plt
import seaborn as sns
import joblib

# ============================================================
# preprocessing.py
# Prépare le dataset ML final pour l'entraînement des modèles
#
# Étapes :
#   1. Chargement
#   2. Encodage des variables catégorielles
#   3. Transformations (log)
#   4. Construction de la variable cible enrichie
#   5. Normalisation (StandardScaler)
#   6. Sélection de features
#   7. Sauvegarde du dataset preprocessé + scalers
# ============================================================

FINAL_PATH  = "data/final"
MODELS_PATH = "data/models"
os.makedirs(MODELS_PATH, exist_ok=True)

# ============================================================
# ÉTAPE 1 — CHARGEMENT
# ============================================================
print("=" * 60)
print("📂 ÉTAPE 1 — Chargement")
print("=" * 60)

df = pd.read_csv(
    os.path.join(FINAL_PATH, "dataset_ml_final.csv"),
    dtype={"code_geo": str, "code_commune": str}
)

print(f"✅ Dataset chargé : {df.shape[0]} lignes × {df.shape[1]} colonnes")

# Séparer 2017 (train) et 2022 (test) dès le début
df_2017 = df[df["annee"] == 2017].copy().reset_index(drop=True)
df_2022 = df[df["annee"] == 2022].copy().reset_index(drop=True)

print(f"   Train (2017) : {len(df_2017)} communes")
print(f"   Test  (2022) : {len(df_2022)} communes")

# ============================================================
# ÉTAPE 2 — ENCODAGE DES VARIABLES CATÉGORIELLES
# ============================================================
print("\n" + "=" * 60)
print("🔤 ÉTAPE 2 — Encodage")
print("=" * 60)

# ---- 2A : Encodage des blocs candidats (bloc_cand1..5) ----
# Mapping explicite pour garder la sémantique
mapping_bloc = {
    "DROITE":  0,
    "CENTRE":  1,
    "GAUCHE":  2,
    "DIVERS":  3,
    "ABSENT":  -1   # candidat absent (NaN structurel)
}

cols_bloc_cand = [c for c in df.columns if c.startswith("bloc_cand")]
print(f"\n📋 Encodage blocs candidats : {cols_bloc_cand}")

for col in cols_bloc_cand:
    col_enc = col + "_enc"
    df[col_enc] = df[col].map(mapping_bloc).fillna(-1).astype(int)
    print(f"   {col:<15} → {col_enc} | valeurs : {sorted(df[col_enc].unique())}")

# ---- 2B : Encodage de la variable cible multi-classe ----
# bloc_vainqueur → 4 classes
mapping_cible = {
    "DROITE": 0,
    "CENTRE": 1,
    "GAUCHE": 2,
    "DIVERS": 3
}
df["cible_multiclasse"] = df["bloc_vainqueur"].map(mapping_cible)
print(f"\n📋 Encodage cible multiclasse :")
print(df["cible_multiclasse"].value_counts().sort_index().to_string())

# ---- 2C : Nom vainqueur → encodage (pour info, pas feature ML) ----
le_nom = LabelEncoder()
df["nom_vainqueur_enc"] = le_nom.fit_transform(df["nom_vainqueur"].fillna("INCONNU"))
print(f"\n📋 Candidats vainqueurs encodés : {dict(zip(le_nom.classes_, le_nom.transform(le_ns.classes_)))}" if False else "")
print(f"📋 LabelEncoder nom_vainqueur   : {list(le_nom.classes_)}")

# Sauvegarder le LabelEncoder pour décodage futur
joblib.dump(le_nom, os.path.join(MODELS_PATH, "le_nom_vainqueur.pkl"))

# ============================================================
# ÉTAPE 3 — TRANSFORMATIONS
# ============================================================
print("\n" + "=" * 60)
print("🔧 ÉTAPE 3 — Transformations")
print("=" * 60)

# ---- 3A : Log-transform population (distribution très asymétrique) ----
df["log_population"] = np.log1p(df["population_municipale"])
print(f"\n📋 log_population :")
print(f"   Avant : min={df['population_municipale'].min():,}  max={df['population_municipale'].max():,}  std={df['population_municipale'].std():,.0f}")
print(f"   Après : min={df['log_population'].min():.2f}  max={df['log_population'].max():.2f}  std={df['log_population'].std():.2f}")

# ---- 3B : Log-transform délinquance (outliers 800‰) ----
# Plafonner d'abord au 95e percentile par année, puis log
p95 = df.groupby("annee")["taux_delinquance"].transform(lambda x: x.quantile(0.95))
df["taux_delinquance_cap"] = df["taux_delinquance"].clip(upper=p95)
df["log_delinquance"] = np.log1p(df["taux_delinquance_cap"])

print(f"\n📋 log_delinquance (après cap au P95) :")
print(f"   Avant cap : max={df['taux_delinquance'].max():.1f}‰")
print(f"   Après cap : max={df['taux_delinquance_cap'].max():.1f}‰")
print(f"   Log       : max={df['log_delinquance'].max():.2f}  std={df['log_delinquance'].std():.2f}")

# ---- 3C : Vérification distributions après transformation ----
fig, axes = plt.subplots(2, 2, figsize=(12, 8))

axes[0, 0].hist(df["population_municipale"], bins=50, color="#3498db", alpha=0.7)
axes[0, 0].set_title("Population — AVANT log")

axes[0, 1].hist(df["log_population"], bins=50, color="#2ecc71", alpha=0.7)
axes[0, 1].set_title("Population — APRÈS log")

axes[1, 0].hist(df["taux_delinquance"], bins=50, color="#e74c3c", alpha=0.7)
axes[1, 0].set_title("Délinquance — AVANT log+cap")

axes[1, 1].hist(df["log_delinquance"], bins=50, color="#f39c12", alpha=0.7)
axes[1, 1].set_title("Délinquance — APRÈS log+cap")

plt.suptitle("Effet des transformations log", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig(os.path.join(MODELS_PATH, "transformations_log.png"), dpi=120)
plt.show()
print("✅ Graphique sauvegardé → data/models/transformations_log.png")

# ============================================================
# ÉTAPE 4 — CONSTRUCTION DE LA VARIABLE CIBLE ENRICHIE
# ============================================================
print("\n" + "=" * 60)
print("🎯 ÉTAPE 4 — Variable cible enrichie")
print("=" * 60)

# ---- Matrice de transition pour construire la cible ----
df_2017_tmp = df[df["annee"] == 2017][["code_geo", "bloc_vainqueur"]].rename(
    columns={"bloc_vainqueur": "bloc_2017"}
)
df_2022_tmp = df[df["annee"] == 2022][["code_geo", "bloc_vainqueur"]].rename(
    columns={"bloc_vainqueur": "bloc_2022"}
)
df_transition = df_2017_tmp.merge(df_2022_tmp, on="code_geo")
df_transition["transition"] = df_transition["bloc_2017"] + "→" + df_transition["bloc_2022"]

print("\n📊 Transitions détectées :")
print(df_transition["transition"].value_counts().to_string())

# ---- Cible enrichie : STABLE_DROITE / BASCULE_CENTRE / AUTRE ----
# Basée sur les communes 2017 uniquement (ce qu'on veut prédire)
def encoder_cible_transition(row, df_trans):
    """Pour une commune en 2017, quelle va être sa transition ?"""
    match = df_trans[df_trans["code_geo"] == row["code_geo"]]
    if len(match) == 0:
        return "INCONNU"
    trans = match.iloc[0]["transition"]
    if trans == "DROITE→DROITE":
        return "STABLE_DROITE"
    elif trans == "DROITE→CENTRE":
        return "BASCULE_CENTRE"
    elif trans in ["CENTRE→CENTRE", "CENTRE→DROITE"]:
        return "CENTRE"
    elif trans in ["GAUCHE→GAUCHE", "GAUCHE→DROITE"]:
        return "GAUCHE"
    else:
        return "AUTRE"

# Ajouter la cible transition uniquement sur les lignes 2017
df_2017_enrichi = df[df["annee"] == 2017].copy()
df_2017_enrichi["cible_transition"] = df_2017_enrichi.apply(
    lambda r: encoder_cible_transition(r, df_transition), axis=1
)

print(f"\n📊 Distribution cible_transition (dataset 2017 = train) :")
counts = df_2017_enrichi["cible_transition"].value_counts()
for label, count in counts.items():
    print(f"   {label:<20} : {count:>4} communes  ({count/len(df_2017_enrichi)*100:.1f}%)")

# Mapping numérique pour le modèle
mapping_transition = {
    "STABLE_DROITE":   0,
    "BASCULE_CENTRE":  1,
    "CENTRE":          2,
    "GAUCHE":          3,
    "AUTRE":           4,
    "INCONNU":        -1
}
df_2017_enrichi["cible_transition_enc"] = df_2017_enrichi["cible_transition"].map(mapping_transition)

# ============================================================
# ÉTAPE 5 — NORMALISATION (StandardScaler)
# ============================================================
print("\n" + "=" * 60)
print("📏 ÉTAPE 5 — Normalisation")
print("=" * 60)

# Features numériques à normaliser
features_a_normaliser = [
    "taux_chomage",
    "revenu_median",
    "taux_faible_diplome",
    "log_delinquance",
    "log_population",
    "pct_abstention",
]

print(f"\n📋 Features normalisées : {features_a_normaliser}")

# Scaler entraîné UNIQUEMENT sur les données 2017 (train)
# → appliqué sur 2022 (test) sans re-entraîner
df_train = df[df["annee"] == 2017][features_a_normaliser].copy()
df_test  = df[df["annee"] == 2022][features_a_normaliser].copy()

scaler = StandardScaler()
scaler.fit(df_train)  # ← FIT sur train uniquement

# Créer les colonnes normalisées dans df complet
for feat in features_a_normaliser:
    col_scaled = feat + "_scaled"
    df.loc[df["annee"] == 2017, col_scaled] = scaler.transform(df_train)[:, features_a_normaliser.index(feat)]
    df.loc[df["annee"] == 2022, col_scaled] = scaler.transform(df_test)[:, features_a_normaliser.index(feat)]

# Vérification : moyenne ≈ 0, std ≈ 1 sur train
print(f"\n📊 Vérification normalisation (train 2017) :")
print(f"   {'Feature':<35} {'Moy':>8} {'Std':>8}")
print("-" * 55)
for feat in features_a_normaliser:
    col_scaled = feat + "_scaled"
    sub = df[df["annee"] == 2017][col_scaled]
    print(f"   {col_scaled:<35} {sub.mean():>8.3f} {sub.std():>8.3f}")

# Sauvegarder le scaler
joblib.dump(scaler, os.path.join(MODELS_PATH, "scaler.pkl"))
print(f"\n✅ Scaler sauvegardé → data/models/scaler.pkl")

# ============================================================
# ÉTAPE 6 — SÉLECTION DE FEATURES
# ============================================================
print("\n" + "=" * 60)
print("🔍 ÉTAPE 6 — Sélection de features")
print("=" * 60)

# Features candidates pour le ML
features_ml_candidates = (
    [f + "_scaled" for f in features_a_normaliser] +
    [c + "_enc" for c in cols_bloc_cand]
)
features_ml_candidates = [f for f in features_ml_candidates if f in df.columns]

print(f"\n📋 Features candidates ({len(features_ml_candidates)}) :")
for f in features_ml_candidates:
    print(f"   - {f}")

# ---- Mutual Information avec cible_binaire (sur 2017) ----
df_train_mi = df[df["annee"] == 2017][features_ml_candidates + ["cible_binaire"]].dropna()
X_mi = df_train_mi[features_ml_candidates]
y_mi = df_train_mi["cible_binaire"]

mi_scores = mutual_info_classif(X_mi, y_mi, random_state=42)
mi_df = pd.DataFrame({
    "feature":   features_ml_candidates,
    "mi_score":  mi_scores
}).sort_values("mi_score", ascending=False)

print(f"\n📊 Scores Mutual Information (feature ↔ cible_binaire) :")
for _, row in mi_df.iterrows():
    barre = "█" * int(row["mi_score"] * 100)
    print(f"   {row['feature']:<40} : {row['mi_score']:.4f}  {barre}")

# Visualisation MI scores
fig, ax = plt.subplots(figsize=(10, 6))
colors = ["#e74c3c" if s > 0.02 else "#95a5a6" for s in mi_df["mi_score"]]
ax.barh(mi_df["feature"], mi_df["mi_score"], color=colors)
ax.axvline(x=0.02, color="red", linestyle="--", alpha=0.5, label="Seuil = 0.02")
ax.set_xlabel("Mutual Information Score")
ax.set_title("Importance des features (Mutual Information)", fontweight="bold")
ax.legend()
plt.tight_layout()
plt.savefig(os.path.join(MODELS_PATH, "feature_importance_mi.png"), dpi=120)
plt.show()
print("✅ Graphique sauvegardé → data/models/feature_importance_mi.png")

# Features retenues (MI > seuil)
SEUIL_MI = 0.02
features_retenues = mi_df[mi_df["mi_score"] >= SEUIL_MI]["feature"].tolist()
features_eliminées = mi_df[mi_df["mi_score"] < SEUIL_MI]["feature"].tolist()

print(f"\n✅ Features retenues (MI >= {SEUIL_MI}) : {len(features_retenues)}")
for f in features_retenues:
    print(f"   ✅ {f}")

if features_eliminées:
    print(f"\n❌ Features éliminées (MI < {SEUIL_MI}) : {len(features_eliminées)}")
    for f in features_eliminées:
        print(f"   ❌ {f}")

# ============================================================
# ÉTAPE 7 — CONSTRUCTION DES DEUX VERSIONS DE DATASET
# ============================================================
print("\n" + "=" * 60)
print("💾 ÉTAPE 7 — Construction des deux versions de dataset")
print("=" * 60)

# ----------------------------------------------------------------
# VERSION A — Avec bloc_cand (résultats électoraux passés inclus)
# Fort pouvoir prédictif MAIS risque de tautologie pour 2027
# Usage : mesurer le plafond théorique du modèle
# ----------------------------------------------------------------
features_A = [
    "bloc_cand1_enc",             # bloc candidat arrivé 1er
    "bloc_cand2_enc",             # bloc candidat arrivé 2e
    "bloc_cand3_enc",             # bloc candidat arrivé 3e
    "bloc_cand5_enc",             # bloc candidat arrivé 5e
    "taux_faible_diplome_scaled", # meilleure feature socio-éco
    "revenu_median_scaled",       # richesse relative
    "log_delinquance_scaled",     # insécurité perçue
]

# ----------------------------------------------------------------
# VERSION B — Sans bloc_cand (purement socio-économique)
# Pouvoir prédictif plus faible MAIS réellement utilisable en 2027
# Usage : prédiction prospective réelle
# ----------------------------------------------------------------
# ⚠️  taux_chomage SUPPRIMÉ car corrélé à -0.69 avec revenu_median
#     → multicolinéarité nuisible à la Régression Logistique
#     → revenu_median conservé : signal cible 6x plus fort
#     ⚠️  inscrits ABSENT car quasi-identique à population (r=0.99)
features_B = [
    "taux_faible_diplome_scaled", # clivage éducatif — signal le plus fort (r=0.66 en 2022)
    "revenu_median_scaled",       # niveau de vie — signal fort (r=0.57 en 2022)
    "log_delinquance_scaled",     # insécurité perçue
    "log_population_scaled",      # taille commune
    "pct_abstention_scaled",      # désengagement civique
]

print(f"\n📋 Version A — Avec historique électoral : {len(features_A)} features")
for f in features_A:
    print(f"   - {f}")

print(f"\n📋 Version B — Purement socio-éco : {len(features_B)} features")
for f in features_B:
    print(f"   - {f}")

# Colonnes identifiants
cols_ids = ["code_geo", "libelle_commune", "annee",
            "bloc_vainqueur", "cible_binaire", "cible_multiclasse"]

# ---- Vérification que toutes les features existent ----
for version, feats in [("A", features_A), ("B", features_B)]:
    manquantes = [f for f in feats if f not in df.columns]
    if manquantes:
        print(f"⚠️  Version {version} — features manquantes : {manquantes}")
    else:
        print(f"✅ Version {version} — toutes les features présentes")

# ---- Construction des 4 fichiers (train/test × A/B) ----
datasets = {}

for version, feats in [("A", features_A), ("B", features_B)]:

    # Train 2017
    df_train = df[df["annee"] == 2017][cols_ids + feats].copy()
    df_train = df_train.merge(
        df_2017_enrichi[["code_geo", "cible_transition", "cible_transition_enc"]],
        on="code_geo", how="left"
    )

    # Test 2022
    df_test = df[df["annee"] == 2022][cols_ids + feats].copy()

    datasets[f"train_{version}"] = df_train
    datasets[f"test_{version}"]  = df_test

    # Sauvegarde
    path_train = os.path.join(FINAL_PATH, f"ml_train_2017_v{version}.csv")
    path_test  = os.path.join(FINAL_PATH, f"ml_test_2022_v{version}.csv")

    df_train.to_csv(path_train, index=False, encoding="utf-8")
    df_test.to_csv(path_test,   index=False, encoding="utf-8")

    print(f"\n   Version {version} :")
    print(f"   → {path_train}  ({len(df_train)} lignes × {len(feats)} features)")
    print(f"   → {path_test}   ({len(df_test)} lignes × {len(feats)} features)")

# ---- Sauvegarde des métadonnées ----
import json

features_info = {
    "version_A": {
        "features":     features_A,
        "description":  "Avec historique électoral (bloc_cand) — plafond théorique",
        "usage":        "Comparer les modèles, mesurer l'apport des données électorales"
    },
    "version_B": {
        "features":     features_B,
        "description":  "Purement socio-économique — prédiction prospective",
        "usage":        "Prédiction 2027 à partir des conditions socio-éco actuelles"
    },
    "mapping_bloc":       mapping_bloc,
    "mapping_cible":      mapping_cible,
    "mapping_transition": mapping_transition,
}

with open(os.path.join(MODELS_PATH, "features_info.json"), "w", encoding="utf-8") as f:
    json.dump(features_info, f, indent=2, ensure_ascii=False)

joblib.dump(scaler, os.path.join(MODELS_PATH, "scaler.pkl"))

print(f"\n✅ Métadonnées sauvegardées → data/models/features_info.json")
print(f"✅ Scaler sauvegardé        → data/models/scaler.pkl")

# ============================================================
# BILAN FINAL
# ============================================================
print("\n" + "=" * 60)
print("📋 BILAN PREPROCESSING")
print("=" * 60)
print(f"""
✅ Encodage
   bloc_cand1..5    → entiers (DROITE=0, CENTRE=1, GAUCHE=2, DIVERS=3, ABSENT=-1)
   bloc_vainqueur   → cible_binaire (0/1) + cible_multiclasse (0-3)
   cible_transition → STABLE_DROITE=0, BASCULE_CENTRE=1, CENTRE=2, GAUCHE=3

✅ Transformations
   population       → log1p (distribution normalisée)
   delinquance      → cap P95 + log1p (outliers neutralisés)

✅ Normalisation
   {len(features_a_normaliser)} features → StandardScaler (fit sur 2017 uniquement)
   Moyenne ≈ 0, Std ≈ 1 sur train

✅ Deux versions de dataset
   Version A — {len(features_A)} features (avec historique électoral)
     → Usage : mesurer le plafond théorique du modèle
     → Accuracy attendue : élevée (~85-95%)

   Version B — {len(features_B)} features (purement socio-éco)
     → Usage : prédiction prospective 2027
     → Accuracy attendue : modérée (~65-80%)

✅ Fichiers produits
   ml_train_2017_vA.csv / ml_test_2022_vA.csv
   ml_train_2017_vB.csv / ml_test_2022_vB.csv

📊 Ce que la comparaison A vs B va démontrer
   Écart A-B = apport prédictif des données électorales passées
   Si écart > 15pts → le passé électoral prime sur le socio-éco
   Si écart < 10pts → les conditions socio-éco suffisent à prédire

🚀 Prochaine étape : model_binaire.py + model_multiclasse.py
""")