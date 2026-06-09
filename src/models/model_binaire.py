import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import os

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.metrics import (
    accuracy_score, f1_score, roc_auc_score,
    confusion_matrix, classification_report,
    RocCurveDisplay
)

# ============================================================
# model_binaire.py
# Régression Logistique — Prédiction cible_binaire
# (0 = DROITE domine / 1 = EXCEPTION : CENTRE ou GAUCHE)
#
# Comparaison Version A (avec historique électoral)
#          vs Version B (purement socio-éco)
# ============================================================

FINAL_PATH  = "data/final"
MODELS_PATH = "data/models"
os.makedirs(MODELS_PATH, exist_ok=True)

sns.set_theme(style="whitegrid", palette="muted")

# ============================================================
# ÉTAPE 1 — CHARGEMENT DES DATASETS
# ============================================================
print("=" * 60)
print("📂 ÉTAPE 1 — Chargement")
print("=" * 60)

datasets = {}
for version in ["A", "B"]:
    train = pd.read_csv(
        os.path.join(FINAL_PATH, f"ml_train_2017_v{version}.csv"),
        dtype={"code_geo": str}
    )
    test = pd.read_csv(
        os.path.join(FINAL_PATH, f"ml_test_2022_v{version}.csv"),
        dtype={"code_geo": str}
    )
    datasets[version] = {"train": train, "test": test}
    cols_cibles_affichage = ["cible_binaire", "cible_multiclasse", "cible_transition_enc"]
    features_affichage = [c for c in train.columns
                          if (c.endswith("_scaled") or c.endswith("_enc"))
                          and c not in cols_cibles_affichage]
    print(f"\n   Version {version} :")
    print(f"   Train : {len(train)} lignes | Features : {features_affichage}")
    print(f"   Test  : {len(test)} lignes")

# ============================================================
# ÉTAPE 2 — ENTRAÎNEMENT ET CROSS-VALIDATION
# ============================================================
print("\n" + "=" * 60)
print("🤖 ÉTAPE 2 — Entraînement (K-Fold k=5 stratifié)")
print("=" * 60)

resultats = {}

for version in ["A", "B"]:
    print(f"\n{'─'*40}")
    print(f"   VERSION {version}")
    print(f"{'─'*40}")

    train = datasets[version]["train"]
    test  = datasets[version]["test"]

    # Features et cible
    # ⚠️ Exclure les colonnes cibles qui finissent aussi en _enc
    cols_cibles = ["cible_binaire", "cible_multiclasse", "cible_transition_enc"]
    features = [c for c in train.columns
                if (c.endswith("_scaled") or c.endswith("_enc"))
                and c not in cols_cibles]
    X_train = train[features].values
    y_train = train["cible_binaire"].values
    X_test  = test[features].values
    y_test  = test["cible_binaire"].values

    print(f"\n   Features utilisées ({len(features)}) : {features}")
    print(f"\n   Distribution train : "
          f"DROITE={( y_train==0).sum()} / EXCEPTION={(y_train==1).sum()}")
    print(f"   Distribution test  : "
          f"DROITE={(y_test==0).sum()} / EXCEPTION={(y_test==1).sum()}")

    # ---- Modèle ----
    # class_weight='balanced' : compense le déséquilibre DROITE/EXCEPTION
    # C=0.1 : régularisation renforcée pour gérer la multicolinéarité
    # max_iter=1000 : évite les warnings de convergence
    model = LogisticRegression(
        class_weight="balanced",
        C=0.1,
        max_iter=1000,
        random_state=42,
        solver="lbfgs"
    )

    # ---- Cross-validation k=5 stratifié ----
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_results = cross_validate(
        model, X_train, y_train,
        cv=cv,
        scoring=["accuracy", "f1", "roc_auc"],
        return_train_score=True
    )

    print(f"\n   📊 Cross-validation (k=5) :")
    print(f"   {'Métrique':<20} {'Train':>10} {'Val':>10} {'Écart':>10}")
    print(f"   {'-'*52}")
    for metric in ["accuracy", "f1", "roc_auc"]:
        train_score = cv_results[f"train_{metric}"].mean()
        val_score   = cv_results[f"test_{metric}"].mean()
        ecart       = train_score - val_score
        overfit     = "⚠️" if ecart > 0.1 else "✅"
        print(f"   {metric:<20} {train_score:>10.3f} {val_score:>10.3f} {ecart:>+9.3f} {overfit}")

    # ---- Entraînement final sur tout le train ----
    model.fit(X_train, y_train)

    # ---- Évaluation sur le test 2022 ----
    y_pred      = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    acc   = accuracy_score(y_test, y_pred)
    f1    = f1_score(y_test, y_pred, average="binary")
    auc   = roc_auc_score(y_test, y_pred_proba)
    cm    = confusion_matrix(y_test, y_pred)

    print(f"\n   📊 Évaluation sur test 2022 :")
    print(f"   Accuracy  : {acc:.3f}  ({acc*100:.1f}%)")
    print(f"   F1 Score  : {f1:.3f}")
    print(f"   AUC-ROC   : {auc:.3f}")
    print(f"\n   Rapport de classification :")
    print(classification_report(y_test, y_pred,
                                 target_names=["DROITE", "EXCEPTION"],
                                 digits=3))

    # ---- Coefficients ----
    coefs = pd.DataFrame({
        "feature":     features,
        "coefficient": model.coef_[0]
    }).sort_values("coefficient", ascending=False)

    print(f"   Coefficients (impact sur P(EXCEPTION)) :")
    for _, row in coefs.iterrows():
        signe = "↑" if row["coefficient"] > 0 else "↓"
        barre = "█" * int(abs(row["coefficient"]) * 5)
        print(f"   {signe} {row['feature']:<40} : {row['coefficient']:+.3f}  {barre}")

    # Sauvegarder
    resultats[version] = {
        "model":        model,
        "features":     features,
        "cv_results":   cv_results,
        "acc":          acc,
        "f1":           f1,
        "auc":          auc,
        "cm":           cm,
        "y_test":       y_test,
        "y_pred":       y_pred,
        "y_pred_proba": y_pred_proba,
        "coefs":        coefs,
        "X_train":      X_train,
        "y_train":      y_train,
        "X_test":       X_test,
        "test_df":      test,
    }

    # Sauvegarder le modèle
    joblib.dump(model, os.path.join(MODELS_PATH, f"model_binaire_v{version}.pkl"))
    print(f"\n   ✅ Modèle sauvegardé → data/models/model_binaire_v{version}.pkl")

# ============================================================
# ÉTAPE 3 — COMPARAISON A vs B
# ============================================================
print("\n" + "=" * 60)
print("📊 ÉTAPE 3 — Comparaison Version A vs Version B")
print("=" * 60)

print(f"\n   {'Métrique':<15} {'Version A':>12} {'Version B':>12} {'Écart A-B':>12}")
print(f"   {'-'*55}")
for metric, key in [("Accuracy", "acc"), ("F1 Score", "f1"), ("AUC-ROC", "auc")]:
    va = resultats["A"][key]
    vb = resultats["B"][key]
    ecart = va - vb
    print(f"   {metric:<15} {va:>12.3f} {vb:>12.3f} {ecart:>+11.3f}")

print(f"""
   Interprétation :
   → L'écart A-B mesure l'apport prédictif de l'historique électoral
   → Si écart > 0.15 : le passé électoral est indispensable
   → Si écart < 0.10 : les conditions socio-éco suffisent à prédire
""")

# ============================================================
# ÉTAPE 4 — VISUALISATIONS
# ============================================================
print("=" * 60)
print("📊 ÉTAPE 4 — Visualisations")
print("=" * 60)

fig = plt.figure(figsize=(18, 14))

# ---- 4A : Matrices de confusion ----
for i, version in enumerate(["A", "B"]):
    ax = fig.add_subplot(3, 3, i + 1)
    cm = resultats[version]["cm"]
    cm_pct = cm.astype(float) / cm.sum(axis=1, keepdims=True) * 100
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", ax=ax,
                xticklabels=["DROITE", "EXCEPTION"],
                yticklabels=["DROITE", "EXCEPTION"],
                linewidths=1)
    # Ajouter les pourcentages
    for row in range(2):
        for col in range(2):
            ax.text(col + 0.5, row + 0.75, f"({cm_pct[row,col]:.1f}%)",
                    ha="center", va="center", fontsize=8, color="gray")
    acc = resultats[version]["acc"]
    ax.set_title(f"Matrice confusion — Version {version}\nAccuracy={acc:.1%}", fontweight="bold")
    ax.set_xlabel("Prédit")
    ax.set_ylabel("Réel")

# ---- 4B : Courbes ROC ----
ax_roc = fig.add_subplot(3, 3, 3)
colors = {"A": "#e74c3c", "B": "#3498db"}
for version in ["A", "B"]:
    RocCurveDisplay.from_predictions(
        resultats[version]["y_test"],
        resultats[version]["y_pred_proba"],
        name=f"Version {version} (AUC={resultats[version]['auc']:.3f})",
        color=colors[version],
        ax=ax_roc
    )
ax_roc.set_title("Courbes ROC — A vs B", fontweight="bold")
ax_roc.plot([0, 1], [0, 1], "k--", alpha=0.3, label="Aléatoire")
ax_roc.legend(loc="lower right")

# ---- 4C : Coefficients Version A ----
ax_coef_a = fig.add_subplot(3, 3, 4)
coefs_a = resultats["A"]["coefs"]
colors_a = ["#e74c3c" if c > 0 else "#3498db" for c in coefs_a["coefficient"]]
ax_coef_a.barh(coefs_a["feature"], coefs_a["coefficient"], color=colors_a)
ax_coef_a.axvline(x=0, color="black", linewidth=0.8)
ax_coef_a.set_title("Coefficients — Version A", fontweight="bold")
ax_coef_a.set_xlabel("Impact sur P(EXCEPTION)")

# ---- 4D : Coefficients Version B ----
ax_coef_b = fig.add_subplot(3, 3, 5)
coefs_b = resultats["B"]["coefs"]
colors_b = ["#e74c3c" if c > 0 else "#3498db" for c in coefs_b["coefficient"]]
ax_coef_b.barh(coefs_b["feature"], coefs_b["coefficient"], color=colors_b)
ax_coef_b.axvline(x=0, color="black", linewidth=0.8)
ax_coef_b.set_title("Coefficients — Version B\n(prédiction 2027)", fontweight="bold")
ax_coef_b.set_xlabel("Impact sur P(EXCEPTION)")

# ---- 4E : Comparaison métriques ----
ax_comp = fig.add_subplot(3, 3, 6)
metriques = ["Accuracy", "F1 Score", "AUC-ROC"]
vals_A = [resultats["A"]["acc"], resultats["A"]["f1"], resultats["A"]["auc"]]
vals_B = [resultats["B"]["acc"], resultats["B"]["f1"], resultats["B"]["auc"]]
x = np.arange(len(metriques))
w = 0.35
bars_a = ax_comp.bar(x - w/2, vals_A, w, label="Version A", color="#e74c3c", alpha=0.8)
bars_b = ax_comp.bar(x + w/2, vals_B, w, label="Version B", color="#3498db", alpha=0.8)
for bar in bars_a:
    ax_comp.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
                 f"{bar.get_height():.3f}", ha="center", va="bottom", fontsize=9)
for bar in bars_b:
    ax_comp.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
                 f"{bar.get_height():.3f}", ha="center", va="bottom", fontsize=9)
ax_comp.set_xticks(x)
ax_comp.set_xticklabels(metriques)
ax_comp.set_ylim(0, 1.1)
ax_comp.set_title("Comparaison A vs B", fontweight="bold")
ax_comp.legend()
ax_comp.axhline(y=0.5, color="gray", linestyle="--", alpha=0.3, label="Aléatoire")

# ---- 4F : Probabilités de bascule par commune (Version B) ----
ax_proba = fig.add_subplot(3, 1, 3)
test_df = resultats["B"]["test_df"].copy()
test_df["proba_exception"] = resultats["B"]["y_pred_proba"]
test_df["correct"] = (resultats["B"]["y_pred"] == resultats["B"]["y_test"])
test_df_sorted = test_df.sort_values("proba_exception", ascending=False).head(30)

colors_bar = []
for _, row in test_df_sorted.iterrows():
    if row["bloc_vainqueur"] == "DROITE":
        colors_bar.append("#c0392b")
    elif row["bloc_vainqueur"] == "CENTRE":
        colors_bar.append("#f39c12")
    else:
        colors_bar.append("#2980b9")

ax_proba.bar(range(len(test_df_sorted)),
             test_df_sorted["proba_exception"],
             color=colors_bar, alpha=0.8, edgecolor="white")
ax_proba.axhline(y=0.5, color="black", linestyle="--", alpha=0.5, label="Seuil 0.5")
ax_proba.set_xticks(range(len(test_df_sorted)))
ax_proba.set_xticklabels(test_df_sorted["libelle_commune"],
                          rotation=45, ha="right", fontsize=8)
ax_proba.set_ylabel("P(EXCEPTION)")
ax_proba.set_title("Top 30 communes — Probabilité d'exception (Version B, test 2022)\n"
                   "Rouge=DROITE / Orange=CENTRE / Bleu=GAUCHE (résultat réel)",
                   fontweight="bold")
ax_proba.legend()

plt.suptitle("Modèle Binaire — Régression Logistique\n"
             "DROITE (0) vs EXCEPTION (1)",
             fontsize=14, fontweight="bold", y=1.01)
plt.tight_layout()
plt.savefig(os.path.join(MODELS_PATH, "model_binaire_resultats.png"),
            dpi=120, bbox_inches="tight")
plt.show()
print("✅ Graphique sauvegardé → data/models/model_binaire_resultats.png")

# ============================================================
# ÉTAPE 5 — ANALYSE DES ERREURS
# ============================================================
print("\n" + "=" * 60)
print("🔍 ÉTAPE 5 — Analyse des erreurs (Version B)")
print("=" * 60)

test_b  = resultats["B"]["test_df"].copy()
test_b["y_pred"]        = resultats["B"]["y_pred"]
test_b["y_reel"]        = resultats["B"]["y_test"]
test_b["proba_exception"] = resultats["B"]["y_pred_proba"]

# Faux négatifs : EXCEPTION réelle prédite DROITE
faux_negatifs = test_b[(test_b["y_reel"] == 1) & (test_b["y_pred"] == 0)]
# Faux positifs : DROITE réelle prédite EXCEPTION
faux_positifs = test_b[(test_b["y_reel"] == 0) & (test_b["y_pred"] == 1)]

print(f"\n   Faux négatifs (exceptions ratées) : {len(faux_negatifs)}")
print(f"   → Communes qui ont échappé à DROITE mais qu'on n'a pas prédit")
if len(faux_negatifs) > 0:
    print(faux_negatifs[["libelle_commune", "bloc_vainqueur",
                           "proba_exception"]].to_string(index=False))

print(f"\n   Faux positifs (fausses alarmes)   : {len(faux_positifs)}")
print(f"   → Communes prédites EXCEPTION mais restées DROITE")
if len(faux_positifs) > 0:
    print(faux_positifs[["libelle_commune", "bloc_vainqueur",
                          "proba_exception"]].head(10).to_string(index=False))

# ============================================================
# BILAN FINAL
# ============================================================
print("\n" + "=" * 60)
print("📋 BILAN MODÈLE BINAIRE")
print("=" * 60)

for version in ["A", "B"]:
    r = resultats[version]
    cv_acc = r["cv_results"]["test_accuracy"].mean()
    print(f"""
   Version {version} :
   CV Accuracy (k=5)  : {cv_acc:.3f} ({cv_acc*100:.1f}%)
   Test Accuracy 2022 : {r['acc']:.3f} ({r['acc']*100:.1f}%)
   F1 Score           : {r['f1']:.3f}
   AUC-ROC            : {r['auc']:.3f}
""")

ecart_acc = resultats["A"]["acc"] - resultats["B"]["acc"]
ecart_auc = resultats["A"]["auc"] - resultats["B"]["auc"]
print(f"   Écart A-B (Accuracy) : {ecart_acc:+.3f}")
print(f"   Écart A-B (AUC-ROC)  : {ecart_auc:+.3f}")

if ecart_acc > 0.15:
    interpretation = "Le passé électoral est indispensable pour prédire"
elif ecart_acc > 0.08:
    interpretation = "Le passé électoral apporte un signal significatif"
else:
    interpretation = "Les conditions socio-éco suffisent à prédire"

print(f"\n   → Interprétation : {interpretation}")
print(f"\n🚀 Prochaine étape : model_multiclasse.py")