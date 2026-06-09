"""
scripts/run_models.py
=====================
Compare 5 modèles sur Version B (socio-éco pur) en deux modes :
  - Classification BINAIRE   : DROITE vs EXCEPTION (CENTRE + GAUCHE)
  - Classification MULTICLASSE : DROITE / CENTRE / GAUCHE

Modèles comparés :
    1. Logistic Regression
    2. Random Forest
    3. Gradient Boosting
    4. SVM (kernel RBF)
    5. Voting Classifier (LR + RF + GB ensemblés)

Sorties modèles :
    data/models/results_binaire.csv
    data/models/results_multiclasse.csv
    data/models/best_model_binaire_vB.pkl
    data/models/best_model_multiclasse_vB.pkl
    data/models/best_model_meta.json

Sorties graphiques :
    results/figures/01_comparaison_binaire.png
    results/figures/02_comparaison_multiclasse.png
    results/figures/03_matrices_confusion.png
    results/figures/04_courbes_roc.png
    results/figures/05_courbes_apprentissage.png
    results/figures/06_precision_rappel.png
    results/figures/07_cv_folds.png

Exécuter depuis la RACINE :
    python scripts/run_models.py
"""

import warnings
warnings.filterwarnings("ignore")

import os, sys, time, json, pickle
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (
    RandomForestClassifier,
    GradientBoostingClassifier,
    VotingClassifier,
)
from sklearn.svm import SVC
from sklearn.preprocessing import LabelEncoder, label_binarize
from sklearn.model_selection import (
    StratifiedKFold, cross_validate, learning_curve
)
from sklearn.metrics import (
    accuracy_score, f1_score, roc_auc_score, classification_report,
    confusion_matrix, roc_curve, auc,
    precision_recall_curve, ConfusionMatrixDisplay,
)


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# ── Couleurs console ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── Chemins ───────────────────────────────────────────────────────────────────
FINAL_PATH  = Path("data/final")
MODELS_PATH = Path("data/models")
FIG_PATH    = Path("results/figures")          # ← dossier résultats à la racine

MODELS_PATH.mkdir(parents=True, exist_ok=True)
FIG_PATH.mkdir(parents=True, exist_ok=True)

TRAIN_B = FINAL_PATH / "ml_train_2017_vB.csv"
TEST_B  = FINAL_PATH / "ml_test_2022_vB.csv"

FEATURES_B = [
    "revenu_median_scaled",
    "taux_faible_diplome_scaled",
    "pct_abstention_scaled",
    "log_delinquance_scaled",
    "log_population_scaled",
]

# ── Palette ElectioAnalytics ──────────────────────────────────────────────────
PALETTE = {
    "DROITE":   "#E63946",
    "CENTRE":   "#457B9D",
    "GAUCHE":   "#E76F51",
    "EXCEPTION":"#457B9D",
    "lr":       "#2D6A4F",
    "rf":       "#74C69D",
    "gb":       "#F4A261",
    "svm":      "#E63946",
    "vc":       "#6D6875",
    "bg":       "#F8F9FA",
    "grid":     "#DEE2E6",
    "text":     "#212529",
    "accent":   "#1D3557",
}

MODEL_COLORS = [
    PALETTE["lr"], PALETTE["rf"], PALETTE["gb"],
    PALETTE["svm"], PALETTE["vc"],
]

plt.rcParams.update({
    "font.family":        "DejaVu Sans",
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "grid.alpha":         0.4,
    "grid.color":         PALETTE["grid"],
    "figure.facecolor":   "white",
    "axes.facecolor":     "white",
    "font.size":          11,
})


# ══════════════════════════════════════════════════════════════════════════════
# UTILITAIRES CONSOLE
# ══════════════════════════════════════════════════════════════════════════════

def titre(msg):
    print(f"\n{BOLD}{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {msg}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*60}{RESET}")


def section(msg):
    print(f"\n{BOLD}  ── {msg} ──{RESET}")


# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT DES DONNÉES
# ══════════════════════════════════════════════════════════════════════════════

def charger_donnees():
    if not TRAIN_B.exists() or not TEST_B.exists():
        print(f"{RED}❌ Fichiers introuvables. Lance d'abord run_pipeline.py{RESET}")
        sys.exit(1)

    train = pd.read_csv(TRAIN_B)
    test  = pd.read_csv(TEST_B)

    manquantes = [f for f in FEATURES_B if f not in train.columns]
    if manquantes:
        print(f"{RED}❌ Features manquantes : {manquantes}{RESET}")
        print(f"   Colonnes dispo : {list(train.columns)}")
        sys.exit(1)

    X_train = train[FEATURES_B].values
    X_test  = test[FEATURES_B].values

    print(f"  {GREEN}✅ Train : {X_train.shape[0]} communes × {X_train.shape[1]} features{RESET}")
    print(f"  {GREEN}✅ Test  : {X_test.shape[0]} communes × {X_test.shape[1]} features{RESET}")

    return X_train, X_test, train, test


# ══════════════════════════════════════════════════════════════════════════════
# DÉFINITION DES MODÈLES
# ══════════════════════════════════════════════════════════════════════════════

def definir_modeles():
    lr = LogisticRegression(
        C=0.1,
        class_weight="balanced",
        max_iter=1000,
        random_state=42,
        solver="lbfgs",
    )
    rf = RandomForestClassifier(
        n_estimators=300,
        class_weight="balanced",
        max_depth=8,
        min_samples_leaf=3,
        random_state=42,
        n_jobs=-1,
    )
    gb = GradientBoostingClassifier(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=4,
        min_samples_leaf=5,
        subsample=0.8,
        random_state=42,
    )
    svm = SVC(
        kernel="rbf",
        C=1.0,
        gamma="scale",
        class_weight="balanced",
        probability=True,
        random_state=42,
    )
    voting = VotingClassifier(
        estimators=[("lr", lr), ("rf", rf), ("gb", gb)],
        voting="soft",
        n_jobs=-1,
    )

    return {
        "Logistic Regression": lr,
        "Random Forest":       rf,
        "Gradient Boosting":   gb,
        "SVM (RBF)":           svm,
        "Voting Classifier":   voting,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ÉVALUATION D'UN MODÈLE
# ══════════════════════════════════════════════════════════════════════════════

def evaluer_modele(nom, modele, X_train, X_test, y_train, y_test,
                   classes, mode="multiclasse"):
    print(f"\n  {YELLOW}▶  {nom}{RESET}")
    t0 = time.time()

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_res = cross_validate(
        modele, X_train, y_train,
        cv=cv,
        scoring=["accuracy", "f1_macro", "f1_weighted"],
        return_train_score=True,
        n_jobs=-1,
    )

    cv_acc      = cv_res["test_accuracy"].mean()
    cv_acc_std  = cv_res["test_accuracy"].std()
    cv_f1_macro = cv_res["test_f1_macro"].mean()
    overfit     = (cv_res["train_accuracy"].mean() - cv_acc) > 0.05

    # Entraînement final
    modele.fit(X_train, y_train)
    y_pred  = modele.predict(X_test)
    y_proba = modele.predict_proba(X_test) if hasattr(modele, "predict_proba") else None

    test_acc         = accuracy_score(y_test, y_pred)
    test_f1_macro    = f1_score(y_test, y_pred, average="macro",    zero_division=0)
    test_f1_weighted = f1_score(y_test, y_pred, average="weighted", zero_division=0)

    # AUC-ROC
    auc_val = None
    if y_proba is not None:
        try:
            if mode == "binaire":
                auc_val = roc_auc_score(y_test, y_proba[:, 1])
            else:
                auc_val = roc_auc_score(y_test, y_proba,
                                        multi_class="ovr", average="macro")
            auc_val = round(auc_val, 4)
        except Exception:
            auc_val = None

    duree = time.time() - t0

    print(f"     CV Accuracy   : {cv_acc:.3f} ± {cv_acc_std:.3f}  "
          f"{'⚠️ overfit' if overfit else '✅'}")
    print(f"     CV F1 macro   : {cv_f1_macro:.3f}")
    print(f"     Test Accuracy : {test_acc:.3f}")
    print(f"     Test F1 macro : {test_f1_macro:.3f}")
    if auc_val:
        print(f"     AUC-ROC       : {auc_val:.3f}")
    print(f"     Durée         : {duree:.1f}s")

    report = classification_report(
        y_test, y_pred,
        target_names=classes,
        output_dict=True,
        zero_division=0,
    )

    return {
        "model_name":        nom,
        "cv_accuracy":       round(cv_acc, 4),
        "cv_accuracy_std":   round(cv_acc_std, 4),
        "cv_f1_macro":       round(cv_f1_macro, 4),
        "test_accuracy":     round(test_acc, 4),
        "test_f1_macro":     round(test_f1_macro, 4),
        "test_f1_weighted":  round(test_f1_weighted, 4),
        "auc_roc":           auc_val,
        "overfit":           overfit,
        "duration_s":        round(duree, 1),
        "report":            report,
        "model_object":      modele,
    }


# ══════════════════════════════════════════════════════════════════════════════
# AFFICHAGE ET SAUVEGARDE DES MÉTRIQUES
# ══════════════════════════════════════════════════════════════════════════════

def afficher_comparatif(resultats, mode):
    titre(f"Comparaison 5 modèles — Version B — {mode.upper()}")
    header = (f"  {'Modèle':<25} {'CV Acc':>7} {'Test Acc':>9} "
              f"{'F1 macro':>9} {'AUC':>7} {'Overfit':>8}")
    print(header)
    print(f"  {'─'*70}")
    for r in sorted(resultats, key=lambda x: x["test_f1_macro"], reverse=True):
        flag = "⚠️" if r["overfit"] else "✅"
        auc_s = f"{r['auc_roc']:.3f}" if r["auc_roc"] else "  —  "
        print(
            f"  {r['model_name']:<25}"
            f"  {r['cv_accuracy']:.3f}  "
            f"  {r['test_accuracy']:.3f}   "
            f"  {r['test_f1_macro']:.3f}   "
            f"  {auc_s}  "
            f"  {flag}"
        )


def sauvegarder(resultats, mode, classes):
    rows = [{k: v for k, v in r.items() if k not in ("report", "model_object")}
            for r in resultats]
    csv_path = MODELS_PATH / f"results_{mode}.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8")
    print(f"\n  {GREEN}✅ Métriques → {csv_path}{RESET}")

    meilleur = max(resultats, key=lambda x: x["test_f1_macro"])
    print(f"\n  {BOLD}🏆 Meilleur ({mode}) : {meilleur['model_name']}{RESET}")
    print(f"     Accuracy  : {meilleur['test_accuracy']:.3f}")
    print(f"     F1 macro  : {meilleur['test_f1_macro']:.3f}")
    if meilleur["auc_roc"]:
        print(f"     AUC-ROC   : {meilleur['auc_roc']:.3f}")

    titre(f"Rapport détaillé — {meilleur['model_name']} ({mode})")
    for classe in classes:
        r = meilleur["report"].get(classe, {})
        print(f"  {classe:<12} "
              f"precision={r.get('precision', 0):.3f}  "
              f"recall={r.get('recall', 0):.3f}  "
              f"f1={r.get('f1-score', 0):.3f}  "
              f"support={int(r.get('support', 0))}")

    pkl_path = MODELS_PATH / f"best_model_{mode}_vB.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(meilleur["model_object"], f)
    print(f"\n  {GREEN}✅ Modèle sauvegardé → {pkl_path}{RESET}")

    return meilleur


# ══════════════════════════════════════════════════════════════════════════════
# GRAPHIQUES
# ══════════════════════════════════════════════════════════════════════════════

def _save(fig, path):
    """Sauvegarde une figure et affiche la confirmation."""
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  {GREEN}✅ {path.name}{RESET}")


# ── Figure 1 & 2 : Comparaison des métriques ─────────────────────────────────
def fig_comparaison(csv_path, titre_fig, out_path):
    df = pd.read_csv(csv_path)
    modeles = df["model_name"].tolist()
    n = len(modeles)
    x = np.arange(n)
    width = 0.22

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle(titre_fig, fontsize=13, fontweight="bold",
                 color=PALETTE["accent"], y=1.01)

    metrics = [
        ("cv_accuracy",   "CV Accuracy",   0.0),
        ("test_accuracy", "Test Accuracy", width),
        ("test_f1_macro", "F1 Macro",      width * 2),
    ]
    if "auc_roc" in df.columns and df["auc_roc"].notna().any():
        metrics.append(("auc_roc", "AUC-ROC", width * 3))

    bar_colors = ["#ADB5BD", PALETTE["accent"], PALETTE["lr"], PALETTE["svm"]]

    for i, (col, label, offset) in enumerate(metrics):
        vals = df[col].fillna(0).values
        bars = ax.bar(x + offset, vals, width, label=label,
                      color=bar_colors[i], alpha=0.85, zorder=3)
        for bar, v in zip(bars, vals):
            if v > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.005,
                        f"{v:.3f}", ha="center", va="bottom",
                        fontsize=8, color=PALETTE["text"])

    for j, row in df.iterrows():
        if row.get("overfit", False):
            ax.annotate("⚠️", (x[j] + width, 0.05), ha="center",
                        fontsize=12, color="#E63946")

    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels(modeles, rotation=15, ha="right", fontsize=10)
    ax.set_ylim(0, 1.08)
    ax.set_ylabel("Score", fontsize=11)
    ax.legend(loc="upper right", framealpha=0.9, fontsize=9)
    ax.set_title("⚠️ = overfit détecté (train >> validation)",
                 fontsize=9, color="#6C757D", pad=4)

    plt.tight_layout()
    _save(fig, out_path)


# ── Figure 3 : Matrices de confusion ─────────────────────────────────────────
def fig_matrices_confusion(X_train, X_test,
                           y_train_bin, y_test_bin,
                           y_train_multi, y_test_multi,
                           classes, meilleur_bin, meilleur_multi,
                           out_path):
    # Réutilise les objets modèles déjà entraînés
    lr_model  = meilleur_bin["model_object"]
    svm_model = meilleur_multi["model_object"]

    y_pred_bin   = lr_model.predict(X_test)
    y_pred_multi = svm_model.predict(X_test)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Matrices de Confusion — Meilleurs Modèles (Test 2022)",
                 fontsize=14, fontweight="bold", color=PALETTE["accent"])

    cm_bin = confusion_matrix(y_test_bin, y_pred_bin)
    ConfusionMatrixDisplay(cm_bin, display_labels=["DROITE", "EXCEPTION"]).plot(
        ax=axes[0], colorbar=False, cmap="Blues")
    axes[0].set_title(
        f"Logistic Regression — Binaire\n"
        f"Accuracy: {meilleur_bin['test_accuracy']:.1%}  |  "
        f"F1: {meilleur_bin['test_f1_macro']:.3f}",
        fontsize=11, fontweight="bold")
    axes[0].set_xlabel("Prédit", fontsize=10)
    axes[0].set_ylabel("Réel", fontsize=10)

    cm_multi = confusion_matrix(y_test_multi, y_pred_multi)
    ConfusionMatrixDisplay(cm_multi, display_labels=classes).plot(
        ax=axes[1], colorbar=False, cmap="Reds")
    axes[1].set_title(
        f"SVM (RBF) — Multiclasse\n"
        f"Accuracy: {meilleur_multi['test_accuracy']:.1%}  |  "
        f"F1: {meilleur_multi['test_f1_macro']:.3f}",
        fontsize=11, fontweight="bold")
    axes[1].set_xlabel("Prédit", fontsize=10)
    axes[1].set_ylabel("Réel", fontsize=10)

    plt.tight_layout()
    _save(fig, out_path)


# ── Figure 4 : Courbes ROC ────────────────────────────────────────────────────
def fig_courbes_roc(X_train, X_test,
                    y_train_bin, y_test_bin,
                    y_train_multi, y_test_multi,
                    classes, resultats_bin, meilleur_multi,
                    out_path):

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Courbes ROC — Version B (socio-éco pur)",
                 fontsize=14, fontweight="bold", color=PALETTE["accent"])

    # Binaire : toutes les courbes des 5 modèles (déjà entraînés)
    ax = axes[0]
    ax.plot([0, 1], [0, 1], "k--", alpha=0.4, lw=1, label="Aléatoire")

    colors_map = {
        "Logistic Regression": PALETTE["lr"],
        "Random Forest":       PALETTE["rf"],
        "Gradient Boosting":   PALETTE["gb"],
        "SVM (RBF)":           PALETTE["svm"],
        "Voting Classifier":   PALETTE["vc"],
    }
    for r in resultats_bin:
        model = r["model_object"]
        if not hasattr(model, "predict_proba"):
            continue
        proba = model.predict_proba(X_test)[:, 1]
        fpr, tpr, _ = roc_curve(y_test_bin, proba)
        roc_auc_val = auc(fpr, tpr)
        ax.plot(fpr, tpr, lw=2,
                color=colors_map.get(r["model_name"], "gray"),
                label=f"{r['model_name']} (AUC={roc_auc_val:.3f})")

    ax.set_xlabel("Taux Faux Positifs", fontsize=10)
    ax.set_ylabel("Taux Vrais Positifs", fontsize=10)
    ax.set_title("Binaire — DROITE vs EXCEPTION", fontsize=11, fontweight="bold")
    ax.legend(loc="lower right", fontsize=8, framealpha=0.9)
    ax.set_xlim([0, 1]); ax.set_ylim([0, 1.02])

    # Multiclasse : ROC OvR pour le meilleur (SVM)
    ax2 = axes[1]
    ax2.plot([0, 1], [0, 1], "k--", alpha=0.4, lw=1)

    svm_model = meilleur_multi["model_object"]
    y_proba_multi    = svm_model.predict_proba(X_test)
    y_test_bin_ovr   = label_binarize(y_test_multi, classes=range(len(classes)))
    colors_blocs     = [PALETTE["CENTRE"], PALETTE["DROITE"], PALETTE["GAUCHE"]]

    for i, (classe, color) in enumerate(zip(classes, colors_blocs)):
        fpr, tpr, _ = roc_curve(y_test_bin_ovr[:, i], y_proba_multi[:, i])
        roc_auc_val = auc(fpr, tpr)
        ax2.plot(fpr, tpr, lw=2.5, color=color,
                 label=f"{classe} (AUC={roc_auc_val:.3f})")

    ax2.set_xlabel("Taux Faux Positifs", fontsize=10)
    ax2.set_ylabel("Taux Vrais Positifs", fontsize=10)
    ax2.set_title("Multiclasse — SVM (RBF) — One vs Rest",
                  fontsize=11, fontweight="bold")
    ax2.legend(loc="lower right", fontsize=9, framealpha=0.9)
    ax2.set_xlim([0, 1]); ax2.set_ylim([0, 1.02])

    plt.tight_layout()
    _save(fig, out_path)


# ── Figure 5 : Courbes d'apprentissage ───────────────────────────────────────
def fig_courbes_apprentissage(X_train, y_train_bin, y_train_multi, out_path):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Courbes d'Apprentissage — Score vs Taille du Dataset",
                 fontsize=14, fontweight="bold", color=PALETTE["accent"])

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    train_sizes = np.linspace(0.1, 1.0, 10)

    configs = [
        (axes[0],
         LogisticRegression(C=0.1, class_weight="balanced",
                            max_iter=1000, random_state=42, solver="lbfgs"),
         y_train_bin, "Logistic Regression — Binaire"),
        (axes[1],
         SVC(kernel="rbf", C=1.0, gamma="scale", class_weight="balanced",
             probability=True, random_state=42),
         y_train_multi, "SVM (RBF) — Multiclasse"),
    ]

    for ax, model, y_tr, titre_ax in configs:
        sizes, train_scores, val_scores = learning_curve(
            model, X_train, y_tr,
            train_sizes=train_sizes,
            cv=cv,
            scoring="f1_macro",
            n_jobs=-1,
            shuffle=True,
            random_state=42,
        )

        train_mean = train_scores.mean(axis=1)
        train_std  = train_scores.std(axis=1)
        val_mean   = val_scores.mean(axis=1)
        val_std    = val_scores.std(axis=1)

        ax.plot(sizes, train_mean, "o-", color=PALETTE["accent"],
                lw=2, label="Score entraînement")
        ax.fill_between(sizes, train_mean - train_std,
                        train_mean + train_std,
                        alpha=0.15, color=PALETTE["accent"])
        ax.plot(sizes, val_mean, "s--", color=PALETTE["svm"],
                lw=2, label="Score validation (CV)")
        ax.fill_between(sizes, val_mean - val_std,
                        val_mean + val_std,
                        alpha=0.15, color=PALETTE["svm"])

        gap_final = train_mean[-1] - val_mean[-1]
        ax.axhline(val_mean[-1], color="gray", linestyle=":", alpha=0.5, lw=1)
        ax.set_xlabel("Communes d'entraînement (2017)", fontsize=10)
        ax.set_ylabel("F1 Macro", fontsize=10)
        ax.set_title(f"{titre_ax}\nÉcart final train-val : {gap_final:.3f}",
                     fontsize=11, fontweight="bold")
        ax.legend(fontsize=9, framealpha=0.9)
        ax.set_ylim([0, 1.05])
        ax.annotate(
            f"Val max : {val_mean[-1]:.3f}",
            xy=(sizes[-1], val_mean[-1]),
            xytext=(sizes[-2], val_mean[-1] + 0.08),
            arrowprops=dict(arrowstyle="->", color="gray"),
            fontsize=9, color="gray",
        )

    plt.tight_layout()
    _save(fig, out_path)


# ── Figure 6 : Courbes Précision-Rappel ──────────────────────────────────────
def fig_precision_rappel(X_train, X_test,
                         y_train_bin, y_test_bin,
                         y_train_multi, y_test_multi,
                         classes, meilleur_bin, meilleur_multi,
                         out_path):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Courbes Précision-Rappel — Meilleurs Modèles",
                 fontsize=14, fontweight="bold", color=PALETTE["accent"])

    # Binaire — LR (déjà entraîné)
    lr_model  = meilleur_bin["model_object"]
    proba_bin = lr_model.predict_proba(X_test)[:, 1]
    prec, rec, thresh = precision_recall_curve(y_test_bin, proba_bin)
    pr_auc_val = auc(rec, prec)

    ax = axes[0]
    ax.plot(rec, prec, lw=2.5, color=PALETTE["lr"],
            label=f"EXCEPTION (AUC={pr_auc_val:.3f})")
    ax.axhline(y=y_test_bin.mean(), color="gray", linestyle="--",
               lw=1.2, label=f"Baseline ({y_test_bin.mean():.2f})")

    f1_scores = 2 * prec * rec / (prec + rec + 1e-8)
    idx_best  = np.argmax(f1_scores)
    ax.scatter(rec[idx_best], prec[idx_best], s=120,
               color=PALETTE["svm"], zorder=5,
               label=f"Seuil optimal ({thresh[idx_best]:.2f})")

    ax.set_xlabel("Rappel", fontsize=10)
    ax.set_ylabel("Précision", fontsize=10)
    ax.set_title("Logistic Regression — Binaire\n(DROITE vs EXCEPTION)",
                 fontsize=11, fontweight="bold")
    ax.legend(fontsize=9)
    ax.set_xlim([0, 1]); ax.set_ylim([0, 1.05])

    # Multiclasse — SVM OvR (déjà entraîné)
    svm_model     = meilleur_multi["model_object"]
    y_proba_multi = svm_model.predict_proba(X_test)
    y_bin_ovr     = label_binarize(y_test_multi, classes=range(len(classes)))

    ax2 = axes[1]
    colors_blocs = [PALETTE["CENTRE"], PALETTE["DROITE"], PALETTE["GAUCHE"]]
    for i, (classe, color) in enumerate(zip(classes, colors_blocs)):
        prec_c, rec_c, _ = precision_recall_curve(
            y_bin_ovr[:, i], y_proba_multi[:, i])
        pr_auc_c = auc(rec_c, prec_c)
        ax2.plot(rec_c, prec_c, lw=2.5, color=color,
                 label=f"{classe} (AUC={pr_auc_c:.3f})")

    ax2.set_xlabel("Rappel", fontsize=10)
    ax2.set_ylabel("Précision", fontsize=10)
    ax2.set_title("SVM (RBF) — Multiclasse — One vs Rest",
                  fontsize=11, fontweight="bold")
    ax2.legend(fontsize=9)
    ax2.set_xlim([0, 1]); ax2.set_ylim([0, 1.05])

    plt.tight_layout()
    _save(fig, out_path)


# ── Figure 7 : Score par fold ─────────────────────────────────────────────────
def fig_cv_folds(X_train, y_train_bin, y_train_multi, out_path):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Score par Fold — Validation Croisée Stratifiée (k=5)",
                 fontsize=14, fontweight="bold", color=PALETTE["accent"])

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    configs = [
        (axes[0],
         LogisticRegression(C=0.1, class_weight="balanced",
                            max_iter=1000, random_state=42, solver="lbfgs"),
         y_train_bin, "Logistic Regression — Binaire"),
        (axes[1],
         SVC(kernel="rbf", C=1.0, gamma="scale", class_weight="balanced",
             probability=True, random_state=42),
         y_train_multi, "SVM (RBF) — Multiclasse"),
    ]

    folds = [f"Fold {i+1}" for i in range(5)]

    for ax, model, y_tr, titre_ax in configs:
        res = cross_validate(
            model, X_train, y_tr,
            cv=cv,
            scoring=["accuracy", "f1_macro"],
            return_train_score=True,
        )

        x = np.arange(5)
        w = 0.35
        bars1 = ax.bar(x - w/2, res["train_accuracy"], w,
                       label="Train Accuracy", color=PALETTE["accent"],
                       alpha=0.7, zorder=3)
        bars2 = ax.bar(x + w/2, res["test_accuracy"], w,
                       label="Val Accuracy", color=PALETTE["svm"],
                       alpha=0.7, zorder=3)

        ax2 = ax.twinx()
        ax2.plot(x, res["test_f1_macro"], "D--", color=PALETTE["lr"],
                 lw=2, ms=7, label="Val F1 Macro", zorder=4)
        ax2.set_ylabel("F1 Macro", fontsize=9, color=PALETTE["lr"])
        ax2.tick_params(axis="y", labelcolor=PALETTE["lr"])
        ax2.set_ylim([0, 1])
        ax2.spines["top"].set_visible(False)

        for bar in bars1:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.005,
                    f"{bar.get_height():.3f}",
                    ha="center", va="bottom", fontsize=7.5)
        for bar in bars2:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.005,
                    f"{bar.get_height():.3f}",
                    ha="center", va="bottom", fontsize=7.5)

        mean_val = res["test_accuracy"].mean()
        std_val  = res["test_accuracy"].std()
        ax.set_title(f"{titre_ax}\nVal Accuracy : {mean_val:.3f} ± {std_val:.3f}",
                     fontsize=11, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(folds)
        ax.set_ylim([0, 1.1])
        ax.set_ylabel("Accuracy", fontsize=10)
        ax.legend(loc="upper left", fontsize=8)

    plt.tight_layout()
    _save(fig, out_path)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    titre("ElectioAnalytics — Comparaison 5 modèles (Version B)")

    # ── Chargement ────────────────────────────────────────────────────────────
    titre("Chargement des données Version B")
    X_train, X_test, train, test = charger_donnees()

    # Labels binaires
    y_train_bin = train["bloc_vainqueur"].apply(
        lambda x: 0 if x == "DROITE" else 1).values
    y_test_bin  = test["bloc_vainqueur"].apply(
        lambda x: 0 if x == "DROITE" else 1).values

    # Labels multiclasse
    le = LabelEncoder()
    y_train_multi = le.fit_transform(train["bloc_vainqueur"])
    y_test_multi  = le.transform(test["bloc_vainqueur"])
    classes_multi = list(le.classes_)

    # ──────────────────────────────────────────────────────────────────────────
    # PARTIE 1 — Classification BINAIRE
    # ──────────────────────────────────────────────────────────────────────────
    titre("PARTIE 1 — Classification BINAIRE (DROITE vs EXCEPTION)")

    print(f"  Train → DROITE: {(y_train_bin==0).sum()}  "
          f"EXCEPTION: {(y_train_bin==1).sum()}")
    print(f"  Test  → DROITE: {(y_test_bin==0).sum()}   "
          f"EXCEPTION: {(y_test_bin==1).sum()}")

    modeles_bin   = definir_modeles()
    resultats_bin = []
    for nom, modele in modeles_bin.items():
        res = evaluer_modele(
            nom, modele,
            X_train, X_test,
            y_train_bin, y_test_bin,
            classes=["DROITE", "EXCEPTION"],
            mode="binaire",
        )
        resultats_bin.append(res)

    afficher_comparatif(resultats_bin, "binaire")
    meilleur_bin = sauvegarder(resultats_bin, "binaire",
                               classes=["DROITE", "EXCEPTION"])

    # ──────────────────────────────────────────────────────────────────────────
    # PARTIE 2 — Classification MULTICLASSE
    # ──────────────────────────────────────────────────────────────────────────
    titre("PARTIE 2 — Classification MULTICLASSE (DROITE / CENTRE / GAUCHE)")

    print(f"  Classes : {classes_multi}")
    for c, n in zip(*np.unique(y_train_multi, return_counts=True)):
        print(f"  Train → {le.classes_[c]:<10} : {n} communes "
              f"({n/len(y_train_multi)*100:.1f}%)")

    modeles_multi   = definir_modeles()
    resultats_multi = []
    for nom, modele in modeles_multi.items():
        res = evaluer_modele(
            nom, modele,
            X_train, X_test,
            y_train_multi, y_test_multi,
            classes=classes_multi,
            mode="multiclasse",
        )
        resultats_multi.append(res)

    afficher_comparatif(resultats_multi, "multiclasse")
    meilleur_multi = sauvegarder(resultats_multi, "multiclasse",
                                 classes=classes_multi)

    # ──────────────────────────────────────────────────────────────────────────
    # BILAN GLOBAL
    # ──────────────────────────────────────────────────────────────────────────
    titre("BILAN GLOBAL")
    print(f"\n  {'Mode':<15} {'Meilleur modèle':<25} "
          f"{'Accuracy':>9} {'F1 macro':>9} {'AUC':>7}")
    print(f"  {'─'*70}")

    for mode, meilleur in [("Binaire", meilleur_bin), ("Multiclasse", meilleur_multi)]:
        auc_s = f"{meilleur['auc_roc']:.3f}" if meilleur["auc_roc"] else "  —  "
        print(
            f"  {mode:<15}"
            f"  {meilleur['model_name']:<25}"
            f"  {meilleur['test_accuracy']:.3f}   "
            f"  {meilleur['test_f1_macro']:.3f}   "
            f"  {auc_s}"
        )

    meta = {
        "binaire": {
            "best_model":    meilleur_bin["model_name"],
            "test_accuracy": meilleur_bin["test_accuracy"],
            "test_f1_macro": meilleur_bin["test_f1_macro"],
            "auc_roc":       meilleur_bin["auc_roc"],
        },
        "multiclasse": {
            "best_model":    meilleur_multi["model_name"],
            "test_accuracy": meilleur_multi["test_accuracy"],
            "test_f1_macro": meilleur_multi["test_f1_macro"],
            "auc_roc":       meilleur_multi["auc_roc"],
        },
        "features": FEATURES_B,
        "classes":  classes_multi,
        "dataset":  "Version B — socio-éco pur",
        "train":    "2017",
        "test":     "2022",
    }
    meta_path = MODELS_PATH / "best_model_meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"\n  {GREEN}✅ Métadonnées → {meta_path}{RESET}")

    # ──────────────────────────────────────────────────────────────────────────
    # GÉNÉRATION DES GRAPHIQUES
    # ──────────────────────────────────────────────────────────────────────────
    titre("Génération des graphiques → results/figures/")

    section("Graphiques comparatifs des métriques")
    fig_comparaison(
        MODELS_PATH / "results_binaire.csv",
        ("Comparaison 5 Modèles — Classification Binaire (DROITE vs EXCEPTION)\n"
         "Version B — Socio-économique pur — Train 2017 / Test 2022"),
        FIG_PATH / "01_comparaison_binaire.png",
    )
    fig_comparaison(
        MODELS_PATH / "results_multiclasse.csv",
        ("Comparaison 5 Modèles — Classification Multiclasse (DROITE / CENTRE / GAUCHE)\n"
         "Version B — Socio-économique pur — Train 2017 / Test 2022"),
        FIG_PATH / "02_comparaison_multiclasse.png",
    )

    section("Matrices de confusion")
    fig_matrices_confusion(
        X_train, X_test,
        y_train_bin, y_test_bin,
        y_train_multi, y_test_multi,
        classes_multi, meilleur_bin, meilleur_multi,
        FIG_PATH / "03_matrices_confusion.png",
    )

    section("Courbes ROC")
    fig_courbes_roc(
        X_train, X_test,
        y_train_bin, y_test_bin,
        y_train_multi, y_test_multi,
        classes_multi, resultats_bin, meilleur_multi,
        FIG_PATH / "04_courbes_roc.png",
    )

    section("Courbes d'apprentissage (patience ~30s)")
    fig_courbes_apprentissage(
        X_train, y_train_bin, y_train_multi,
        FIG_PATH / "05_courbes_apprentissage.png",
    )

    section("Courbes Précision-Rappel")
    fig_precision_rappel(
        X_train, X_test,
        y_train_bin, y_test_bin,
        y_train_multi, y_test_multi,
        classes_multi, meilleur_bin, meilleur_multi,
        FIG_PATH / "06_precision_rappel.png",
    )

    section("Scores par fold (CV)")
    fig_cv_folds(
        X_train, y_train_bin, y_train_multi,
        FIG_PATH / "07_cv_folds.png",
    )

    # ──────────────────────────────────────────────────────────────────────────
    # RÉCAPITULATIF FINAL
    # ──────────────────────────────────────────────────────────────────────────
    titre("Récapitulatif des sorties")
    print(f"\n  {BOLD}Modèles & métriques :{RESET}")
    print(f"    → data/models/results_binaire.csv")
    print(f"    → data/models/results_multiclasse.csv")
    print(f"    → data/models/best_model_binaire_vB.pkl")
    print(f"    → data/models/best_model_multiclasse_vB.pkl")
    print(f"    → data/models/best_model_meta.json")
    print(f"\n  {BOLD}Graphiques :{RESET}")
    for i in range(1, 8):
        names = [
            "01_comparaison_binaire.png",
            "02_comparaison_multiclasse.png",
            "03_matrices_confusion.png",
            "04_courbes_roc.png",
            "05_courbes_apprentissage.png",
            "06_precision_rappel.png",
            "07_cv_folds.png",
        ]
        print(f"    → results/figures/{names[i-1]}")

    print(f"\n  {GREEN}{BOLD}✅ Pipeline complet !{RESET}")
    print(f"\n  Prochaine étape :")
    print(f"  → notebooks/05_projections_2027.ipynb")


if __name__ == "__main__":
    main()