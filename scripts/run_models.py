"""
scripts/run_models.py
=====================
Compare 5 algorithmes de classification sur le dataset Version B
(features socio-économiques uniquement — utilisable pour 2027).

Exécuter depuis la RACINE du projet :
    python scripts/run_models.py

Modèles comparés :
    1. Logistic Regression  (baseline interprétable)
    2. Random Forest        (baseline ensemble)
    3. Gradient Boosting    (XGBoost-like, scikit-learn)
    4. SVM (kernel RBF)     (robuste sur petits datasets)
    5. Voting Classifier    (RF + GB + LR ensemblés)

Sorties :
    data/models/comparison_results.csv   ← métriques détaillées
    data/models/best_model_vB.pkl        ← meilleur modèle sauvegardé
    data/models/scaler_vB.pkl            ← scaler associé
"""

import warnings
warnings.filterwarnings("ignore")

import os
import sys
import time
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (
    RandomForestClassifier,
    GradientBoostingClassifier,
    VotingClassifier,
)
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.metrics import (
    accuracy_score, f1_score, classification_report, confusion_matrix
)

# ── Couleurs terminal ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def titre(msg):
    print(f"\n{BOLD}{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {msg}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*60}{RESET}")

# ── Chemins ───────────────────────────────────────────────────────────────────
FINAL_PATH  = Path("data/final")
MODELS_PATH = Path("data/models")
MODELS_PATH.mkdir(parents=True, exist_ok=True)

TRAIN_B = FINAL_PATH / "ml_train_2017_vB.csv"
TEST_B  = FINAL_PATH / "ml_test_2022_vB.csv"

# ── Features Version B (socio-éco pur) ───────────────────────────────────────
FEATURES_B = [
    "revenu_median_scaled",
    "taux_faible_diplome_scaled",
    "pct_abstention_scaled",
    "log_delinquance_scaled",
    "log_population_scaled",
]
TARGET = "bloc_vainqueur"


def charger_donnees():
    """Charge train/test Version B."""
    if not TRAIN_B.exists() or not TEST_B.exists():
        print(f"{RED}❌ Fichiers introuvables : {TRAIN_B} / {TEST_B}{RESET}")
        print("   Lance d'abord : python scripts/run_pipeline.py")
        sys.exit(1)

    train = pd.read_csv(TRAIN_B)
    test  = pd.read_csv(TEST_B)

    # Vérification features
    manquantes = [f for f in FEATURES_B if f not in train.columns]
    if manquantes:
        print(f"{RED}❌ Features manquantes : {manquantes}{RESET}")
        print(f"   Colonnes disponibles : {list(train.columns)}")
        sys.exit(1)

    X_train = train[FEATURES_B].values
    X_test  = test[FEATURES_B].values

    # Encodage cible
    le = LabelEncoder()
    y_train = le.fit_transform(train[TARGET])
    y_test  = le.transform(test[TARGET])

    print(f"  {GREEN}✅ Train : {X_train.shape[0]} communes × {X_train.shape[1]} features{RESET}")
    print(f"  {GREEN}✅ Test  : {X_test.shape[0]}  communes × {X_test.shape[1]} features{RESET}")
    print(f"  Classes : {list(le.classes_)}")

    dist = pd.Series(le.inverse_transform(y_train)).value_counts()
    for bloc, n in dist.items():
        print(f"    {bloc:<10} : {n} communes ({n/len(y_train)*100:.1f}%)")

    return X_train, X_test, y_train, y_test, le


def definir_modeles():
    """Retourne le dictionnaire des 5 modèles à comparer."""
    lr = LogisticRegression(
        C=0.1,
        class_weight="balanced",
        max_iter=1000,
        random_state=42,
        multi_class="multinomial",
        solver="lbfgs"
    )
    rf = RandomForestClassifier(
        n_estimators=300,
        class_weight="balanced",
        max_depth=8,
        min_samples_leaf=3,
        random_state=42,
        n_jobs=-1
    )
    gb = GradientBoostingClassifier(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=4,
        min_samples_leaf=5,
        subsample=0.8,
        random_state=42
    )
    svm = SVC(
        kernel="rbf",
        C=1.0,
        gamma="scale",
        class_weight="balanced",
        probability=True,
        random_state=42
    )
    voting = VotingClassifier(
        estimators=[("lr", lr), ("rf", rf), ("gb", gb)],
        voting="soft",
        n_jobs=-1
    )

    return {
        "Logistic Regression": lr,
        "Random Forest":       rf,
        "Gradient Boosting":   gb,
        "SVM (RBF)":           svm,
        "Voting Classifier":   voting,
    }


def evaluer_modele(nom, modele, X_train, X_test, y_train, y_test, le):
    """Cross-validation + évaluation test pour un modèle."""
    print(f"\n  {YELLOW}▶  {nom}{RESET}")
    t0 = time.time()

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    # Cross-validation
    cv_results = cross_validate(
        modele, X_train, y_train,
        cv=cv,
        scoring=["accuracy", "f1_macro", "f1_weighted"],
        return_train_score=True,
        n_jobs=-1
    )

    cv_acc     = cv_results["test_accuracy"].mean()
    cv_acc_std = cv_results["test_accuracy"].std()
    cv_f1      = cv_results["test_f1_macro"].mean()
    overfit    = (cv_results["train_accuracy"].mean() - cv_acc) > 0.05

    # Entraînement final + test
    modele.fit(X_train, y_train)
    y_pred = modele.predict(X_test)

    test_acc = accuracy_score(y_test, y_pred)
    test_f1_macro    = f1_score(y_test, y_pred, average="macro",    zero_division=0)
    test_f1_weighted = f1_score(y_test, y_pred, average="weighted", zero_division=0)

    duree = time.time() - t0

    # Rapport détaillé
    print(f"     CV Accuracy  : {cv_acc:.3f} ± {cv_acc_std:.3f}  {'⚠️ overfit' if overfit else '✅'}")
    print(f"     CV F1 macro  : {cv_f1:.3f}")
    print(f"     Test Accuracy: {test_acc:.3f}")
    print(f"     Test F1 macro: {test_f1_macro:.3f}")
    print(f"     Durée        : {duree:.1f}s")

    # Rapport classification par classe
    report = classification_report(
        y_test, y_pred,
        target_names=le.classes_,
        output_dict=True,
        zero_division=0
    )

    return {
        "model_name":       nom,
        "cv_accuracy":      round(cv_acc, 4),
        "cv_accuracy_std":  round(cv_acc_std, 4),
        "cv_f1_macro":      round(cv_f1, 4),
        "test_accuracy":    round(test_acc, 4),
        "test_f1_macro":    round(test_f1_macro, 4),
        "test_f1_weighted": round(test_f1_weighted, 4),
        "overfit":          overfit,
        "duration_s":       round(duree, 1),
        "report":           report,
        "model_object":     modele,
    }


def afficher_comparatif(resultats):
    """Affiche le tableau comparatif final."""
    titre("Comparaison des 5 modèles — Version B (socio-éco)")

    header = f"  {'Modèle':<25} {'CV Acc':>8} {'Test Acc':>9} {'F1 macro':>9} {'F1 weighted':>12} {'Overfit':>8}"
    print(header)
    print(f"  {'─'*75}")

    for r in sorted(resultats, key=lambda x: x["test_f1_macro"], reverse=True):
        flag = "⚠️" if r["overfit"] else "✅"
        print(
            f"  {r['model_name']:<25}"
            f"  {r['cv_accuracy']:.3f}  "
            f"  {r['test_accuracy']:.3f}    "
            f"  {r['test_f1_macro']:.3f}      "
            f"  {r['test_f1_weighted']:.3f}      "
            f"  {flag}"
        )


def sauvegarder_resultats(resultats, le):
    """Sauvegarde CSV des métriques + meilleur modèle en .pkl."""

    # CSV métriques
    rows = [{k: v for k, v in r.items() if k not in ("report", "model_object")}
            for r in resultats]
    df_results = pd.DataFrame(rows)
    csv_path = MODELS_PATH / "comparison_results.csv"
    df_results.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"\n  {GREEN}✅ Métriques sauvegardées → {csv_path}{RESET}")

    # Meilleur modèle (critère : F1 macro sur test)
    meilleur = max(resultats, key=lambda x: x["test_f1_macro"])
    print(f"\n  {BOLD}🏆 Meilleur modèle : {meilleur['model_name']}{RESET}")
    print(f"     Test Accuracy : {meilleur['test_accuracy']:.3f}")
    print(f"     F1 macro      : {meilleur['test_f1_macro']:.3f}")
    print(f"     F1 weighted   : {meilleur['test_f1_weighted']:.3f}")

    # Rapport classification détaillé
    titre(f"Rapport détaillé — {meilleur['model_name']}")
    for classe in le.classes_:
        r = meilleur["report"].get(classe, {})
        print(f"  {classe:<10} precision={r.get('precision',0):.3f}"
              f"  recall={r.get('recall',0):.3f}"
              f"  f1={r.get('f1-score',0):.3f}"
              f"  support={r.get('support',0):.0f}")

    # Sauvegarde pkl
    model_path = MODELS_PATH / "best_model_vB.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(meilleur["model_object"], f)
    print(f"\n  {GREEN}✅ Modèle sauvegardé → {model_path}{RESET}")

    # Métadonnées JSON
    meta = {
        "best_model":       meilleur["model_name"],
        "test_accuracy":    meilleur["test_accuracy"],
        "test_f1_macro":    meilleur["test_f1_macro"],
        "test_f1_weighted": meilleur["test_f1_weighted"],
        "features":         FEATURES_B,
        "classes":          list(le.classes_),
        "dataset_version":  "B",
        "trained_on":       "2017",
        "tested_on":        "2022",
    }
    meta_path = MODELS_PATH / "best_model_meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"  {GREEN}✅ Métadonnées → {meta_path}{RESET}")

    return meilleur


def main():
    titre("ElectioAnalytics — Comparaison des modèles (Version B)")

    # 1. Chargement
    titre("Chargement des données")
    X_train, X_test, y_train, y_test, le = charger_donnees()

    # 2. Définition des modèles
    modeles = definir_modeles()

    # 3. Évaluation
    titre("Entraînement et évaluation (StratifiedKFold k=5)")
    resultats = []
    for nom, modele in modeles.items():
        res = evaluer_modele(nom, modele, X_train, X_test, y_train, y_test, le)
        resultats.append(res)

    # 4. Comparatif
    afficher_comparatif(resultats)

    # 5. Sauvegarde
    titre("Sauvegarde des résultats")
    meilleur = sauvegarder_resultats(resultats, le)

    print(f"\n{GREEN}{BOLD}  Comparaison terminée !{RESET}")
    print(f"  Résultats : data/models/comparison_results.csv")
    print(f"  Prochaine étape :")
    print(f"  → notebooks/04_model_comparison.ipynb  (visualisations)")
    print(f"  → notebooks/05_projections_2027.ipynb  (prédictions)")


if __name__ == "__main__":
    main()
