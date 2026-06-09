import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import os
import warnings
warnings.filterwarnings('ignore')

from sklearn.model_selection import StratifiedKFold, cross_validate, GridSearchCV
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, classification_report, confusion_matrix
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

# Chemins
FINAL_PATH = "data/final"
MODELS_PATH = "data/models"
BENCH_PATH = "data/models/benchmark"
os.makedirs(BENCH_PATH, exist_ok=True)

# Styles
sns.set_theme(style="whitegrid", palette="muted")

# ============================================================
# 1. CHARGEMENT DES DONNÉES (identique à votre script)
# ============================================================
print("=" * 60)
print("📂 Chargement des datasets")
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
    print(f"Version {version} : Train={len(train)} / Test={len(test)}")

# ============================================================
# 2. DÉFINITION DES MODÈLES À TESTER
# ============================================================
# On garde le même jeu de features que votre script original
# Pour la version A : features avec historique électoral
# Pour la version B : features purement socio-économiques

models = {
    "Logistic Regression": {
        "model": LogisticRegression(class_weight="balanced", C=0.1, max_iter=1000, random_state=42),
        "params": {}  # pas d'optimisation ici pour rester simple
    },
    "Random Forest": {
        "model": RandomForestClassifier(class_weight="balanced", random_state=42, n_jobs=-1),
        "params": {
            "n_estimators": [100, 200],
            "max_depth": [5, 10, None],
            "min_samples_split": [5, 10]
        }
    },
    "XGBoost": {
        "model": XGBClassifier(eval_metric="logloss", random_state=42, use_label_encoder=False),
        "params": {
            "n_estimators": [100, 200],
            "max_depth": [3, 6],
            "learning_rate": [0.05, 0.1],
            "scale_pos_weight": [10]  # pour déséquilibre (598/50 ≈ 12, on prend 10)
        }
    },
    "SVM": {
        "model": SVC(class_weight="balanced", probability=True, random_state=42),
        "params": {
            "C": [0.1, 1, 10],
            "gamma": ["scale", "auto"],
            "kernel": ["rbf"]
        }
    },
    "Gradient Boosting": {
        "model": GradientBoostingClassifier(random_state=42),
        "params": {
            "n_estimators": [100, 200],
            "max_depth": [3, 5],
            "learning_rate": [0.05, 0.1]
        }
    }
}

# ============================================================
# 3. BOUCLE D'ENTRAÎNEMENT ET ÉVALUATION
# ============================================================
print("\n" + "=" * 60)
print("🤖 Benchmark supervisé - Classification binaire")
print("=" * 60)

resultats_benchmark = {}  # structure: resultats_benchmark[version][model_name] = dict

for version in ["A", "B"]:
    print(f"\n{'='*60}")
    print(f"📌 VERSION {version}")
    print(f"{'='*60}")
    
    train = datasets[version]["train"]
    test = datasets[version]["test"]
    
    # Features (exclure les colonnes cibles)
    cols_cibles = ["cible_binaire", "cible_multiclasse", "cible_transition_enc"]
    features = [c for c in train.columns 
                if (c.endswith("_scaled") or c.endswith("_enc")) 
                and c not in cols_cibles]
    
    X_train = train[features].values
    y_train = train["cible_binaire"].values
    X_test = test[features].values
    y_test = test["cible_binaire"].values
    
    print(f"Features ({len(features)}) : {features}")
    print(f"Distribution train : DROITE={sum(y_train==0)} / EXCEPTION={sum(y_train==1)}")
    print(f"Distribution test  : DROITE={sum(y_test==0)} / EXCEPTION={sum(y_test==1)}")
    
    # Pour SVM, on rescale (car pas de scaling intégré)
    scaler = StandardScaler()
    X_train_svm = scaler.fit_transform(X_train)
    X_test_svm = scaler.transform(X_test)
    
    resultats_benchmark[version] = {}
    
    for model_name, config in models.items():
        print(f"\n{'─'*40}")
        print(f"🔍 {model_name}")
        print(f"{'─'*40}")
        
        model = config["model"]
        
        # Cross-validation (k=5 stratifié)
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        
        # Utiliser le bon dataset pour SVM (scalé)
        if model_name == "SVM":
            X_cv = X_train_svm
        else:
            X_cv = X_train
        
        # Recherche d'hyperparamètres (GridSearchCV) si params non vide
        if config["params"]:
            print("   Optimisation des hyperparamètres (GridSearchCV)...")
            grid = GridSearchCV(
                model, config["params"], 
                cv=cv, scoring="f1", 
                n_jobs=-1, verbose=0
            )
            grid.fit(X_cv, y_train)
            best_model = grid.best_estimator_
            print(f"   Meilleurs paramètres : {grid.best_params_}")
            cv_results = cross_validate(
                best_model, X_cv, y_train,
                cv=cv, scoring=["accuracy", "f1", "roc_auc"],
                return_train_score=True
            )
        else:
            best_model = model
            cv_results = cross_validate(
                best_model, X_cv, y_train,
                cv=cv, scoring=["accuracy", "f1", "roc_auc"],
                return_train_score=True
            )
        
        # Affichage CV
        print(f"\n   📊 Cross-validation (k=5) :")
        print(f"   {'Métrique':<20} {'Train':>10} {'Val':>10} {'Écart':>10}")
        print(f"   {'-'*52}")
        for metric in ["accuracy", "f1", "roc_auc"]:
            train_score = cv_results[f"train_{metric}"].mean()
            val_score = cv_results[f"test_{metric}"].mean()
            ecart = train_score - val_score
            overfit = "⚠️" if ecart > 0.1 else "✅"
            print(f"   {metric:<20} {train_score:>10.3f} {val_score:>10.3f} {ecart:>+9.3f} {overfit}")
        
        # Entraînement final sur tout le train
        best_model.fit(X_cv, y_train)
        
        # Prédictions sur le test
        if model_name == "SVM":
            X_test_used = X_test_svm
        else:
            X_test_used = X_test
        
        y_pred = best_model.predict(X_test_used)
        if hasattr(best_model, "predict_proba"):
            y_pred_proba = best_model.predict_proba(X_test_used)[:, 1]
        else:
            # Pour SVM sans proba (mais on a mis probability=True)
            y_pred_proba = best_model.decision_function(X_test_used)
            # normalisation approximative pour AUC
            y_pred_proba = (y_pred_proba - y_pred_proba.min()) / (y_pred_proba.max() - y_pred_proba.min())
        
        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_pred_proba)
        
        print(f"\n   📊 Évaluation sur test 2022 :")
        print(f"   Accuracy  : {acc:.3f}  ({acc*100:.1f}%)")
        print(f"   F1 Score  : {f1:.3f}")
        print(f"   AUC-ROC   : {auc:.3f}")
        
        # Rapport détaillé
        print(f"\n   Classification report :")
        print(classification_report(y_test, y_pred, target_names=["DROITE", "EXCEPTION"], digits=3))
        
        # Sauvegarde du modèle et des résultats
        model_filename = f"model_{model_name.lower().replace(' ', '_')}_v{version}.pkl"
        joblib.dump(best_model, os.path.join(BENCH_PATH, model_filename))
        print(f"   ✅ Modèle sauvegardé → {model_filename}")
        
        # Stocker les résultats
        resultats_benchmark[version][model_name] = {
            "model": best_model,
            "cv_results": cv_results,
            "acc": acc,
            "f1": f1,
            "auc": auc,
            "y_pred": y_pred,
            "y_pred_proba": y_pred_proba
        }

# ============================================================
# 4. COMPARAISON GLOBALE DES MODÈLES
# ============================================================
print("\n" + "=" * 60)
print("📊 COMPARAISON FINALE - Tous modèles")
print("=" * 60)

for version in ["A", "B"]:
    print(f"\n{'='*40}")
    print(f"Version {version}")
    print(f"{'='*40}")
    print(f"\n{'Modèle':<25} {'Accuracy':>12} {'F1':>12} {'AUC-ROC':>12}")
    print(f"{'-'*65}")
    for model_name, res in resultats_benchmark[version].items():
        print(f"{model_name:<25} {res['acc']:>12.3f} {res['f1']:>12.3f} {res['auc']:>12.3f}")

# ============================================================
# 5. VISUALISATIONS COMPARATIVES (Version B, la plus réaliste)
# ============================================================
print("\n" + "=" * 60)
print("📊 Visualisations comparatives (Version B)")
print("=" * 60)

version_visu = "B"  # on choisit B car sans leakage
res_visu = resultats_benchmark[version_visu]

fig = plt.figure(figsize=(16, 12))

# 5.1 Courbes ROC
ax_roc = fig.add_subplot(2, 2, 1)
colors = plt.cm.tab10(np.linspace(0, 1, len(res_visu)))
for (model_name, res), color in zip(res_visu.items(), colors):
    from sklearn.metrics import RocCurveDisplay
    RocCurveDisplay.from_predictions(
        datasets[version_visu]["test"]["cible_binaire"].values,
        res["y_pred_proba"],
        name=f"{model_name} (AUC={res['auc']:.3f})",
        color=color,
        ax=ax_roc
    )
ax_roc.plot([0,1], [0,1], 'k--', alpha=0.3)
ax_roc.set_title(f"Courbes ROC - Version {version_visu}")
ax_roc.legend(loc="lower right")

# 5.2 Barplot des métriques
ax_bar = fig.add_subplot(2, 2, 2)
model_names = list(res_visu.keys())
accs = [res_visu[m]['acc'] for m in model_names]
f1s = [res_visu[m]['f1'] for m in model_names]
aucs = [res_visu[m]['auc'] for m in model_names]

x = np.arange(len(model_names))
width = 0.25
ax_bar.bar(x - width, accs, width, label='Accuracy', alpha=0.8)
ax_bar.bar(x, f1s, width, label='F1 Score', alpha=0.8)
ax_bar.bar(x + width, aucs, width, label='AUC-ROC', alpha=0.8)
ax_bar.set_xticks(x)
ax_bar.set_xticklabels(model_names, rotation=45, ha='right')
ax_bar.set_ylim(0, 1.05)
ax_bar.set_title(f"Comparaison des métriques - Version {version_visu}")
ax_bar.legend()
for i, (acc, f1, auc) in enumerate(zip(accs, f1s, aucs)):
    ax_bar.text(i - width, acc + 0.01, f"{acc:.2f}", ha='center', fontsize=8)
    ax_bar.text(i, f1 + 0.01, f"{f1:.2f}", ha='center', fontsize=8)
    ax_bar.text(i + width, auc + 0.01, f"{auc:.2f}", ha='center', fontsize=8)

# 5.3 Matrices de confusion pour les meilleurs modèles
best_model_name = max(res_visu, key=lambda x: res_visu[x]['f1'])
best_res = res_visu[best_model_name]
y_test_true = datasets[version_visu]["test"]["cible_binaire"].values
cm = confusion_matrix(y_test_true, best_res['y_pred'])

ax_cm = fig.add_subplot(2, 2, 3)
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax_cm,
            xticklabels=['DROITE', 'EXCEPTION'],
            yticklabels=['DROITE', 'EXCEPTION'])
ax_cm.set_title(f"Matrice confusion - Meilleur modèle : {best_model_name}\nF1={best_res['f1']:.3f}")

# 5.4 Feature importance pour Random Forest (s'il est dans la liste)
ax_imp = fig.add_subplot(2, 2, 4)
if "Random Forest" in res_visu:
    rf_model = res_visu["Random Forest"]["model"]
    # Récupérer les features (les mêmes que pour version B)
    features_b = [c for c in datasets["B"]["train"].columns 
                  if (c.endswith("_scaled") or c.endswith("_enc")) 
                  and c not in ["cible_binaire", "cible_multiclasse", "cible_transition_enc"]]
    importances = rf_model.feature_importances_
    indices = np.argsort(importances)[-10:]
    ax_imp.barh(np.array(features_b)[indices], importances[indices], color='teal')
    ax_imp.set_title("Importances des features (Random Forest)\nVersion B")
    ax_imp.set_xlabel("Importance")
else:
    ax_imp.text(0.5, 0.5, "Random Forest non disponible\npour l'affichage", 
                ha='center', va='center', transform=ax_imp.transAxes)
    ax_imp.set_title("Feature importance non disponible")

plt.tight_layout()
plt.savefig(os.path.join(BENCH_PATH, "benchmark_model_comparison.png"), dpi=120, bbox_inches='tight')
plt.show()
print(f"✅ Graphique sauvegardé : {os.path.join(BENCH_PATH, 'benchmark_model_comparison.png')}")

# ============================================================
# 6. RÉCAPITULATIF FINAL
# ============================================================
print("\n" + "=" * 60)
print("📋 RÉCAPITULATIF - Meilleurs modèles par métrique")
print("=" * 60)

for version in ["A", "B"]:
    print(f"\n🔹 Version {version}")
    res = resultats_benchmark[version]
    best_acc = max(res.items(), key=lambda x: x[1]['acc'])
    best_f1 = max(res.items(), key=lambda x: x[1]['f1'])
    best_auc = max(res.items(), key=lambda x: x[1]['auc'])
    print(f"   Meilleur Accuracy : {best_acc[0]} ({best_acc[1]['acc']:.3f})")
    print(f"   Meilleur F1       : {best_f1[0]} ({best_f1[1]['f1']:.3f})")
    print(f"   Meilleur AUC-ROC  : {best_auc[0]} ({best_auc[1]['auc']:.3f})")

print("\n" + "=" * 60)
print("🏁 Benchmark terminé. Modèles sauvegardés dans data/models/benchmark/")
print("=" * 60)