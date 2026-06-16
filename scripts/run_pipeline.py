"""
scripts/run_pipeline.py
=======================
Pipeline complet de données ElectioAnalytics.
Exécuter depuis la RACINE du projet :

    python scripts/run_pipeline.py

Étapes :
    1. Nettoyage des données électorales (2017, 2022)
    2. Pivot électoral (format long → format large)
    3. Nettoyage des données socio-économiques
    4. Construction du dataset ML final
    5. Preprocessing (normalisation, features enginering)
"""

import subprocess
import sys
import time
from pathlib import Path
from sklearn.neural_network import MLPClassifier

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

def ok(msg):    print(f"  {GREEN}✅ {msg}{RESET}")
def warn(msg):  print(f"  {YELLOW}⚠️  {msg}{RESET}")
def erreur(msg):print(f"  {RED}❌ {msg}{RESET}")

# ── Étapes du pipeline ────────────────────────────────────────────────────────
# (label, chemin_script)
ETAPES = [
    # --- Nettoyage électoral ---
    ("Nettoyage élections 2017",           "src/cleaning/electoral_data_cleaner_2017.py"),
    ("Nettoyage élections 2022",           "src/cleaning/electoral_data_cleaner_2022.py"),
    ("Pivot électoral (long → large)",     "src/cleaning/pivot_election.py"),
    # --- Nettoyage socio-économique ---
    ("Nettoyage démographie 2016",         "src/cleaning/demographic_data_cleaner_2016.py"),
    ("Nettoyage démographie 2021",         "src/cleaning/demographic_data_cleaner_2021.py"),
    ("Nettoyage chômage",                  "src/cleaning/unemployment_data_cleaner.py"),
    ("Nettoyage délinquance",              "src/cleaning/delinquency_data_cleaner.py"),
    ("Nettoyage revenus médians",          "src/cleaning/economic_data_cleaner.py"),
    ("Nettoyage diplômes",                 "src/cleaning/education_data_cleaner.py"),
    # --- Construction dataset ---
    ("Construction dataset ML final",      "src/pipeline/build_dataset.py"),
    ("Preprocessing (normalisation + split)", "src/pipeline/preprocessing.py"),
]

def run_script(label, chemin):
    """Exécute un script Python et retourne (succès, durée_secondes)."""
    path = Path(chemin)
    if not path.exists():
        warn(f"Script absent : {chemin} — étape ignorée")
        return None, 0

    print(f"\n  {YELLOW}▶  {label}{RESET}")
    print(f"     {chemin}")

    t0 = time.time()
    result = subprocess.run(
        [sys.executable, str(path)],
        capture_output=False,   # affiche la sortie en temps réel
        text=True
    )
    duree = time.time() - t0

    if result.returncode == 0:
        ok(f"Terminé en {duree:.1f}s")
        return True, duree
    else:
        erreur(f"Échec (code {result.returncode}) après {duree:.1f}s")
        return False, duree


def main():
    titre("ElectioAnalytics — Pipeline de données complet")
    print(f"  Python : {sys.executable}")
    print(f"  Répertoire de travail : {Path('.').resolve()}")

    resultats = []
    t_total = time.time()

    for label, chemin in ETAPES:
        succes, duree = run_script(label, chemin)
        resultats.append((label, succes, duree))

        # Arrêt si étape critique échoue
        if succes is False and chemin in [
            "src/pipeline/build_dataset.py",
            "src/pipeline/preprocessing.py"
        ]:
            erreur("Étape critique échouée — pipeline interrompu.")
            break

    # ── Résumé ────────────────────────────────────────────────────────────────
    titre("Résumé du pipeline")
    total = time.time() - t_total
    ok_count  = sum(1 for _, s, _ in resultats if s is True)
    err_count = sum(1 for _, s, _ in resultats if s is False)
    skip_count = sum(1 for _, s, _ in resultats if s is None)

    for label, succes, duree in resultats:
        icone = "✅" if succes else ("❌" if succes is False else "⏭")
        print(f"  {icone}  {label:<45} {duree:>5.1f}s")

    print(f"\n  {GREEN if err_count == 0 else RED}"
          f"Résultat : {ok_count} OK  |  {err_count} erreurs  |  {skip_count} ignorés{RESET}")
    print(f"  Durée totale : {total:.1f}s")

    if err_count == 0:
        print(f"\n{GREEN}{BOLD}  Pipeline complété avec succès !{RESET}")
        print(f"  Dataset disponible : data/final/dataset_ml_final.csv")
        print(f"  Datasets ML : data/final/ml_train_2017_v[AB].csv")
        print(f"\n  Prochaine étape :")
        print(f"  → python scripts/run_models.py")
    else:
        print(f"\n{RED}  Des erreurs ont été rencontrées. Vérifier les logs ci-dessus.{RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()
