"""
scripts/run_cleaning.py
=======================
Exécute uniquement les 9 scripts de nettoyage des données brutes.
Utile pour relancer le nettoyage sans refaire build_dataset + preprocessing.

Exécuter depuis la RACINE du projet :
    python scripts/run_cleaning.py
"""

import subprocess
import sys
import time
from pathlib import Path

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

def ok(msg):     print(f"  {GREEN}✅ {msg}{RESET}")
def warn(msg):   print(f"  {YELLOW}⚠️  {msg}{RESET}")
def erreur(msg): print(f"  {RED}❌ {msg}{RESET}")

ETAPES_NETTOYAGE = [
    ("Élections 2017",       "src/cleaning/electoral_data_cleaner_2017.py"),
    ("Élections 2022",       "src/cleaning/electoral_data_cleaner_2022.py"),
    ("Pivot électoral",      "src/cleaning/pivot_election.py"),
    ("Démographie 2016",     "src/cleaning/demographic_data_cleaner_2016.py"),
    ("Démographie 2021",     "src/cleaning/demographic_data_cleaner_2021.py"),
    ("Chômage",              "src/cleaning/unemployment_data_cleaner.py"),
    ("Délinquance",          "src/cleaning/delinquency_data_cleaner.py"),
    ("Revenus médians",      "src/cleaning/economic_data_cleaner.py"),
    ("Diplômes",             "src/cleaning/education_data_cleaner.py"),
]

def run_script(label, chemin):
    path = Path(chemin)
    if not path.exists():
        warn(f"Script absent : {chemin} — ignoré")
        return None, 0
    print(f"\n  {YELLOW}▶  {label}{RESET}  ({chemin})")
    t0 = time.time()
    result = subprocess.run([sys.executable, str(path)])
    duree = time.time() - t0
    if result.returncode == 0:
        ok(f"OK — {duree:.1f}s")
        return True, duree
    else:
        erreur(f"Échec (code {result.returncode})")
        return False, duree

def main():
    titre("ElectioAnalytics — Nettoyage des données brutes")

    resultats = []
    t_total = time.time()

    for label, chemin in ETAPES_NETTOYAGE:
        succes, duree = run_script(label, chemin)
        resultats.append((label, succes, duree))

    titre("Résumé")
    ok_count   = sum(1 for _, s, _ in resultats if s is True)
    err_count  = sum(1 for _, s, _ in resultats if s is False)
    skip_count = sum(1 for _, s, _ in resultats if s is None)

    for label, succes, duree in resultats:
        icone = "✅" if succes else ("❌" if succes is False else "⏭")
        print(f"  {icone}  {label:<30} {duree:>5.1f}s")

    print(f"\n  Résultat : {ok_count} OK  |  {err_count} erreurs  |  {skip_count} ignorés")
    print(f"  Durée totale : {time.time() - t_total:.1f}s")

    if err_count == 0:
        print(f"\n{GREEN}{BOLD}  Nettoyage terminé !{RESET}")
        print(f"  Données disponibles dans data/curated/")
        print(f"\n  Prochaine étape :")
        print(f"  → python scripts/run_pipeline.py   (build_dataset + preprocessing)")
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
