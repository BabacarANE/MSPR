"""
scripts/fix_paths.py
====================
Corrige automatiquement les chemins relatifs dans tous les scripts
déplacés dans src/cleaning/ et src/pipeline/.

Exécuter UNE SEULE FOIS après reorganize.py :
    python scripts/fix_paths.py

Transformation appliquée à chaque script :
    Avant  →  RAW_PATH = "data/raw/elections"
    Après  →  BASE_DIR = Path(__file__).resolve().parent.parent.parent
              RAW_PATH = str(BASE_DIR / "data/raw/elections")
"""

import re
from pathlib import Path

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── Scripts à patcher et leurs chemins data/ utilisés ────────────────────────
# Format : (chemin_script, [liste des variables de chemin à corriger])
SCRIPTS = [
    # cleaning (2 niveaux au-dessus = racine)
    ("src/cleaning/electoral_data_cleaner_2017.py", 2),
    ("src/cleaning/electoral_data_cleaner_2022.py", 2),
    ("src/cleaning/pivot_election.py",              2),
    ("src/cleaning/demographic_data_cleaner_2016.py", 2),
    ("src/cleaning/demographic_data_cleaner_2021.py", 2),
    ("src/cleaning/unemployment_data_cleaner.py",   2),
    ("src/cleaning/delinquency_data_cleaner.py",    2),
    ("src/cleaning/economic_data_cleaner.py",       2),
    ("src/cleaning/education_data_cleaner.py",      2),
    # pipeline (aussi 2 niveaux)
    ("src/pipeline/build_dataset.py",               2),
    ("src/pipeline/preprocessing.py",               2),
    ("src/pipeline/merge_2017.py",                  2),
    ("src/pipeline/merge_2022.py",                  2),
]

# En-tête à injecter en haut de chaque script (après les imports existants)
HEADER_TEMPLATE = """\
from pathlib import Path as _Path
_BASE_DIR = _Path(__file__).resolve().parents[{levels}]
"""

def patcher_script(chemin_str, niveaux):
    """
    Patch un script Python :
    1. Ajoute l'import Path + calcul BASE_DIR
    2. Remplace toutes les chaînes "data/..." par str(_BASE_DIR / "data/...")
    """
    path = Path(chemin_str)
    if not path.exists():
        print(f"  {YELLOW}⏭  Absent : {chemin_str}{RESET}")
        return False

    contenu = path.read_text(encoding="utf-8")

    # ── Vérifier si déjà patché ──────────────────────────────────────────────
    if "_BASE_DIR" in contenu:
        print(f"  {YELLOW}⏭  Déjà patché : {chemin_str}{RESET}")
        return False

    # ── 1. Trouver la position après le bloc d'imports ───────────────────────
    lignes = contenu.split("\n")
    derniere_import = 0
    for i, ligne in enumerate(lignes):
        stripped = ligne.strip()
        if stripped.startswith("import ") or stripped.startswith("from "):
            derniere_import = i

    # Insérer le header après le dernier import
    header = HEADER_TEMPLATE.format(levels=niveaux)
    lignes.insert(derniere_import + 1, "\n" + header)
    contenu = "\n".join(lignes)

    # ── 2. Remplacer les chemins "data/..." ───────────────────────────────────
    # Pattern : toute chaîne entre guillemets commençant par "data/"
    def remplacer_chemin(match):
        guillemet = match.group(1)   # " ou '
        chemin    = match.group(2)   # ex: data/raw/elections
        return f'str(_BASE_DIR / {guillemet}{chemin}{guillemet})'

    # Remplace "data/xxx" et 'data/xxx' (pas os.path.join déjà géré)
    contenu = re.sub(
        r'(["\'])(data/[^"\']+)(["\'])',
        lambda m: f'str(_BASE_DIR / "{m.group(2)}")',
        contenu
    )

    path.write_text(contenu, encoding="utf-8")
    print(f"  {GREEN}✅ Patché : {chemin_str}{RESET}")
    return True


def main():
    print(f"\n{BOLD}{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}{CYAN}  Correction des chemins relatifs{RESET}")
    print(f"{BOLD}{CYAN}{'─'*60}{RESET}\n")

    ok_count = skip_count = 0
    for chemin, niveaux in SCRIPTS:
        patché = patcher_script(chemin, niveaux)
        if patché:
            ok_count += 1
        else:
            skip_count += 1

    print(f"\n  {ok_count} scripts patchés  |  {skip_count} ignorés")

    if ok_count > 0:
        print(f"\n{GREEN}{BOLD}  Chemins corrigés !{RESET}")
        print(f"  Tous les scripts utilisent maintenant des chemins absolus.")
        print(f"  Tu peux lancer le pipeline depuis n'importe quel répertoire.")
    else:
        print(f"\n{YELLOW}  Aucun script patché (tous déjà corrects ou absents).{RESET}")


if __name__ == "__main__":
    main()
