import pandas as pd

# ============================================================
# test_pivot.py
# Vérifie que le correctif bloc_vainqueur est bien appliqué
# À lancer APRÈS pivot_election.py
# ============================================================

print("=" * 60)
print("🧪 TEST — Vérification correctif bloc_vainqueur")
print("=" * 60)

# ---- CHARGEMENT ----
df_2017 = pd.read_csv("data/final/elections_2017_pivot.csv", dtype={"code_geo": str})
df_2022 = pd.read_csv("data/final/elections_2022_pivot.csv", dtype={"code_geo": str})

# ============================================================
# TEST 1 — Distribution des blocs vainqueurs
# Attendu 2017 : DROITE ~598 / CENTRE ~36 / GAUCHE ~14
# Attendu 2022 : DROITE ~574 / CENTRE ~54 / GAUCHE ~20
# ============================================================
print("\n📊 TEST 1 — Distribution bloc_vainqueur")
print("-" * 40)

for annee, df in [("2017", df_2017), ("2022", df_2022)]:
    print(f"\n   {annee} :")
    counts = df["bloc_vainqueur"].value_counts()
    pcts   = df["bloc_vainqueur"].value_counts(normalize=True) * 100
    for bloc in counts.index:
        print(f"   {bloc:<10} : {counts[bloc]:>4} communes  ({pcts[bloc]:.1f}%)")
    print(f"   {'TOTAL':<10} : {len(df):>4} communes")

# ============================================================
# TEST 2 — Cohérence bloc_vainqueur vs nom_vainqueur
# Le bloc du vainqueur doit correspondre au mapping candidat→bloc
# ============================================================
print("\n\n📊 TEST 2 — Cohérence nom_vainqueur / bloc_vainqueur")
print("-" * 40)

mapping_nom_bloc_attendu = {
    "LE PEN":        "DROITE",
    "FILLON":        "DROITE",
    "DUPONT-AIGNAN": "DROITE",
    "ZEMMOUR":       "DROITE",
    "PÉCRESSE":      "DROITE",
    "MACRON":        "CENTRE",
    "MÉLENCHON":     "GAUCHE",
    "HAMON":         "GAUCHE",
    "HIDALGO":       "GAUCHE",
    "JADOT":         "GAUCHE",
    "ROUSSEL":       "GAUCHE",
    "LASSALLE":      "DIVERS",
    "ASSELINEAU":    "DIVERS",
    "ARTHAUD":       "GAUCHE",
    "POUTOU":        "GAUCHE",
    "CHEMINADE":     "DIVERS",
}

for annee, df in [("2017", df_2017), ("2022", df_2022)]:
    erreurs = []
    for _, row in df.iterrows():
        nom   = row.get("nom_vainqueur", "")
        bloc  = row.get("bloc_vainqueur", "")
        attendu = mapping_nom_bloc_attendu.get(str(nom).upper(), None)
        if attendu and attendu != bloc:
            erreurs.append({
                "commune":  row.get("libelle_commune"),
                "nom":      nom,
                "bloc_reel": bloc,
                "bloc_attendu": attendu
            })

    if erreurs:
        print(f"\n   ❌ {annee} : {len(erreurs)} incohérences détectées")
        for e in erreurs[:5]:
            print(f"      {e['commune']:<25} {e['nom']:<15} → {e['bloc_reel']} (attendu {e['bloc_attendu']})")
    else:
        print(f"\n   ✅ {annee} : 0 incohérence nom/bloc")

# ============================================================
# TEST 3 — Vérification manuelle sur communes connues
# Lille, Roubaix, Tourcoing → grandes villes, doivent être GAUCHE ou CENTRE
# ============================================================
print("\n\n📊 TEST 3 — Vérification communes connues")
print("-" * 40)

communes_test = ["Lille", "Roubaix", "Tourcoing", "Valenciennes", "Dunkerque"]

for annee, df in [("2017", df_2017), ("2022", df_2022)]:
    print(f"\n   {annee} :")
    for commune in communes_test:
        row = df[df["libelle_commune"].str.upper() == commune.upper()]
        if len(row) == 0:
            print(f"   {commune:<20} : ⚠️  non trouvée")
        else:
            row = row.iloc[0]
            print(f"   {commune:<20} : {row['nom_vainqueur']:<15} → {row['bloc_vainqueur']}")

# ============================================================
# TEST 4 — Avant/Après : comparer avec l'ancienne logique
# Si CENTRE = 0 en 2017 → bug encore présent
# ============================================================
print("\n\n📊 TEST 4 — Détection du bug résiduel")
print("-" * 40)

centre_2017 = (df_2017["bloc_vainqueur"] == "CENTRE").sum()
gauche_2017 = (df_2017["bloc_vainqueur"] == "GAUCHE").sum()
centre_2022 = (df_2022["bloc_vainqueur"] == "CENTRE").sum()

if centre_2017 == 0:
    print("\n   ❌ BUG TOUJOURS PRÉSENT : CENTRE = 0 en 2017")
    print("      → pivot_election.py n'a pas été correctement relancé")
elif centre_2017 < 30:
    print(f"\n   ⚠️  CENTRE 2017 = {centre_2017} communes (attendu ~36) — à vérifier")
else:
    print(f"\n   ✅ CENTRE 2017 = {centre_2017} communes — cohérent")

if gauche_2017 < 10:
    print(f"   ⚠️  GAUCHE 2017 = {gauche_2017} communes (attendu ~14) — à vérifier")
else:
    print(f"   ✅ GAUCHE 2017 = {gauche_2017} communes — cohérent")

if centre_2022 < 40:
    print(f"   ⚠️  CENTRE 2022 = {centre_2022} communes (attendu ~54) — à vérifier")
else:
    print(f"   ✅ CENTRE 2022 = {centre_2022} communes — cohérent")

# ============================================================
# TEST 5 — Résumé global
# ============================================================
print("\n\n📊 TEST 5 — Top vainqueurs par commune")
print("-" * 40)

for annee, df in [("2017", df_2017), ("2022", df_2022)]:
    print(f"\n   {annee} :")
    print(df["nom_vainqueur"].value_counts().to_string())

print("\n" + "=" * 60)
print("✅ Tests terminés")
print("=" * 60)