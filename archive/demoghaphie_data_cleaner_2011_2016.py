import pandas as pd
import os

# ---- CHEMINS ----
RAW_PATH = "data/raw/demographie"
CURATED_PATH = "data/curated/demographie"

os.makedirs(CURATED_PATH, exist_ok=True)


def nettoyer_population(nom_fichier, annee):
    """Nettoie un fichier population INSEE"""

    fichier_raw = os.path.join(RAW_PATH, nom_fichier)
    print(f"\n📂 Lecture de {fichier_raw}...")

    # Lire d'abord le fichier sans header pour inspecter la structure
    df_raw = pd.read_excel(fichier_raw, sheet_name="Communes", header=None)

    print(f"📊 Structure brute : {df_raw.shape[0]} lignes, {df_raw.shape[1]} colonnes")
    print("🔍 Premières lignes :")
    for i in range(min(10, len(df_raw))):
        print(f"   Ligne {i}: {df_raw.iloc[i].tolist()[:5]}...")  # Afficher les 5 premières colonnes

    # Trouver la ligne qui contient les en-têtes
    header_row = None
    for idx, row in df_raw.iterrows():
        # Convertir la ligne en chaîne et chercher les mots-clés
        row_str = ' '.join([str(val) for val in row.values if pd.notna(val)])
        if 'Code région' in row_str or 'Code département' in row_str or 'Nom de la commune' in row_str:
            header_row = idx
            print(f"\n✅ En-têtes trouvés à la ligne {idx}")
            break

    if header_row is None:
        print("⚠️ En-têtes non trouvés, utilisation de la ligne 8 par défaut")
        header_row = 8

    # Re-lire le fichier avec le bon header
    df = pd.read_excel(
        fichier_raw,
        sheet_name="Communes",
        header=header_row
    )

    print(f"\n✅ Fichier chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")
    print(f"📋 Colonnes détectées : {list(df.columns)}")

    # Nettoyer les noms de colonnes (supprimer les espaces superflus, retours à la ligne)
    df.columns = df.columns.str.strip().str.replace('\n', ' ').str.replace('\r', '')

    print(f"📋 Colonnes après nettoyage : {list(df.columns)}")

    # ---- FILTRAGE COLONNES UTILES ----
    cols_utiles = [
        "Code région",
        "Code département",
        "Code arrondissement",
        "Code canton",
        "Code commune",
        "Nom de la commune",
        "Population municipale",
        "Population comptée à part",
        "Population totale"
    ]

    # Vérifier quelles colonnes existent
    cols_existantes = [col for col in cols_utiles if col in df.columns]
    print(f"✅ Colonnes trouvées : {cols_existantes}")

    if not cols_existantes:
        print("❌ Aucune colonne attendue trouvée. Voici toutes les colonnes disponibles :")
        print(list(df.columns))
        return pd.DataFrame()  # Retourner un DataFrame vide

    df = df[cols_existantes].copy()

    # ---- SUPPRESSION DES LIGNES VIDES ----
    if "Code commune" in df.columns:
        df = df.dropna(subset=["Code commune"])
    if "Nom de la commune" in df.columns and len(df) > 0:
        df = df.dropna(subset=["Nom de la commune"])

    print(f"✅ Après suppression lignes vides : {df.shape[0]} lignes")

    # ---- FILTRAGE NORD (59) ----
    if "Code département" in df.columns:
        df["Code département"] = df["Code département"].astype(str).str.strip()
        df = df[df["Code département"] == "59"].copy()
        print(f"✅ Après filtrage Nord (59) : {df.shape[0]} lignes")
    else:
        print("⚠️ Colonne 'Code département' non trouvée, impossible de filtrer")

    if len(df) == 0:
        print("⚠️ Aucune donnée pour le Nord (59)")
        return df

    # ---- STANDARDISATION CODE COMMUNE ----
    if "Code département" in df.columns and "Code commune" in df.columns:
        df["Code département"] = df["Code département"].astype(str).str.zfill(2)
        df["Code commune"] = df["Code commune"].astype(str).str.zfill(3)
        df["code_commune"] = df["Code département"] + df["Code commune"]
    else:
        print("⚠️ Colonnes de code manquantes")
        df["code_commune"] = None

    # ---- RENOMMAGE DES COLONNES ----
    rename_dict = {
        "Code région": "code_region",
        "Code département": "code_departement",
        "Code arrondissement": "code_arrondissement",
        "Code canton": "code_canton",
        "Nom de la commune": "libelle_commune",
        "Population municipale": "population_municipale",
        "Population comptée à part": "population_comptee_a_part",
        "Population totale": "population_totale"
    }

    # Renommer uniquement les colonnes qui existent
    cols_a_renommer = {k: v for k, v in rename_dict.items() if k in df.columns}
    if cols_a_renommer:
        df = df.rename(columns=cols_a_renommer)

    # ---- TYPES DE DONNÉES ----
    for col in ["population_municipale", "population_comptee_a_part", "population_totale"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ---- AJOUT ANNÉE ----
    df["annee"] = annee

    # ---- RÉORGANISATION DES COLONNES ----
    colonnes_finales = [
        "code_commune",
        "libelle_commune",
        "code_region",
        "code_departement",
        "code_arrondissement",
        "code_canton",
        "population_municipale",
        "population_comptee_a_part",
        "population_totale",
        "annee"
    ]

    # Garder uniquement les colonnes qui existent
    colonnes_finales_existantes = [col for col in colonnes_finales if col in df.columns]
    df = df[colonnes_finales_existantes]

    # ---- VÉRIFICATION ----
    if len(df) > 0:
        print(f"\n📊 Résumé :")
        print(f"   Lignes totales   : {len(df)}")
        if "code_commune" in df.columns:
            print(f"   Communes uniques : {df['code_commune'].nunique()}")
        if "population_municipale" in df.columns:
            print(f"   Population min   : {df['population_municipale'].min()}")
            print(f"   Population max   : {df['population_municipale'].max()}")
        print(f"   Valeurs manquantes : {df.isnull().sum().sum()}")
        print(f"\n🔍 Aperçu :")
        print(df.head(10).to_string())
    else:
        print("⚠️ Aucune donnée après traitement")

    return df


# ---- TRAITEMENT 2011 ----
try:
    df_2011 = nettoyer_population("population2011.xls", 2011)
    if len(df_2011) > 0:
        fichier_2011 = os.path.join(CURATED_PATH, "population_2011.csv")
        df_2011.to_csv(fichier_2011, index=False, encoding="utf-8")
        print(f"\n✅ Sauvegardé → {fichier_2011}")
    else:
        print("❌ Aucune donnée pour 2011, fichier non sauvegardé")
except Exception as e:
    print(f"❌ Erreur lors du traitement de 2011 : {e}")

# ---- TRAITEMENT 2016 ----
try:
    df_2016 = nettoyer_population("population2016.xls", 2016)
    if len(df_2016) > 0:
        fichier_2016 = os.path.join(CURATED_PATH, "population_2016.csv")
        df_2016.to_csv(fichier_2016, index=False, encoding="utf-8")
        print(f"\n✅ Sauvegardé → {fichier_2016}")
    else:
        print("❌ Aucune donnée pour 2016, fichier non sauvegardé")
except Exception as e:
    print(f"❌ Erreur lors du traitement de 2016 : {e}")

# ---- FICHIER GLOBAL ----
if 'df_2011' in locals() and 'df_2016' in locals() and len(df_2011) > 0 and len(df_2016) > 0:
    df_global = pd.concat([df_2011, df_2016], ignore_index=True)
    fichier_global = os.path.join(CURATED_PATH, "population_2011_2016.csv")
    df_global.to_csv(fichier_global, index=False, encoding="utf-8")
    print(f"\n✅ Fichier global → {fichier_global} ({len(df_global)} lignes)")
else:
    print("\n⚠️ Impossible de créer le fichier global (données manquantes)")

print("\n✅ Traitement terminé !")