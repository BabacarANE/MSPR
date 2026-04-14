import pandas as pd
import numpy as np

elections_2017 = pd.read_csv("/Users/Princia/MSPR/data/final/elections_2017_pivot.csv")
demographie_2016 = pd.read_csv("/Users/Princia/MSPR/data/curated/demographie/population_2016.csv")
emploi_2016 = pd.read_csv("/Users/Princia/MSPR/data/curated/emploi/chomage_2016.csv")

def clean_insee(df):
    df["code_geo"] = df["code_geo"].astype(str).str.strip().str.zfill(5)
    return df

elections_2017   = clean_insee(elections_2017)
demographie_2016 = clean_insee(demographie_2016)
emploi_2016      = clean_insee(emploi_2016)


# ---- SUPPRESSION DOUBLONS (SÉCURITÉ) ----
demographie_2016 = demographie_2016.drop_duplicates(subset=["code_geo"])
emploi_2016      = emploi_2016.drop_duplicates(subset=["code_geo"])


# ---- MERGE ----
df_2017 = elections_2017.copy()

df_2017 = df_2017.merge(demographie_2016, on="code_geo", how="left")
df_2017 = df_2017.merge(emploi_2016,      on="code_geo", how="left")

# ---- VÉRIFICATIONS ----
print("\n📊 Vérification après merge :")
print("Shape :", df_2017.shape)
print("\nNaN par colonne :")
print(df_2017.isnull().sum().sort_values(ascending=False).head(10))

# ---- GESTION DES VALEURS MANQUANTES ----
cols_num = df_2017.select_dtypes(include=np.number).columns
df_2017[cols_num] = df_2017[cols_num].fillna(df_2017[cols_num].mean())

# ---- AJOUT ANNÉE (sécurité si absent) ----
df_2017["annee"] = 2017

# ---- SAUVEGARDE ----
df_2017.to_csv(PATH + "dataset_2017_ready.csv", index=False)

print("\n✅ Dataset 2017 prêt :", df_2017.shape)