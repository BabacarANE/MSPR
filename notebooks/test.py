import pandas as pd
from pathlib import Path

BASE = Path(".")

df_demo = pd.read_csv(BASE / "../data/curated/demographie/population_2016.csv",
                      dtype={"code_commune": str})
print("Colonnes:", list(df_demo.columns))
print("code_commune exemples:", df_demo["code_commune"].head(5).tolist())

# Après construction code_geo
df_demo["code_geo"] = df_demo["code_commune"].str.strip().str.zfill(5)
print("code_geo exemples:", df_demo["code_geo"].head(5).tolist())

# Comparer avec le pivot électoral
df_elec = pd.read_csv(BASE / "data/final/elections_2017_pivot.csv",
                      dtype={"code_geo": str})
print("\ncode_geo électoral exemples:", df_elec["code_geo"].head(5).tolist())