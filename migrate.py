"""
ETL : dataset_ml_final.csv → PostgreSQL
pip install pandas psycopg2-binary sqlalchemy
Usage : python migrate.py --csv data/final/dataset_ml_final.csv
"""

import argparse
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://electio_user:electio_pass@localhost:5432/electio_db"


def load_csv(path):
    df = pd.read_csv(path, dtype={"code_geo": str, "code_commune": str})
    print(f"✅ CSV chargé : {len(df)} lignes")
    return df


def insert_communes(df, engine):
    with engine.connect() as conn:
        communes = df[["code_geo","code_commune","libelle_commune",
                        "population_municipale","population_totale"]].drop_duplicates()
        for _, r in communes.iterrows():
            conn.execute(text("""
                INSERT INTO dim_commune (code_geo,code_commune,libelle_commune,
                    population_municipale,population_totale)
                VALUES (:cg,:cc,:lib,:pm,:pt)
                ON CONFLICT (code_geo,code_commune) DO NOTHING
            """), dict(cg=r.code_geo, cc=r.code_commune, lib=r.libelle_commune,
                       pm=r.get("population_municipale"), pt=r.get("population_totale")))
        conn.commit()
        rows = conn.execute(text(
            "SELECT id_commune,code_geo,code_commune FROM dim_commune")).fetchall()
    print(f"  ↳ dim_commune : {len(rows)} communes")
    return {(r.code_geo, r.code_commune): r.id_commune for r in rows}


def insert_temps(df, engine):
    with engine.connect() as conn:
        for annee in df["annee"].unique():
            conn.execute(text("""
                INSERT INTO dim_temps (annee,type_election,tour)
                VALUES (:a,'Présidentielle',1)
                ON CONFLICT DO NOTHING
            """), {"a": int(annee)})
        conn.commit()
        rows = conn.execute(text("SELECT id_temps,annee FROM dim_temps")).fetchall()
    print(f"  ↳ dim_temps : {[r.annee for r in rows]}")
    return {r.annee: r.id_temps for r in rows}


def migrate(csv_path):
    print("\n🚀 Migration Electio-Analytics\n")
    engine = create_engine(DB_URL)
    df = load_csv(csv_path)
    commune_map = insert_communes(df, engine)
    temps_map   = insert_temps(df, engine)

    errors = 0
    with engine.connect() as conn:
        for i, row in df.iterrows():
            try:
                id_commune = commune_map[(row["code_geo"], row["code_commune"])]
                id_temps   = temps_map[row["annee"]]

                id_socio = conn.execute(text("""
                    INSERT INTO dim_socio (id_commune,id_temps,taux_chomage,
                        revenu_median,taux_faible_diplome,taux_delinquance,nb_faits_total)
                    VALUES (:ic,:it,:tc,:rm,:td,:tdel,:nb) RETURNING id_socio
                """), dict(ic=id_commune, it=id_temps,
                           tc=row.get("taux_chomage"), rm=row.get("revenu_median"),
                           td=row.get("taux_faible_diplome"),
                           tdel=row.get("taux_delinquance"),
                           nb=row.get("nb_faits_total"))).fetchone()[0]

                id_fait = conn.execute(text("""
                    INSERT INTO fait_electoral (id_commune,id_temps,id_socio,
                        inscrits,abstentions,votants,exprimes,pct_abstention,
                        pct_centre,pct_droite,pct_gauche,pct_divers,
                        bloc_vainqueur,nom_vainqueur,cible_binaire)
                    VALUES (:ic,:it,:is_,:ins,:abs,:vot,:exp,:pabs,
                            :pc,:pd,:pg,:pdiv,:bv,:nv,:cb)
                    RETURNING id_fait
                """), dict(ic=id_commune, it=id_temps, is_=id_socio,
                           ins=row.get("inscrits"), abs=row.get("abstentions"),
                           vot=row.get("votants"), exp=row.get("exprimes"),
                           pabs=row.get("pct_abstention"),
                           pc=row.get("pct_CENTRE"), pd=row.get("pct_DROITE"),
                           pg=row.get("pct_GAUCHE"), pdiv=row.get("pct_DIVERS"),
                           bv=row.get("bloc_vainqueur"), nv=row.get("nom_vainqueur"),
                           cb=int(row.get("cible_binaire", 0)))).fetchone()[0]

                vainqueur = row.get("nom_vainqueur", "")
                for rang in range(1, 6):
                    nom  = row.get(f"nom_cand{rang}")
                    pct  = row.get(f"pct_cand{rang}")
                    bloc = row.get(f"bloc_cand{rang}")
                    if pd.isna(nom) or nom == "":
                        continue
                    conn.execute(text("""
                        INSERT INTO dim_candidat
                            (id_fait,rang,nom_candidat,bloc_politique,pct_voix,est_vainqueur)
                        VALUES (:if_,:r,:n,:b,:p,:ev)
                    """), dict(if_=id_fait, r=rang, n=str(nom),
                               b=str(bloc) if not pd.isna(bloc) else None,
                               p=float(pct) if not pd.isna(pct) else None,
                               ev=(str(nom) == str(vainqueur))))

            except Exception as e:
                print(f"  ⚠️ Ligne {i} ignorée : {e}")
                errors += 1
                conn.rollback()
                continue

        conn.commit()

    print(f"\n✅ Terminé — {len(df)-errors}/{len(df)} lignes importées")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    args = parser.parse_args()
    migrate(args.csv)