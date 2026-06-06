-- DIMENSION COMMUNE
CREATE TABLE IF NOT EXISTS dim_commune (
    id_commune            SERIAL PRIMARY KEY,
    code_geo              VARCHAR(10)  NOT NULL,
    code_commune          VARCHAR(10)  NOT NULL,
    libelle_commune       VARCHAR(100) NOT NULL,
    population_municipale INTEGER,
    population_totale     INTEGER,
    UNIQUE (code_geo, code_commune)
);

-- DIMENSION TEMPS
CREATE TABLE IF NOT EXISTS dim_temps (
    id_temps       SERIAL PRIMARY KEY,
    annee          SMALLINT NOT NULL CHECK (annee IN (2017, 2022)),
    type_election  VARCHAR(50) DEFAULT 'Présidentielle',
    tour           SMALLINT   DEFAULT 1
);

-- DIMENSION SOCIO
CREATE TABLE IF NOT EXISTS dim_socio (
    id_socio             SERIAL PRIMARY KEY,
    id_commune           INT REFERENCES dim_commune(id_commune),
    id_temps             INT REFERENCES dim_temps(id_temps),
    taux_chomage         NUMERIC(5,2),
    revenu_median        NUMERIC(10,2),
    taux_faible_diplome  NUMERIC(5,2),
    taux_delinquance     NUMERIC(8,2),
    nb_faits_total       INT
);

-- FAIT ELECTORAL
CREATE TABLE IF NOT EXISTS fait_electoral (
    id_fait          SERIAL PRIMARY KEY,
    id_commune       INT NOT NULL REFERENCES dim_commune(id_commune),
    id_temps         INT NOT NULL REFERENCES dim_temps(id_temps),
    id_socio         INT REFERENCES dim_socio(id_socio),
    inscrits         INT,
    abstentions      INT,
    votants          INT,
    exprimes         INT,
    pct_abstention   NUMERIC(5,2),
    pct_centre       NUMERIC(5,2),
    pct_droite       NUMERIC(5,2),
    pct_gauche       NUMERIC(5,2),
    pct_divers       NUMERIC(5,2),
    bloc_vainqueur   VARCHAR(20),
    nom_vainqueur    VARCHAR(100),
    cible_binaire    SMALLINT CHECK (cible_binaire IN (0,1))
);

-- DIMENSION CANDIDAT
CREATE TABLE IF NOT EXISTS dim_candidat (
    id_candidat    SERIAL PRIMARY KEY,
    id_fait        INT NOT NULL REFERENCES fait_electoral(id_fait),
    rang           SMALLINT NOT NULL CHECK (rang BETWEEN 1 AND 5),
    nom_candidat   VARCHAR(100) NOT NULL,
    bloc_politique VARCHAR(20),
    pct_voix       NUMERIC(5,2),
    est_vainqueur  BOOLEAN DEFAULT FALSE
);

-- INDEX
CREATE INDEX IF NOT EXISTS idx_fait_commune  ON fait_electoral(id_commune);
CREATE INDEX IF NOT EXISTS idx_fait_temps    ON fait_electoral(id_temps);
CREATE INDEX IF NOT EXISTS idx_candidat_fait ON dim_candidat(id_fait);
CREATE INDEX IF NOT EXISTS idx_socio_commune ON dim_socio(id_commune);