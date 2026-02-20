-- ============================================================================
-- VÉRIFICATION DES DOUBLONS
-- ============================================================================
SELECT reference, COUNT(*)
FROM staging.joconde
GROUP BY reference
HAVING COUNT(*) > 1
ORDER BY reference;

-- ============================================================================
-- TAILLE DES COLONNES
-- ============================================================================
SELECT
    MAX(LENGTH(reference))      AS max_len_reference,
    MAX(LENGTH(appellation))    AS max_len_appellation,
    MAX(LENGTH(auteur))         AS max_len_auteur,
    MAX(LENGTH(date_creation))  AS max_len_date_creation,
    MAX(LENGTH(region))         AS max_len_region,
    MAX(LENGTH(departement))    AS max_len_departement,
    MAX(LENGTH(description))    AS max_len_description,
    MAX(LENGTH(source_system))  AS max_len_source_system,
    MAX(LENGTH(load_process))   AS max_len_load_process
FROM staging.joconde;

-- ============================================================================
-- TABLE DE RÉFÉRENCE
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS ref;

CREATE TABLE ref.departement_region (
    departement VARCHAR(30) PRIMARY KEY NOT NULL,
    region VARCHAR(30) NOT NULL
);

INSERT INTO ref.departement_region
SELECT DISTINCT COALESCE(departement, 'inconnu'), COALESCE(region, 'inconnu')
FROM staging.joconde;

-- ============================================================================
-- CRÉATION DE LA TABLE DE DESTINATION
-- ============================================================================
CREATE TABLE joconde_oeuvre (
    reference VARCHAR(20) PRIMARY KEY NOT NULL,
    appellation TEXT,
    auteur TEXT,
    annee_creation INTEGER,
    departement VARCHAR(30) REFERENCES ref.departement_region (departement),
    description TEXT,
    date_import_utc TIMESTAMPTZ,
    source_system VARCHAR(20)
);