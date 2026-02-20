-- ============================================================================
-- ANALYSES STAGING.JOCONDE - POSTGRESQL
-- ============================================================================

-- Connexion à la base (commentaire - à faire dans pgAdmin ou psql)
-- \c joconde_staging

-- ============================================================================
-- 1. COMPTAGE TOTAL
-- ============================================================================
SELECT COUNT(*) as total_oeuvres FROM staging.joconde;

-- ============================================================================
-- 2. NOMBRE D'ŒUVRES PAR RÉGION
-- ============================================================================
SELECT
    region,
    COUNT(*) AS nombre_oeuvres,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM staging.joconde
WHERE region IS NOT NULL
GROUP BY region
ORDER BY nombre_oeuvres DESC;

-- ============================================================================
-- 3. RÉPARTITION PAR DÉPARTEMENT ET PRÉSENCE D'AUTEUR
-- ============================================================================
SELECT
    departement,
    COUNT(*) AS total,
    COUNT(CASE WHEN auteur IS NULL THEN 1 END) AS sans_auteur,
    COUNT(CASE WHEN auteur IS NOT NULL THEN 1 END) AS avec_auteur,
    ROUND(COUNT(CASE WHEN auteur IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_avec_auteur
FROM staging.joconde
WHERE departement IS NOT NULL
GROUP BY departement
ORDER BY total DESC;

-- ============================================================================
-- 4. AUTEURS LES PLUS FRÉQUENTS (TOP 10)
-- ============================================================================
SELECT
    auteur,
    COUNT(*) AS oeuvres
FROM staging.joconde
WHERE auteur IS NOT NULL
GROUP BY auteur
ORDER BY oeuvres DESC
LIMIT 10;

-- ============================================================================
-- 5. LONGUEUR MOYENNE DES DESCRIPTIONS PAR RÉGION
-- ============================================================================
SELECT
    region,
    ROUND(AVG(LENGTH(description))::numeric, 2) AS longueur_moyenne,
    COUNT(CASE WHEN description IS NOT NULL THEN 1 END) AS nb_avec_description,
    COUNT(*) AS total_oeuvres
FROM staging.joconde
WHERE region IS NOT NULL
GROUP BY region
ORDER BY longueur_moyenne DESC NULLS LAST;

-- ============================================================================
-- 6. CRÉATION DE VUE : AUTEURS PAR DÉPARTEMENT
-- ============================================================================
CREATE OR REPLACE VIEW staging.v_auteurs_par_departement AS
SELECT
    departement,
    COUNT(*) AS total,
    COUNT(CASE WHEN auteur IS NULL THEN 1 END) AS sans_auteur,
    COUNT(CASE WHEN auteur IS NOT NULL THEN 1 END) AS avec_auteur,
    ROUND(COUNT(CASE WHEN auteur IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_avec_auteur
FROM staging.joconde
WHERE departement IS NOT NULL
GROUP BY departement
ORDER BY total DESC;

-- Utiliser la vue
SELECT * FROM staging.v_auteurs_par_departement LIMIT 20;

-- ============================================================================
-- 7. AUTRES VUES UTILES
-- ============================================================================

-- Vue : Statistiques par région
CREATE OR REPLACE VIEW staging.v_stats_regions AS
SELECT
    region,
    COUNT(*) AS total_oeuvres,
    COUNT(DISTINCT departement) AS nb_departements,
    COUNT(DISTINCT auteur) AS nb_auteurs,
    COUNT(CASE WHEN description IS NOT NULL THEN 1 END) AS avec_description,
    ROUND(AVG(LENGTH(description))::numeric, 2) AS longueur_moy_description
FROM staging.joconde
WHERE region IS NOT NULL
GROUP BY region
ORDER BY total_oeuvres DESC;

-- Vue : Top auteurs par région
CREATE OR REPLACE VIEW staging.v_top_auteurs_region AS
WITH auteurs_region AS (
    SELECT
        region,
        auteur,
        COUNT(*) AS nb_oeuvres,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY COUNT(*) DESC) AS rang
    FROM staging.joconde
    WHERE region IS NOT NULL AND auteur IS NOT NULL
    GROUP BY region, auteur
)
SELECT
    region,
    auteur,
    nb_oeuvres,
    rang
FROM auteurs_region
WHERE rang <= 5
ORDER BY region, rang;

-- Vue : Distribution des descriptions
CREATE OR REPLACE VIEW staging.v_distribution_descriptions AS
SELECT
    CASE 
        WHEN description IS NULL THEN '0. Sans description'
        WHEN LENGTH(description) < 50 THEN '1. Très courte (< 50 car.)'
        WHEN LENGTH(description) < 100 THEN '2. Courte (50-100 car.)'
        WHEN LENGTH(description) < 200 THEN '3. Moyenne (100-200 car.)'
        WHEN LENGTH(description) < 500 THEN '4. Longue (200-500 car.)'
        ELSE '5. Très longue (> 500 car.)'
    END AS categorie_description,
    COUNT(*) as nombre_oeuvres,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM staging.joconde
GROUP BY categorie_description
ORDER BY categorie_description;

-- ============================================================================
-- 8. ANALYSES AVANCÉES
-- ============================================================================

-- Top 10 départements par région
SELECT
    region,
    departement,
    COUNT(*) AS nb_oeuvres
FROM staging.joconde
WHERE region IS NOT NULL AND departement IS NOT NULL
GROUP BY region, departement
ORDER BY region, nb_oeuvres DESC;

-- Œuvres sans métadonnées importantes
SELECT
    COUNT(*) AS total_incomplet,
    COUNT(CASE WHEN auteur IS NULL THEN 1 END) AS sans_auteur,
    COUNT(CASE WHEN description IS NULL THEN 1 END) AS sans_description,
    COUNT(CASE WHEN auteur IS NULL AND description IS NULL THEN 1 END) AS sans_auteur_ni_desc,
    ROUND(COUNT(CASE WHEN auteur IS NULL AND description IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_incomplet
FROM staging.joconde;

-- Distribution temporelle des chargements
SELECT
    DATE(load_timestamp_utc) AS date_chargement,
    COUNT(*) AS nb_oeuvres_chargees,
    MIN(load_timestamp_utc) AS heure_debut,
    MAX(load_timestamp_utc) AS heure_fin
FROM staging.joconde
GROUP BY DATE(load_timestamp_utc)
ORDER BY date_chargement DESC;

-- Analyse de qualité des données
SELECT
    'Total œuvres' AS indicateur,
    COUNT(*)::text AS valeur
FROM staging.joconde
UNION ALL
SELECT
    'Avec référence',
    COUNT(CASE WHEN reference IS NOT NULL THEN 1 END)::text
FROM staging.joconde
UNION ALL
SELECT
    'Avec appellation',
    COUNT(CASE WHEN appellation IS NOT NULL THEN 1 END)::text
FROM staging.joconde
UNION ALL
SELECT
    'Avec auteur',
    COUNT(CASE WHEN auteur IS NOT NULL THEN 1 END)::text
FROM staging.joconde
UNION ALL
SELECT
    'Avec description',
    COUNT(CASE WHEN description IS NOT NULL THEN 1 END)::text
FROM staging.joconde
UNION ALL
SELECT
    'Avec région',
    COUNT(CASE WHEN region IS NOT NULL THEN 1 END)::text
FROM staging.joconde
UNION ALL
SELECT
    'Taux complétude (%)',
    ROUND(
        (COUNT(CASE WHEN reference IS NOT NULL AND auteur IS NOT NULL AND description IS NOT NULL THEN 1 END) * 100.0 / COUNT(*))::numeric,
        2
    )::text
FROM staging.joconde;

-- ============================================================================
-- 9. REQUÊTES DE VÉRIFICATION
-- ============================================================================

-- Vérifier l'absence de doublons sur la référence
SELECT
    reference,
    COUNT(*) AS nb_occurrences
FROM staging.joconde
WHERE reference IS NOT NULL
GROUP BY reference
HAVING COUNT(*) > 1
ORDER BY nb_occurrences DESC;

-- Statistiques sur les longueurs de champs
SELECT
    'reference' AS champ,
    MIN(LENGTH(reference)) AS min_longueur,
    MAX(LENGTH(reference)) AS max_longueur,
    ROUND(AVG(LENGTH(reference))::numeric, 2) AS moy_longueur
FROM staging.joconde
WHERE reference IS NOT NULL
UNION ALL
SELECT
    'auteur',
    MIN(LENGTH(auteur)),
    MAX(LENGTH(auteur)),
    ROUND(AVG(LENGTH(auteur))::numeric, 2)
FROM staging.joconde
WHERE auteur IS NOT NULL
UNION ALL
SELECT
    'description',
    MIN(LENGTH(description)),
    MAX(LENGTH(description)),
    ROUND(AVG(LENGTH(description))::numeric, 2)
FROM staging.joconde
WHERE description IS NOT NULL;

-- ============================================================================
-- 10. RÉSUMÉ COMPLET
-- ============================================================================
SELECT
    'RÉSUMÉ COMPLET STAGING.JOCONDE' AS titre,
    '================================================' AS separateur;

SELECT
    COUNT(*) as total_oeuvres,
    COUNT(DISTINCT region) as nb_regions,
    COUNT(DISTINCT departement) as nb_departements,
    COUNT(DISTINCT auteur) as nb_auteurs_uniques,
    COUNT(CASE WHEN description IS NOT NULL THEN 1 END) as nb_avec_description,
    ROUND(COUNT(CASE WHEN description IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_avec_description,
    MIN(load_timestamp_utc) as premiere_insertion,
    MAX(load_timestamp_utc) as derniere_insertion
FROM staging.joconde;