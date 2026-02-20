-- ============================================================================
-- IMPORTATION.SQL - POSTGRESQL
-- Chargement des données depuis staging vers production
-- ============================================================================

-- Vider la table avant insertion
TRUNCATE TABLE joconde_oeuvre;

-- Insertion des données
INSERT INTO joconde_oeuvre (
    reference,
    appellation,
    auteur,
    annee_creation,
    departement,
    description,
    date_import_utc,
    source_system
)
SELECT
    -- Nettoyage des identifiants
    TRIM(reference) AS reference,
    
    -- Formatage des colonnes texte
    TRIM(appellation) AS appellation,
    TRIM(auteur) AS auteur,
    
    -- Extraction de l'année depuis une chaîne comme "1997" ou "19e siècle"
    CASE 
        WHEN SUBSTRING(TRIM(date_creation), 1, 4) ~ '^\d{4}$' 
        THEN SUBSTRING(TRIM(date_creation), 1, 4)::INTEGER
        ELSE NULL
    END AS annee_creation,
    
    -- On ne garde que les départements qui existent dans la table de référence
    TRIM(s.departement) AS departement,
    
    -- Description éventuellement tronquée si très longue
    CASE 
        WHEN LENGTH(description) > 1000 
        THEN SUBSTRING(description, 1, 1000) || '...'
        ELSE description
    END AS description,
    
    -- Reprise du timestamp d'import
    s.load_timestamp_utc,
    s.source_system
FROM staging.joconde s
INNER JOIN ref.departement_region d
    ON COALESCE(s.departement, 'inconnu') = d.departement
WHERE
    s.reference IS NOT NULL
    AND SUBSTRING(TRIM(s.date_creation), 1, 4) ~ '^[1-2][0-9]{3}$'  -- filtrage années plausibles
    AND SUBSTRING(TRIM(s.date_creation), 1, 4)::INTEGER 
        BETWEEN 1000 AND EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER;

-- Vérification du nombre de lignes insérées
SELECT COUNT(*) as nb_lignes_inserees FROM joconde_oeuvre;

-- Comparaison staging vs production
SELECT 
    'staging.joconde' as table_name,
    COUNT(*) as nb_lignes
FROM staging.joconde
UNION ALL
SELECT 
    'joconde_oeuvre (production)',
    COUNT(*)
FROM joconde_oeuvre;

-- Statistiques finales
SELECT 
    COUNT(*) as total_oeuvres,
    COUNT(DISTINCT departement) as nb_departements,
    COUNT(DISTINCT auteur) as nb_auteurs,
    COUNT(CASE WHEN annee_creation IS NOT NULL THEN 1 END) as nb_avec_annee,
    MIN(annee_creation) as annee_min,
    MAX(annee_creation) as annee_max
FROM joconde_oeuvre;

SELECT 'Importation terminée avec succès !' as status;