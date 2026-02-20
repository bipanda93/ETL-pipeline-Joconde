SELECT 
    COALESCE(appellation, 'inconnu') AS appellation,
    COALESCE(auteur, 'inconnu') AS auteur,
    COALESCE(date_creation, 'inconnu') AS date_creation,
    COALESCE(region, 'inconnu') AS region, 
    COALESCE(departement, 'inconnu') AS departement, 
    SUBSTRING(description, 1, 300) AS description
FROM staging.joconde
WHERE reference IS NOT NULL
