INSERT INTO joconde_oeuvres_temporelle (
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
    TRIM(reference) AS reference,
    TRIM(appellation) AS appellation,
    TRIM(auteur) AS auteur,
    CASE 
        WHEN SUBSTRING(date_creation, 1, 4) ~ '^\d{4}$' 
        THEN SUBSTRING(date_creation, 1, 4)::INTEGER
        ELSE NULL
    END AS annee_creation,
    TRIM(departement) AS departement,
    description,
    load_timestamp_utc as date_import_utc,
    source_system
FROM staging.joconde
WHERE reference IS NOT NULL
ON CONFLICT (reference, sys_start_time) 
DO UPDATE SET
    appellation = EXCLUDED.appellation,
    auteur = EXCLUDED.auteur,
    annee_creation = EXCLUDED.annee_creation,
    departement = EXCLUDED.departement,
    description = EXCLUDED.description,
    date_import_utc = EXCLUDED.date_import_utc,
    source_system = EXCLUDED.source_system
WHERE 
    COALESCE(joconde_oeuvres_temporelle.appellation, '') IS DISTINCT FROM COALESCE(EXCLUDED.appellation, '') OR
    COALESCE(joconde_oeuvres_temporelle.auteur, '') IS DISTINCT FROM COALESCE(EXCLUDED.auteur, '') OR
    COALESCE(joconde_oeuvres_temporelle.annee_creation, -1) IS DISTINCT FROM COALESCE(EXCLUDED.annee_creation, -1) OR
    COALESCE(joconde_oeuvres_temporelle.departement, '') IS DISTINCT FROM COALESCE(EXCLUDED.departement, '') OR
    COALESCE(joconde_oeuvres_temporelle.description, '') IS DISTINCT FROM COALESCE(EXCLUDED.description, '');