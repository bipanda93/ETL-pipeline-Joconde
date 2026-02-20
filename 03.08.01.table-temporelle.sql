-- ============================================================================
-- TABLE TEMPORELLE AVEC HISTORISATION - POSTGRESQL
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE joconde_oeuvres_temporelle (
    reference VARCHAR(50) NOT NULL,
    appellation TEXT,
    auteur TEXT,
    annee_creation INTEGER,
    departement VARCHAR(100),
    description TEXT,
    date_import_utc TIMESTAMPTZ,
    source_system VARCHAR(50),
    
    -- Colonnes temporelles
    sys_start_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sys_end_time TIMESTAMPTZ NOT NULL DEFAULT 'infinity'::TIMESTAMPTZ,
    
    CONSTRAINT pk_joconde_temporelle PRIMARY KEY (reference, sys_start_time),
    CONSTRAINT valid_period CHECK (sys_start_time < sys_end_time),
    EXCLUDE USING gist (reference WITH =, tstzrange(sys_start_time, sys_end_time) WITH &&)
);

CREATE TABLE joconde_oeuvres_temporelle_archive (
    reference VARCHAR(50) NOT NULL,
    appellation TEXT,
    auteur TEXT,
    annee_creation INTEGER,
    departement VARCHAR(100),
    description TEXT,
    date_import_utc TIMESTAMPTZ,
    source_system VARCHAR(50),
    sys_start_time TIMESTAMPTZ NOT NULL,
    sys_end_time TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_archive_reference ON joconde_oeuvres_temporelle_archive(reference);
CREATE INDEX idx_archive_period ON joconde_oeuvres_temporelle_archive(sys_start_time, sys_end_time);

CREATE OR REPLACE FUNCTION archive_joconde_oeuvre()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'UPDATE' OR TG_OP = 'DELETE') THEN
        INSERT INTO joconde_oeuvres_temporelle_archive
        VALUES (
            OLD.reference,
            OLD.appellation,
            OLD.auteur,
            OLD.annee_creation,
            OLD.departement,
            OLD.description,
            OLD.date_import_utc,
            OLD.source_system,
            OLD.sys_start_time,
            CURRENT_TIMESTAMP
        );
    END IF;
    
    IF (TG_OP = 'UPDATE') THEN
        NEW.sys_start_time := CURRENT_TIMESTAMP;
        NEW.sys_end_time := 'infinity'::TIMESTAMPTZ;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_archive_joconde
BEFORE UPDATE OR DELETE ON joconde_oeuvres_temporelle
FOR EACH ROW
EXECUTE FUNCTION archive_joconde_oeuvre();