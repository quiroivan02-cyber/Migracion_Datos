-- =============================================================================
-- 3.2 ESQUEMA DESTINO — Sistema RAV (Registro Único de Víctimas)
-- PostgreSQL 16
-- =============================================================================
-- NOTA DE DISEÑO: Capa "Raw / Staging". 
-- Se omiten restricciones NOT NULL y se amplían tamaños de VARCHAR para 
-- tolerar datos sucios, atípicos o mal formateados desde el archivo original.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS rav;

DROP TABLE IF EXISTS rav.victimas CASCADE;

CREATE TABLE rav.victimas (

    -- ── Identificación de origen ──────────────────────────────────────────────
    origen                      VARCHAR(50),
    fuente                      VARCHAR(50),
    programa                    VARCHAR(50),
    idpersona                   INTEGER,
    idhogar                     INTEGER,

    -- ── Documento ─────────────────────────────────────────────────────────────
    tipodocumento               VARCHAR(50),
    documento                   VARCHAR(50),

    -- ── Nombres y apellidos ───────────────────────────────────────────────────
    primernombre                VARCHAR(50),
    segundonombre               VARCHAR(50),               
    primerapellido              VARCHAR(50),
    segundoapellido             VARCHAR(50),               
    nombrecompleto              TEXT,

    -- ── Datos personales ──────────────────────────────────────────────────────
    fechanacimiento             DATE,           
    expediciondocumento         TEXT,                      -- VACÍA: 100% nulos
    fechaexpediciondocumento    TEXT,                      -- VACÍA: 100% nulos
    pertenenciaetnica           VARCHAR(100),
    genero                      VARCHAR(50),

    -- ── Hecho victimizante ────────────────────────────────────────────────────
    tipohecho                   TEXT,                      -- 97.6% nulos
    hecho                       TEXT,
    fechaocurrencia             DATE,           
    coddanemunicipioocurrencia  INTEGER,        
    zonaocurrencia              TEXT,                      -- 59.3% nulos
    ubicacionocurrencia         TEXT,                      -- VACÍA: 99.5% nulos
    presuntoactor               TEXT,                      -- 46.3% nulos
    presuntovictimizante        TEXT,                      -- VACÍA: 100% nulos
    fechareporte                DATE,           

    -- ── Clasificación de víctima ──────────────────────────────────────────────
    tipopoblacion               VARCHAR(50),
    tipovictima                 VARCHAR(50),
    estadovictima               VARCHAR(50),

    -- ── Residencia ───────────────────────────────────────────────────────────
    pais                        VARCHAR(50),
    ciudad                      VARCHAR(100),              -- 35.8% nulos
    coddanemunicipioresidencia  INTEGER,        
    zonaresidencia              TEXT,                      -- VACÍA: 100% nulos
    ubicacionresidencia         TEXT,                      -- VACÍA: 100% nulos
    direccion                   TEXT,                      -- VACÍA: 100% nulos

    -- ── Contacto ─────────────────────────────────────────────────────────────
    numtelefonofijo             TEXT,                      -- VACÍA: 100% nulos
    numtelefonocelular          TEXT,                      -- VACÍA: 100% nulos
    email                       TEXT,                      -- VACÍA: 100% nulos

    -- ── Valoración ───────────────────────────────────────────────────────────
    fechavaloracion             DATE,           

    -- ── Identificadores internos ──────────────────────────────────────────────
    idsiniestro                 INTEGER,        
    idmijefe                    INTEGER,        
    tipodesplazamiento          VARCHAR(50),               -- 0.9% nulos
    registraduria               INTEGER,        
    vigenciadocumento           TEXT,                      -- VACÍA: 100% nulos
    conspersona                 INTEGER,        
    relacion                    TEXT,                      -- 2.5% nulos
    coddanedeclaracion          INTEGER,        
    coddanellegada              INTEGER,        
    codigohecho                 INTEGER,        

    -- ── Discapacidad ─────────────────────────────────────────────────────────
    discapacidad                SMALLINT,       
    descripciondiscapacidad     TEXT,                      -- 10.1% nulos
    fud_ficha                   VARCHAR(50),               -- 7.6% nulos
    afectaciones                TEXT,                      -- 41.2% nulos

    -- ── Auditoría ────────────────────────────────────────────────────────────
    cargado_en                  TIMESTAMPTZ     DEFAULT now() NOT NULL
);

-- =============================================================================
-- RESTRICCIONES (CONSTRAINTS)
-- Necesario para la Idempotencia (ON CONFLICT DO NOTHING) en la carga masiva.
-- Se usa NULLS NOT DISTINCT para que Postgres evalúe los nulos en la validación
-- =============================================================================
ALTER TABLE rav.victimas 
ADD CONSTRAINT idx_victimas_unique UNIQUE NULLS NOT DISTINCT (idpersona, codigohecho, fechaocurrencia);


-- =============================================================================
-- DOCUMENTACIÓN DE COLUMNAS VACÍAS
-- Estas columnas existen en el archivo fuente pero no contienen datos.
-- Se conservan para mantener fidelidad con el esquema original del RAV.
-- =============================================================================
COMMENT ON COLUMN rav.victimas.expediciondocumento      IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.fechaexpediciondocumento IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.ubicacionocurrencia      IS 'VACÍA: 99.5% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.presuntovictimizante     IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.zonaresidencia           IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.ubicacionresidencia      IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.direccion                IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.numtelefonofijo          IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.numtelefonocelular       IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.email                    IS 'VACÍA: 100% nulos en fuente original';
COMMENT ON COLUMN rav.victimas.vigenciadocumento        IS 'VACÍA: 100% nulos en fuente original';


-- =============================================================================
-- ÍNDICES DE OPTIMIZACIÓN DE BÚSQUEDA
-- =============================================================================
CREATE INDEX idx_victimas_idpersona              ON rav.victimas (idpersona);
CREATE INDEX idx_victimas_documento              ON rav.victimas (documento);
CREATE INDEX idx_victimas_hecho                  ON rav.victimas (hecho);
CREATE INDEX idx_victimas_fechaocurrencia        ON rav.victimas (fechaocurrencia);
CREATE INDEX idx_victimas_codigohecho            ON rav.victimas (codigohecho);
CREATE INDEX idx_victimas_municipio_ocurrencia   ON rav.victimas (coddanemunicipioocurrencia);
CREATE INDEX idx_victimas_municipio_residencia   ON rav.victimas (coddanemunicipioresidencia);
CREATE INDEX idx_victimas_estadovictima          ON rav.victimas (estadovictima);
CREATE INDEX idx_victimas_genero                 ON rav.victimas (genero);
CREATE INDEX idx_victimas_tipovictima            ON rav.victimas (tipovictima);
CREATE INDEX idx_victimas_cargado_en             ON rav.victimas (cargado_en);