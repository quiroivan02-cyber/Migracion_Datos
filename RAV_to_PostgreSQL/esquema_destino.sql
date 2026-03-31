-- =============================================================================
-- 3.2 ESQUEMA DESTINO — Sistema RAV (Registro Único de Víctimas)
-- PostgreSQL 16
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS rav;

DROP TABLE IF EXISTS rav.victimas CASCADE;

CREATE TABLE rav.victimas (
    origen                      VARCHAR(50),
    fuente                      VARCHAR(50),
    programa                    VARCHAR(50),
    idpersona                   INTEGER,
    idhogar                     INTEGER,
    tipodocumento               VARCHAR(50),
    documento                   VARCHAR(50),
    primernombre                VARCHAR(50),
    segundonombre               VARCHAR(50),               
    primerapellido              VARCHAR(50),
    segundoapellido             VARCHAR(50),               
    nombrecompleto              TEXT,
    fechanacimiento             DATE,           
    expediciondocumento         TEXT,
    fechaexpediciondocumento    TEXT,
    pertenenciaetnica           VARCHAR(100),
    genero                      VARCHAR(50),
    tipohecho                   TEXT,
    hecho                       TEXT,
    fechaocurrencia             DATE,           
    coddanemunicipioocurrencia  INTEGER,        
    zonaocurrencia              TEXT,
    ubicacionocurrencia         TEXT,
    presuntoactor               TEXT,
    presuntovictimizante        TEXT,
    fechareporte                DATE,           
    tipopoblacion               VARCHAR(50),
    tipovictima                 VARCHAR(50),
    estadovictima               VARCHAR(50),
    pais                        VARCHAR(50),
    ciudad                      VARCHAR(100),
    coddanemunicipioresidencia  INTEGER,        
    zonaresidencia              TEXT,
    ubicacionresidencia         TEXT,
    direccion                   TEXT,
    numtelefonofijo             TEXT,
    numtelefonocelular          TEXT,
    email                       TEXT,
    fechavaloracion             DATE,           
    idsiniestro                 INTEGER,        
    idmijefe                    INTEGER,        
    tipodesplazamiento          VARCHAR(50),
    registraduria               INTEGER,        
    vigenciadocumento           TEXT,
    conspersona                 INTEGER,        
    relacion                    TEXT,
    coddanedeclaracion          INTEGER,        
    coddanellegada              INTEGER,        
    codigohecho                 INTEGER,        
    discapacidad                SMALLINT,       
    descripciondiscapacidad     TEXT,
    fud_ficha                   VARCHAR(50),
    afectaciones                TEXT,
    cargado_en                  TIMESTAMPTZ     DEFAULT now() NOT NULL
);

ALTER TABLE rav.victimas 
ADD CONSTRAINT idx_victimas_unique UNIQUE NULLS NOT DISTINCT (idpersona, codigohecho, fechaocurrencia);
