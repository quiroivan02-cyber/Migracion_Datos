# extraccion/control.py
# Registro de extracciones en SQLite — tabla control_extracciones

import sqlite3
import logging
from datetime import datetime
from config.settings import CONTROL_DB

log = logging.getLogger(__name__)


def init_control_db():
    """Crea la tabla de control si no existe. Retorna la conexión."""
    con = sqlite3.connect(CONTROL_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS control_extracciones (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tabla         TEXT    NOT NULL,
            fecha_inicio  TEXT,
            fecha_fin     TEXT,
            filas_oracle  INTEGER,
            filas_extraidas INTEGER,
            estado        TEXT,
            mensaje       TEXT,
            ejecutado_en  TEXT    NOT NULL
        )
    """)
    con.commit()
    log.info("Control SQLite inicializado")
    return con


def registrar_extraccion(con, tabla, fecha_inicio, fecha_fin,
                         filas_oracle, filas_extraidas, estado, mensaje=""):
    """Inserta un registro de extracción en la tabla de control."""
    con.execute("""
        INSERT INTO control_extracciones
            (tabla, fecha_inicio, fecha_fin, filas_oracle, filas_extraidas,
             estado, mensaje, ejecutado_en)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        tabla, fecha_inicio, fecha_fin,
        filas_oracle, filas_extraidas,
        estado, mensaje,
        datetime.now().isoformat()
    ))
    con.commit()
    log.info(f"Control registrado: {tabla} → {estado} ({filas_extraidas} filas)")


def obtener_ultima_extraccion(tabla: str) -> dict | None:
    """Retorna el último registro exitoso de una tabla. Útil para reanudar."""
    con = sqlite3.connect(CONTROL_DB)
    con.row_factory = sqlite3.Row
    cur = con.execute("""
        SELECT * FROM control_extracciones
        WHERE tabla = ? AND estado = 'OK'
        ORDER BY ejecutado_en DESC
        LIMIT 1
    """, (tabla,))
    fila = cur.fetchone()
    con.close()
    return dict(fila) if fila else None