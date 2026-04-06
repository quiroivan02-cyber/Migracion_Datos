# transformacion/perfilador.py
# Generación de reporte de calidad por tabla

import os
import json
import logging
import pandas as pd
from datetime import datetime
from config.settings import DATOS_DIR, REPORTES_DIR, ETL_VERSION

log = logging.getLogger(__name__)

os.makedirs(REPORTES_DIR, exist_ok=True)


# ── Análisis de calidad de un DataFrame ──────────────────────────────────────
def perfilar_tabla(nombre: str, df: pd.DataFrame) -> dict:
    """Analiza calidad de datos de un DataFrame y retorna dict con métricas."""
    total = len(df)
    perfil = {
        "tabla":        nombre,
        "total_filas":  total,
        "total_cols":   len(df.columns),
        "columnas":     {},
    }

    for col in df.columns:
        serie = df[col]
        nulos = int(serie.isna().sum())
        duplicados = int(serie.duplicated().sum())

        perfil["columnas"][col] = {
            "nulos":       nulos,
            "pct_nulos":   round(nulos / total * 100, 2) if total > 0 else 0,
            "duplicados":  duplicados,
            "tipo":        str(serie.dtype),
            "unicos":      int(serie.nunique()),
        }

    log.info(f"  {nombre}: perfil generado ({total} filas, {len(df.columns)} cols)")
    return perfil


# ── Cargar todos los CSVs del período ────────────────────────────────────────
def cargar_csvs(fecha_inicio: str, fecha_fin: str) -> dict:
    """Carga todos los CSVs del período y retorna dict {tabla: DataFrame}."""
    tablas = {}
    for archivo in os.listdir(DATOS_DIR):
        if not archivo.endswith(".csv"):
            continue
        # Formato esperado: tabla_YYYY-MM-DD_YYYY-MM-DD.csv
        if f"{fecha_inicio}_{fecha_fin}" not in archivo:
            continue
        nombre = archivo.replace(f"_{fecha_inicio}_{fecha_fin}.csv", "").upper()
        ruta   = os.path.join(DATOS_DIR, archivo)
        tablas[nombre] = pd.read_csv(ruta, low_memory=False)
        log.info(f"  Cargado: {archivo} ({len(tablas[nombre])} filas)")
    return tablas


# ── Proceso principal ─────────────────────────────────────────────────────────
def ejecutar_perfilado(fecha_inicio: str, fecha_fin: str) -> str:
    """
    Perfila todos los CSVs del período y guarda:
      - reporte_calidad_{fecha_inicio}_{fecha_fin}.json   (detalle técnico)
      - entregable_2A_{fecha_inicio}_{fecha_fin}.json     (resumen ejecutivo)

    Retorna la ruta del reporte_calidad generado.
    """
    log.info(f"=== INICIO PERFILADO: {fecha_inicio} al {fecha_fin} ===")

    tablas = cargar_csvs(fecha_inicio, fecha_fin)
    if not tablas:
        log.error(f"No se encontraron CSVs para el período {fecha_inicio}_{fecha_fin}")
        return None

    reporte = {
        "periodo_inicio": fecha_inicio,
        "periodo_fin":    fecha_fin,
        "generado_en":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generado_por":   ETL_VERSION,
        "tablas":         {},
    }

    for nombre, df in tablas.items():
        reporte["tablas"][nombre] = perfilar_tabla(nombre, df)

    # ── Guardar reporte_calidad ───────────────────────────────────────────────
    nombre_reporte = f"reporte_calidad_{fecha_inicio}_{fecha_fin}.json"
    ruta_reporte   = os.path.join(REPORTES_DIR, nombre_reporte)
    with open(ruta_reporte, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)
    log.info(f"Reporte guardado: {ruta_reporte}")

    # ── Guardar entregable_2A (resumen ejecutivo) ─────────────────────────────
    entregable = {
        "periodo_inicio": fecha_inicio,
        "periodo_fin":    fecha_fin,
        "generado_en":    reporte["generado_en"],
        "generado_por":   ETL_VERSION,
        "resumen": {
            nombre: {
                "total_filas": datos["total_filas"],
                "columnas_con_nulos": sum(
                    1 for c in datos["columnas"].values() if c["nulos"] > 0
                ),
                "max_pct_nulos": max(
                    (c["pct_nulos"] for c in datos["columnas"].values()), default=0
                ),
            }
            for nombre, datos in reporte["tablas"].items()
        }
    }
    nombre_2A = f"entregable_2A_{fecha_inicio}_{fecha_fin}.json"
    ruta_2A   = os.path.join(REPORTES_DIR, nombre_2A)
    with open(ruta_2A, "w", encoding="utf-8") as f:
        json.dump(entregable, f, indent=2, ensure_ascii=False)
    log.info(f"Entregable 2A guardado: {ruta_2A}")

    log.info(f"=== PERFILADO FINALIZADO: {len(tablas)} tablas ===")
    return ruta_reporte


if __name__ == "__main__":
    from datetime import date
    hoy = date.today().isoformat()
    ejecutar_perfilado("2020-01-01", hoy)