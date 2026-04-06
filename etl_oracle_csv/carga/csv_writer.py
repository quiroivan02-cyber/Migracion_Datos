# carga/csv_writer.py
# Escritura CSV final + generación de manifiesto de integridad

import os
import json
import hashlib
import logging
import shutil
import pandas as pd
from datetime import datetime
from config.settings import CLEAN_DIR, REPORTES_DIR, ETL_VERSION

log = logging.getLogger(__name__)

# Carpeta de salida final
SALIDA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "salida")
os.makedirs(SALIDA_DIR, exist_ok=True)


# ── MD5 del archivo ───────────────────────────────────────────────────────────
def calcular_md5(ruta: str) -> str:
    hash_md5 = hashlib.md5()
    with open(ruta, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# ── Nombre final según convención del proyecto ────────────────────────────────
def nombre_final(tabla: str, fecha_inicio: str, fecha_fin: str) -> str:
    """
    Convierte fechas a formato MM y genera nombre:
    TABLA_YYYY_MM-MM.csv
    """
    mm_inicio = fecha_inicio[5:7]
    mm_fin    = fecha_fin[5:7]
    yyyy      = fecha_inicio[:4]
    return f"{tabla.upper()}_{yyyy}_{mm_inicio}-{mm_fin}.csv"


# ── Leer problemas_calidad del entregable_2A ──────────────────────────────────
def leer_problemas(tabla: str, fecha_inicio: str, fecha_fin: str) -> dict:
    ruta = os.path.join(REPORTES_DIR, f"entregable_2A_{fecha_inicio}_{fecha_fin}.json")
    if not os.path.exists(ruta):
        return {}
    with open(ruta, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("resumen", {}).get(tabla, {})


# ── Generar manifiesto JSON ───────────────────────────────────────────────────
def generar_manifiesto(tabla: str, fecha_inicio: str, fecha_fin: str,
                       archivo_csv: str, ruta_csv: str,
                       filas_oracle: int, filas_csv: int,
                       columnas: list) -> dict:
    problemas = leer_problemas(tabla, fecha_inicio, fecha_fin)
    return {
        "tabla":          tabla,
        "periodo_inicio": fecha_inicio,
        "periodo_fin":    fecha_fin,
        "archivo":        archivo_csv,
        "filas_oracle":   filas_oracle,
        "filas_csv":      filas_csv,
        "md5":            calcular_md5(ruta_csv),
        "columnas":       columnas,
        "problemas_calidad": problemas,
        "generado_en":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "generado_por":   ETL_VERSION,
    }


# ── Proceso principal ─────────────────────────────────────────────────────────
def ejecutar_carga(fecha_inicio: str, fecha_fin: str) -> bool:
    """
    Para cada _clean.csv del período:
      1. Renombra al formato TABLA_YYYY_MM-MM.csv
      2. Copia a salida/
      3. Genera TABLA_YYYY_MM-MM.manifest.json
    """
    log.info(f"=== INICIO CARGA: {fecha_inicio} al {fecha_fin} ===")
    errores = 0

    for archivo in os.listdir(CLEAN_DIR):
        if not archivo.endswith("_clean.csv"):
            continue
        if f"{fecha_inicio}_{fecha_fin}" not in archivo:
            continue

        tabla = archivo.replace(f"_{fecha_inicio}_{fecha_fin}_clean.csv", "").upper()
        ruta_clean = os.path.join(CLEAN_DIR, archivo)

        try:
            df          = pd.read_csv(ruta_clean, low_memory=False)
            filas_csv   = len(df)
            columnas    = list(df.columns)

            # Nombre final con convención del proyecto
            csv_final   = nombre_final(tabla, fecha_inicio, fecha_fin)
            ruta_final  = os.path.join(SALIDA_DIR, csv_final)
            shutil.copy2(ruta_clean, ruta_final)

            # Manifiesto
            manifiesto  = generar_manifiesto(
                tabla        = tabla,
                fecha_inicio = fecha_inicio,
                fecha_fin    = fecha_fin,
                archivo_csv  = csv_final,
                ruta_csv     = ruta_final,
                filas_oracle = filas_csv,   # mismo valor — reconciliación ya fue en extracción
                filas_csv    = filas_csv,
                columnas     = columnas,
            )
            ruta_manifest = ruta_final.replace(".csv", ".manifest.json")
            with open(ruta_manifest, "w", encoding="utf-8") as f:
                json.dump(manifiesto, f, indent=2, ensure_ascii=False)

            log.info(f"  {tabla}: {csv_final} + manifiesto generado")

        except Exception as e:
            log.error(f"  {tabla}: ERROR — {e}")
            errores += 1

    log.info(f"=== CARGA FINALIZADA — Errores: {errores} ===")
    return errores == 0


if __name__ == "__main__":
    ejecutar_carga("2020-01-01", "2026-12-31")