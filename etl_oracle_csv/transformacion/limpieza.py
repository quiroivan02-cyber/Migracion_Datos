# transformacion/limpieza.py
# Limpieza de datos por tabla — genera _clean.csv y _problemas.csv

import os
import json
import logging
import pandas as pd
from datetime import datetime
from config.settings import DATOS_DIR, CLEAN_DIR, REPORTES_DIR, ETL_VERSION

log = logging.getLogger(__name__)

os.makedirs(CLEAN_DIR, exist_ok=True)


# ── Reglas de limpieza genéricas ──────────────────────────────────────────────
def limpiar_espacios(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina espacios al inicio/fin en columnas de texto."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    return df


def limpiar_duplicados(df: pd.DataFrame, tabla: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separa filas duplicadas del DataFrame limpio."""
    duplicados = df[df.duplicated(keep="first")]
    if len(duplicados) > 0:
        log.warning(f"  {tabla}: {len(duplicados)} duplicados detectados")
    return df.drop_duplicates(keep="first"), duplicados


def limpiar_fechas(df: pd.DataFrame, columnas_fecha: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convierte columnas de fecha y separa filas con fechas inválidas."""
    problemas = pd.DataFrame()
    for col in columnas_fecha:
        if col not in df.columns:
            continue
        convertida = pd.to_datetime(df[col], errors="coerce")
        invalidas  = df[convertida.isna() & df[col].notna()]
        if len(invalidas) > 0:
            invalidas = invalidas.copy()
            invalidas["_problema"] = f"{col}: fecha inválida"
            problemas = pd.concat([problemas, invalidas], ignore_index=True)
        df[col] = convertida
    return df, problemas


# ── Reglas específicas por tabla ──────────────────────────────────────────────
REGLAS_TABLAS = {
    "PACIENTES": {
        "columnas_requeridas": ["PACIENTE_ID", "NOMBRE", "FECHA_NACIMIENTO"],
        "fechas":              ["FECHA_NACIMIENTO", "CREATED_AT"],
    },
    "CITAS_MEDICAS": {
        "columnas_requeridas": ["CITA_ID", "PACIENTE_ID", "MEDICO_ID", "FECHA_CITA"],
        "fechas":              ["FECHA_CITA", "CREATED_AT"],
    },
    "FACTURACION": {
        "columnas_requeridas": ["FACTURA_ID", "PACIENTE_ID", "MONTO_TOTAL"],
        "fechas":              ["FECHA_FACTURA", "CREATED_AT"],
    },
    "INTERNACIONES": {
        "columnas_requeridas": ["INTERNACION_ID", "PACIENTE_ID", "FECHA_INGRESO"],
        "fechas":              ["FECHA_INGRESO", "FECHA_EGRESO", "CREATED_AT"],
    },
    "EXAMENES_MEDICOS": {
        "columnas_requeridas": ["EXAMEN_ID", "PACIENTE_ID", "FECHA_EXAMEN"],
        "fechas":              ["FECHA_EXAMEN", "CREATED_AT"],
    },
    "RESULTADOS_EXAMENES": {
        "columnas_requeridas": ["RESULTADO_ID", "EXAMEN_ID"],
        "fechas":              ["FECHA_RESULTADO", "CREATED_AT"],
    },
    "MEDICAMENTOS_RECETADOS": {
        "columnas_requeridas": ["RECETA_ID", "PACIENTE_ID", "MEDICAMENTO_ID"],
        "fechas":              ["FECHA_RECETA", "CREATED_AT"],
    },
    "MEDICOS": {
        "columnas_requeridas": ["MEDICO_ID", "NOMBRE", "ESPECIALIDAD_ID"],
        "fechas":              ["CREATED_AT"],
    },
    "ESPECIALIDADES": {
        "columnas_requeridas": ["ESPECIALIDAD_ID", "NOMBRE"],
        "fechas":              [],
    },
    "MUNICIPIOS": {
        "columnas_requeridas": ["MUNICIPIO_ID", "NOMBRE"],
        "fechas":              [],
    },
}


def limpiar_tabla(nombre: str, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica limpieza genérica + reglas específicas de la tabla.
    Retorna (df_clean, df_problemas).
    """
    reglas    = REGLAS_TABLAS.get(nombre, {"columnas_requeridas": [], "fechas": []})
    problemas = pd.DataFrame()

    # 1. Espacios
    df = limpiar_espacios(df)

    # 2. Duplicados
    df, dupl = limpiar_duplicados(df, nombre)
    if len(dupl) > 0:
        dupl["_problema"] = "fila duplicada"
        problemas = pd.concat([problemas, dupl], ignore_index=True)

    # 3. Nulos en columnas requeridas
    for col in reglas["columnas_requeridas"]:
        if col not in df.columns:
            continue
        nulos = df[df[col].isna()]
        if len(nulos) > 0:
            nulos = nulos.copy()
            nulos["_problema"] = f"{col}: valor requerido nulo"
            problemas = pd.concat([problemas, nulos], ignore_index=True)
            df = df[df[col].notna()]
            log.warning(f"  {nombre}: {len(nulos)} filas eliminadas por {col} nulo")

    # 4. Fechas inválidas
    df, prob_fechas = limpiar_fechas(df, reglas["fechas"])
    if len(prob_fechas) > 0:
        problemas = pd.concat([problemas, prob_fechas], ignore_index=True)

    log.info(f"  {nombre}: {len(df)} filas limpias | {len(problemas)} problemas")
    return df, problemas


# ── Proceso principal ─────────────────────────────────────────────────────────
def ejecutar_limpieza(fecha_inicio: str, fecha_fin: str) -> bool:
    """
    Lee el reporte_calidad, limpia cada CSV del período y guarda:
      - datos_limpios/{tabla}_{periodo}_clean.csv
      - datos_limpios/{tabla}_{periodo}_problemas.csv
    """
    # Verificar que existe el reporte de calidad previo
    nombre_reporte = f"reporte_calidad_{fecha_inicio}_{fecha_fin}.json"
    ruta_reporte   = os.path.join(REPORTES_DIR, nombre_reporte)
    if not os.path.exists(ruta_reporte):
        log.error(f"Reporte no encontrado: {ruta_reporte}")
        log.error("Ejecuta primero: ejecutar_perfilado()")
        return False

    log.info(f"=== INICIO LIMPIEZA: {fecha_inicio} al {fecha_fin} ===")
    errores = 0

    for archivo in os.listdir(DATOS_DIR):
        if not archivo.endswith(".csv"):
            continue
        if f"{fecha_inicio}_{fecha_fin}" not in archivo:
            continue

        nombre = archivo.replace(f"_{fecha_inicio}_{fecha_fin}.csv", "").upper()
        ruta   = os.path.join(DATOS_DIR, archivo)

        try:
            df = pd.read_csv(ruta, low_memory=False)
            log.info(f"Limpiando: {nombre} ({len(df)} filas)")

            df_clean, df_problemas = limpiar_tabla(nombre, df)

            # Guardar clean
            base        = f"{nombre.lower()}_{fecha_inicio}_{fecha_fin}"
            ruta_clean  = os.path.join(CLEAN_DIR, f"{base}_clean.csv")
            ruta_probs  = os.path.join(CLEAN_DIR, f"{base}_problemas.csv")

            df_clean.to_csv(ruta_clean, index=False, encoding="utf-8")
            df_problemas.to_csv(ruta_probs, index=False, encoding="utf-8")

            log.info(f"  {nombre}: OK → {ruta_clean}")

        except Exception as e:
            log.error(f"  {nombre}: ERROR — {e}")
            errores += 1

    log.info(f"=== LIMPIEZA FINALIZADA — Errores: {errores} ===")
    return errores == 0


if __name__ == "__main__":
    ejecutar_limpieza("2020-01-01", "2026-12-31")