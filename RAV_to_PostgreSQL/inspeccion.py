"""
=============================================================================
inspector.py — 3.1 Inspección del Dataset (Enfoque Python)
=============================================================================
Autor       : Ivan Quiroga
Descripción : Analiza la estructura de un archivo masivo sin saturar la RAM.
              Cuenta filas totales y toma una muestra representativa para
              inferir tipos de datos (str, int, float) y porcentaje de nulos.
              Ideal para crear el DDL (Data Definition Language) destino.
=============================================================================
"""

import csv
import json
import os
from collections import defaultdict, deque

# ── Configuración ──────────────────────────────────────────────────────────────
ARCHIVO   = "/home/san/Escritorio/Migracion_Datos/RAV_to_PostgreSQL/dataset/0002_UNIVERSO_VICTIMAS_LB (1).txt"
SEPARADOR = "»"
ENCODING  = "latin-1"
MUESTRA   = 100_000   # Cantidad de filas a analizar para inferir tipos

# Columnas que se desean excluir del análisis (si las hay)
COLUMNAS_IGNORADAS = []


# ── Función: Contar filas y columnas ──────────────────────────────────────────
def contar_filas_columnas(archivo, separador="»", encoding="latin-1"):
    """
    Cuenta el total de filas y columnas del archivo .txt
    usando un iterador para no cargarlo completo en memoria.
    """
    total_filas  = 0
    num_columnas = 0

    try:
        with open(archivo, encoding=encoding, errors="replace") as f:
            for i, linea in enumerate(f):
                if i == 0:
                    num_columnas = len(linea.strip().split(separador))
                    continue
                total_filas += 1
    except FileNotFoundError:
        print(f"❌ Error: Archivo no encontrado en {archivo}")
        exit(1)

    print("=" * 45)
    print("  CONTEO TOTAL DEL ARCHIVO")
    print("=" * 45)
    print(f"  Total de filas   : {total_filas:,}")
    print(f"  Total de columnas: {num_columnas}")
    print("=" * 45)

    return total_filas, num_columnas


# ── Función: Inferir tipo Python ──────────────────────────────────────────────
def inferir_tipo_python(v: str) -> str:
    """Infiere si un string representa numéricamente un int, un float o se queda como str"""
    if v.lstrip('-').isdigit():
        return "int"
    try:
        float(v)
        return "float"
    except ValueError:
        return "str"


# ── Función: Inspección completa ──────────────────────────────────────────────
def inspeccionar():
    print("🔍 Iniciando inspección del dataset...\n")

    # Paso 1: Contar filas y columnas totales (recorre el archivo completo rápido)
    print("📂 Paso 1: Contando filas y columnas totales...")
    total_filas_real, _ = contar_filas_columnas(ARCHIVO, SEPARADOR, ENCODING)

    # Paso 2: Análisis de tipos y nulos sobre la muestra
    print(f"\n📂 Paso 2: Analizando muestra de {MUESTRA:,} filas...\n")

    stats          = defaultdict(lambda: {"nulos": 0, "tipos_detectados": set()})
    total_filas    = 0
    primeras_filas = []
    ultimas_filas  = deque(maxlen=5)

    with open(ARCHIVO, encoding=ENCODING, errors="replace") as f:
        reader = csv.reader(f, delimiter=SEPARADOR)

        # Leer y limpiar encabezados
        encabezados_crudos  = next(reader)
        # Limpia espacios, comillas y pasa a minúsculas
        encabezados_limpios = [c.strip().replace('"', '').lower() for c in encabezados_crudos]

        # Filtrar columnas ignoradas
        indices_validos     = [i for i, col in enumerate(encabezados_limpios) if col not in COLUMNAS_IGNORADAS]
        encabezados_finales = [encabezados_limpios[i] for i in indices_validos]

        print(f"📊 Analizando {len(encabezados_finales)} columnas...")

        # Procesar filas de la muestra
        for fila in reader:
            if total_filas >= MUESTRA:
                break

            if total_filas > 0 and total_filas % 20_000 == 0:
                print(f"  ⏳ Procesando... {total_filas:,} filas")

            fila_procesada = []

            for idx_final, idx_original in enumerate(indices_validos):
                col_nombre = encabezados_finales[idx_final]
                try:
                    v = fila[idx_original].strip().lower()
                except IndexError:
                    v = ""

                # Detección de nulos
                if v in ("", "null", "none"):
                    stats[col_nombre]["nulos"] += 1
                    fila_procesada.append(None)
                else:
                    tipo_py = inferir_tipo_python(v)
                    stats[col_nombre]["tipos_detectados"].add(tipo_py)

                    if tipo_py == "int":
                        fila_procesada.append(int(v))
                    elif tipo_py == "float":
                        fila_procesada.append(float(v))
                    else:
                        fila_procesada.append(v)

            # Guardar muestras para el reporte final
            if total_filas < 5:
                primeras_filas.append(fila_procesada)
            ultimas_filas.append(fila_procesada)

            total_filas += 1

    # ── Reporte: tipos y nulos ─────────────────────────────────────────────────
    reporte_json = {"filas_analizadas": total_filas, "columnas": []}

    print("\n" + "=" * 60)
    print("📋 RESUMEN DE TIPOS DE DATOS (PYTHON) Y NULOS")
    print(f"   (muestra: {total_filas:,} de {total_filas_real:,} filas totales)")
    print("=" * 60)
    print(f"{'COLUMNA':<30} | {'TIPO PYTHON':<15} | {'% NULOS':<10}")
    print("-" * 60)

    for col in encabezados_finales:
        datos_col = stats[col]
        pct_nulos = round((datos_col["nulos"] / total_filas) * 100, 2) if total_filas > 0 else 0

        # Resolución de tipos: Si hay al menos un string, la columna entera debe ser string
        tipos = datos_col["tipos_detectados"]
        if "str" in tipos or len(tipos) == 0:
            tipo_final = "str"
        elif "float" in tipos:
            tipo_final = "float"
        else:
            tipo_final = "int"

        reporte_json["columnas"].append({
            "columna": col, 
            "tipo_inferido": tipo_final, 
            "porcentaje_nulos": pct_nulos
        })
        print(f"{col:<30} | {tipo_final:<15} | {pct_nulos:>5}%")

    # ── Columnas críticas ──────────────────────────────────────────────────────
    columnas_criticas = [d for d in reporte_json["columnas"] if d["porcentaje_nulos"] > 30]
    print("\n" + "=" * 60)
    print("⚠️  COLUMNAS CON MÁS DEL 30% DE NULOS")
    print("=" * 60)
    if columnas_criticas:
        columnas_criticas.sort(key=lambda x: x["porcentaje_nulos"], reverse=True)
        for c in columnas_criticas:
            print(f"{c['columna']:<30} | {c['porcentaje_nulos']:>5}%")
    else:
        print("✅ Ninguna columna supera el 30% de nulos.")

    # ── Guardar reporte JSON ───────────────────────────────────────────────────
    os.makedirs("resultados", exist_ok=True)
    with open("resultados/reporte_inspeccion.json", "w", encoding="utf-8") as f:
        json.dump(reporte_json, f, indent=2, ensure_ascii=False)
    print(f"\n💾 Reporte guardado en: resultados/reporte_inspeccion.json")

    # ── Primeras 5 filas ───────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📋 PRIMERAS 5 FILAS")
    print("=" * 60)
    for i, fila in enumerate(primeras_filas, 1):
        print(f"\n  ── Fila {i} ──")
        for col, val in zip(encabezados_finales, fila):
            print(f"    {col:<30}: {val}")

    # ── Últimas 5 filas ────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📋 ÚLTIMAS 5 FILAS (de la muestra)")
    print("=" * 60)
    for i, fila in enumerate(ultimas_filas, 1):
        print(f"\n  ── Fila {i} ──")
        for col, val in zip(encabezados_finales, fila):
            print(f"    {col:<30}: {val}")

    print("\n" + "=" * 60)
    print("✅ Inspección completada")
    print("=" * 60)


if __name__ == "__main__":
    inspeccionar()