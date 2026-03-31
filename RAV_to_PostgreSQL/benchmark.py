"""
benchmark.py — Benchmark de Tamaños de Lote y Métodos de Inserción
Compara execute_values vs COPY vía StringIO
Métricas: tiempo (s), throughput (filas/s), RAM pico (MB)
"""

import csv
import time
import tracemalloc
import json
import os
import io
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

# ── Configuración (Entorno Linux / Docker) ─────────────────────────────────────
ARCHIVO   = "/home/san/Escritorio/Migracion_Datos/RAV_to_PostgreSQL/dataset/0002_UNIVERSO_VICTIMAS_LB (1).txt"
SEPARADOR = "»"
ENCODING  = "latin-1"

DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 5432,
    "dbname"  : "migracion_db",
    "user"    : "admin_migra",
    "password": "migracion123"
}

TABLA      = "rav.victimas"
MUESTRA    = 50_000                     # Reducido a 50k para el benchmark de prueba
LOTES_EV   = [5_000, 10_000, 25_000]    # Tamaños de lote para execute_values
LOTES_COPY = [10_000, 25_000, 50_000]   # Tamaños de lote para COPY StringIO

FECHA_NULA    = "01/01/1900"
CODDANE_NULOS = {"0"}

COLUMNAS = [
    "origen", "fuente", "programa", "idpersona", "idhogar",
    "tipodocumento", "documento",
    "primernombre", "segundonombre", "primerapellido", "segundoapellido", "nombrecompleto",
    "fechanacimiento", "expediciondocumento", "fechaexpediciondocumento",
    "pertenenciaetnica", "genero",
    "tipohecho", "hecho", "fechaocurrencia",
    "coddanemunicipioocurrencia", "zonaocurrencia", "ubicacionocurrencia",
    "presuntoactor", "presuntovictimizante", "fechareporte",
    "tipopoblacion", "tipovictima", "estadovictima",
    "pais", "ciudad", "coddanemunicipioresidencia",
    "zonaresidencia", "ubicacionresidencia", "direccion",
    "numtelefonofijo", "numtelefonocelular", "email",
    "fechavaloracion",
    "idsiniestro", "idmijefe", "tipodesplazamiento", "registraduria",
    "vigenciadocumento", "conspersona", "relacion",
    "coddanedeclaracion", "coddanellegada", "codigohecho",
    "discapacidad", "descripciondiscapacidad", "fud_ficha", "afectaciones"
]

# ── Lógica de Limpieza Reutilizada ─────────────────────────────────────────────
def limpiar_fecha(val: str):
    val = val.strip()
    if not val or val == FECHA_NULA: return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%y-%m-%d", "%d-%m-%y"):
        try:
            fecha = datetime.strptime(val, fmt)
            if 1900 <= fecha.year <= 2100: return fecha.strftime("%Y-%m-%d")
        except ValueError: continue
    return None

def limpiar_fila(fila_raw: dict) -> tuple:
    valores = []
    for col in COLUMNAS:
        val = fila_raw.get(col, "")
        if "fecha" in col: val = limpiar_fecha(val)
        elif "coddane" in col and val.strip() == "0": val = None
        elif col in {"idpersona", "idhogar", "idsiniestro"} and val.strip() == "": val = None
        else: val = " ".join(val.split()).lower() if val.strip() else None
        valores.append(val)
    return tuple(valores)


# ── Lectura de Muestra ─────────────────────────────────────────────────────────
def leer_muestra():
    print(f"📥 Cargando y limpiando muestra de {MUESTRA:,} filas...")
    filas = []
    with open(ARCHIVO, encoding=ENCODING, errors="replace") as f:
        reader = csv.DictReader(f, delimiter=SEPARADOR)
        reader.fieldnames = [c.strip().lower() for c in reader.fieldnames]
        for i, fila in enumerate(reader):
            if i >= MUESTRA: break
            filas.append(limpiar_fila(fila))
    print(f"   ✅ {len(filas):,} filas listas | {len(COLUMNAS)} columnas\n")
    return COLUMNAS, filas

# ── Truncar tabla ──────────────────────────────────────────────────────────────
def truncar(conn):
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {TABLA};")
    conn.commit()


# ── Benchmark execute_values ───────────────────────────────────────────────────
def bench_execute_values(conn, filas, chunk_size, encabezados):
    truncar(conn)
    cols_str = ", ".join(encabezados)
    tracemalloc.start()
    inicio = time.perf_counter()

    with conn.cursor() as cur:
        for i in range(0, len(filas), chunk_size):
            lote = filas[i: i + chunk_size]
            execute_values(
                cur,
                f"INSERT INTO {TABLA} ({cols_str}) VALUES %s ON CONFLICT DO NOTHING",
                lote,
                page_size=1_000
            )
            conn.commit()

    fin = time.perf_counter()
    _, pico_ram = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return round(fin - inicio, 2), round(pico_ram / (1024 ** 2), 1)


# ── Benchmark COPY vía StringIO ────────────────────────────────────────────────
def bench_copy_stringio(conn, filas, chunk_size, encabezados):
    truncar(conn)
    cols_str = ", ".join(encabezados)
    tracemalloc.start()
    inicio = time.perf_counter()

    with conn.cursor() as cur:
        for i in range(0, len(filas), chunk_size):
            lote = filas[i: i + chunk_size]

            buffer = io.StringIO()
            for fila in lote:
                fila_tsv = []
                for val in fila:
                    if val is None:
                        fila_tsv.append(r"\N")
                    else:
                        val_str = str(val).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ")
                        fila_tsv.append(val_str)
                buffer.write("\t".join(fila_tsv) + "\n")
            buffer.seek(0)

            # Usar tabla temporal para simular el ON CONFLICT exacto de carga masiva
            cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS staging_bench (LIKE {TABLA} INCLUDING DEFAULTS);")
            cur.copy_expert(f"COPY staging_bench ({cols_str}) FROM STDIN WITH (FORMAT text, DELIMITER '\t', NULL '\\N')", buffer)
            cur.execute(f"INSERT INTO {TABLA} ({cols_str}) SELECT {cols_str} FROM staging_bench ON CONFLICT DO NOTHING;")
            cur.execute("TRUNCATE staging_bench;")
            conn.commit()

    fin = time.perf_counter()
    _, pico_ram = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return round(fin - inicio, 2), round(pico_ram / (1024 ** 2), 1)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("⚡ BENCHMARK — execute_values vs COPY vía StringIO")
    print("=" * 70 + "\n")

    encabezados, filas = leer_muestra()
    conn = psycopg2.connect(**DB_CONFIG)
    resultados = []

    # ── execute_values ──────────────────
    for chunk in LOTES_EV:
        print(f"🔄 execute_values | lote {chunk:>7,} ...", end=" ", flush=True)
        try:
            if conn.closed: conn = psycopg2.connect(**DB_CONFIG)
            t, ram = bench_execute_values(conn, filas, chunk, encabezados)
            fps = round(len(filas) / t)
            print(f"✅  {t:>7.2f}s | RAM: {ram:>7.1f} MB | {fps:>10,} filas/s")
            resultados.append({"metodo": "execute_values", "lote": chunk, "tiempo_s": t, "ram_mb": ram, "filas_s": fps})
        except Exception as e:
            print(f"❌ Error: {e}")

    # ── COPY StringIO ───────────────────
    for chunk in LOTES_COPY:
        print(f"🔄 COPY StringIO   | lote {chunk:>7,} ...", end=" ", flush=True)
        try:
            if conn.closed: conn = psycopg2.connect(**DB_CONFIG)
            t, ram = bench_copy_stringio(conn, filas, chunk, encabezados)
            fps = round(len(filas) / t)
            print(f"✅  {t:>7.2f}s | RAM: {ram:>7.1f} MB | {fps:>10,} filas/s")
            resultados.append({"metodo": "COPY_StringIO", "lote": chunk, "tiempo_s": t, "ram_mb": ram, "filas_s": fps})
        except Exception as e:
            print(f"❌ Error: {e}")

    conn.close()

    # ── Tabla resumen ──────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"{'MÉTODO':<20} {'LOTE':>10} {'TIEMPO':>10} {'RAM MB':>10} {'FILAS/S':>12}")
    print(f"{'-'*70}")
    for r in resultados:
        print(f"{r['metodo']:<20} {r['lote']:>10,} {r['tiempo_s']:>9.2f}s {r['ram_mb']:>9.1f}MB {r['filas_s']:>12,}")

    mejor_ram = min(resultados, key=lambda x: x["ram_mb"])
    mejor_vel = max(resultados, key=lambda x: x["filas_s"])

    print(f"\n🏆 Menor RAM      : {mejor_ram['metodo']} lote={mejor_ram['lote']:,} ({mejor_ram['ram_mb']} MB)")
    print(f"🚀 Mayor velocidad: {mejor_vel['metodo']} lote={mejor_vel['lote']:,} ({mejor_vel['filas_s']:,} f/s)")

    os.makedirs("resultados", exist_ok=True)
    with open("resultados/benchmark.json", "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=2, ensure_ascii=False)

    print(f"\n💾 Guardado en: resultados/benchmark.json")
    print("=" * 70)

if __name__ == "__main__":
    main()