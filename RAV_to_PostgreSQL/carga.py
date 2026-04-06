r"""
=============================================================================
ETL - Carga Masiva de Datos (RAV) hacia PostgreSQL
=============================================================================
Autor       : Ivan Quiroga
Descripción : Script de extracción, transformación y carga (ETL) optimizado
              para procesar archivos gigantes (ej. 15 millones de registros)
              sin saturar la memoria RAM.
              
Características Técnicas:
- Rendimiento : Uso de `COPY` a través de memoria (StringIO) en lugar de INSERTs.
- Resiliencia : Sistema de "Checkpoints" para reanudar cargas tras fallos de red/energía.
- Idempotencia: Uso de `ON CONFLICT DO NOTHING` para evitar registros duplicados.
- Limpieza    : Estandarización de fechas (manejando errores de años a 2 dígitos), 
                nulos (\N para Postgres), espacios y formateo de texto.
=============================================================================
"""

import csv
import os
import time
import json
import logging
from io import StringIO
from datetime import datetime
import psycopg2

# ── 1. Configuración de Logs ───────────────────────────────────────────────────
# Se genera un archivo de log por cada ejecución para tener trazabilidad.
os.makedirs("logs", exist_ok=True)
log_file = f"logs/carga_rav_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt  = "%Y-%m-%d %H:%M:%S",
    handlers = [
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ── 2. Parámetros de Conexión y Archivo ────────────────────────────────────────
# NOTA: En un entorno de producción estricto, esto debería ir en variables de entorno (.env)
DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 5432,
    "dbname"  : "migracion_db",
    "user"    : "admin_migra",
    "password": "migracion123"
}

ARCHIVO   = "/home/san/Escritorio/Migracion_Datos/RAV_to_PostgreSQL/dataset/0002_UNIVERSO_VICTIMAS_LB (1).txt"
SEPARADOR = "»"
ENCODING  = "latin-1"
TABLA     = "victimas"

# Parámetros de optimización de memoria
TAM_LOTE  = 10_000       # Cantidad de filas a procesar antes de hacer COMMIT en BD
MAX_FILAS = 15_000_000   # Límite de seguridad opcional

# Constantes para limpieza de datos
FECHA_NULA    = "01/01/1900"
CODDANE_NULOS = {"0"}
NULL_COPY     = r"\N"    # Representación de NULL requerida por el comando COPY de PostgreSQL

# Esquema de columnas mapeadas exactamente a la tabla destino
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

COLS_FECHA = {
    "fechanacimiento", "fechaexpediciondocumento",
    "fechaocurrencia", "fechareporte", "fechavaloracion"
}

COLS_INT = {
    "idpersona", "idhogar", "coddanemunicipioocurrencia",
    "coddanemunicipioresidencia", "idsiniestro", "idmijefe",
    "registraduria", "conspersona", "coddanedeclaracion",
    "coddanellegada", "codigohecho", "discapacidad"
}

COLS_CODDANE = {
    "coddanemunicipioocurrencia", "coddanemunicipioresidencia",
    "coddanedeclaracion", "coddanellegada"
}


# ── 3. Lógica de Checkpoint (Tolerancia a fallos) ──────────────────────────────
CHECKPOINT_FILE = "estado_carga.json"

def leer_checkpoint():
    """Lee la última fila procesada exitosamente para reanudar la carga."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f).get("ultima_fila", 0)
        except Exception:
            return 0
    return 0

def guardar_checkpoint(fila_num):
    """Guarda el número de fila actual en un archivo local. 
    Se ejecuta sólo después de un COMMIT exitoso en la BD."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"ultima_fila": fila_num}, f)


# ── 4. Transformación y Limpieza de Datos (ETL) ────────────────────────────────
def limpiar_fecha(val: str):
    """Convierte cadenas de texto en formato YYYY-MM-DD válido para Postgres.
    Maneja años en 2 dígitos (ej. '17' pasa a '2017' en vez del año 17 d.C.) y valores nulos."""
    val = val.strip()
    if not val or val == FECHA_NULA:
        return None
    
    formatos = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%y-%m-%d", "%d-%m-%y"]
    
    for fmt in formatos:
        try:
            fecha = datetime.strptime(val, fmt)
            # Descartamos años fuera de lógica (evita error 'out of range' en Postgres)
            if fecha.year < 1900 or fecha.year > 2100:
                continue
            return fecha.strftime("%Y-%m-%d")
        except ValueError:
            continue
            
    return None # Si es basura irrecuperable, se envía como NULL

def limpiar_entero(val: str, col: str):
    """Castea valores a enteros. Si el valor es un CODDANE = 0, se vuelve nulo."""
    val = val.strip()
    if val == "":
        return None
    try:
        numero = int(val)
        if col in COLS_CODDANE and numero == 0:
            return None
        return numero
    except ValueError:
        return None

def limpiar_texto(val: str):
    """Normaliza los espacios en blanco y convierte el texto a minúsculas."""
    val = " ".join(val.split())
    return val.lower() if val else None

def limpiar_fila(fila_raw: dict) -> list:
    """Aplica las reglas de limpieza a toda la fila según el tipo de columna."""
    valores = []
    for col in COLUMNAS:
        val = fila_raw.get(col, "")
        if col in COLS_FECHA:
            val = limpiar_fecha(val)
        elif col in COLS_INT:
            val = limpiar_entero(val, col)
        else:
            val = limpiar_texto(val)
        valores.append(val)
    return valores


# ── 5. Carga Masiva en PostgreSQL (COPY) ───────────────────────────────────────
def construir_buffer(lote: list) -> StringIO:
    """Convierte una lista de listas en un buffer de texto plano (TSV en memoria).
    Evita escribir archivos temporales en disco."""
    buffer = StringIO()
    for fila in lote:
        fila_tsv = []
        for val in fila:
            if val is None:
                fila_tsv.append(NULL_COPY)
            else:
                # Sanitizamos caracteres de escape que romperían el formato TSV
                val_str = str(val).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", " ")
                fila_tsv.append(val_str)
        buffer.write("\t".join(fila_tsv) + "\n")
    buffer.seek(0)
    return buffer

def insertar_copy(cur, lote: list):
    """
    Inserta un bloque de datos usando el comando COPY (la forma más rápida en PG).
    Utiliza una tabla 'staging' temporal para manejar registros duplicados 
    (ON CONFLICT DO NOTHING) sin interrumpir la carga del lote.
    """
    cur.execute("SET search_path TO rav, public;")

    # 1. Volcamos los datos en la tabla temporal
    cur.copy_from(
        construir_buffer(lote),
        "staging_victimas",
        sep="\t",
        null=NULL_COPY,
        columns=COLUMNAS
    )

    # 2. Insertamos a la tabla final ignorando conflictos (Idempotencia)
    cols = ", ".join(COLUMNAS)
    cur.execute(f"""
        INSERT INTO rav.victimas ({cols})
        SELECT {cols} FROM staging_victimas
        ON CONFLICT (idpersona, codigohecho, fechaocurrencia) DO NOTHING;

        TRUNCATE staging_victimas;
    """)


# ── 6. Verificaciones Previas ──────────────────────────────────────────────────
def verificar_conexion():
    """Valida la conectividad con el motor de PostgreSQL en Docker."""
    logging.info("Verificando conexión con Docker...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        cur.execute("SELECT version();")
        logging.info(f"Conexión exitosa | {cur.fetchone()[0]}")
        cur.close()
        conn.close()
        return True
    except psycopg2.OperationalError as e:
        logging.error(f"Error de conexión: {e}")
        logging.error("Verifica: docker start migra_postgres16")
        return False

def verificar_tabla(conn):
    """Garantiza que la estructura destino exista antes de procesar."""
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'rav' AND table_name = 'victimas'
        );
    """)
    existe = cur.fetchone()[0]
    cur.close()
    if not existe:
        logging.error("La tabla rav.victimas no existe. Ejecuta primero el DDL.")
        return False
    logging.info("Tabla rav.victimas verificada correctamente")
    return True


# ── 7. Bucle Principal de Ejecución ────────────────────────────────────────────
def cargar():
    ultima_fila = leer_checkpoint()

    logging.info("=" * 60)
    logging.info("INICIO | CARGA MASIVA RAV → PostgreSQL 16 | Método: COPY")
    logging.info("=" * 60)
    
    if ultima_fila > 0:
        logging.info(f"🔄 REANUDANDO DESDE LA FILA {ultima_fila:,} (saltando las anteriores)...")
        
    logging.info(f"Archivo  : {os.path.basename(ARCHIVO)}")
    logging.info(f"Tamaño   : {os.path.getsize(ARCHIVO) / (1024**3):.2f} GB")
    logging.info(f"Lote     : {TAM_LOTE:,} filas")
    logging.info(f"Destino  : rav.{TABLA}")
    logging.info("=" * 60)

    if not verificar_conexion():
        return

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur  = conn.cursor()
    cur.execute("SET search_path TO rav, public;")
    
    # Crea tabla de paso idéntica a la original para hacer el Upsert seguro
    cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS staging_victimas
            (LIKE rav.victimas INCLUDING DEFAULTS);
    """)
    conn.commit()
    logging.info("Staging temporal creado ✅")

    if not verificar_tabla(conn):
        conn.close()
        return

    total_insertadas = 0
    total_errores    = 0
    lote             = []
    tiempo_inicio    = time.perf_counter()
    
    # Contadores absolutos para los logs
    num_lote         = ultima_fila // TAM_LOTE 
    num_fila         = ultima_fila

    try:
        # Se lee como generador para procesar línea por línea sin cargar todo a RAM
        with open(ARCHIVO, encoding=ENCODING, errors="replace") as f:
            reader = csv.DictReader(f, delimiter=SEPARADOR)
            reader.fieldnames = [c.strip().lower() for c in reader.fieldnames]

            # SALTO RÁPIDO: Avanzamos el lector si hay un checkpoint guardado
            if ultima_fila > 0:
                for _ in range(ultima_fila):
                    next(reader, None)

            # EMPEZAR PROCESAMIENTO
            for num_fila, fila in enumerate(reader, start=ultima_fila + 1):
                if num_fila > MAX_FILAS:
                    break

                lote.append(limpiar_fila(fila))

                # Ejecutar inserción cuando el bloque alcance el tamaño límite
                if len(lote) >= TAM_LOTE:
                    try:
                        insertar_copy(cur, lote)
                        conn.commit() # Asegura los datos en disco
                        total_insertadas += len(lote)
                        
                        guardar_checkpoint(num_fila) # Salva estado por si falla el próximo

                        elapsed      = time.perf_counter() - tiempo_inicio
                        fps_total    = total_insertadas / elapsed if elapsed > 0 else 0
                        num_lote += 1
                        
                        logging.info(
                            f"LOTE {num_lote:>4} | "
                            f"fila arch: {num_fila:>10,} | "
                            f"velocidad: {fps_total:>8,.0f} f/s | "
                            f"tiempo sesión: {elapsed:>6.1f}s"
                        )

                    except Exception as e:
                        conn.rollback() # Revierte transacciones corruptas
                        total_errores += 1
                        logging.error(f"Error en lote {num_lote + 1} | fila ~{num_fila:,} | {e}")

                    lote = [] # Vaciado de memoria

            # Procesamiento del último lote sobrante (menor al tamaño máximo)
            if lote:
                try:
                    insertar_copy(cur, lote)
                    conn.commit()
                    total_insertadas += len(lote)
                    guardar_checkpoint(num_fila)
                    num_lote += 1
                    logging.info(f"LOTE FINAL {num_lote} OK | insertadas: {total_insertadas:,}")
                except Exception as e:
                    conn.rollback()
                    total_errores += 1
                    logging.error(f"Error en último lote | {e}")

    except FileNotFoundError:
        logging.error(f"Archivo no encontrado: {ARCHIVO}")
        conn.close()
        return

    tiempo_total = time.perf_counter() - tiempo_inicio

    # ── 8. Validación Final y Resumen ──────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM rav.victimas;")
    total_bd = cur.fetchone()[0]

    logging.info("=" * 60)
    logging.info("RESUMEN FINAL DE EJECUCIÓN")
    logging.info("=" * 60)
    logging.info(f"Filas procesadas (histórico) : {num_fila:,}")
    logging.info(f"Filas insertadas (sesión)    : {total_insertadas:,}")
    logging.info(f"Total en Base de Datos       : {total_bd:,}")
    logging.info(f"Lotes con error              : {total_errores}")
    logging.info(f"Tiempo de ejecución          : {tiempo_total:.2f} segundos")
    
    if tiempo_total > 0:
        logging.info(f"Rendimiento Promedio         : {total_insertadas / tiempo_total:,.0f} filas/seg")

    # Generación de reporte en JSON para auditoría de la migración
    os.makedirs("resultados", exist_ok=True)
    reporte = {
        "fecha_ejecucion" : datetime.now().isoformat(),
        "archivo_origen"  : os.path.basename(ARCHIVO),
        "metodo_carga"    : "COPY via StringIO",
        "destino"         : f"rav.{TABLA}",
        "filas_procesadas": num_fila,
        "insert_sesion"   : total_insertadas,
        "total_en_bd"     : total_bd,
        "tiempo_segundos" : round(tiempo_total, 2)
    }
    
    with open("resultados/carga_copy_resumen.json", "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)

    logging.info("=" * 60)

    cur.close()
    conn.close()

if __name__ == "__main__":
    cargar()