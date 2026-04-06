# config/settings.py
import os
from dotenv import load_dotenv

# Carga las variables del .env automáticamente
load_dotenv()

# ── Rutas base ────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATOS_DIR    = os.path.join(BASE_DIR, "datos")
CLEAN_DIR    = os.path.join(BASE_DIR, "datos_limpios")
REPORTES_DIR = os.path.join(BASE_DIR, "reportes")
LOGS_DIR     = os.path.join(BASE_DIR, "logs")
CONTROL_DB   = os.path.join(BASE_DIR, "control_etl.db")

# ── Conexión Oracle — leída desde .env ───────────────────────────────────────
ORACLE_CONFIG = {
    "host":         os.getenv("ORACLE_HOST",    "localhost"),
    "port":         int(os.getenv("ORACLE_PORT", "1521")),
    "service_name": os.getenv("ORACLE_SERVICE", "XEPDB1"),
    "user":         os.getenv("ORACLE_USER",    "hospital_admin"),
    "password":     os.getenv("ORACLE_PASSWORD", ""),
}

# ── Extracción ────────────────────────────────────────────────────────────────
PAGE_SIZE = 10_000

# ── Metadatos del proyecto ────────────────────────────────────────────────────
ETL_VERSION = "etl_oracle_csv v1.0"