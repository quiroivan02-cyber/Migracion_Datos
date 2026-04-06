# pipeline.py
import os
import yaml
import logging
import argparse
from datetime import datetime
from config.settings import BASE_DIR, DATOS_DIR, LOGS_DIR

# ── Logging global ────────────────────────────────────────────────────────────
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [
        logging.FileHandler(
            os.path.join(LOGS_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            encoding="utf-8"
        ),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

from extraccion import OracleReader, init_control_db, registrar_extraccion, obtener_ultima_extraccion
from transformacion import ejecutar_perfilado, ejecutar_limpieza
from carga import ejecutar_carga


def cargar_tablas_config() -> dict:
    ruta = os.path.join(BASE_DIR, "config", "tablas.yaml")
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)["tablas"]


def fase_extraccion(fecha_inicio: str, fecha_fin: str,
                    tablas_filtro: list, reanudar: bool = True) -> bool:
    log.info("━━━ FASE E: EXTRACCIÓN ━━━")
    tablas_config = cargar_tablas_config()
    control_con   = init_control_db()
    reader        = OracleReader()
    errores       = 0

    # Filtrar tablas si se especificó una en particular
    if tablas_filtro and tablas_filtro != ["TODAS"]:
        tablas_config = {k: v for k, v in tablas_config.items()
                         if k.upper() in [t.upper() for t in tablas_filtro]}

    try:
        reader.conectar()
        pks = reader.detectar_pks(list(tablas_config.keys()))

        for tabla, cfg in tablas_config.items():
            if reanudar:
                ultimo = obtener_ultima_extraccion(tabla)
                if ultimo and ultimo["fecha_inicio"] == fecha_inicio:
                    log.info(f"  {tabla}: ya extraída → saltando")
                    continue

            col_fecha = cfg.get("fecha_col")
            pk        = pks.get(tabla.upper(), "ROWID")

            try:
                total_oracle = reader.count_tabla(tabla, col_fecha, fecha_inicio, fecha_fin)
                df           = reader.extraer_tabla(tabla, col_fecha, pk, fecha_inicio, fecha_fin)
                total_csv    = len(df)

                if total_csv != total_oracle:
                    msg = f"DISCREPANCIA: Oracle={total_oracle} | CSV={total_csv}"
                    log.error(f"  {tabla}: {msg}")
                    registrar_extraccion(control_con, tabla, fecha_inicio, fecha_fin,
                                         total_oracle, total_csv, "ERROR", msg)
                    errores += 1
                    break

                ruta_csv = os.path.join(DATOS_DIR, f"{tabla.lower()}_{fecha_inicio}_{fecha_fin}.csv")
                df.to_csv(ruta_csv, index=False, encoding="utf-8")
                registrar_extraccion(control_con, tabla, fecha_inicio, fecha_fin,
                                      total_oracle, total_csv, "OK")
                log.info(f"  {tabla}: OK — {total_csv} filas")

            except Exception as e:
                log.error(f"  {tabla}: ERROR — {e}")
                registrar_extraccion(control_con, tabla, fecha_inicio, fecha_fin,
                                      0, 0, "ERROR", str(e))
                errores += 1
                break

    finally:
        reader.cerrar()
        control_con.close()

    return errores == 0


def ejecutar_pipeline(fecha_inicio: str, fecha_fin: str,
                      tablas: list = None, reanudar: bool = True) -> bool:
    if tablas is None:
        tablas = ["TODAS"]

    log.info("=" * 55)
    log.info(f"  PIPELINE ETL — {fecha_inicio} → {fecha_fin}")
    log.info(f"  Tablas: {', '.join(tablas)}")
    log.info("=" * 55)

    if not fase_extraccion(fecha_inicio, fecha_fin, tablas, reanudar):
        log.error(" Pipeline detenido en EXTRACCIÓN")
        return False

    log.info("━━━ FASE T: TRANSFORMACIÓN ━━━")
    if not ejecutar_perfilado(fecha_inicio, fecha_fin):
        log.error(" Pipeline detenido en PERFILADO")
        return False

    if not ejecutar_limpieza(fecha_inicio, fecha_fin):
        log.error(" Pipeline detenido en LIMPIEZA")
        return False

    log.info("━━━ FASE L: CARGA ━━━")
    if not ejecutar_carga(fecha_inicio, fecha_fin):
        log.error(" Pipeline detenido en CARGA")
        return False

    log.info("PIPELINE COMPLETADO EXITOSAMENTE")
    return True


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL Oracle → CSV")
    parser.add_argument("--tabla",  default="TODAS",
                        help="Nombre de la tabla o TODAS (default: TODAS)")
    parser.add_argument("--inicio", required=True,
                        help="Fecha inicio en formato YYYY-MM-DD")
    parser.add_argument("--fin",    required=True,
                        help="Fecha fin en formato YYYY-MM-DD")
    parser.add_argument("--forzar", action="store_true",
                        help="Fuerza re-extracción ignorando el checkpoint")
    args = parser.parse_args()

    tablas = [args.tabla] if args.tabla != "TODAS" else ["TODAS"]

    ejecutar_pipeline(
        fecha_inicio = args.inicio,
        fecha_fin    = args.fin,
        tablas       = tablas,
        reanudar     = not args.forzar,
    )