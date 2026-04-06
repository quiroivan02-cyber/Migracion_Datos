# extraccion/oracle_reader.py
import oracledb
import pandas as pd
import logging
from config.settings import ORACLE_CONFIG, PAGE_SIZE

log = logging.getLogger(__name__)


class OracleReader:

    def __init__(self):
        self.con    = None
        self.cursor = None

    def conectar(self):
        dsn = (
            f"{ORACLE_CONFIG['host']}:"
            f"{ORACLE_CONFIG['port']}/"
            f"{ORACLE_CONFIG['service_name']}"
        )
        self.con    = oracledb.connect(
            user     = ORACLE_CONFIG["user"],
            password = ORACLE_CONFIG["password"],
            dsn      = dsn,
        )
        self.cursor = self.con.cursor()
        log.info("Conexión Oracle establecida")

    def cerrar(self):
        if self.cursor:
            self.cursor.close()
        if self.con:
            self.con.close()
        log.info("Conexión Oracle cerrada")

    # ── Conversión de LOBs ────────────────────────────────────────────────────
    @staticmethod
    def _leer_fila(fila):
        resultado = []
        for valor in fila:
            if isinstance(valor, oracledb.LOB):
                resultado.append(valor.read())
            else:
                resultado.append(valor)
        return resultado

    # ── Auto-detección de PKs ─────────────────────────────────────────────────
    def detectar_pks(self, tablas: list) -> dict:
        placeholders = ", ".join(f":t{i}" for i in range(len(tablas)))
        sql = f"""
            SELECT cons.table_name,
                   MIN(cols.column_name)
                       KEEP (DENSE_RANK FIRST ORDER BY cols.position)
            FROM all_constraints cons
            JOIN all_cons_columns cols
              ON cons.constraint_name = cols.constraint_name
             AND cons.owner           = cols.owner
            WHERE cons.constraint_type = 'P'
              AND cons.table_name IN ({placeholders})
            GROUP BY cons.table_name
        """
        bind = {f"t{i}": t.upper() for i, t in enumerate(tablas)}
        self.cursor.execute(sql, bind)
        pks = {row[0]: row[1] for row in self.cursor.fetchall()}

        for t in tablas:
            if t.upper() not in pks:
                log.warning(f"{t}: sin PK definida → usando ROWID")
                pks[t.upper()] = "ROWID"

        log.info(f"PKs detectadas: {pks}")
        return pks

    # ── COUNT para reconciliación ─────────────────────────────────────────────
    def count_tabla(self, tabla: str, col_fecha, fecha_inicio: str, fecha_fin: str) -> int:
        if col_fecha:
            sql = f"""
                SELECT COUNT(*) FROM {tabla}
                WHERE {col_fecha} >= TO_TIMESTAMP(:fecha_ini, 'YYYY-MM-DD')
                  AND {col_fecha} <  TO_TIMESTAMP(:fecha_fin, 'YYYY-MM-DD')
                                     + INTERVAL '1' DAY
            """
            self.cursor.execute(sql, fecha_ini=fecha_inicio, fecha_fin=fecha_fin)
        else:
            self.cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        return self.cursor.fetchone()[0]

    # ── Extracción paginada ───────────────────────────────────────────────────
    def extraer_tabla(self, tabla: str, col_fecha, pk: str,
                      fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
        todos    = []
        offset   = 0
        columnas = None

        while True:
            if col_fecha:
                sql = f"""
                    SELECT * FROM {tabla}
                    WHERE {col_fecha} >= TO_TIMESTAMP(:fecha_ini, 'YYYY-MM-DD')
                      AND {col_fecha} <  TO_TIMESTAMP(:fecha_fin, 'YYYY-MM-DD')
                                         + INTERVAL '1' DAY
                    ORDER BY {pk}
                    OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
                """
                self.cursor.execute(sql,
                    fecha_ini=fecha_inicio, fecha_fin=fecha_fin,
                    offset=offset, page_size=PAGE_SIZE)
            else:
                sql = f"""
                    SELECT * FROM {tabla}
                    ORDER BY {pk}
                    OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
                """
                self.cursor.execute(sql, offset=offset, page_size=PAGE_SIZE)

            if columnas is None:
                columnas = [col[0] for col in self.cursor.description]

            filas = self.cursor.fetchall()
            if not filas:
                break

            # ── Convertir LOBs antes de crear el DataFrame ────────────────
            filas_limpias = [self._leer_fila(f) for f in filas]
            todos.append(pd.DataFrame(filas_limpias, columns=columnas))

            offset += PAGE_SIZE
            log.info(f"  {tabla}: {offset} filas leídas...")

        return (
            pd.concat(todos, ignore_index=True)
            if todos
            else pd.DataFrame(columns=columnas or [])
        )