# ETL Oracle → CSV

Pipeline de migración de datos desde Oracle Database hacia archivos CSV con manifiesto de integridad.

## Descripción

Extrae tablas del sistema hospitalario desde Oracle XE, aplica perfilado y limpieza de datos, y genera CSVs con manifiestos JSON que permiten verificar la integridad de cada archivo en cualquier momento futuro.

## Estructura del proyecto
etl_oracle_csv/
├── config/
│ ├── settings.py # DSN, rutas, constantes globales
│ └── tablas.yaml # Definición de tablas: fecha_col
├── extraccion/
│ ├── oracle_reader.py # Clase OracleReader: conexión y paginación
│ └── control.py # Registro de extracciones en SQLite
├── transformacion/
│ ├── perfilador.py # Generación de reporte de calidad
│ └── limpieza.py # Limpieza por tabla
├── carga/
│ └── csv_writer.py # Escritura CSV final + manifiesto JSON
├── tests/
│ ├── test_perfilador.py
│ └── test_limpieza.py
├── datos/ # CSVs extraídos desde Oracle
├── datos_limpios/ # CSVs limpios y archivos de problemas
├── salida/ # CSVs finales + manifiestos .json
├── reportes/ # reporte_calidad + entregable_2A
├── logs/ # Logs por ejecución
├── pipeline.py # Orquestación completa E → T → L
└── requirements.txt

## Requisitos

- Python 3.10+
- Oracle Database XE (modo thin — sin Oracle Client)
- Dependencias:

```bash
pip install -r requirements.txt
```

**`requirements.txt`**
oracledb
pandas
pyyaml
pytest

## Configuración

Edita `config/settings.py` con los datos de tu Oracle:

```python
ORACLE_CONFIG = {
    "host":         "localhost",
    "port":         1521,
    "service_name": "XEPDB1",
    "user":         "hospital_admin",
    "password":     "Hospital123",
}
```

Para agregar o modificar tablas edita `config/tablas.yaml`:

```yaml
tablas:
  NUEVA_TABLA:
    fecha_col: created_at   # null si es tabla de catálogo
```

## Uso

### Pipeline completo

```bash
python pipeline.py
```

### Solo una fase

```python
# Extracción
from extraccion import OracleReader

# Transformación
from transformacion import ejecutar_perfilado, ejecutar_limpieza
ejecutar_perfilado("2020-01-01", "2026-12-31")
ejecutar_limpieza("2020-01-01", "2026-12-31")

# Carga
from carga import ejecutar_carga
ejecutar_carga("2020-01-01", "2026-12-31")
```

### Forzar re-extracción completa

```python
# pipeline.py
ejecutar_pipeline("2020-01-01", "2026-12-31", reanudar=False)
```

## Convención de archivos de salida
TABLA_YYYY_MM-MM.csv
TABLA_YYYY_MM-MM.manifest.json

**Ejemplo:**
CITAS_MEDICAS_2020_01-12.csv
CITAS_MEDICAS_2020_01-12.manifest.json

## Estructura del manifiesto

```json
{
  "tabla":          "CITAS_MEDICAS",
  "periodo_inicio": "2020-01-01",
  "periodo_fin":    "2026-12-31",
  "archivo":        "CITAS_MEDICAS_2020_01-12.csv",
  "filas_oracle":   150000,
  "filas_csv":      150000,
  "md5":            "d41d8cd98f00b204e9800998ecf8427e",
  "columnas":       ["CITA_ID", "PACIENTE_ID", "..."],
  "problemas_calidad": {},
  "generado_en":    "2026-04-05T03:24:34Z",
  "generado_por":   "etl_oracle_csv v1.0"
}
```

## Tests

```bash
pytest tests/ -v
```

## Tablas incluidas

| Tabla | Filas | Tipo |
|---|---|---|
| PACIENTES | 100,000 | Transaccional |
| CITAS_MEDICAS | 150,000 | Transaccional |
| FACTURACION | 123,085 | Transaccional |
| EXAMENES_MEDICOS | 49,197 | Transaccional |
| RESULTADOS_EXAMENES | 49,197 | Transaccional |
| MEDICAMENTOS_RECETADOS | 57,427 | Transaccional |
| INTERNACIONES | 20,000 | Transaccional |
| MEDICOS | 200 | Transaccional |
| ESPECIALIDADES | 15 | Catálogo |
| MUNICIPIOS | 15 | Catálogo |

## Autor

Ivan Quiroga — Entrenamiento en Migración de Datos
