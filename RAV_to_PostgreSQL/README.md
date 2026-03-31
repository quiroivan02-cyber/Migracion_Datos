# 🚀 Data Pipeline: Migración Masiva RAV a PostgreSQL

Pipeline ETL de alto rendimiento diseñado para procesar y cargar archivos masivos de más de **4.5 GB (15 millones de registros)** desde texto plano hacia PostgreSQL, manteniendo un consumo de memoria mínimo y garantizando la integridad de los datos.

## 📌 El Reto
Cargar un dataset gubernamental gigante (Registro Único de Víctimas) en una base de datos relacional. 
**El problema:** Métodos tradicionales como `pandas` o `INSERT` saturaban la memoria RAM y requerían tiempos de ejecución inviables ante posibles desconexiones o fallos de red.

## 💡 La Solución
Se desarrolló una arquitectura en Python puro utilizando **Generadores**, limpieza en tránsito y el comando **`COPY` nativo de PostgreSQL** ejecutado desde la RAM. 

### ✨ Features Principales
* **🚀 Alto Rendimiento:** Alcanza velocidades de **~12,000 filas/segundo** al evitar escrituras en disco intermedio mediante `io.StringIO`.
* **🧠 Eficiencia de Memoria:** Lee el archivo en chunks. El script nunca consume más de 50 MB de RAM, sin importar si el archivo pesa 1 GB o 50 GB.
* **🛡️ Tolerancia a Fallos (Checkpoints):** Implementa guardado de estado automático. Si se va la luz o el internet, el script reanuda desde el último registro exitoso, sin volver a empezar.
* **🔄 Idempotencia (Upsert):** Uso de tabla temporal (`staging`) y `ON CONFLICT DO NOTHING` para garantizar cero registros duplicados.
* **🧹 Limpieza Dinámica:** Estandarización automática de fechas defectuosas (ej. años en 2 dígitos), casteo seguro de nulos (`\N`) y limpieza de strings corruptos.

## 🛠️ Stack Tecnológico
* **Lenguaje:** Python 3.12 (`csv`, `io`, `json`, `datetime`)
* **Base de Datos:** PostgreSQL 16
* **Infraestructura:** Docker (Contenedor `postgres:16.2`)
* **Librerías principales:** `psycopg2-binary`

## 📂 Estructura del Repositorio
\`\`\`bash
├── dataset/                                # (Archivos crudos excluidos por tamaño)
├── logs/                                   # Historial de ejecución por lotes
├── resultados/                             # Reportes JSON de auditoría y métricas
├── esquema_destino.sql                     # DDL de la BD (con soporte para nulos)
├── inspeccion.py                            # Script EDA (Exploratory Data Analysis)
├── benchmark.py                            # Pruebas comparativas (INSERT vs COPY)
├── carga.py                           # Script principal del ETL
└── requirements.txt                        # Dependencias del entorno
\`\`\`

## 🚀 Guía Rápida de Ejecución

### 1. Levantar la Base de Datos
\`\`\`bash
docker run --name migra_postgres16 -e POSTGRES_PASSWORD=migracion123 -e POSTGRES_USER=admin_migra -e POSTGRES_DB=migracion_db -p 5432:5432 -d postgres:16
\`\`\`

### 2. Crear la Estructura (Tabla destino e Índices)
\`\`\`bash
docker exec -i migra_postgres16 psql -U admin_migra -d migracion_db < esquema_destino.sql
\`\`\`

### 3. Ejecutar la Migración
\`\`\`bash
pip install -r requirements.txt
python3 carga_copy.py
\`\`\`

## 📊 Benchmark y Resultados
Para seleccionar la arquitectura, se evaluaron distintos tamaños de lote (*chunk sizes*) y métodos de inserción sobre una muestra de 50,000 registros del archivo original:

| Método de Inserción | Tamaño de Lote | Tiempo Total | RAM Pico | Velocidad |
| :--- | :--- | :--- | :--- | :--- |
| `execute_values` (INSERT) | 5,000 filas | 18.52 s | 45.2 MB | 2,700 f/s |
| `execute_values` (INSERT) | 10,000 filas | 16.10 s | 52.1 MB | 3,105 f/s |
| `execute_values` (INSERT) | 25,000 filas | 15.80 s | 78.5 MB | 3,164 f/s |
| **`COPY` vía StringIO** | **10,000 filas** | **4.20 s** | **35.0 MB** | **11,904 f/s** |
| `COPY` vía StringIO | 25,000 filas | 3.95 s | 68.2 MB | 12,658 f/s |
| `COPY` vía StringIO | 50,000 filas | 4.10 s | 125.4 MB | 12,195 f/s |

**Decisión Técnica:** Se implementó `COPY` vía `StringIO` con lotes de **10,000 filas**, logrando una mejora de velocidad superior al 380% respecto a `execute_values`, mientras se mantiene una huella de memoria (35 MB) estable y segura para procesos de horas de duración.

---
**Desarrollado por:** [Ivan Quiroga](https://github.com/quiroivan02-cyber)  
*Data Engineer Trainee / Software Developer*