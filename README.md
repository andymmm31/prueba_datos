
# Pipeline de Procesamiento de Seguros

Este proyecto implementa un pipeline de procesamiento de datos End-to-End para una aseguradora, manejando la ingesta, validación, transformación y carga de datos de pólizas y siniestros.

## Descripción General

El pipeline está diseñado para procesar archivos CSV diarios, validarlos, transformarlos con PySpark y orquestar el flujo completo con Airflow.

### Flujo del Proceso

1.  **Generación de Datos**: Se generan archivos CSV sintéticos (`polizas_YYYYMMDD.csv`, `siniestros_YYYYMMDD.csv`) con un ~5% de errores.
2.  **Validación**: Se valida el esquema y la calidad de los datos. Si la tasa de error supera el 10%, los archivos se mueven a una carpeta de cuarentena.
3.  **Transformación**: Si los datos son válidos, un job de PySpark los une y calcula un resumen agregado por producto.
4.  **Carga**: Los datos transformados se cargan en una tabla de BigQuery (simulado).
5.  **Auditoría**: Se registra el resultado de cada ejecución en una tabla de auditoría.

## Estructura del Repositorio

```
pipeline-seguros/
├── README.md
├── requirements.txt
├── dags/
│   └── pipeline_polizas_dag.py
├── scripts/
│   ├── generar_datos.py
│   ├── validacion.py
│   └── transformaciones.py
├── sql/
│   └── create_tables.sql
└── tests/
    ├── test_generador.py
    ├── test_validacion.py
    └── test_transformaciones.py
```

## Guía de Inicio Rápido (Quickstart)

### Prerrequisitos

-   Python 3.8+
-   Apache Spark (para ejecución local)
-   Airflow (para orquestación)

### 1. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 2. Generar Datos de Prueba

```bash
python scripts/generar_datos.py
# Esto creará polizas_YYYYMMDD.csv y siniestros_YYYYMMDD.csv
```

### 3. Ejecutar Validación Manualmente

```bash
python scripts/validacion.py polizas_YYYYMMDD.csv siniestros_YYYYMMDD.csv
# Salida esperada: VALIDO o CORRUPTO
```

### 4. Ejecutar Transformación Manualmente

```bash
spark-submit scripts/transformaciones.py polizas_YYYYMMDD.csv siniestros_YYYYMMDD.csv /tmp/output_resumen
# Reemplaza la ruta de salida según sea necesario
```

### 5. Ejecutar Pruebas Unitarias

```bash
python -m unittest discover tests/
```

## Decisiones Técnicas

-   **Simplicidad**: El código es intencionalmente simple y directo, evitando complejidad innecesaria para facilitar la comprensión.
-   **Modularidad**: Los scripts de generación, validación y transformación están separados, permitiendo pruebas y mantenimiento independientes.
-   **PySpark**: Elegido por su capacidad de procesamiento distribuido y escalabilidad, incluso en modo local.
-   **Airflow**: Utilizado para la orquestación por ser el estándar de la industria, proporcionando robustez, reintentos y programación.
-   **Pruebas Unitarias**: Se incluyen pruebas para cada componente crítico para asegurar la fiabilidad y facilitar el desarrollo futuro.
-   **SQL Declarativo**: El esquema de la base de datos se define en un archivo `.sql` para mantener la infraestructura como código.
