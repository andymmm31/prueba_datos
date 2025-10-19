-- DDL para Tablas de BigQuery

-- Tabla: resumen_producto_diario
-- Descripción: Almacena el resumen diario de pólizas y siniestros por producto.
-- Particionada por: fecha_carga (diaria)
CREATE OR REPLACE TABLE `your_project.your_dataset.resumen_producto_diario` (
    producto STRING,
    numero_polizas INT64,
    total_suma_asegurada FLOAT64,
    total_prima_mensual FLOAT64,
    numero_siniestros INT64,
    total_monto_reclamado FLOAT64,
    fecha_carga DATE
)
PARTITION BY fecha_carga
OPTIONS (
    description="Resumen diario de pólizas y siniestros por producto."
);


-- Tabla: auditoria_proceso
-- Descripción: Registra los detalles de ejecución del pipeline de datos.
-- Particionada por: fecha_proceso (diaria)
CREATE OR REPLACE TABLE `your_project.your_dataset.auditoria_proceso` (
    proceso_id STRING,
    fecha_proceso DATE,
    nombre_archivo STRING,
    registros_validos INT64,
    registros_invalidos INT64,
    tasa_error FLOAT64,
    tiempo_ejecucion_seg INT64,
    estado_final STRING,
    mensaje_error STRING
)
PARTITION BY fecha_proceso
OPTIONS (
    description="Registro de auditoría para cada ejecución del pipeline."
);
