
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, lit, current_date
from pyspark.sql.types import DoubleType

def run_transformations(spark, polizas_path, siniestros_path, output_path):
    """
    Ejecuta transformaciones con PySpark para crear un resumen por producto.
    """
    # --- 1. Leer Datos ---
    polizas_df = spark.read.csv(polizas_path, header=True, inferSchema=True)
    siniestros_df = spark.read.csv(siniestros_path, header=True, inferSchema=True)

    # --- 2. Limpiar y Preparar Datos ---
    # Eliminar filas con nulos en columnas clave
    polizas_df = polizas_df.dropna(subset=['poliza_id', 'producto'])
    siniestros_df = siniestros_df.dropna(subset=['siniestro_id', 'poliza_id'])

    # Convertir columnas numéricas para asegurar cálculos correctos
    polizas_df = polizas_df.withColumn("suma_asegurada", col("suma_asegurada").cast(DoubleType())) \
                           .withColumn("prima_mensual", col("prima_mensual").cast(DoubleType()))

    siniestros_df = siniestros_df.withColumn("monto_reclamado", col("monto_reclamado").cast(DoubleType()))

    # --- 3. Unir DataFrames ---
    # Left join para mantener todas las pólizas, incluso si no tienen siniestros
    joined_df = polizas_df.join(siniestros_df, "poliza_id", "left")

    # --- 4. Agregar por Producto ---
    resumen_producto = joined_df.groupBy("producto") \
        .agg(
            count("poliza_id").alias("numero_polizas"),
            sum("suma_asegurada").alias("total_suma_asegurada"),
            sum("prima_mensual").alias("total_prima_mensual"),
            count("siniestro_id").alias("numero_siniestros"),
            sum("monto_reclamado").alias("total_monto_reclamado")
        )

    # --- 5. Añadir Columna de Auditoría ---
    final_df = resumen_producto.withColumn("fecha_carga", current_date())

    # --- 6. Escribir Salida ---
    # Escribir como un único archivo Parquet, particionado por fecha de carga
    print(f"Escribiendo resumen a {output_path}")
    final_df.coalesce(1).write.mode("overwrite").parquet(output_path)

    print("Transformación completada.")

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Uso: python transformaciones.py <path_polizas.csv> <path_siniestros.csv> <output_path>")
        sys.exit(1)

    polizas_path_arg = sys.argv[1]
    siniestros_path_arg = sys.argv[2]
    output_path_arg = sys.argv[3]

    spark_session = SparkSession.builder \
        .appName("SegurosTransformations") \
        .master("local[*]") \
        .getOrCreate()

    run_transformations(spark_session, polizas_path_arg, siniestros_path_arg, output_path_arg)

    spark_session.stop()
