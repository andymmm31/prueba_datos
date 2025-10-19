
import pandas as pd
import sys

# --- Constantes ---
CORRUPTION_THRESHOLD = 0.10

# --- Definiciones de Esquema ---
POLIZAS_SCHEMA = {
    'poliza_id': object,
    'cliente_id': object,
    'producto': object,
    'suma_asegurada': float,
    'prima_mensual': float,
    'fecha_inicio': object, # Se mantiene como objeto, se puede validar formato si es necesario
    'estado': object,
    'region': object
}

SINIESTROS_SCHEMA = {
    'siniestro_id': object,
    'poliza_id': object,
    'fecha_siniestro': object,
    'tipo_siniestro': object,
    'monto_reclamado': float,
    'estado': object
}

# --- Lógica de Validación ---
def validate_schema(df, schema):
    """Valida que el DataFrame tenga las columnas requeridas."""
    for col in schema.keys():
        if col not in df.columns:
            print(f"Error: La columna faltante '{col}'")
            return False
    return True

def validate_data_quality(polizas_df, siniestros_df):
    """Valida la calidad de los datos y devuelve el número de errores."""
    errors = 0
    total_rows = len(polizas_df) + len(siniestros_df)

    # --- Validar Pólizas ---
    # No nulos
    errors += polizas_df.isnull().sum().sum()

    # suma_asegurada > 0
    errors += (polizas_df['suma_asegurada'] <= 0).sum()

    # prima_mensual > 0
    errors += (polizas_df['prima_mensual'] <= 0).sum()

    # --- Validar Siniestros ---
    # No nulos
    errors += siniestros_df.isnull().sum().sum()

    # monto_reclamado > 0
    errors += (siniestros_df['monto_reclamado'] <= 0).sum()

    # poliza_id existe en pólizas
    valid_poliza_ids = set(polizas_df['poliza_id'])
    errors += (~siniestros_df['poliza_id'].isin(valid_poliza_ids)).sum()

    error_rate = errors / total_rows if total_rows > 0 else 0
    return error_rate, errors

# --- Ejecución Principal ---
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python validacion.py <path_polizas.csv> <path_siniestros.csv>")
        sys.exit(1)

    polizas_path = sys.argv[1]
    siniestros_path = sys.argv[2]

    # --- Cargar Datos ---
    try:
        polizas = pd.read_csv(polizas_path)
        siniestros = pd.read_csv(siniestros_path)
    except FileNotFoundError as e:
        print(f"Error: Archivo no encontrado - {e}")
        sys.exit(1)

    # --- Validación de Esquema ---
    if not validate_schema(polizas, POLIZAS_SCHEMA) or \
       not validate_schema(siniestros, SINIESTROS_SCHEMA):
        print("Resultado: CORRUPTO (Fallo de esquema)")
        sys.exit(1)

    # --- Validación de Calidad de Datos ---
    error_rate, error_count = validate_data_quality(polizas, siniestros)

    print(f"Tasa de errores encontrada: {error_rate:.2%}")
    print(f"Número de errores: {error_count}")

    if error_rate > CORRUPTION_THRESHOLD:
        print("Resultado: CORRUPTO")
        sys.exit(1)
    else:
        print("Resultado: VALIDO")
        sys.exit(0)
