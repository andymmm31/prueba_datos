
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# --- Constantes ---
NUM_POLIZAS = 10000
NUM_SINIESTROS = 2000
ERROR_RATE = 0.05
FECHA_PROCESO = datetime.now()

PRODUCTOS = ['AUTO', 'VIDA', 'HOGAR']
ESTADOS_POLIZA = ['ACTIVA', 'CANCELADA']
REGIONES = ['CDMX', 'GDL', 'MTY']

TIPOS_SINIESTRO = ['CHOQUE', 'ROBO', 'INCENDIO']
ESTADOS_SINIESTRO = ['PENDIENTE', 'APROBADO', 'RECHAZADO']

# --- Funciones Auxiliares ---
def introduce_errors(df, error_rate):
    """Introduce errores en un DataFrame."""
    n_rows = len(df)
    n_errors = int(n_rows * error_rate)

    for _ in range(n_errors):
        row_idx = random.randint(0, n_rows - 1)
        col_name = random.choice(df.columns)

        # Introduce diferentes tipos de errores
        error_type = random.choice(['null', 'invalid_value'])

        if error_type == 'null':
            # Evita que las llaves primarias sean nulas
            if col_name not in ['poliza_id', 'siniestro_id']:
                 df.loc[row_idx, col_name] = np.nan

        elif error_type == 'invalid_value':
            if col_name == 'suma_asegurada' or col_name == 'prima_mensual':
                df.loc[row_idx, col_name] = -100
            elif col_name == 'monto_reclamado':
                df.loc[row_idx, col_name] = 0

    return df

# --- Generación de Datos ---
def generar_polizas(n):
    """Genera datos sintéticos de pólizas."""
    data = []
    for i in range(n):
        fecha_inicio = FECHA_PROCESO - timedelta(days=random.randint(30, 3650))
        poliza = {
            'poliza_id': f"POL{i+1:07}",
            'cliente_id': fake.uuid4(),
            'producto': random.choice(PRODUCTOS),
            'suma_asegurada': random.randint(50000, 1000000),
            'prima_mensual': random.randint(500, 10000),
            'fecha_inicio': fecha_inicio.strftime('%Y-%m-%d'),
            'estado': random.choice(ESTADOS_POLIZA),
            'region': random.choice(REGIONES)
        }
        data.append(poliza)

    df = pd.DataFrame(data)
    return introduce_errors(df, ERROR_RATE)

def generar_siniestros(n, polizas_df):
    """Genera datos sintéticos de siniestros basados en las pólizas."""
    data = []
    poliza_ids = polizas_df['poliza_id'].tolist()

    for i in range(n):
        # 5% de probabilidad de que un poliza_id no exista
        if random.random() < 0.05:
            poliza_id_siniestro = f"POL{random.randint(NUM_POLIZAS + 1, NUM_POLIZAS + 100):07}"
        else:
            poliza_id_siniestro = random.choice(poliza_ids)

        fecha_siniestro = FECHA_PROCESO - timedelta(days=random.randint(1, 1000))
        siniestro = {
            'siniestro_id': f"SIN{i+1:07}",
            'poliza_id': poliza_id_siniestro,
            'fecha_siniestro': fecha_siniestro.strftime('%Y-%m-%d'),
            'tipo_siniestro': random.choice(TIPOS_SINIESTRO),
            'monto_reclamado': random.randint(1000, 500000),
            'estado': random.choice(ESTADOS_SINIESTRO)
        }
        data.append(siniestro)

    df = pd.DataFrame(data)
    return introduce_errors(df, ERROR_RATE)

# --- Ejecución Principal ---
if __name__ == "__main__":

    # Generar datos
    polizas_df = generar_polizas(NUM_POLIZAS)
    siniestros_df = generar_siniestros(NUM_SINIESTROS, polizas_df)

    # Guardar en CSV
    fecha_str = FECHA_PROCESO.strftime('%Y%m%d')
    polizas_filename = f"polizas_{fecha_str}.csv"
    siniestros_filename = f"siniestros_{fecha_str}.csv"

    polizas_df.to_csv(polizas_filename, index=False)
    siniestros_df.to_csv(siniestros_filename, index=False)

    print(f"Archivos generados: {polizas_filename}, {siniestros_filename}")
