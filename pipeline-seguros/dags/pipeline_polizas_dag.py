
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# --- Constantes y Variables ---
# Idealmente, estas rutas se configurarían en las Variables de Airflow
BASE_PATH = "/opt/airflow/data" # Ruta simulada dentro del entorno de Airflow
SCRIPTS_PATH = "/opt/airflow/scripts"
QUARANTINE_PATH = f"{BASE_PATH}/quarantine"
OUTPUT_PATH = f"{BASE_PATH}/output"
FECHA_PROCESO = "{{ ds_nodash }}" # Fecha de ejecución de Airflow

POLIZAS_FILE = f"{BASE_PATH}/polizas_{FECHA_PROCESO}.csv"
SINIESTROS_FILE = f"{BASE_PATH}/siniestros_{FECHA_PROCESO}.csv"
TRANSFORMED_OUTPUT = f"{OUTPUT_PATH}/resumen_{FECHA_PROCESO}"

# --- Argumentos por Defecto ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

# --- Definición del DAG ---
dag = DAG(
    'pipeline_polizas_dag',
    default_args=default_args,
    description='Pipeline diario para procesamiento de pólizas y siniestros.',
    schedule_interval='0 2 * * *', # 2 AM diario
    catchup=False,
    tags=['seguros', 'data-engineering'],
)

# --- Definición de Tareas ---
inicio = DummyOperator(
    task_id='inicio',
    dag=dag,
)

# Esta tarea simula la verificación de archivos. En un caso real, podría ser un FileSensor.
validar_archivos = BashOperator(
    task_id='validar_archivos',
    bash_command=f"echo 'Validando archivos...' && test -f {POLIZAS_FILE} && test -f {SINIESTROS_FILE}",
    dag=dag,
)

# Ejecuta el script de validación. El script sale con 0 si es válido, 1 si está corrupto.
# El código de salida se guarda automáticamente en XComs.
validar_calidad = BashOperator(
    task_id='validar_calidad',
    bash_command=f"python {SCRIPTS_PATH}/validacion.py {POLIZAS_FILE} {SINIESTROS_FILE}",
    dag=dag,
)

# Decide qué ruta tomar según el resultado de la validación.
def decidir_ruta(**kwargs):
    ti = kwargs['ti']
    exit_code = ti.xcom_pull(task_ids='validar_calidad', key='return_value')
    if exit_code == 0:
        return 'transformar'
    else:
        return 'mover_a_cuarentena'

decidir_procesar = BranchPythonOperator(
    task_id='decidir_procesar',
    python_callable=decidir_ruta,
    provide_context=True,
    dag=dag,
)

# --- Ruta de Éxito ---
transformar = BashOperator(
    task_id='transformar',
    bash_command=f"python {SCRIPTS_PATH}/transformaciones.py {POLIZAS_FILE} {SINIESTROS_FILE} {TRANSFORMED_OUTPUT}",
    dag=dag,
)

# En un caso real, esto usaría el BigQueryOperator.
cargar_bq = BashOperator(
    task_id='cargar_bq',
    bash_command=f"echo 'Cargando {TRANSFORMED_OUTPUT} a BigQuery... (simulado)'",
    dag=dag,
)

# En un caso real, esto registraría métricas en la tabla auditoria_proceso.
auditoria = BashOperator(
    task_id='auditoria',
    bash_command="echo 'Registrando metricas de exito en tabla de auditoria... (simulado)'",
    dag=dag,
)

notificar_exito = BashOperator(
    task_id='notificar_exito',
    bash_command="echo 'Proceso completado exitosamente. Enviando notificacion... (simulado)'",
    trigger_rule='all_success', # Asegura que todas las tareas anteriores en la ruta hayan tenido éxito
    dag=dag,
)


# --- Ruta de Fallo ---
mover_a_cuarentena = BashOperator(
    task_id='mover_a_cuarentena',
    bash_command=f"mkdir -p {QUARANTINE_PATH} && mv {POLIZAS_FILE} {QUARANTINE_PATH}/ && mv {SINIESTROS_FILE} {QUARANTINE_PATH}/",
    dag=dag,
)

notificar_error = BashOperator(
    task_id='notificar_error',
    bash_command="echo 'Proceso fallido debido a datos corruptos. Enviando notificacion... (simulado)'",
    trigger_rule='all_success',
    dag=dag,
)

# --- Dependencias de Tareas ---
inicio >> validar_archivos >> validar_calidad >> decidir_procesar

# Rama de éxito
decidir_procesar >> transformar >> cargar_bq >> auditoria >> notificar_exito

# Rama de fallo
decidir_procesar >> mover_a_cuarentena >> notificar_error
