from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
from datetime import datetime
import psycopg2
import random
import pandas as pd

### TUVE QUE PASAR TODAS LAS FUNCIONES Y LA FUNCION MAIN A ESTE ARCHIVO
### YA QUE AUN AGREGANDO LA FUNCION AL PATH, AIRFLOW NO ME TOMABA LAS
### FUNCIONES IMPORTADAS DE OTROS ARCHIVOS EXTERNOS

"""FUNCIONES DEL ARCHIVO funcions.py"""


api_key = Variable.get('OPENWEATHER_API_KEY')
url = 'https://api.openweathermap.org/data/2.5/weather'

def fetch_random_city_weather_data(lat, lon):
    """Obtiene datos del clima de OpenWeatherMap usando coordenadas aleatorias."""
    lat = lat  # Latitud obtenida del archivo csv
    lon = lon  # Longitud obtenida del archivo csv
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': 'metric'  # Unidades en grados Celsius
    }
    response = requests.get(url, params=params)
    
    lat2 = round(lat, 4)
    lon2 = round(lon, 4)
    
    if response.status_code == 200:
        try:
            return response.json(), lat2, lon2  # Asegurarse de que devuelve JSON
        except ValueError:
            print(f"Error al parsear JSON para latitud {lat} y longitud {lon}: {response.text}")
            return None
    else:
        print(f"Error al obtener datos del clima para latitud {lat} y longitud {lon}: {response.status_code}")
        return None

def process_weather_data(weather_data, lat, lon):
    """Procesa un único diccionario de datos del clima."""
    if weather_data:
        entry = {
            'lat': lat,
            'lon': lon,
            'temperature': weather_data['main']['temp'],
            'humidity': weather_data['main']['humidity'],
            'weather_description': weather_data['weather'][0]['description'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return [entry]  # Devolvemos una lista con un solo elemento para que sea compatible con el resto del flujo
    return []

def load_data_to_redshift(df):
    """Carga o actualiza los datos del DataFrame en la base de datos Redshift."""
    redshift_credentials = {
        'user': Variable.get('REDSHIFT_USER'),
        'host': Variable.get('REDSHIFT_HOST'),
        'pass': Variable.get('REDSHIFT_PASS'),
        'db': Variable.get('REDSHIFT_DB'),
        'port': Variable.get('REDSHIFT_PORT')
    }


    try:
        conn = psycopg2.connect(
            dbname=redshift_credentials['db'],
            user=redshift_credentials['user'],
            password=redshift_credentials['pass'],
            host=redshift_credentials['host'],
            port=redshift_credentials['port']
        )
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            lat FLOAT,          
            lon FLOAT,          
            temperature FLOAT,
            humidity INTEGER,
            weather_description VARCHAR(255),
            timestamp TIMESTAMP,
            PRIMARY KEY (lat, lon)
        )
        ''')

        for index, row in df.iterrows():
            if row.isnull().any():  
                print(f"Fila {index} omitida debido a valores nulos: {row.to_dict()}")
                continue

            cursor.execute('''
            DELETE FROM weather WHERE lat = %s AND lon = %s
            ''', (row['lat'], row['lon']))

            cursor.execute('''
            INSERT INTO weather (lat, lon, temperature, humidity, weather_description, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ''', (row['lat'], row['lon'], row['temperature'], row['humidity'], row['weather_description'], row['timestamp']))

        conn.commit()

    except Exception as e:
        print(f"Error al cargar los datos en Redshift: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def process_csv_data(archivo_csv):
    csv = pd.read_csv(archivo_csv)
    random_row = csv.sample()
    random_lat = random_row['lat'].values[0]
    random_lon = random_row['lon'].values[0]
    lat = round(random_lat, 6)
    lon = round(random_lon, 6)
    return lat, lon


"""FUNCION MAIN DEL ARCHIVO main.py"""

def main():
    data = process_csv_data('/opt/airflow/lat_lon_data.csv')
    lat = data[0]
    lon = data[1]

    weather_data_list = fetch_random_city_weather_data(lat, lon)

    data2 = process_weather_data(weather_data_list[0], weather_data_list[1], weather_data_list[2])

    df = pd.DataFrame(data2)

    print(df)

    load_data_to_redshift(df)

main()


def check_value(**kwargs):
    # Obtén el valor que quieres verificar (puede ser de XCom u otra fuente)
    valor = kwargs['dag_run'].conf.get('valor')  # Ejemplo para obtener el valor desde la ejecución del DAG
    limite = 100  # Define tu límite

    if valor > limite:
        return 'send_alert'
    else:
        return 'do_nothing'

# Configuración de DAG
default_args={
    'owner': 'Agustin',
    'retries':5,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_success': False,
    'email_on_retry': True
}

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='Un DAG para obtener y cargar datos del clima',
    schedule_interval='* * * * *',
    start_date=datetime(2024,9,20),
    catchup=False,
)

task1 = BashOperator(task_id='primera_tarea',
    bash_command='echo Iniciando...' # && exit 1
)

task2 = PythonOperator(
    task_id='main',
    python_callable=main,
    dag=dag,
)

task3 = BashOperator(
    task_id= 'tercera_tarea',
    bash_command='echo Proceso completado...'
)

alert_failure = EmailOperator(
    task_id='send_failure_email',
    to='galloagustin785@gmail.com',
    subject='Alerta: Falla en el DAG',
    html_content='No se pudo realizar la carga del dato en REDSHIFT.',
    trigger_rule='one_failed'  # Se ejecuta si alguna tarea falla
    )
 
task1 >> [task2, task3]
task1 >> alert_failure
task2 >> alert_failure
task3 >> alert_failure