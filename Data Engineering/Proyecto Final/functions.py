import requests
from datetime import datetime
import os
import psycopg2
from dotenv import load_dotenv
import random
import pandas as pd

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de la API
api_key = os.getenv('OPENWEATHER_API_KEY')
url = 'https://api.openweathermap.org/data/2.5/weather'

def fetch_random_city_weather_data():
    """Obtiene datos del clima de OpenWeatherMap usando coordenadas aleatorias."""
    lat = random.uniform(-90, 90)  # Latitud aleatoria
    lon = random.uniform(-180, 180)  # Longitud aleatoria
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
        'user': os.getenv('REDSHIFT_USER'),
        'host': os.getenv('REDSHIFT_HOST'),
        'pass': os.getenv('REDSHIFT_PASS'),
        'db': os.getenv('REDSHIFT_DB'),
        'port': os.getenv('REDSHIFT_PORT')
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

