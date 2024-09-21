El proyecto se trata sobre ETL de una API de clima a partir de latitud y longitud. Dichos valores son extraidos a partir de un archivo csv estatico con mil pares de valores, con dichos valores se consulta a la api y se extrae la data. 
MODELADO DE LA TABLA SQL:
        CREATE TABLE IF NOT EXISTS weather (
            lat FLOAT,          
            lon FLOAT,          
            temperature FLOAT,
            humidity INTEGER,
            weather_description VARCHAR(255),
            timestamp TIMESTAMP,
            PRIMARY KEY (lat, lon)
        )
