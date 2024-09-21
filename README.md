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

Obsevaciones:

En las alertas por emails, si bien en la consigna me pedia que se dispare cada vez que cierto valor supera un limite, en este caso no le encontre sentido a hacerlo de esa manera, por lo que directamente deje la alerta para cuando alguna tarea falla o se repite.
En el caso del modelado me pedia una fecha de informacion extraida y otra de informacion cargada, en este caso entiendo que es la misma para ambos casos por lo que decidi dejar una sola columna.



