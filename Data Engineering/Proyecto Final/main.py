import pandas as pd
from functions import fetch_random_city_weather_data, process_weather_data, load_data_to_redshift, process_csv_data 


def main():
    data = process_csv_data('./Carpeta_Prueba/lat_lon_data.csv')
    lat = data[0]
    lon = data[1]

    weather_data_list = fetch_random_city_weather_data(lat, lon)

    data2 = process_weather_data(weather_data_list[0], weather_data_list[1], weather_data_list[2])

    df = pd.DataFrame(data2)

    print(df)

    load_data_to_redshift(df)

main()