import pandas as pd
import numpy as np

def add_lat_lon(df:pd.DataFrame, station_name:str):
    lat_lon_data = "data/pre_processing/additional_features/latitude_longitude_data.csv"
    lat_lon_df = pd.read_csv(lat_lon_data, index_col ='Station Name')

    # We extract the latitude and longitude values
    lat_lon = lat_lon_df.loc[station_name]

    # We create a dataframe with reapeating latitude and longitude values
    latitude, longitude = lat_lon['Latitude'], lat_lon['Longitude']
    lat_long = np.array([[latitude for _ in range(len(df.index))], [longitude for _ in range(len(df.index))]]).T
    lat_long_features = pd.DataFrame(lat_long, columns=['Latitude', 'Longitude'])

    # We concatinate the latitude features
    df = pd.concat([df, lat_long_features], axis=1)

    return df
