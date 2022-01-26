# -*- coding: utf-8 -*-

import pandas as pd

from src.api_utils.geodata_api import collect_geo_data
from src.api_utils.weather_api import (
    get_centre_current_forecast_weather,
    get_centre_historical_weather,
)


def enrich_with_geo_data(df: pd.DataFrame):
    """
    Enrich given DataFrame with geographical addresses requested from
    external API of local cache using coordinates, presented in DataFrame
    Modifies DataFrame inplace

    :param df: pandas DataFrame with hotels data
    """

    df_lat_lon = df[["Latitude", "Longitude"]]
    addr_lst = collect_geo_data(df_lat_lon)
    df["Address"] = addr_lst


def enrich_with_weather_data(centre_df: pd.DataFrame):
    """

    :param centre_df: pandas DataFrame of city centres
    :return: Sorted centres DataFrame, enriched with weather data
    """

    lats = centre_df["Latitude"].values
    lons = centre_df["Latitude"].values
    cities = centre_df["City"].values
    weather_dfs = []

    for lat, lon, city in zip(lats, lons, cities):
        weather_dfs.append(get_centre_historical_weather(lat, lon, city))
        weather_dfs.append(get_centre_current_forecast_weather(lat, lon, city))

    complete_weather_df = pd.concat(weather_dfs, ignore_index=True)
    complete_weather_df = complete_weather_df.sort_values("Date", ignore_index=True)

    return complete_weather_df


if __name__ == "__main__":
    pass
