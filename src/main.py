# -*- coding: utf-8 -*-
import pandas as pd

from src.connection_utils.weather_api import (
    build_historical_weather_data_df,
    get_city_centre_historical_weather,
)
from src.processing.post_process import generate_centres_df
from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter
from src.service_utils import project_root


def main():
    """"""

    data_frames_gen = df_generator(str(project_root() / "data/hotels.zip"))
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames_gen])

    centre_info = generate_centres_df(df_full)

    lats = centre_info["Latitude"].values
    lons = centre_info["Latitude"].values
    cities = centre_info["City"].values

    weather1 = get_city_centre_historical_weather(lats[0], lons[0], cities[0])
    weather2 = get_city_centre_historical_weather(lats[1], lons[1], cities[1])

    wdf1 = build_historical_weather_data_df(weather1)
    wdf2 = build_historical_weather_data_df(weather2)

    new_wdf = pd.concat([wdf1, wdf2])

    return new_wdf


if __name__ == "__main__":
    main()
