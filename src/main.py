# -*- coding: utf-8 -*-

import pandas as pd

from src.api_utils.weather_api import (
    get_centre_current_forecast_weather,
    get_centre_historical_weather,
)
from src.processing.post_process import generate_centres_df, generate_top_df
from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter
from src.save_results.plotters import plot_max, plot_min
from src.service_utils import project_root


def main():
    """"""

    data_frames_gen = df_generator(str(project_root() / "data/hotels.zip"))
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames_gen])

    centre_info = generate_centres_df(df_full)

    lats = centre_info["Latitude"].values
    lons = centre_info["Latitude"].values
    cities = centre_info["City"].values
    weather_dfs = []

    for lat, lon, city in zip(lats, lons, cities):
        weather_dfs.append(get_centre_historical_weather(lat, lon, city))
        weather_dfs.append(get_centre_current_forecast_weather(lat, lon, city))

    complete_weather_df = pd.concat(weather_dfs, ignore_index=True)
    s_complete_weather_df = complete_weather_df.sort_values("Date")

    plot_min(s_complete_weather_df, "Paris")
    plot_max(s_complete_weather_df, "Paris")

    top_df = generate_top_df(s_complete_weather_df)

    return s_complete_weather_df, top_df


if __name__ == "__main__":
    main()
