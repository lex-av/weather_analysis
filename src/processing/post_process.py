# -*- coding: utf-8 -*-

import pandas as pd

from src.api_utils.geodata_api import calc_centre


def generate_centres_df(cities_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates DataFrame of city centres and their coordinates

    :param cities_df: pandas DataFrame of City/Hotels data
    :return: DataFrame of Cities and their centre coordinates
    """

    data_dict = {"City": list(cities_df["City"].unique())}
    centre_list = []

    for city in data_dict["City"]:
        centre_list.append(calc_centre(cities_df[cities_df["City"] == city]))

    data_dict["Latitude"] = [centre[0] for centre in centre_list]
    data_dict["Longitude"] = [centre[1] for centre in centre_list]
    df_centres = pd.DataFrame.from_dict(data_dict)

    return df_centres


def generate_top_df(centres_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates DataFrame with calculated overall statistics about
    given city centres. Collects following metrics:
        -City with max temperature
        -City with max delta of max temp
        -City with min temperature
        -City with max delta of max and min temp

    :param centres_df: pandas DataFrame with weather data for
    city centres
    :return: Smaller DataFrame with calculated overall statistics
    """

    # max temp
    max_temp_row = list(centres_df.iloc[centres_df["MaxTemp"].idxmax()][["City", "Date"]])

    city_list = list(centres_df["City"].unique())
    delta_dct = {}
    for city in city_list:
        min_ = centres_df[centres_df["City"] == city]["MaxTemp"].min()
        max_ = centres_df[centres_df["City"] == city]["MaxTemp"].max()
        delta = abs(max_ - min_)
        delta_dct[delta] = city

    max_delta_val = max(delta_dct.keys())
    max_delta_city = delta_dct[max_delta_val]
    # random data for this row to avoid nan in resulting df
    max_delta_row = [max_delta_city, centres_df["Date"][0]]

    # min temp
    min_temp_row = list(centres_df.iloc[centres_df["MaxTemp"].idxmin()][["City", "Date"]])

    # max min max delta
    centres_df["Delta"] = centres_df["MaxTemp"] - centres_df["MinTemp"]
    max_min_delta_row = centres_df.iloc[centres_df["MaxTemp"].idxmax()][["City", "Date"]]

    # DataFrame building
    all_rows = [max_temp_row, max_delta_row, min_temp_row, max_min_delta_row]
    cities = [row[0] for row in all_rows]
    dates = [row[1] for row in all_rows]
    indexes = [
        "City with max temperature",
        "City with max delta of max temp",
        "City with min temperature",
        "City with max delta of max and min temp",
    ]

    data_dct = {"City": cities, "Date": dates}
    result_df = pd.DataFrame(data_dct, index=indexes)

    return result_df


if __name__ == "__main__":
    pass
