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


if __name__ == "__main__":
    pass
