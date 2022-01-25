# -*- coding: utf-8 -*-

import pathlib
from typing import Union

import pandas as pd

from src.save_results.plotters import plot_max, plot_min


def generate_and_save_plots(centres_df: pd.DataFrame, base_dir: Union[str, pathlib.Path]):
    """
    Generates plots of day minimum and day maximum temperature for every
    city centre in given DataFrame. Given directory to dump plots
    should exist

    :param centres_df: pandas DataFrame, containing weather
    information about city centres
    :param base_dir: path to previously created directory where created
    plots will be stored
    """

    for city in centres_df["City"].unique():
        plot_min(centres_df, city, base_dir)
        plot_max(centres_df, city, base_dir)


def initialise_dir_structure(basedir: Union[str, pathlib.Path], hotels_df: pd.DataFrame):
    """
    Initialises dir structure for collected data about hotels and
    city centers

    :param basedir: Directory to store all collected data
    :param hotels_df: DataFrame with info about cities, countries
    and hotels
    """

    countries_list = list(hotels_df["Country"].unique())
    countries_and_cities = {}
    for country in countries_list:
        countries_and_cities[country] = list(hotels_df[hotels_df["Country"] == country]["City"].unique())

    countries_and_cities_paths = []
    for country, city_list in countries_and_cities.items():
        countries_and_cities_paths += [f"{str(basedir)}/{country}/{city}/plots" for city in city_list]
        countries_and_cities_paths += [f"{str(basedir)}/{country}/{city}/hotels" for city in city_list]
        countries_and_cities_paths += [f"{str(basedir)}/{country}/{city}/centre_info" for city in city_list]

    for path_ in countries_and_cities_paths:
        path = pathlib.Path(path_)
        path.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    pass
