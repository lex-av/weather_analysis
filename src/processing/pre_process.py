# -*- coding: utf-8 -*-

from typing import Iterator, List, Union
from zipfile import ZipFile

import pandas as pd

from src.api_utils.geodata_api import collect_geo_data


def df_generator(path: str) -> Iterator:
    """
    Generator of dataframes for next steps of data processing
    Drops rows with Nan or incorrect values on the fly

    :param path: Path to zip file with csv files
    :return: Generator of pandas dataframes
    """

    zip_src = ZipFile(path)
    tables = zip_src.namelist()

    for table in tables:
        with zip_src.open(table) as file_csv:
            # Skip Id from csv
            yield pd.read_csv(file_csv, usecols=["Name", "Country", "City", "Latitude", "Longitude"])


def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleaner to drop invalid rows from hotels DataFrames
    Can detect wrong latitude/longitude values
    Does not work inplace

    :param df: Pandas dataframe to clear
    :return: Cleared DataFrame
    """

    new_df = df.copy()

    # Drop rows, containing values like "abd176.2"
    new_df.Latitude = pd.to_numeric(new_df.Latitude, errors="coerce")
    new_df.Longitude = pd.to_numeric(new_df.Longitude, errors="coerce")
    new_df.dropna(inplace=True)

    # Drop invalid values of Longitude and Latitude
    new_df.drop(new_df[(new_df.Latitude > 90.0) | (new_df.Latitude < -90.0)].index, inplace=True)
    new_df.drop(new_df[(new_df.Longitude > 180.0) | (new_df.Longitude < -180.0)].index, inplace=True)

    return new_df


def df_group_and_filter(iterable: Union[List[pd.DataFrame], Iterator]) -> pd.DataFrame:
    """
    Concatenates smaller DataFrames into one bigger and
    groups it - for every country pick city
    with the most hotels and drop others

    :param iterable: List of DataFrames or Generator/Iterator of DataFrames
    :return: filtered concatenated DataFrame
    """

    # Ignoring index is important. Unexpected filtering otherwise
    df_complete = pd.concat(iterable, sort=False, ignore_index=True)

    # Prepare auxiliary data
    country_list = df_complete["Country"].unique()
    country_city_mapping = {}
    country_city_df = df_complete[["Country", "City"]]

    # Find and map best cities to their countries
    for country in country_list:
        this_country_cities = country_city_df[country_city_df["Country"] == country]
        best_city = (
            this_country_cities.groupby("City", sort=False)["Country"].count().sort_values(ascending=False).index[0]
        )

        country_city_mapping[country] = best_city

    # Drop all, except best cities
    for map_country, map_city in country_city_mapping.items():
        df_complete.drop(
            df_complete[(df_complete.Country == map_country) & (df_complete.City != map_city)].index, inplace=True
        )

    return df_complete


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


if __name__ == "__main__":
    pass
