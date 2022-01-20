# -*- coding: utf-8 -*-

from typing import Iterator
from zipfile import ZipFile

import pandas as pd


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
            yield pd.read_csv(file_csv)


def df_cleaner(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleaner to drop invalid rows from hotels DataFrames
    Can detect wrong latitude/longitude values
    Does not work inplace

    :param new_df: Pandas dataframe to clear
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


if __name__ == "__main__":
    new_gen = df_generator(r"C:\Storage\Coding\EPAM_Traininng\weather_analysis\data\hotels.zip")
    next(new_gen)
    df = next(new_gen)
    df_2 = df_cleaner(df)
    print()
