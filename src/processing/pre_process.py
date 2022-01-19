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
            df = pd.read_csv(file_csv)

            # Using to_numeric to replace to NaN all invalid values
            df.Latitude = pd.to_numeric(df.Latitude, errors="coerce")
            df.Longitude = pd.to_numeric(df.Longitude, errors="coerce")
            df.dropna(inplace=True)
            yield df


if __name__ == "__main__":
    new_gen = df_generator(r"C:\Storage\Coding\EPAM_Traininng\weather_analysis\data\hotels.zip")
    next(new_gen)
    df = next(new_gen)
    print()
