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


if __name__ == "__main__":
    pass
