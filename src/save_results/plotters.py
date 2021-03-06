# -*- coding: utf-8 -*-
import pathlib
from typing import Union

import matplotlib.pyplot as plt
import pandas as pd


def plot_min(centres_df: pd.DataFrame, hotels_df: pd.DataFrame, city: str, base_dir: Union[str, pathlib.Path]):
    """
    Plots information about day minimum temperature of
    given city

    :param centres_df: pandas DataFrame, containing weather information about city centre
    :param hotels_df: pandas DataFrame, containing weather information about
    cities, countries and hotels
    information about city centres
    :param city: City from hotels_df to plot (Capitalized)
    """

    city_name = city
    country = hotels_df[hotels_df["City"] == city]["Country"].unique()[0]
    dates_vector = centres_df[centres_df["City"] == city]["Date"]
    temps_vector = centres_df[centres_df["City"] == city]["MinTemp"]
    delta = (max(temps_vector) - min(temps_vector)) // 2  # for plots prettifying

    plt.plot(dates_vector, temps_vector, linewidth=2, color="green")
    plt.xlabel("Date")
    plt.ylabel("Temperature, Celsius")
    plt.title(f"Min temperatures in centre of {city}")
    plt.grid()
    plt.ylim(top=max(temps_vector) + delta)
    plt.ylim(bottom=min(temps_vector) - delta)
    plt.xticks(rotation=90)
    plt.savefig(str(base_dir) + f"/{country}/{city_name}/plots/{city_name.lower()}_min_temperatures.png")
    plt.close()


def plot_max(centres_df: pd.DataFrame, hotels_df: pd.DataFrame, city: str, base_dir: Union[str, pathlib.Path]):
    """
    Plots information about day maximum temperature of
    given city

    :param centres_df: pandas DataFrame, containing weather
    information about city centres
    :param hotels_df: pandas DataFrame, containing weather information about
    cities, countries and hotels
    :param city: City from hotels_df to plot (Capitalized)
    """

    city_name = city
    country = hotels_df[hotels_df["City"] == city]["Country"].unique()[0]
    dates_vector = centres_df[centres_df["City"] == city]["Date"]
    temps_vector = centres_df[centres_df["City"] == city]["MaxTemp"]
    delta = (max(temps_vector) - min(temps_vector)) // 2  # for plots prettifying

    plt.plot(dates_vector, temps_vector, linewidth=2, color="green")
    plt.xlabel("Date")
    plt.ylabel("Temperature, Celsius")
    plt.title(f"Max temperatures in centre of {city}")
    plt.grid()
    plt.ylim(top=max(temps_vector) + delta)
    plt.ylim(bottom=min(temps_vector) - delta)
    plt.xticks(rotation=90)
    plt.savefig(str(base_dir) + f"/{country}/{city_name}/plots/{city_name.lower()}_max_temperatures.png")
    plt.close()


if __name__ == "__main__":
    pass
