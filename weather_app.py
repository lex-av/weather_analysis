# -*- coding: utf-8 -*-

import logging
from pathlib import Path

import click

from src.processing.enriching import enrich_with_geo_data, enrich_with_weather_data
from src.processing.post_process import generate_centres_df, generate_top_df
from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter
from src.save_results.data_saving_utils import (
    generate_and_save_plots,
    initialise_dir_structure,
    save_centre_data,
    save_general_statistics,
    slice_and_save_city_hotels_data,
)

logging.basicConfig(level=logging.INFO)


@click.command()
@click.option("--data_path", help="Path to zip archive with hotels data. Relative paths is allowed")
@click.option("--output_path", help="Path to dir, where output data will be stored. Relative paths is allowed")
def main(data_path, output_path):
    """
    Project main pipeline

    :param data_path: Path to zip archive with hotels data
    :param output_path: Path to dir, where output data will be stored
    """

    # Specify data paths
    if not Path(data_path).is_absolute():
        data_path = str(Path().cwd() / data_path)
    if not Path(output_path).is_absolute():
        output_path = str(Path().cwd() / output_path)

    # Forming tables from local data
    logging.info("Collecting data from .zip ...")
    data_frames_gen = df_generator(data_path)
    df_hotels = df_group_and_filter([df_cleaner(df) for df in data_frames_gen])
    centre_info = generate_centres_df(df_hotels)
    logging.info("Done!")

    # Enriching from external APIs
    logging.info("Collecting geodata from API ...")
    enrich_with_geo_data(df_hotels)
    logging.info("Done!")
    logging.info("Collecting weather data from API ...")
    df_weather = enrich_with_weather_data(centre_info)
    logging.info("Done!")

    # Create directories for storing output
    logging.info("Collecting and saving collected info ...")
    initialise_dir_structure(output_path, df_hotels)

    # Save collected hotels data and weather data for every city
    for city in df_hotels["City"].unique():
        slice_and_save_city_hotels_data(output_path, df_hotels, city)
        save_centre_data(output_path, df_weather, df_hotels, city)

    # Generate general statistics generation and save
    top_df = generate_top_df(df_weather)
    save_general_statistics(output_path, top_df)
    logging.info("Done!")

    logging.info("Generating and saving plots ...")
    generate_and_save_plots(df_weather, df_hotels, output_path)
    logging.info("Done!")


if __name__ == "__main__":
    main()
