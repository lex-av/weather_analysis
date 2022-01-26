# -*- coding: utf-8 -*-

from src.processing.enriching import enrich_with_geo_data, enrich_with_weather_data
from src.processing.post_process import generate_centres_df, generate_top_df
from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter
from src.save_results.data_saving_utils import (
    generate_and_save_plots,
    initialise_dir_structure,
    save_centre_data,
    slice_and_save_city_hotels_data,
)
from src.service_utils import project_root


def main():
    """"""

    data_frames_gen = df_generator(str(project_root() / "data/hotels.zip"))
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames_gen])
    enrich_with_geo_data(df_full)

    initialise_dir_structure("D:/plots", df_full)
    centre_info = generate_centres_df(df_full)
    slice_and_save_city_hotels_data("D:/plots", df_full, "Paris")

    complete_weather_df = enrich_with_weather_data(centre_info)
    save_centre_data("D:/plots", complete_weather_df, "FR", "Paris")

    top_df = generate_top_df(complete_weather_df)

    generate_and_save_plots(complete_weather_df, df_full, "D:/plots")

    return top_df


if __name__ == "__main__":
    # [f"{i:02}" for i in range(115)]
    main()
