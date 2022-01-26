# -*- coding: utf-8 -*-

from src.processing.post_process import generate_centres_df
from src.processing.pre_process import df_cleaner, df_generator, df_group_and_filter


def test_generate_centres_df(get_path):
    data_frames = df_generator(get_path + "/tests/test_data/hotels_test_data.zip")
    df_full = df_group_and_filter([df_cleaner(df) for df in data_frames])
    centre_df = generate_centres_df(df_full)

    assert list(centre_df.keys()) == ["City", "Latitude", "Longitude"]
