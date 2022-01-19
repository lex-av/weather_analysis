# -*- coding: utf-8 -*-

import pytest

from src.processing.pre_process import df_generator


def test_df_generator_read_csv():
    new_gen = df_generator(r"test_data/hotels_test_data.zip")
    assert list(next(new_gen).columns) == ["Id", "Name", "Country", "City", "Latitude", "Longitude"]


def test_df_generator_read_all():
    new_gen = df_generator(r"test_data/hotels_test_data.zip")
    next(new_gen)
    next(new_gen)
    with pytest.raises(StopIteration):
        next(new_gen)
