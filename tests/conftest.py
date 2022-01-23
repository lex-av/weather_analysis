# -*- coding: utf-8 -*-

import os

import pytest


@pytest.fixture()
def get_path():
    return os.getcwd()  # CWD in configuration has to be root of a project
