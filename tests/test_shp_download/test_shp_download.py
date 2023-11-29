import pytest
from pathlib import Path
import os
import shutil

from src.special_steps.shp_file_downloader import get_shp_files
from src.utils.get_config import config

shp_urls_list = config.shp_urls
shape_files_directory = Path(__file__).parent / 'test_shp_files'
gminy = shape_files_directory / 'gminy'
jednostki_ewidencyjne = shape_files_directory / 'jednostki_ewidencyjne'
powiaty = shape_files_directory / 'powiaty'


def test_get_shp_files():
    assert not shape_files_directory.is_dir()

    get_shp_files(shp_urls_list, shape_files_directory)

    assert shape_files_directory.is_dir()
    assert jednostki_ewidencyjne.is_dir()
    assert powiaty.is_dir()
    assert gminy.is_dir()
    assert len(os.listdir(jednostki_ewidencyjne)) > 0
    assert len(os.listdir(powiaty)) > 0
    assert len(os.listdir(gminy)) > 0

    shutil.rmtree(shape_files_directory)

    assert not shape_files_directory.is_dir()
