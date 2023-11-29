import time
import os
from pathlib import Path


def timer(func):
    def f(*args, **kwargs):
        start = time.time()
        rv = func(*args, **kwargs)
        end = time.time()
        print(end - start)
        return rv
    return f


def check_if_shp_exists(shape_files_directory: Path) -> None:
    gminy = shape_files_directory / 'gminy'
    jednostki_ewidencyjne = shape_files_directory / 'jednostki_ewidencyjne'
    powiaty = shape_files_directory / 'powiaty'

    cond = all([shape_files_directory.is_dir(),
                jednostki_ewidencyjne.is_dir(),
                powiaty.is_dir(),
                gminy.is_dir()])
    if cond:
        cond = all([len(os.listdir(jednostki_ewidencyjne)) > 0,
                    len(os.listdir(powiaty)) > 0,
                    len(os.listdir(gminy)) > 0])
    return cond
