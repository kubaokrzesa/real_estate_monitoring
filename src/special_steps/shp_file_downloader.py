"""
Downloading shapefiles that will be used in other steps of the pipeline
"""
from typing import Optional, List
import requests
import os
from urllib.parse import unquote
import zipfile
from src.utils.setting_logger import Logger
from src.utils.get_config import config


logger = Logger(__name__).get_logger()


def get_disposition_filename(disposition: str) -> Optional[str]:
    """
    Extracts the filename from the Content-Disposition header.

    Args:
        disposition (str): Content-Disposition header content.

    Returns:
        Optional[str]: The extracted filename, if available; otherwise, None.
    """
    if not disposition:
        return None
    parts = disposition.split(';')
    for part in parts:
        if 'filename=' in part:
            filename = part.split('=')[1]
            if filename.lower().startswith("utf-8''"):
                filename = unquote(filename[7:])  # decode UTF-8 escaped chars
            else:
                filename = filename.strip("\"'")
            return filename
    return None


def download_file(url: str, local_folder: str) -> str:
    """
    Downloads a file from a given URL to a specified local folder.

    Args:
        url (str): The URL of the file to download.
        local_folder (str): The local directory to save the downloaded file.

    Returns:
        str: The path to the downloaded file.
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        # Getting file name from Content-Disposition header
        filename = get_disposition_filename(r.headers.get('Content-Disposition'))
        if not filename:
            # Use last part of URL as a file name if no Content-Disposition
            filename = url.split('/')[-1]

        local_filename = os.path.join(local_folder, filename)

        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return local_filename


def unzip_file(zip_file: str) -> str:
    """
    Extracts the contents of a zip file to a directory.

    Args:
        zip_file (str): The path to the zip file.

    Returns:
        str: The name of the output directory where files were extracted.
    """
    output_dir_name = zip_file.split('.')[0]
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(output_dir_name)
    return output_dir_name


def remove_remaining_zip(zip_file: str) -> None:
    """
    Deletes the specified zip file if it exists.

    Args:
        zip_file (str): The path to the zip file to delete.
    """
    if os.path.exists(zip_file):
        os.remove(zip_file)
        logger.info(f"File {zip_file} has been deleted")
    else:
        logger.info(f"File {zip_file} does not exist")


def get_shp_files(shp_urls: List[str], shp_files_directory_name: str) -> None:
    """
    Downloads and unzips shapefiles from given URLs to a specified directory.

    Args:
        shp_urls (List[str]): A list of URLs of shapefiles to download.
        shp_files_directory_name (str): The directory name where shapefiles will be stored.
    """
    os.makedirs(shp_files_directory_name, exist_ok=True)

    for shp_url in shp_urls:
        downloaded_file_path = download_file(shp_url, shp_files_directory_name)
        logger.info(f'File has been downloaded and saved as: {downloaded_file_path}')

        output_dir_name = unzip_file(downloaded_file_path)
        logger.info(f'File has been unzipped to: {output_dir_name}')

        remove_remaining_zip(downloaded_file_path)


if __name__ == "__main__":
    get_shp_files(config.shp_urls, config.shp_files_directory_name)



