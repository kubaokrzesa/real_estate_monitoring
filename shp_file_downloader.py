import requests
import os
from urllib.parse import unquote
import zipfile


shp_files_directory_name = "shp_files"

shp_urls = ["https://www.gis-support.pl/downloads/2022/gminy.zip",
"https://www.gis-support.pl/downloads/2022/powiaty.zip"]


def get_disposition_filename(disposition):
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


def download_file(url, local_folder):
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


def unzip_file(zip_file):
    output_dir_name = zip_file.split('.')[0]
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(output_dir_name)
    return output_dir_name


def remove_remaining_zip(zip_file):
    if os.path.exists(zip_file):
        os.remove(zip_file)
        print(f"File {zip_file} has been deleted")
    else:
        print(f"File {zip_file} does not exist")


def get_shp_files(shp_urls, shp_files_directory_name):
    os.makedirs(shp_files_directory_name, exist_ok=True)

    for shp_url in shp_urls:
        downloaded_file_path = download_file(shp_url, shp_files_directory_name)
        print(f'File has been downloaded and saved as: {downloaded_file_path}')

        output_dir_name = unzip_file(downloaded_file_path)
        print(f'File has been unzipped to: {output_dir_name}')

        remove_remaining_zip(downloaded_file_path)


if __name__ == "__main__":
    get_shp_files(shp_urls, shp_files_directory_name)



