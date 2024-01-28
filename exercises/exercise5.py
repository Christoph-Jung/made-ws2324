import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, Float
import os
import urllib.request
import zipfile

DATABASE_NAME = "gtfs.sqlite"
TABLE_NAME = "stops"
URL = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"


def download_and_load_data(url: str) -> pd.DataFrame:
    file_name = "stops.txt"
    local_zip, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(local_zip, 'r') as file_pointer:
        file_pointer.extract(file_name)

    return pd.read_csv(file_name, sep=",", on_bad_lines="warn")


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    # Only the columns stop_id, stop_name, stop_lat, stop_lon, zone_id with fitting data types
    keep_col = ["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"]
    data.drop(data.columns.difference(keep_col), 1, inplace=True)

    # Filter: Only keep stops from zone 2001
    data = data.loc[data['zone_id'] == 2001]

    # stop_lat/stop_lon must be a geographic coordinates between -90 and 90, including upper/lower bounds
    # check for numeric values first
    data = data[pd.to_numeric(data['stop_lat'], errors='coerce').notnull()]
    data = data[pd.to_numeric(data['stop_lon'], errors='coerce').notnull()]
    data['stop_lat'] = pd.to_numeric(data['stop_lat'], errors='coerce')
    data['stop_lon'] = pd.to_numeric(data['stop_lon'], errors='coerce')

    data = data.loc[(data.stop_lat >= -90) & (data.stop_lat <= 90) & (data.stop_lon >= -90) & (data.stop_lon <= 90)]

    return data


def get_dtypes() -> dict:
    type_dict = {'stop_id': Integer(), 'stop_name': String(), 'stop_lat': Float(),
                 'stop_lon': Float(), 'zone_id': Integer()}
    return type_dict


def create_db(data: pd.DataFrame, type_dict: dict) -> None:
    cwd = os.getcwd()
    if os.path.split(cwd)[1] != "exercises":
        path_db = f"sqlite:///{DATABASE_NAME}"
    else:
        path_db = f"sqlite:///../{DATABASE_NAME}"
    engine = create_engine(path_db)
    data.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, dtype=type_dict)


def main() -> None:
    data = download_and_load_data(URL)
    data = clean_data(data)
    type_dict = get_dtypes()
    create_db(data, type_dict)


if __name__ == '__main__':
    main()
