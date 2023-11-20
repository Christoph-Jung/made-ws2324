import os
from typing import Tuple
import kaggle
import pandas as pd
import urllib3

from zipfile import ZipFile
from sqlalchemy import create_engine


def authenticate() -> None:
    try:
        kaggle.api.authenticate()
        print("Authenticated using kaggle.json")
    except OSError:
        raise RuntimeError("Couldn't find kaggle.json in ~ directory. Aborting...")


def download_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        kaggle.api.dataset_download_files("isaienkov/nba2k20-player-dataset", path="./data", unzip=True)
        kaggle.api.dataset_download_file(dataset="vivovinco/19912021-nba-stats",
                                         file_name="player_mvp_stats.csv",
                                         path="./data")

    except kaggle.rest.ApiException:
        raise RuntimeError("A Kaggle REST Api Exception occured, please check paths and status of dataset")
    except urllib3.exceptions.MaxRetryError:
        raise SystemExit("Network connection timed out")
    except Exception as ex:
        raise SystemExit(f"Download failed with unknown error {ex}")

    # unzip
    if os.path.exists("./data/player_mvp_stats.csv.zip"):
        with ZipFile("./data/player_mvp_stats.csv.zip", 'r') as file_pointer:
            file_pointer.extractall(path="./data")

    # read into pandas
    stats_2k = pd.read_csv("./data/nba2k-full.csv")
    stats_player = pd.read_csv("./data/player_mvp_stats.csv",
                               delimiter=";",
                               encoding="us-ascii",
                               encoding_errors='replace')

    # remove csv files to free space
    os.remove("./data/nba2k-full.csv")
    os.remove("./data/player_mvp_stats.csv")

    return stats_2k, stats_player


def process_data(stats_2k: pd.DataFrame, stats_player: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    stats_2k = stats_2k[['full_name', 'rating', 'version']]
    stats_player_cleaned = stats_player.where(stats_player['Year'] == 2020)
    stats_player_cleaned.dropna(inplace=True)

    return stats_2k, stats_player_cleaned


def save_sqlite(db: pd.DataFrame, db_name: str, table_name: str, type_dict: dict = None) -> None:
    cwd = os.getcwd()
    if os.path.split(cwd)[1] != "project":
        path_db = f"sqlite:///data/{db_name}"
    else:
        path_db = f"sqlite:///../data/{db_name}"
    print(f"Saving the table {table_name} in database {db_name} at {path_db}")
    engine = create_engine(path_db)

    if type_dict is None:
        db.to_sql(table_name, engine, if_exists="replace", index=False)
    else:
        db.to_sql(table_name, engine, if_exists="replace", index=False, dtype=type_dict)


def workflow():
    authenticate()
    d1, d2 = download_data() 
    stats_2k_cleaned, stats_player_cleaned = process_data(d1, d2)
    save_sqlite(stats_2k_cleaned, "main_db", "stats_2k")
    save_sqlite(stats_player_cleaned, "main_db", "stats_player")


if __name__ == "__main__":
    workflow()
