import os
import kaggle
import pandas as pd

from zipfile import ZipFile

import urllib3


def authenticate() -> None:
    try:
        kaggle.api.authenticate()
        print("Authenticated using kaggle.json")
    except OSError:
        raise RuntimeError("Couldn't find kaggle.json in ~ directory. Aborting...")


def download_data() -> None:
    try:
        kaggle.api.dataset_download_files("isaienkov/nba2k20-player-dataset", path="./data", unzip=True)
        kaggle.api.dataset_download_files("vivovinco/19912021-nba-stats", "player_mvp_stats.csv", path="./data")
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
    stats_player = pd.read_csv("./data/player_mvp_stats.csv")

    # remove csv files to free space
    os.remove("./data/nba2k-full.csv")
    os.remove("./data/player_mvp_stats.csv")

    return stats_2k, stats_player


def workflow():
    authenticate()
    return download_data()
