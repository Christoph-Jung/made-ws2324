import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String
import os

DATABASE_NAME = "cars.sqlite"
TABLE_NAME = "cars"
URL = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"


def read_data_csv(url: str) -> pd.DataFrame:
    return pd.read_csv(URL, sep=";", dtype={1: str}, header=None,
                       encoding='iso-8859-1', on_bad_lines="warn",
                       skiprows=7, skipfooter=4, engine='python')


def get_dtypes(data: pd.DataFrame) -> dict:
    type_dict = {'date': String(), 'CIN': String(), 'name': String(),
                 'petrol': Integer(), 'diesel': Integer(), 'gas': Integer(),
                 'electro': Integer(), 'hybrid': Integer(), 'plugInHybrid': Integer(),
                 'others': Integer()}
    return type_dict


def create_db(data: pd.DataFrame, type_dict: dict) -> None:
    cwd = os.getcwd()
    if os.path.split(cwd)[1] != "exercises":
        path_db = f"sqlite:///{DATABASE_NAME}"
    else:
        path_db = f"sqlite:///../{DATABASE_NAME}"
    engine = create_engine(path_db)
    data.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, dtype=type_dict)


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    keep_col = [0, 1, 2, 12, 22, 32, 42, 52, 62, 72]
    data.drop(data.columns.difference(keep_col), 1, inplace=True)

    # rename columns
    data.columns = ["date", "CIN", "name", "petrol", "diesel", "gas", "electro", "hybrid",
                    "plugInHybrid", "others"]

    # get every column except the given three
    cols = data.columns.difference(["date", "CIN", "name"])
    data[cols] = data[cols].apply(pd.to_numeric, errors='coerce', axis=1)
    data.dropna(inplace=True)

    # make this prettier later
    data.drop(data[data.petrol <= 0].index, inplace=True)
    data.drop(data[data.diesel <= 0].index, inplace=True)
    data.drop(data[data.gas <= 0].index, inplace=True)
    data.drop(data[data.electro <= 0].index, inplace=True)
    data.drop(data[data.hybrid <= 0].index, inplace=True)
    data.drop(data[data.plugInHybrid <= 0].index, inplace=True)
    data.drop(data[data.others <= 0].index, inplace=True)

    data = data[data['CIN'].str.contains(r'[0-9][1-9][0-9]{3}|[1-9][0-9]{4}$', na=False)]

    return data


def main() -> None:
    data = read_data_csv(URL)
    data = clean_data(data)
    type_dict = get_dtypes(data)
    create_db(data, type_dict)


if __name__ == '__main__':
    main()
