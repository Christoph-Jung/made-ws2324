import pandas as pd
from sqlalchemy import create_engine
import os
import math

DATABASE_NAME = "airports.sqlite"
TABLE_NAME = "airports"
URL = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"


def read_data_csv(url: str) -> pd.DataFrame:
    return pd.read_csv(url, sep=";", on_bad_lines="warn")


def get_dtypes(data: pd.DataFrame) -> dict:
    type_dict = {}
    for column in data.columns:
        type_dict[str(column)] = get_dtype_col(data[column])


def get_dtype_col(col) -> str:
    '''
        choose one of the five
            NULL. The value is a NULL value.
            INTEGER. The value is a signed integer, stored in 0, 1, 2, 3, 4, 6, or 8 bytes depending on
                the magnitude of the value.
            REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
            TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
            BLOB. The value is a blob of data, stored exactly as it was input.

        available types for the column
    '''
    data_type = col.dtype
    if data_type == "float64":
        return "REAL"
    elif data_type == "int64":
        return "INTEGER"
    else:
        for entry in col:
            if not isinstance(entry, str):
                if entry is None or math.isnan(entry):
                    continue
                print(entry)
                return "BLOB"
        return "TEXT"


def create_db(data: pd.DataFrame, type_dict: dict) -> None:
    cwd = os.getcwd()
    if os.path.split(cwd)[1] != "exercises":
        # path_db = f"sqlite:///data/{DATABASE_NAME}"
        path_db = f"sqlite:///{DATABASE_NAME}"
    else:
        # path_db = f"sqlite:///../data/{DATABASE_NAME}"
        path_db = f"sqlite:///../{DATABASE_NAME}"
    engine = create_engine(path_db)
    data.to_sql(TABLE_NAME, engine, if_exists="replace", dtype=type_dict)


def main() -> None:
    data = read_data_csv(URL)
    type_dict = get_dtypes(data)
    create_db(data, type_dict)


if __name__ == '__main__':
    main()
