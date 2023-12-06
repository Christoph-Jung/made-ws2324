from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError
from project import setup as data_pipeline


def test_store_dataframe():
    """ Tests the method to store a dataframe in a sqlite database """
    db_name = data_pipeline.DB_NAME

    try:
        engine = create_engine(f"sqlite:///data/{db_name}")
    except OperationalError:
        raise OperationalError(f"Couldn't open database under path data/{db_name}") from None

    try:
        inspector = inspect(engine)
    except OperationalError:
        raise RuntimeError(f"Couldn't open database under path data/{db_name}") from None

    tables = inspector.get_table_names()

    assert "stats_2k" in tables and "stats_player" in tables, "At least one table has an erroneous name"


if __name__ == '__main__':
    test_store_dataframe()
    print("Finished all further tests successfully!")
