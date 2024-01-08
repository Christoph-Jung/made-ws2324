import pandas as pd
import numpy as np
import os

from sqlalchemy import create_engine
from typing import Tuple
from sklearn.linear_model import LogisticRegression


def read_in_data(db_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Read in the source data from the sqlite database into two dataframes
    '''
    cwd = os.getcwd()
    if os.path.split(cwd)[1] != "project":
        path_db = f"sqlite:///data/{db_name}"
    else:
        path_db = f"sqlite:///../data/{db_name}"

    try:
        engine = create_engine(path_db)
        with engine.connect() as eng:
            stats_2k = pd.read_sql_table("stats_2k", eng)
            stats_player = pd.read_sql_table("stats_player", eng)
    except ValueError:
        raise ValueError(f"Couldn't find database {path_db} or one of the two tables. Aborting...") from None

    return stats_2k, stats_player


def create_common_dataframe(stats_2k: pd.DataFrame, stats_player: pd.DataFrame) -> pd.DataFrame:
    '''
    Take the two datasets and combine them into one dataset for analysis
    Drop all columns, which are unnecessary for the prediction
    '''
    # drop columns, which are not useful for predicting rating
    DROP_COLS = ['Pos', 'Age', 'Tm', 'GS', 'Year', 'Pts Won', 'Pts Max', 'Share', 'Team', 'W', 'L',
                 'W/L%', 'GB', 'PS/G', 'PA/G', 'SRS']

    stats_player.drop(DROP_COLS, axis=1, inplace=True)

    new_df = None
    for index, orig_row in stats_player.iterrows():
        df_row = stats_player.loc[[index]]  # need row as dataframe, not Series -> do not use orig_row

        row = stats_2k.loc[stats_2k['full_name'] == df_row['Player'][index]]
        if row.shape[0] > 1:
            row = row.loc[row['version'] == "NBA2k20"]
        if row.shape[0] == 0:
            continue

        salary = round(int(row['salary'].values[0][1:]) / 1000000, 3)
        rating = row['rating'].values[0]
        new_row = pd.DataFrame(df_row).assign(salary=salary)
        new_row = pd.DataFrame(new_row).assign(rating=rating)

        if new_df is None:
            new_df = new_row
        else:
            new_df = pd.concat([new_row, new_df.loc[:]]).reset_index(drop=True)

    return new_df


def create_groups(analysis_df: pd.DataFrame, group_count: int) -> list[pd.DataFrame]:
    '''
    Shuffle the dataframe row-wise and split it in <group_count> different distinct groups
    '''
    analysis_df = analysis_df.sample(frac=1).reset_index(drop=True)
    return np.array_split(analysis_df, group_count)


def create_regression_model(dataset: pd.DataFrame) -> LogisticRegression:
    '''
    Using saga solver to avoid performance issues of lbfgs for poorly scaled data.
    Saga is used for sparse multinomial logistic regression
    '''
    y = dataset['rating'].to_numpy()
    X = dataset.drop(['Player', 'rating'], axis=1).to_numpy()
    clf = LogisticRegression(solver='saga', max_iter=10000).fit(X, y)
    return clf


def match_values(dataset: pd.DataFrame, models: list[LogisticRegression]) -> pd.DataFrame:
    '''
    Use the <models> to get ratings for the players in <dataset>. Each player gets the average
    of the computed ratings as its appropriate rating
    '''
    ds = dataset.drop(['Player', 'rating'], axis=1).to_numpy()
    results = []
    for model in models:
        results.append(model.predict(ds))

    final_result = np.rint((1 / len(models)) * sum(results))
    dataset.insert(len(dataset.columns), "Appr. Rating", final_result, allow_duplicates=True)

    return dataset


def main_workflow() -> Tuple[pd.DataFrame, list[LogisticRegression]]:
    '''
    Main workflow to create one combined dataframe from the two data sources
    and the regression models
    '''
    stats_2k, stats_player = read_in_data("main_db.sqlite")
    analysis_df = create_common_dataframe(stats_2k, stats_player)

    # split data into four groups and compute a logistic regression model on each of them
    groups = create_groups(analysis_df, 4)
    models = []
    for dataset in groups:
        models.append(create_regression_model(dataset))

    # use the average of the three other groups to get an appropriate rating
    for ind in range(len(groups)):
        groups[ind] = match_values(groups[ind], [x for i, x in enumerate(models) if i != ind])

    # concat the dataframes
    return pd.concat(groups), models


if __name__ == "__main__":
    main_workflow()
