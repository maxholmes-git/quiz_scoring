import polars as pl
from typing import List


def validate_answer_sheet(player, player_df, master_df):
    valid_columns = ["Question", "Answer"]
    invalid_columns = [column for column in player_df.columns if column not in valid_columns]
    if invalid_columns:
        raise ValueError(f"Invalid columns for player: {player}\n"
                         f"Columns found: {player_df.columns}\n"
                         f"Columns expected: {valid_columns}")

    if not player_df.select("Question").frame_equal(master_df.select("Question")):
        raise ValueError(f"Invalid values in Question column for player: {player}")
