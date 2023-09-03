import polars as pl
import pandas as pd
import shutil
import os
from ast import literal_eval
from typing import List

def create_player_answer_sheets(player_list):
    meta_file = "quiz_metadata.xlsx"
    template_file = "template_answer_sheet.xlsx"
    if os.path.exists(meta_file):
        metadata_df = pl.read_excel(source="quiz_metadata.xlsx",
                                    read_csv_options={"dtypes": {"Question": pl.Utf8}})
        metadata_df = metadata_df.drop(["Round_type", "Valid_answers"])\
            .with_columns(pl.lit("").alias("Answer"))
        metadata_df.write_excel(template_file,
                                dtype_formats={pl.Float64: "@"})
        for name in player_list:
            new_file_name = name + ".xlsx"
            if os.path.exists(new_file_name):
                print(f"File {new_file_name} already exists.")
            else:
                shutil.copy(template_file, new_file_name)
                print(f"File {new_file_name} created.")
    else:
        print(f"{meta_file} not found")

def post_quiz_cleanup(player_list):
    # Iterate over the list of file names
    for file_name in [f"{player}.xlsx" for player in player_list]:
        # Delete the file
        try:
            os.remove(file_name)
            print(f"File {file_name} deleted.")
        except FileNotFoundError:
            print(f"File {file_name} not found.")

def create_master_table(player_list):
    master_df = pl.read_excel("quiz_metadata.xlsx",
                              read_csv_options={"dtypes": {"Question": pl.Utf8}})\
        .with_columns(pl.col("Question").cast(pl.Utf8))\
        .with_columns(pl.col("Question").str.split(".").list.get(0).alias("Round"))\
        .with_columns(pl.col("Valid_answers").str.split(","))

    for player in player_list:
        player_df = pl.read_excel(f"{player}.xlsx",
                                  read_csv_options={"dtypes": {"Question": pl.Utf8}})\
            .with_columns(pl.col("Question").cast(pl.Utf8))\
            .select(pl.col("Question"),
                    pl.col("Answer").str.strip().str.to_uppercase().alias(f"{player}_answer"))
        master_df = master_df.join(player_df, "Question", "left")
    return master_df

def score_logic(player, player_list):
    """
    Description:


    Args:
        player:

        player_list:


    Returns:

    """
    logic = pl.when(pl.col("Round_type") == "least_popular").then(least_popular_logic(player, player_list))\
              .when(pl.col("Round_type") == "aggregate_difficulty").then(aggregate_difficulty_logic(player))
    return logic

def least_popular_logic(player: str = "", player_list: List[str] = None) -> pl.Expr:
    """
    Description:
        Return the logic for rounds of type "least_popular_logic". In this round type, answers must be valid
        to get any points.

        A unique answer when noone else has a unique answer will score 3 points.
        A unique answer when someone else also has a unique answer will score 2 points.
        Any other valid answer will score 1 point.

    Args:
        player (str):
            The player name.
        player_list (List[str]):
            List of player names.

    Returns:
        logic (pl.Expr):
            The logic of the column.
    """
    opponent_list = player_list.copy()
    opponent_list.remove(player)

    answers_list = pl.concat_list([f"{item}_answer" for item in player_list])
    opponent_answer_list = pl.concat_list([f"{opponent}_answer" for opponent in opponent_list])
    player_answer_list = pl.concat_list(f"{player}_answer")
    player_opponent_intersect = player_answer_list.list.set_intersection(opponent_answer_list)
    player_valid_answer = player_answer_list.list.set_intersection("Valid_answers")

    player_answer_is_valid = player_valid_answer.list.get(0).is_not_null()
    player_answer_is_unique = player_opponent_intersect.list.get(0).is_null()

    opponent_answer_is_valid_unique = False
    for opponent in opponent_list:
        player_answer_list = pl.concat_list([f"{item}_answer" for item in player_list if item != opponent])
        opponent_answer_list = pl.concat_list(f"{opponent}_answer")
        opponent_player_intersect = opponent_answer_list.list.set_intersection(player_answer_list)
        opponent_valid_answer = opponent_answer_list.list.set_intersection("Valid_answers")

        opponent_answer_is_valid_unique = opponent_answer_is_valid_unique | \
                                          (opponent_valid_answer.list.get(0).is_not_null() &
                                           opponent_player_intersect.list.get(0).is_null())

    logic = pl.when(player_answer_is_valid & player_answer_is_unique & ~opponent_answer_is_valid_unique).then(3)\
              .when(player_answer_is_valid & player_answer_is_unique).then(2)\
              .when(player_answer_is_valid).then(1)\
              .otherwise(0)

    return logic

def aggregate_difficulty_logic(player: str = "") -> pl.Expr:
    """
    Description:

    Args:
        player:

    Returns:

    """
    num_points = pl.col("Question").str.split(".").list.get(1).sort().rank(method="dense").over("Round")

    logic = pl.when(pl.col(f"{player}_answer").is_in(pl.col("Valid_answers"))).then(num_points)\
              .otherwise(0)
    return logic

if __name__ == "__main__":
    player_list = ["Max", "Sophie", "Michael", "Edward"]
    create_player_answer_sheets(player_list)
    master = create_master_table(player_list)

    for player in player_list:
        master = master.with_columns(score_logic(player, player_list).alias(f"{player}_score"))
    print(master)
    print(master.filter(pl.col("Round") == '2')
          .select(["Question"] +
                  [f"{name}_answer" for name in player_list] +
                  [f"{name}_score" for name in player_list])
          )

    #post_quiz_cleanup(player_list)
