import polars as pl
import numpy as np
import os
from typing import List


def fill_test_answers(df, col_to_randomise: str, random_values: List[str], player_list: List[str]):
    """

    Args:
        df:
        col_to_randomise:
        random_values:
        player_list:

    Returns:

    """
    df = df.with_columns(pl.lit(np.random.rand(df.height)).alias("random_nums"))
    logic = False
    num_values = len(random_values)
    chunk_size = 1 / num_values
    for i in range(0, num_values):
        logic = pl.when(pl.col("random_nums") > chunk_size * i).then(pl.lit(random_values[i])).otherwise(logic)
    return df.with_columns(logic.alias(col_to_randomise)).drop("random_nums")

def create_player_answer_sheets(player_list: List[str] = None,
                                write_path: str = "",
                                force_overwrite: bool = False,
                                fill_test_values: bool = False):
    meta_file = "quiz_metadata.xlsx"
    template_file = "template_answer_sheet.xlsx"
    if os.path.exists(meta_file):
        metadata_df = pl.read_excel(source="quiz_metadata.xlsx",
                                    read_csv_options={"dtypes": {"Question": pl.Utf8}})
        metadata_df = metadata_df.drop(["Round_type", "Valid_answers"]) \
            .with_columns(pl.lit("").alias("Answer"))
        metadata_df.write_excel(write_path + template_file,
                                dtype_formats={pl.Float64: "@"})
        for player in player_list:
            file_name_write_path = f"{write_path}{player}.xlsx"
            if not os.path.exists(file_name_write_path) or force_overwrite:
                if fill_test_values:
                    write_player_df = fill_test_answers(metadata_df,
                                                        col_to_randomise="Answer",
                                                        random_values=["A", "B", "C", "D", "E"],
                                                        player_list=player_list)
                else:
                    write_player_df = metadata_df
                write_player_df.write_excel(file_name_write_path,
                                            dtype_formats={pl.Float64: "@"})
                print(f"File {file_name_write_path} created.")
            else:
                print(f"File {file_name_write_path} already exists.")

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

def create_master_table(player_list: List[str] = None, read_path: str = ""):
    master_df = pl.read_excel("quiz_metadata.xlsx",
                              read_csv_options={"dtypes": {"Question": pl.Utf8}})\
        .with_columns(pl.col("Question").cast(pl.Utf8))\
        .with_columns(pl.col("Question").str.split(".").list.get(0).alias("Round"))\
        .with_columns(pl.col("Valid_answers").str.split(","))

    for player in player_list:
        player_df = pl.read_excel(f"{read_path + player}.xlsx",
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
        Return the logic for rounds of type "least_popular_logic".

        Scoring:
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

    logic = pl.when(player_answer_is_valid & player_answer_is_unique & ~opponent_answer_is_valid_unique).then(3) \
        .when(player_answer_is_valid & player_answer_is_unique).then(2) \
        .when(player_answer_is_valid).then(1) \
        .otherwise(0)

    return logic

def aggregate_difficulty_logic(player: str = "") -> pl.Expr:
    """
    Description:
        Return the logic for rounds of type "aggregate_difficulty_logic".

        Scoring:

    Args:
        player (str):
            The player name.

    Returns:
        logic (pl.Expr):
            The logic of the column.
    """
    num_points = pl.col("Question").str.split(".").list.get(1).sort().rank(method="dense").over("Round")

    logic = pl.when(pl.col(f"{player}_answer").is_in(pl.col("Valid_answers"))).then(num_points)\
              .when(pl.col(f"{player}_answer").is_in(["X", " ", ""])).then(pl.lit(0))\
              .otherwise(pl.lit(None))
    return logic

def score_aggregate_difficulty_rounds(df):
    aggregate_difficulty_df = df.filter(pl.col("Round_type") == "aggregate_difficulty")
    aggregate_difficulty_rounds = set([row["Round"] for row in aggregate_difficulty_df.iter_rows(named=True)])
    if len(aggregate_difficulty_rounds) > 1:
        print(">1 aggregate_difficulty_rounds not yet supported.")

    null_count_df = aggregate_difficulty_df.null_count()
    null_count_df = null_count_df.with_columns(
        pl.lit(next(iter(aggregate_difficulty_rounds))).alias("Round")
    )
    return null_count_df
def create_aggregate_scoring_df(df, player_list):
    null_count_df = score_aggregate_difficulty_rounds(df)

    aggregate_df = df.select([f"{player}_score" for player in player_list] + ["Round"])\
        .group_by("Round", maintain_order=True).sum()
    aggregate_df = aggregate_df\
        .join(null_count_df.select(["Round"] + [f"{player}_score" for player in player_list]),
              on=["Round"], how="left", suffix="_null_count")\
        .with_columns(
        [pl.when(pl.col(f"{player}_score_null_count") == 0)
         .then(None)
         .otherwise(pl.col(f"{player}_score_null_count"))
         .alias(f"{player}_score_null_count")
         for player in player_list]
    )
    aggregate_df = aggregate_df.with_columns(
        [pl.when(pl.col(f"{player}_score_null_count").is_null())
         .then(pl.col(f"{player}_score"))\
         .otherwise((pl.col(f"{player}_score") / pl.col(f"{player}_score_null_count")).round(0).cast(int))
         .alias(f"{player}_score")
         for player in player_list]
    )

    grand_total_df = aggregate_df.sum()\
        .with_columns(pl.when(pl.col("Round").is_null()).then(pl.lit("Grand total")).alias("Round"))
    final_df = pl.concat([aggregate_df, grand_total_df], rechunk=True)
    return null_count_df

if __name__ == "__main__":
    player_list = ["Max", "Sophie", "Michael", "Edward"]
    score_sheets_path = "C:\\Users\\ready\\Desktop\\Quiz_player_scoresheets\\"
    create_player_answer_sheets(player_list,
                                write_path=score_sheets_path,
                                force_overwrite=True,
                                fill_test_values=True)
    master_df = create_master_table(player_list, read_path=score_sheets_path)

    for player in player_list:
        master_df = master_df.with_columns(score_logic(player, player_list).alias(f"{player}_score"))

    print(master_df)
    print(master_df.filter(pl.col("Round") == '2')
          .select(["Question"] +
                  [f"{name}_answer" for name in player_list] +
                  [f"{name}_score" for name in player_list])
          )

    aggregate_scoring_df = create_aggregate_scoring_df(master_df, player_list)
    print(aggregate_scoring_df)
    #post_quiz_cleanup(player_list)
