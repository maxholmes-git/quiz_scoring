import polars as pl
from typing import List
from create_answer_sheets import create_answer_sheets
from least_popular_logic import least_popular_logic
from aggregate_difficulty_logic import aggregate_difficulty_logic
from create_master_df import create_master_df

def score_logic(player, player_list):
    """
    Description:
        Returns the score logic for a single player column.

    Args:
        player:
            The player string.
        player_list:
            The list of players.

    Returns:
        logic:
            The polars logic.
    """
    logic = pl.when(pl.col("Round_type") == "least_popular").then(least_popular_logic(player, player_list))\
              .when(pl.col("Round_type") == "aggregate_difficulty").then(aggregate_difficulty_logic(player))\
              .when(pl.col("Round_type") == "text").then(text_logic(player))
    return logic


def count_nulls_per_round(aggregate_difficulty_df):
    aggregate_difficulty_rounds = set([row["Round"] for row in aggregate_difficulty_df.iter_rows(named=True)])
    #Need to come up with something better than this to empty the dataframe
    all_null_counts_df = aggregate_difficulty_df.null_count().filter(pl.col("Round") == pl.lit(1000))\
        .with_columns(pl.col("Round").cast(str))
    for round_number in aggregate_difficulty_rounds:
        round_df = aggregate_difficulty_df.filter(pl.col("Round") == pl.lit(round_number))
        null_count_df = round_df.null_count()
        null_count_df = null_count_df.with_columns(pl.lit(round_number).alias("Round"))
        all_null_counts_df = pl.concat([all_null_counts_df, null_count_df], rechunk=True)
    return all_null_counts_df


def score_aggregate_difficulty_rounds(aggregate_difficulty_df, round_scoring_df, player_list):
    all_null_counts_df = count_nulls_per_round(aggregate_difficulty_df)

    round_scoring_df = round_scoring_df\
        .join(all_null_counts_df.select(["Round"] + [f"{player}_score" for player in player_list]),
              on=["Round"], how="left", suffix="_null_count") \
        .with_columns(
            [pl.when(pl.col(f"{player}_score_null_count") == 0)
             .then(None)
             .otherwise(pl.col(f"{player}_score_null_count"))
             .alias(f"{player}_score_null_count")
             for player in player_list]
        )
    round_scoring_df = round_scoring_df\
        .with_columns(
            [pl.when(pl.col(f"{player}_score_null_count").is_null())
             .then(pl.col(f"{player}_score"))
             .otherwise((pl.col(f"{player}_score") / pl.col(f"{player}_score_null_count")).round(0).cast(int))
             .alias(f"{player}_score")
             for player in player_list]
    )

    return round_scoring_df


def create_round_scoring_df(df, player_list):
    round_scoring_df = df.select([f"{player}_score" for player in player_list] + ["Round"])\
        .group_by("Round", maintain_order=True).sum()

    aggregate_difficulty_df = df.filter(pl.col("Round_type") == "aggregate_difficulty")
    if not aggregate_difficulty_df.is_empty():
        round_scoring_df = score_aggregate_difficulty_rounds(aggregate_difficulty_df,
                                                             round_scoring_df,
                                                             player_list)

    grand_total_df = round_scoring_df.sum()\
        .with_columns(pl.when(pl.col("Round").is_null()).then(pl.lit("Grand total")).alias("Round"))
    final_df = pl.concat([round_scoring_df, grand_total_df], rechunk=True)
    return final_df


if __name__ == "__main__":
    player_list = ["Max", "Sophie", "Michael", "Edward"]
    score_sheets_path = "C:\\Users\\ready\\Desktop\\Quiz_player_scoresheets\\"
    create_answer_sheets(player_list,
                         write_path=score_sheets_path,
                         force_overwrite=False,
                         fill_test_values=True)
    master_df = create_master_df(player_list, read_path=score_sheets_path)
    print(master_df)

    for player in player_list:
        master_df = master_df.with_columns(score_logic(player, player_list).alias(f"{player}_score"))

    print(master_df)
    print(master_df.filter(pl.col("Round") == '2')
          .select(["Question"] +
                  [f"{name}_answer" for name in player_list] +
                  [f"{name}_score" for name in player_list])
          )

    aggregate_scoring_df = create_round_scoring_df(master_df, player_list)
    print(aggregate_scoring_df)
    #delete_files_in_directory(player_list, score_sheets_path)
