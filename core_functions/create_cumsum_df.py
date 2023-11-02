import polars as pl
from core_functions import write_table_to_html


def create_cumsum_df(master_df, player_list):
    """
    Description:
        UNFINISHED. Not sure if I will finish this. The intention is to make it so that we get a cumulative
        score at a question level of granularity. But this is difficult to achieve with
        aggregate_difficulty_rounds in the mix as the score can go downwards.
    Args:
        master_df:
        player_list:

    Returns:

    """
    master_df = master_df.filter(pl.col("Round") == "3")

    cumsum_df = master_df.with_columns(
        [pl.when(pl.col(f"{player}_score").is_null()) \
             .then(pl.lit(0))
             .otherwise(pl.col(f"{player}_score"))
             .alias(f"{player}_score_nonnull") for player in player_list]
    )

    cumsum_df = cumsum_df.with_columns(
        [pl.cumsum(f"{player}_score_nonnull").alias(f"{player}_score_cumsum") for player in player_list] +
        [pl.when(~pl.col(f"{player}_answer").is_in(pl.col("Valid_answers")))
        .then(pl.lit(1)).alias(f"{player}_incorrect_count") for player in player_list]
    )

    cumsum_df = cumsum_df.with_columns(
        [pl.when(pl.col(f"{player}_incorrect_count").is_null()) \
             .then(pl.lit(0))
             .otherwise(pl.col(f"{player}_incorrect_count"))
             .alias(f"{player}_incorrect_count_nonnull") for player in player_list]
    )

    cumsum_df = cumsum_df.with_columns(
        [pl.cumsum(f"{player}_incorrect_count_nonnull").alias(f"{player}_incorrect_cumsum") for player in player_list]
    )

    cumsum_df = cumsum_df.with_columns(
        [pl.when(pl.col(f"{player}_incorrect_cumsum") == 0)
         .then(pl.col(f"{player}_score_cumsum"))
         .otherwise(pl.col(f"{player}_score_cumsum")
                    .floordiv(pl.col(f"{player}_incorrect_cumsum")))
         .alias(f"{player}_score_cumsum2") for player in player_list]
    )
    write_table_to_html(cumsum_df, player_list, "testing_any_table.html")
