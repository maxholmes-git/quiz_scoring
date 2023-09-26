import polars as pl
from core_functions import count_nulls_per_round


def score_aggregate_difficulty_rounds(aggregate_difficulty_df, round_scoring_df, player_list):
    """
    Description:
        Scores rounds of type "aggregate_difficulty" and then joins the resulting scores back onto
        round_scoring_df

    Args:
        aggregate_difficulty_df:
            round_scoring_df filtered for rounds with round_type == "aggregate_difficulty"
        round_scoring_df:
            round_scoring_df in this context is master_df grouped by Round and summed.
        player_list:
            The list of players
    Returns:
        round_scoring_df:
            round_scoring_df with corrected scoring for rounds with round_type == "aggregate_difficulty"
    """
    all_null_counts_df = count_nulls_per_round(aggregate_difficulty_df)

    round_scoring_df = round_scoring_df \
        .join(all_null_counts_df.select(["Round"] + [f"{player}_score" for player in player_list]),
              on=["Round"], how="left", suffix="_null_count") \
        .with_columns(
        [pl.when(pl.col(f"{player}_score_null_count") == 0)
         .then(None)
         .otherwise(pl.col(f"{player}_score_null_count"))
         .alias(f"{player}_score_null_count")
         for player in player_list]
    )
    round_scoring_df = round_scoring_df \
        .with_columns(
        [pl.when(pl.col(f"{player}_score_null_count").is_null())
         .then(pl.col(f"{player}_score"))
         .otherwise((pl.col(f"{player}_score") / pl.col(f"{player}_score_null_count")).round(0).cast(int))
         .alias(f"{player}_score")
         for player in player_list]
    )
    return round_scoring_df
