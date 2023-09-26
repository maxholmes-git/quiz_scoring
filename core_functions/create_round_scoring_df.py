import polars as pl
from core_functions import score_aggregate_difficulty_rounds


def create_round_scoring_df(df, player_list):
    """
    Description:
        Takes in master_df and groups and scores players for each round.

    Args:
        df:
            df containing all players scores and rounds/questions
        player_list:
            The list of players.
    Returns:
        final_df:
            The Dataframe containing total scores for each player by round.
    """
    round_scoring_df = df.select([f"{player}_score" for player in player_list] + ["Round"]) \
        .group_by("Round", maintain_order=True).sum()

    aggregate_difficulty_df = df.filter(pl.col("Round_type") == "aggregate_difficulty")
    if not aggregate_difficulty_df.is_empty():
        round_scoring_df = score_aggregate_difficulty_rounds(aggregate_difficulty_df,
                                                             round_scoring_df,
                                                             player_list)

    final_df = round_scoring_df \
        .drop([f"{player}_score_null_count" for player in player_list]) \
        .select(["*"] +
                [pl.cumsum(f"{player}_score").alias(f"{player}_score_cumsum") for player in player_list])
    return final_df
