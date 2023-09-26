import polars as pl
from round_logic import least_popular_logic, aggregate_difficulty_logic, text_logic


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
