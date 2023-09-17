import polars as pl


def text_logic(player):
    """
    Description:
        Return the logic for rounds of type "text".

        Scoring:
        Correct answers will receive points equal to that defined in Custom_scoring column.
        If no value is present, then the player will receive 1 point.
        If the answer is invalid then the player will receive 0 points.
    Args:
        player:
            The player name.
    Returns:
        logic:
            The logic of the column.
    """
    logic = pl.when(pl.col(f"{player}_answer").is_in(pl.col("Valid_answers")))\
        .then(pl.when(pl.col("Custom_scoring").is_not_null())
              .then(pl.col("Custom_scoring"))
              .otherwise(pl.lit(1)))\
        .otherwise(pl.lit(0))
    return logic
