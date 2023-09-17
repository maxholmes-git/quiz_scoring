import polars as pl


def aggregate_difficulty_logic(player: str) -> pl.Expr:
    """
    Description:
        Return the logic for rounds of type "aggregate_difficulty_logic".

        Scoring:
            If answer is valid and correct, player receives points equal to the row_number.

            ie. if it's Q8, then you would receive 8 points (assuming there is Q1-8 with no gaps)

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
