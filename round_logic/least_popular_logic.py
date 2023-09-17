import polars as pl
from typing import List


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
