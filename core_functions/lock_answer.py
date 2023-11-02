import os
import polars as pl


def lock_answer(master_df, player_list, question_value, write_path=None):
    """
    Description:
        Locks answers for a particular question_value if activate is True. This means that answers provided by
        players at the point that this function is run will be saved and unalterable for the remainder of the
        quiz.

    Args:
        master_df:
            The master dataframe (metadata + answers)
        player_list:
            The list of players.
        question_value:
            The value of the question for which answers are to be locked.
        write_path:
            The path which player answers will be written to at point of locking.
    Returns:
        master_df:
            The master dataframe (metadata + answers) with locked answers merged on.
    """
    filtered_df = master_df\
        .filter(pl.col("Question") == question_value)\
        .select(["Question"] + [f"{player}_answer" for player in player_list])
    filtered_df.write_csv(f"{write_path}{question_value}.csv")
