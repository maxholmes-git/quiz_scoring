import os
import polars as pl


def lock_answers(master_df, player_list, question_value, activate=False, write_path=None):
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
        activate:
            Value determining whether to activate the lock on the question or not.
        write_path:
            The path which player answers will be written to at point of locking.
    Returns:
        master_df:
            The master dataframe (metadata + answers) with locked answers merged on.
    """
    if activate:
        filtered_df = master_df\
            .filter(pl.col("Question") == question_value)\
            .select(["Question"] + [f"{player}_answer" for player in player_list])
        filtered_df.write_csv(f"{write_path}{question_value}.csv")

    locked_filenames = [file for file in os.listdir(write_path) if ".csv" in file]
    if locked_filenames:
        for filename in locked_filenames:
            locked_question_df = pl.read_csv(f"{write_path}{filename}") \
                .with_columns(pl.col("Question").cast(pl.Utf8))
            master_df = master_df.update(locked_question_df, "Question", "left")
    return master_df
