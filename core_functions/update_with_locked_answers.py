import os
import polars as pl


def update_with_locked_answers(master_df, read_path):
    """
    Description:

    Args:
        master_df:
        read_path:

    Returns:

    """
    locked_filenames = [file for file in os.listdir(read_path) if ".csv" in file]
    if locked_filenames:
        for filename in locked_filenames:
            locked_question_df = pl.read_csv(f"{read_path}{filename}") \
                .with_columns(pl.col("Question").cast(pl.Utf8))
            master_df = master_df.update(locked_question_df, "Question", "left")
    return master_df
