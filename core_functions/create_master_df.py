import polars as pl
from typing import List
from validate_inputs import validate_answer_sheet


def create_master_df(player_list: List[str] = None,
                     scoresheets_path: str = "",
                     meta_file: str = ""):
    """
    Description:
        Creates the master dataframe which includes all metadata about rounds and questions joined
        with player answers. Player scores will be calculated on player answers.

    Args:
        player_list:
            List of players.
        scoresheets_path:
            Directory in which player answer sheets exist.

    Returns:
        master_df:
            The master dataframe (metadata + answers)
    """
    master_df = pl.read_excel(meta_file,
                              read_csv_options={"dtypes": {"Question": pl.Utf8},
                                                "truncate_ragged_lines": True}) \
        .with_columns(pl.col("Question").cast(pl.Utf8)) \
        .with_columns(pl.col("Question").str.split(".").list.get(0).alias("Round")) \
        .with_columns(pl.col("Valid_answers").str.to_uppercase().str.split(","))\
        .drop_nulls("Question")

    for player in player_list:
        player_df = pl.read_excel(f"{scoresheets_path + player}.xlsx",
                                  read_csv_options={"dtypes": {"Question": pl.Utf8},
                                                    "truncate_ragged_lines": True})
        validate_answer_sheet(player, player_df, master_df)
        player_df = player_df\
            .with_columns(pl.col("Question").cast(pl.Utf8)) \
            .select(pl.col("Question"),
                    pl.col("Answer").str.strip().str.to_uppercase().alias(f"{player}_answer"))
        master_df = master_df.join(player_df, "Question", "left")
    return master_df

if __name__ == "__main__":
    player_list = ["Max", "Sophie", "Michael", "Edward"]
    scoresheets_path = "C:\\Users\\ready\\Desktop\\Quiz_player_scoresheets\\"
    master_df = create_master_df(player_list, scoresheets_path)

