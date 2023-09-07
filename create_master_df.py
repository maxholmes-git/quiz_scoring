import polars as pl
from typing import List


def create_master_df(player_list: List[str] = None, read_path: str = ""):
    """
    Description:
        Creates the master dataframe which includes all metadata about rounds and questions joined
        with player answers. Player scores will be calculated on player answers.

    Args:
        player_list:
            List of players.
        read_path:
            Directory in which player answer sheets exist.

    Returns:
        master_df:
            The master dataframe (metadata + answers)
    """
    master_df = pl.read_excel("quiz_metadata.xlsx",
                              read_csv_options={"dtypes": {"Question": pl.Utf8}}) \
        .with_columns(pl.col("Question").cast(pl.Utf8)) \
        .with_columns(pl.col("Question").str.split(".").list.get(0).alias("Round")) \
        .with_columns(pl.col("Valid_answers").str.split(","))\
        .drop_nulls()

    for player in player_list:
        player_df = pl.read_excel(f"{read_path + player}.xlsx",
                                  read_csv_options={"dtypes": {"Question": pl.Utf8}}) \
            .with_columns(pl.col("Question").cast(pl.Utf8)) \
            .select(pl.col("Question"),
                    pl.col("Answer").str.strip().str.to_uppercase().alias(f"{player}_answer"))
        master_df = master_df.join(player_df, "Question", "left")
    return master_df

if __name__ == "__main__":
    player_list = ["Max", "Sophie", "Michael", "Edward"]
    read_path = "C:\\Users\\ready\\Desktop\\Quiz_player_scoresheets\\"
    create_master_df = (player_list, read_path)

