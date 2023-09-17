import os
import polars as pl
from typing import List
from random_fill_logic import random_fill_logic
from validate_quiz_metadata import validate_quiz_metadata


def create_answer_sheets(player_list: List[str] = None,
                         write_path: str = "",
                         force_overwrite: bool = False,
                         fill_test_values: bool = False):
    meta_file = "quiz_metadata.xlsx"
    template_file = "template_answer_sheet.xlsx"
    metadata_df = pl.read_excel(source="quiz_metadata.xlsx",
                                read_csv_options={"dtypes": {"Question": pl.Utf8},
                                                  "truncate_ragged_lines": True})\
        .drop_nulls("Question")
    validate_quiz_metadata(metadata_df)
    metadata_df = metadata_df.select(["Question"]) \
        .with_columns(pl.lit("").alias("Answer"))
    print(metadata_df)
    metadata_df.write_excel(write_path + template_file,
                            dtype_formats={pl.Float64: "@"})
    for player in player_list:
        file_name_write_path = f"{write_path}{player}.xlsx"
        if not os.path.exists(file_name_write_path) or force_overwrite:
            if fill_test_values:
                write_player_df = metadata_df.with_columns(
                    random_fill_logic(df=metadata_df,
                                      random_values=["A", "B", "C", "D", "E"]).alias("Answer")
                )
            else:
                write_player_df = metadata_df
            write_player_df.write_excel(file_name_write_path,
                                        dtype_formats={pl.Float64: "@"})
            print(f"File {file_name_write_path} created.")
        else:
            print(f"File {file_name_write_path} already exists.")



if __name__ == "__main__":
    player_list = ["Max", "Sophie", "Michael", "Edward"]
    score_sheets_path = "C:\\Users\\ready\\Desktop\\Quiz_player_scoresheets\\"
    create_answer_sheets(player_list,
                         write_path=score_sheets_path,
                         force_overwrite=True,
                         fill_test_values=True)
