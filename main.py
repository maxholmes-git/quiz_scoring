import polars as pl
import matplotlib.pyplot as plt
from round_logic import score_logic
from core_functions import create_answer_sheets, create_master_df, lock_answers, write_table_to_html


def count_nulls_per_round(aggregate_difficulty_df):
    aggregate_difficulty_rounds = set([row["Round"] for row in aggregate_difficulty_df.iter_rows(named=True)])
    # Need to come up with something better than this to empty the dataframe
    all_null_counts_df = aggregate_difficulty_df.null_count().filter(pl.col("Round") == pl.lit(1000))\
        .with_columns(pl.col("Round").cast(str))
    for round_number in aggregate_difficulty_rounds:
        round_df = aggregate_difficulty_df.filter(pl.col("Round") == pl.lit(round_number))
        null_count_df = round_df.null_count()
        null_count_df = null_count_df.with_columns(pl.lit(round_number).alias("Round"))
        all_null_counts_df = pl.concat([all_null_counts_df, null_count_df], rechunk=True)
    return all_null_counts_df


def score_aggregate_difficulty_rounds(aggregate_difficulty_df, round_scoring_df, player_list):
    all_null_counts_df = count_nulls_per_round(aggregate_difficulty_df)

    round_scoring_df = round_scoring_df\
        .join(all_null_counts_df.select(["Round"] + [f"{player}_score" for player in player_list]),
              on=["Round"], how="left", suffix="_null_count") \
        .with_columns(
            [pl.when(pl.col(f"{player}_score_null_count") == 0)
             .then(None)
             .otherwise(pl.col(f"{player}_score_null_count"))
             .alias(f"{player}_score_null_count")
             for player in player_list]
        )
    round_scoring_df = round_scoring_df\
        .with_columns(
            [pl.when(pl.col(f"{player}_score_null_count").is_null())
             .then(pl.col(f"{player}_score"))
             .otherwise((pl.col(f"{player}_score") / pl.col(f"{player}_score_null_count")).round(0).cast(int))
             .alias(f"{player}_score")
             for player in player_list]
    )
    return round_scoring_df


def create_round_scoring_df(df, player_list):
    round_scoring_df = df.select([f"{player}_score" for player in player_list] + ["Round"])\
        .group_by("Round", maintain_order=True).sum()

    aggregate_difficulty_df = df.filter(pl.col("Round_type") == "aggregate_difficulty")
    if not aggregate_difficulty_df.is_empty():
        round_scoring_df = score_aggregate_difficulty_rounds(aggregate_difficulty_df,
                                                             round_scoring_df,
                                                             player_list)

    final_df = round_scoring_df\
        .drop([f"{player}_score_null_count" for player in player_list])\
        .select(["*"] +
                [pl.cumsum(f"{player}_score").alias(f"{player}_score_cumsum") for player in player_list])
    return final_df


def write_plot(df, player_list):
    for player in player_list:
        plt.plot(df.get_column("Round").to_list(),
                 df.get_column(f"{player}_score_cumsum").to_list(),
                 label=player)
    plt.legend()
    plt.savefig("scores.png")


def main():
    player_list = ["Max", "Sophie", "Michael", "Edward"]
    scoresheets_path = "C:\\Users\\ready\\Desktop\\Quiz_test_files\\Quiz_player_scoresheets\\"
    metadata_path = "C:\\Users\\ready\\PycharmProjects\\Quiz_polars\\quiz_metadata.xlsx"
    create_answer_sheets(player_list,
                         write_path=scoresheets_path,
                         force_overwrite=True,
                         fill_test_values=True,
                         meta_file=metadata_path)
    master_df = create_master_df(player_list,
                                 scoresheets_path=scoresheets_path,
                                 meta_file=metadata_path)

    locked_answers_path = "C:\\Users\\ready\\Desktop\\Quiz_test_files\\Locked_answer_csvs\\"
    master_df = lock_answers(master_df,
                             player_list,
                             question_value="2.3",
                             activate=False,
                             write_path=locked_answers_path)

    for player in player_list:
        master_df = master_df.with_columns(score_logic(player, player_list).alias(f"{player}_score"))
    #print(master_df)

    cumsum_scoring_df = cumsum_scoring(master_df, player_list)
    print(cumsum_scoring_df)

    aggregate_scoring_df = create_round_scoring_df(master_df, player_list)
    #print(aggregate_scoring_df)

    write_table_to_html(aggregate_scoring_df, player_list)
    # delete_files_in_directory(player_list, score_sheets_path)
    return master_df, player_list


def cumsum_scoring(df, player_list):
    df = df.select(["Question"] +
                   [pl.cumsum(f"{player}_score").alias(f"{player}_score_cumsum") for player in player_list])
    return df


if __name__ == "__main__":
    df, player_list = main()
