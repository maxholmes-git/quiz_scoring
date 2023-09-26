import polars as pl
import matplotlib.pyplot as plt
from round_logic import score_logic
from core_functions import create_answer_sheets, create_master_df, \
    lock_answers, write_table_to_html, create_round_scoring_df


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
