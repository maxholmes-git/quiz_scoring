import sys
import yaml
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


def running_from_console(settings_dict):
    player_list = settings_dict["players"]
    scoresheets_path = settings_dict["paths"]["scoresheets"][0]
    metadata_path = settings_dict["paths"]["metadata"][0]
    locked_answers_path = settings_dict["paths"]["locked_answers"][0]

    create_answer_sheets(player_list,
                         write_path=scoresheets_path,
                         force_overwrite=True,
                         fill_test_values=True,
                         meta_file=metadata_path)
    master_df = create_master_df(player_list,
                                 scoresheets_path=scoresheets_path,
                                 meta_file=metadata_path)

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
    print(aggregate_scoring_df)

    write_table_to_html(aggregate_scoring_df, player_list, "player_scoring.html")
    # delete_files_in_directory(player_list, score_sheets_path)
    return master_df, player_list


def cumsum_scoring(df, player_list):
    df = df.select(["Question"] +
                   [pl.cumsum(f"{player}_score").alias(f"{player}_score_cumsum") for player in player_list])
    return df

def running_from_bat(settings_dict):
    player_list = settings_dict["players"]
    scoresheets_path = settings_dict["paths"]["scoresheets"][0]
    metadata_path = settings_dict["paths"]["metadata"][0]
    locked_answers_path = settings_dict["paths"]["locked_answers"][0]
    print("== Loaded settings from Yaml ==")
    user_input = ""
    while user_input not in ("q", "quit"):
        print("""\nInput options:
        - 'setup' or 's'
        - 'run' or 'r'
        - 'lock' or 'l'
        - 'quit' or 'q'
            """)
        user_input = input("input: ").lower().strip()
        if user_input in ("s", "setup"):
            create_answer_sheets(player_list,
                                 write_path=scoresheets_path,
                                 force_overwrite=True,
                                 fill_test_values=True,
                                 meta_file=metadata_path)
        elif user_input in ("r", "run"):
            master_df = create_master_df(player_list,
                                         scoresheets_path=scoresheets_path,
                                         meta_file=metadata_path)
            for player in player_list:
                master_df = master_df.with_columns(score_logic(player, player_list).alias(f"{player}_score"))

            aggregate_scoring_df = create_round_scoring_df(master_df, player_list)
            write_table_to_html(aggregate_scoring_df, player_list, "player_scoring.html")
        elif user_input in ("l", "lock"):
            print("\nLocking questions prevents users from changing their answers that question\n"
                  "for the rest of the game.\n"
                  "\n"
                  "Input options:\n"
                  "- {round_number}.{question_number}\n"
                  "- 'e' or 'exit'\n")
            while user_input:
                user_input = input("input: ").lower().strip()
                if user_input in ("e", "exit"):
                    break
                elif user_input in master_df["Question"].to_list():
                    print(f"Locking Question {user_input}")
                    master_df = lock_answers(master_df,
                                             player_list,
                                             question_value=user_input,
                                             activate=True,
                                             write_path=locked_answers_path)
                else:
                    print("Invalid input")
        elif user_input in ("q", "quit"):
            print("Quitting program")
        else:
            print("Invalid input")


def get_settings_from_yaml(yaml_path):
    with open(yaml_path, "r") as file:
        yaml_file = yaml.safe_load(file)
    return yaml_file


def main():
    settings_dict = get_settings_from_yaml("quiz_settings.yaml")
    if sys.argv[1] == "console_run":
        running_from_bat(settings_dict)
    else:
        print("running from console")
        #df, player_list = running_from_console(settings_dict)

if __name__ == "__main__":
    #running_from_bat(get_settings_from_yaml("quiz_settings.yaml"))

    settings_dict = get_settings_from_yaml("quiz_settings.yaml")
    player_list = settings_dict["players"]
    scoresheets_path = settings_dict["paths"]["scoresheets"][0]
    metadata_path = settings_dict["paths"]["metadata"][0]
    locked_answers_path = settings_dict["paths"]["locked_answers"][0]

    create_answer_sheets(player_list,
                         write_path=scoresheets_path,
                         force_overwrite=True,
                         fill_test_values=True,
                         meta_file=metadata_path)
    master_df = create_master_df(player_list,
                                 scoresheets_path=scoresheets_path,
                                 meta_file=metadata_path)

    for player in player_list:
        master_df = master_df.with_columns(score_logic(player, player_list).alias(f"{player}_score"))

    master_df = master_df.filter(pl.col("Round") == "3")

    cumsum_df = master_df.with_columns(
        [pl.when(pl.col(f"{player}_score").is_null()) \
        .then(pl.lit(0))
        .otherwise(pl.col(f"{player}_score"))
        .alias(f"{player}_score_nonnull") for player in player_list]
    )

    cumsum_df = cumsum_df.with_columns(
        [pl.cumsum(f"{player}_score_nonnull").alias(f"{player}_score_cumsum") for player in player_list] +
        [pl.when(~pl.col(f"{player}_answer").is_in(pl.col("Valid_answers")))
         .then(pl.lit(1)).alias(f"{player}_incorrect_count") for player in player_list]
    )

    cumsum_df = cumsum_df.with_columns(
        [pl.when(pl.col(f"{player}_incorrect_count").is_null()) \
             .then(pl.lit(0))
             .otherwise(pl.col(f"{player}_incorrect_count"))
             .alias(f"{player}_incorrect_count_nonnull") for player in player_list]
    )

    cumsum_df = cumsum_df.with_columns(
        [pl.cumsum(f"{player}_incorrect_count_nonnull").alias(f"{player}_incorrect_cumsum") for player in player_list]
    )

    cumsum_df = cumsum_df.with_columns(
        [pl.when(pl.col(f"{player}_incorrect_cumsum") == 0)
         .then(pl.col(f"{player}_score_cumsum"))
         .otherwise(pl.col(f"{player}_score_cumsum")
                    .floordiv(pl.col(f"{player}_incorrect_cumsum")))
         .alias(f"{player}_score_cumsum2") for player in player_list]
    )
    write_table_to_html(cumsum_df, player_list, "testing_any_table.html")
    #aggregate_scoring_df = create_round_scoring_df(master_df, player_list)
