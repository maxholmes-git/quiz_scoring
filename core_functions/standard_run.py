from core_functions import create_master_df, update_with_locked_answers, write_table_to_html, create_round_scoring_df
from round_logic import score_logic


def standard_run(settings):
    master_df = create_master_df(settings["players"],
                                 scoresheets_path=settings["paths"]["scoresheets"][0],
                                 meta_file=settings["paths"]["metadata"][0])
    master_df = update_with_locked_answers(master_df,
                                           read_path=settings["paths"]["locked_answers"][0])
    for player in settings["players"]:
        master_df = master_df.with_columns(score_logic(player, settings["players"]).alias(f"{player}_score"))

    aggregate_scoring_df = create_round_scoring_df(master_df, settings["players"])
    write_table_to_html(aggregate_scoring_df, settings["players"],
                        "player_scoring.html", replace_in_cols="_score")
    write_table_to_html(master_df, settings["players"],
                        "player_scoring.html", mode="a")
    return master_df
