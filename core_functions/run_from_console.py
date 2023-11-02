import yaml
from core_functions import lock_answer, standard_run, standard_setup


def run_from_console():
    with open("quiz_settings.yaml", "r") as file:
        settings = yaml.safe_load(file)
    user_input = None
    while user_input not in ("q", "quit"):
        print("""\nInput options:
        - 'setup' or 's'
        - 'run' or 'r'
        - 'lock' or 'l'
        - 'quit' or 'q'
            """)
        user_input = input("input: ").lower().strip()
        if user_input in ("s", "setup"):
            standard_setup(settings)
        elif user_input in ("r", "run"):
            master_df = standard_run(settings)
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
                    print(f"\nLocking Question {user_input}\n")
                    master_df = standard_run(settings)
                    lock_answer(master_df,
                                settings["players"],
                                question_value=user_input,
                                write_path=settings["paths"]["locked_answers"][0])
                else:
                    print("Invalid input")
        elif user_input in ("q", "quit"):
            print("Quitting program")
        else:
            print("Invalid input")
