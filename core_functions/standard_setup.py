from os import listdir, mkdir
from pathlib import Path
from core_functions import create_answer_sheets


def standard_setup(settings):
    overwrite_scoresheets = True
    if listdir(settings["paths"]["scoresheets"][0]):
        print(f"Scoresheets already exist at path: {settings['paths']['scoresheets'][0]}\n\n"
              "Overwrite these files? (Y/N)\n")
        user_input = None
        while user_input not in ("y", "n"):
            user_input = input("input: ").lower().strip()
            if user_input == "y":
                print("\n== Overwriting scoresheets ==\n")
                overwrite_scoresheets = True
            elif user_input == "n":
                overwrite_scoresheets = False
            else:
                print("Invalid input\n")
    if listdir(settings["paths"]["locked_answers"][0]):
        print(f"Locked answers already exist at path: {settings['paths']['locked_answers'][0]}"
              "Deleting the files is recommended when starting a new game.\n\n"
              "Delete these files? (Y/N)\n")
        user_input = None
        while user_input not in ("y", "n"):
            user_input = input("input: ").lower().strip()
            if user_input == "y":
                print("\n== Deleting locked answer files ==\n")
                [f.unlink() for f in Path(settings['paths']['locked_answers'][0]).glob("*") if f.is_file()]
            elif user_input == "n":
                print("\n== Skipping deleting locked answer files ==\n")
            else:
                print("Invalid input\n")

    create_answer_sheets(settings["players"],
                         write_path=settings["paths"]["scoresheets"][0],
                         force_overwrite=overwrite_scoresheets,
                         fill_test_values=True,
                         meta_file=settings["paths"]["metadata"][0])
