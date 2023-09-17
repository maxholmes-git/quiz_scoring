import polars as pl


def validate_quiz_metadata(metadata_df):
    """
    Description:
        Validate inputs for quiz metadata xlsx file.

    Args:
        metadata_df:
            DataFrame which has been created from input quiz metadata xlsx file.
    """
    valid_columns = ["Question", "Round_type", "Valid_answers", "Custom_scoring"]
    invalid_columns = [column for column in metadata_df.columns if column not in valid_columns]
    if invalid_columns:
        raise ValueError(f"Invalid columns in metadata\n"
                         f"Columns found: {metadata_df.columns}\n"
                         f"Columns expected: {valid_columns}")

    valid_round_types = ["text", "aggregate_difficulty", "least_popular"]
    invalid_values = set([value for value in metadata_df.get_column("Round_type").to_list()
                          if value not in valid_round_types])
    if invalid_values:
        raise ValueError(f"Invalid values found for Round_type\n"
                         f"Invalid values found: {invalid_values}\n"
                         f"Valid values: {valid_round_types}")

    round_question_values = metadata_df.get_column("Question").to_list()
    for value in round_question_values:
        split_value = value.split(".")
        error_string = (f"Invalid Question value: {value}\n"
                        "Questions must be inputted in the following format:\n"
                        "{round_number}.{question_number}\n"
                        "where both round_number and question_number are integers.")
        if len(split_value) != 2:
            raise ValueError(error_string)

        string_indexes = [0, 1]
        for index in string_indexes:
            try:
                int(split_value[index])
            except ValueError:
                raise ValueError(error_string)

    valid_answer_values = metadata_df.get_column("Valid_answers").to_list()
    for value in valid_answer_values:
        if not value.strip():
            raise ValueError("You must specify at least one valid answer for each question")

    custom_scoring_values = metadata_df.get_column("Custom_scoring").to_list()
    for value in custom_scoring_values:
        if value:
            try:
                int(value)
            except ValueError:
                raise ValueError("Custom_scoring values must be type integer or None.")
