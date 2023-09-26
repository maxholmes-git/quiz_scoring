import polars as pl


def count_nulls_per_round(aggregate_difficulty_df):
    """
    Description:
        Counts the number of nulls in a particular round for all players. This function is intended
        for scoring aggregate_difficulty rounds.

    Args:
        aggregate_difficulty_df:
            The DataFrame for which we are counting nulls on each column
    Returns:
        all_null_counts_df:
            A DataFrame where every column is counted for nulls, per round
    """
    aggregate_difficulty_rounds = set([row["Round"] for row in aggregate_difficulty_df.iter_rows(named=True)])
    # Need to come up with something better than this to empty the dataframe
    all_null_counts_df = aggregate_difficulty_df.null_count().filter(pl.col("Round") == pl.lit(1000)) \
        .with_columns(pl.col("Round").cast(str))
    for round_number in aggregate_difficulty_rounds:
        round_df = aggregate_difficulty_df.filter(pl.col("Round") == pl.lit(round_number))
        null_count_df = round_df.null_count()
        null_count_df = null_count_df.with_columns(pl.lit(round_number).alias("Round"))
        all_null_counts_df = pl.concat([all_null_counts_df, null_count_df], rechunk=True)
    return all_null_counts_df
