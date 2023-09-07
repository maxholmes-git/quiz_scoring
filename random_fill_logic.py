import polars as pl
import numpy as np
from typing import List

def random_fill_logic(df, random_values: List[str]):
    """

    Args:
        df:
        random_values:

    Returns:

    """
    num_values = len(random_values)
    chunk_size = 1 / num_values
    logic = False
    for i in range(0, num_values):
        logic = pl.when(pl.lit(np.random.rand(df.height)) > chunk_size * i)\
                  .then(pl.lit(random_values[i])).otherwise(logic)
    return logic
