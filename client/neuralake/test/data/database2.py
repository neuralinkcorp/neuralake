import polars as pl

from neuralake.core import NlkDataFrame, table

frame3 = pl.LazyFrame({"a": ["x", "y", "z"]})


@table
def new_table_3() -> NlkDataFrame:
    return NlkDataFrame(frame=frame3)
