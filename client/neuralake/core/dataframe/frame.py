import polars as pl


class NlkDataFrame(pl.LazyFrame):
    def __init__(self, frame: pl.LazyFrame = None, *args, **kwargs):
        if frame is not None:
            self._df = frame
        else:
            self._df = pl.LazyFrame(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._df, name)
