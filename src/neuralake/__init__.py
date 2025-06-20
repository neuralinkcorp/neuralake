from neuralake.core import Catalog, NlkDataFrame

__all__ = ["NlkDataFrame", "Catalog"]


def repl():
    """
    Starts an interactive python session with some of the Neuralake imports.
    Allows for quick testing and inspection of data accessible via the Neuralake
    client.
    """

    import IPython
    from neuralake_catalogs import NlkCatalog
    import polars as pl

    from neuralake.core import Catalog, Filter, NlkDataFrame

    print(
        """
------------------------------------------------

Welcome to
     __                     _       _
  /\ \ \___ _   _ _ __ __ _| | __ _| | _____
 /  \/ / _ \ | | | '__/ _` | |/ _` | |/ / _ \\
/ /\  /  __/ |_| | | | (_| | | (_| |   <  __/
\_\ \/ \___|\__,_|_|  \__,_|_|\__,_|_|\_\___|
------------------------------------------------

"""  # noqa
    )

    IPython.start_ipython(
        colors="neutral",
        display_banner=False,
        user_ns={
            "Catalog": Catalog,
            "NlkDataFrame": NlkDataFrame,
            "Filter": Filter,
            "NlkCatalog": NlkCatalog,
            "pl": pl,
        },
        argv=[],
    )
