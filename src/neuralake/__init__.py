from neuralake.core import Catalog, NlkDataFrame

__all__ = ["NlkDataFrame", "Catalog"]


def repl():
    """
    Starts an interactive python session with some of the Neuralake imports.
    Allows for quick testing and inspection of data accessible via the Neuralake
    client.
    """

    import sys
    import importlib
    import importlib.util
    import inspect
    from pathlib import Path
    import IPython
    import polars as pl

    from neuralake.core import Catalog, Filter, NlkDataFrame

    def load_catalog(catalog_path):
        """
        Helper function to load a catalog from a file or module path.
        
        Usage:
            load_catalog('docs/examples/tcph_catalog.py')
            load_catalog('my_package.catalogs.main')
        """
        try:
            # Handle both file paths and module paths
            if catalog_path.endswith('.py'):
                # File path approach
                path = Path(catalog_path)
                if not path.exists():
                    print(f"Error: Catalog file {path} not found")
                    return None
                
                # Add directory to sys.path for imports
                sys.path.insert(0, str(path.parent))
                
                # Load module from file
                spec = importlib.util.spec_from_file_location("catalog_module", path)
                catalog_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(catalog_module)
            else:
                # Module path approach
                catalog_module = importlib.import_module(catalog_path)
            
            # Find Catalog objects and add them to the IPython session
            catalogs = {}
            ip = IPython.get_ipython()
            for name, obj in inspect.getmembers(catalog_module):
                if isinstance(obj, Catalog):
                    catalogs[name] = obj
                    # Add to IPython user namespace
                    if ip:
                        ip.user_ns[name] = obj
                    print(f"Loaded catalog: {name}")
            
            if not catalogs:
                print(f"Warning: No Catalog objects found in {catalog_path}")
            
            return catalogs
                
        except Exception as e:
            print(f"Error loading catalog from {catalog_path}: {e}")
            return None

    # Check for command-line argument to auto-load a catalog
    if len(sys.argv) > 1:
        catalog_path = sys.argv[1]
        print(f"Loading catalog from: {catalog_path}")
        load_catalog(catalog_path)

    print(
        r"""
------------------------------------------------

Welcome to
     __                     _       _
  /\ \ \___ _   _ _ __ __ _| | __ _| | _____
 /  \/ / _ \ | | | '__/ _` | |/ _` | |/ / _ \
/ /\  /  __/ |_| | | | (_| | | (_| |   <  __/
\_\ \/ \___|\__,_|_|  \__,_|_|\__,_|_|\_\___|
------------------------------------------------

Usage:
  neuralake-shell                    # Start interactive shell
  neuralake-shell path/to/catalog.py # Auto-load catalog on startup
  
Tip: Use load_catalog('path/to/catalog.py') to load additional catalogs

"""
    )

    IPython.start_ipython(
        colors="neutral",
        display_banner=False,
        user_ns={
            "Catalog": Catalog,
            "NlkDataFrame": NlkDataFrame,
            "Filter": Filter,
            "pl": pl,
            "load_catalog": load_catalog,
        },
        argv=[],
    )
