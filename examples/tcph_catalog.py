from neuralake.core import Catalog, ModuleDatabase
import tpch_tables

# Create a catalog
dbs = {"tpc-h": ModuleDatabase(tpch_tables)}
TPCHCatalog = Catalog(dbs)
