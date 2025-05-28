from neuralake.core import Catalog, ModuleDatabase
import tcph_tables

# Create a catalog
dbs = {"tpc-h": ModuleDatabase(tcph_tables)}
TPCHCatalog = Catalog(dbs)
