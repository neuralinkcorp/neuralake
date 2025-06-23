from neuralake.core import Catalog, ModuleDatabase
import tpch_tables
import tpcds_tables

# Create a single TPC catalog with both TPC-H and TPC-DS databases
dbs = {
    "tpc-h": ModuleDatabase(tpch_tables),
    "tpc-ds": ModuleDatabase(tpcds_tables),
}
TPCCatalog = Catalog(dbs) 