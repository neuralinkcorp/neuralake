from types import ModuleType
from typing import Any, Protocol, cast
import warnings

from neuralake.core.dataframe import NlkDataFrame
from neuralake.core.tables.metadata import TableProtocol


class Database(Protocol):
    def get_tables(self, show_deprecated: bool = False) -> dict[str, TableProtocol]: ...

    def tables(self, show_deprecated: bool = False) -> list[str]:
        return list(self.get_tables(show_deprecated).keys())

    def table(self, name: str, *args: Any, **kwargs: Any) -> NlkDataFrame: ...


class ModuleDatabase(Database):
    def __init__(self, db: ModuleType) -> None:
        self.db = db

    def __getattr__(self, name: str):
        # HACK: to maintain backwards compability when accessing module attributes
        return self._get_table(name)

    def get_tables(self, show_deprecated: bool = False) -> dict[str, TableProtocol]:
        methods = dir(self.db)

        tables = {}
        for name in methods:
            table = self._get_table(name)
            if table is None:
                continue

            if table.table_metadata.is_deprecated and not show_deprecated:
                continue

            tables[name] = table

        return tables

    def table(self, name: str, *args: Any, **kwargs: Any) -> NlkDataFrame:
        tbl = self._get_table(name)
        if tbl is None:
            raise KeyError(f"Table '{name}' not found in database")

        if tbl.table_metadata.is_deprecated:
            warnings.warn(f"The table '{name}' is deprecated", DeprecationWarning)

        return tbl(*args, **kwargs)

    def _get_table(self, name: str) -> TableProtocol | None:
        table = getattr(self.db, name)
        if not hasattr(table, "table_metadata"):
            return None

        return cast(TableProtocol, table)


class DatabaseWithGlobalArgs(Database):
    def __init__(self, db: Database, global_args: dict[str, Any]):
        self.db = db
        self._global_args = global_args

    def get_tables(self, show_deprecated: bool = False) -> dict[str, TableProtocol]:
        return self.db.get_tables(show_deprecated)

    def tables(self, show_deprecated: bool = False) -> list[str]:
        return self.db.tables(show_deprecated)

    def table(self, name: str, *args: Any, **kwargs: Any) -> NlkDataFrame:
        new_kwargs = {**self._global_args, **kwargs}
        return self.db.table(name, *args, **new_kwargs)


class Catalog:
    def __init__(self, dbs: dict[str, Database]):
        self._dbs = dbs
        self._global_args: dict[str, Any] | None = None

    def set_global_args(self, global_args: dict[str, Any]) -> None:
        self._global_args = global_args

    def db(self, db_name: str) -> Database:
        db = self._dbs.get(db_name)

        if db is None:
            raise KeyError(
                f"Database '{db_name}' not found. Available databases: {self.dbs()}"
            )

        if self._global_args is None:
            return db

        return DatabaseWithGlobalArgs(db, self._global_args)

    def dbs(self) -> list[str]:
        return list(self._dbs.keys())
