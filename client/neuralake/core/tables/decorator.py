import inspect
from typing import Any, Callable, TypeVar

from neuralake.core.dataframe.frame import NlkDataFrame
from neuralake.core.tables.metadata import (
    TableMetadata,
    TableProtocol,
    TableSchema,
)

U = TypeVar("U")


class FunctionTable(TableProtocol):
    def __init__(self, table_metadata: TableMetadata, func: Callable) -> None:
        self.table_metadata = table_metadata
        self.func = func

    def __call__(self, *args: Any, **kwargs: dict[str, Any]) -> NlkDataFrame:
        # Filter to only include kwargs that are in the function signature
        parameters = inspect.signature(self.func).parameters
        accepts_var_kwargs = any(
            param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()
        )
        if not accepts_var_kwargs:
            kwargs = {
                key: value
                for key, value in kwargs.items()
                if key in parameters and key not in args
            }

        return self.func(*args, **kwargs)

    def get_schema(self) -> TableSchema:
        filters = self.table_metadata.docs_args.get("filters", [])

        # Infer partitions from filters
        partitions = [
            {
                "column_name": filter.column,
                "type_annotation": type(filter.value).__name__,
                "value": filter.value,
            }
            for filter in filters
        ]

        columns = None
        if self.table_metadata.docs_args or not partitions:
            fallback_table = self(**self.table_metadata.docs_args)
            columns = [
                {
                    "name": key,
                    "type": type.__str__(),
                }
                for key, type in fallback_table.schema.items()
            ]

        return TableSchema(partitions=partitions, columns=columns)


def table(*args, **kwargs) -> Callable[[U], U] | Callable[[Any], Callable[[U], U]]:
    def wrapper(func):
        return FunctionTable(
            table_metadata=TableMetadata(
                table_type="FUNCTION",
                description=func.__doc__.strip() if func.__doc__ else "",
                docs_args=kwargs.get("docs_args", {}),
                latency_info=kwargs.get("latency_info"),
                example_notebook=kwargs.get("example_notebook"),
                data_input=kwargs.get("data_input"),
                is_deprecated=kwargs.get("is_deprecated", False),
            ),
            func=func,
        )

    if len(args) == 0:
        return wrapper
    else:
        return wrapper(args[0])
