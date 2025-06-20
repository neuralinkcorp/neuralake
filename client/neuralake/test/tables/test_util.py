from datetime import datetime, timezone

import pyarrow as pa
import pytest

from neuralake.core.tables.filters import (
    InputFilters,
    NormalizedFilters,
    normalize_filters,
)
from neuralake.core.tables.util import (
    Filter,
    filter_to_sql_expr,
    filters_to_sql_predicate,
)

test_schema = pa.schema(
    [
        ("str_col", pa.string()),
        ("int_col", pa.int64()),
        ("list_col", pa.list_(pa.int64())),
        ("list_str_col", pa.list_(pa.string())),
        ("date_col", pa.timestamp("us", tz="UTC")),
    ]
)


class TestUtil:
    @pytest.mark.parametrize(
        ("schema", "f", "expected"),
        [
            (test_schema, Filter("int_col", "=", 123), '"int_col"=123'),
            (test_schema, Filter("int_col", "=", "123"), "\"int_col\"='123'"),
            # A tuple with a single element should not have a comma in SQL
            (test_schema, Filter("int_col", "in", (1,)), '"int_col" IN (1)'),
            (test_schema, Filter("int_col", "in", (1, 2)), '"int_col" IN (1,2)'),
            (
                test_schema,
                Filter("int_col", "not in", (1, 2)),
                '"int_col" NOT IN (1,2)',
            ),
            # String columns should be handled to add single quotes
            (test_schema, Filter("str_col", "=", "x"), "\"str_col\"='x'"),
            # Filtering using datetime columns
            (
                test_schema,
                Filter("date_col", ">=", datetime(2024, 4, 5, tzinfo=timezone.utc)),
                "\"date_col\">='2024-04-05T00:00:00+00:00'",
            ),
            (
                test_schema,
                Filter("str_col", "in", ("val1",)),
                "\"str_col\" IN ('val1')",
            ),
            (
                test_schema,
                Filter("str_col", "in", ("val1", "val2")),
                "\"str_col\" IN ('val1','val2')",
            ),
            (
                test_schema,
                Filter("str_col", "contains", "x'"),
                "\"str_col\" LIKE '%x''%'",
            ),
            # Test list columns
            (
                test_schema,
                Filter("list_col", "includes", 1),
                'array_contains("list_col",1)',
            ),
            (
                test_schema,
                Filter("list_str_col", "includes", "x"),
                "array_contains(\"list_str_col\",'x')",
            ),
            (
                test_schema,
                Filter("list_col", "includes all", (1, 2, 3)),
                'array_contains("list_col",1) AND array_contains("list_col",2) AND array_contains("list_col",3)',
            ),
            (
                test_schema,
                Filter("list_col", "includes any", (1, 2, 3)),
                'array_contains("list_col",1) OR array_contains("list_col",2) OR array_contains("list_col",3)',
            ),
        ],
    )
    def test_filter_to_expr(self, schema: pa.Schema, f: Filter, expected: str):
        assert str(filter_to_sql_expr(schema, f)) == expected

    def test_filter_to_expr_raises(self):
        with pytest.raises(ValueError) as e:
            filter_to_sql_expr(test_schema, Filter("invalid_col", "=", 0))

        assert "Invalid column name invalid_col" in str(e.value)

    @pytest.mark.parametrize(
        ("schema", "filters", "expected"),
        [
            (test_schema, [[Filter("str_col", "=", "x")]], "\"str_col\"='x'"),
            (
                test_schema,
                [[Filter("str_col", "=", "x"), Filter("int_col", "=", 123)]],
                '"str_col"=\'x\' AND "int_col"=123',
            ),
            (
                test_schema,
                [
                    [Filter("str_col", "=", "x")],
                    [Filter("int_col", "=", 123), Filter("int_col", "<", 456)],
                ],
                '"str_col"=\'x\' OR ("int_col"=123 AND "int_col"<456)',
            ),
            (
                test_schema,
                # Ensures filters are properly nested and grouped
                [
                    [Filter("str_col", "=", "x")],
                    [
                        Filter("list_col", "includes any", (1, 2, 3)),
                        Filter("list_col", "includes all", (1, 2, 3)),
                    ],
                ],
                '"str_col"=\'x\' OR ((array_contains("list_col",1) OR array_contains("list_col",2) OR array_contains("list_col",3)) AND array_contains("list_col",1) AND array_contains("list_col",2) AND array_contains("list_col",3))',
            ),
        ],
    )
    def test_filters_to_sql_predicate(
        self, schema: pa.Schema, filters: NormalizedFilters, expected: str
    ):
        assert str(filters_to_sql_predicate(schema, filters)) == expected

    @pytest.mark.parametrize(
        ("filters", "expected"),
        [
            (None, []),
            ([], []),
            ([Filter("a", "=", 1)], [[Filter("a", "=", 1)]]),
            ((Filter("a", "=", 1),), [[Filter("a", "=", 1)]]),
            (
                [
                    [Filter("a", "=", 1)],
                    [Filter("b", "=", 2), Filter("c", "=", 3)],
                ],
                [
                    [Filter("a", "=", 1)],
                    [Filter("b", "=", 2), Filter("c", "=", 3)],
                ],
            ),
            (
                (
                    (Filter("a", "=", 1),),
                    (Filter("b", "=", 2), Filter("c", "=", 3)),
                ),
                [
                    [Filter("a", "=", 1)],
                    [Filter("b", "=", 2), Filter("c", "=", 3)],
                ],
            ),
        ],
    )
    def test_normalize_filters(
        self, filters: InputFilters, expected: NormalizedFilters
    ):
        assert normalize_filters(filters) == expected
