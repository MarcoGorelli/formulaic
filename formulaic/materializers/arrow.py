from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Dict, Iterator, Sequence

import pandas
from interface_meta import override

from .pandas import PandasMaterializer

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow


class NarwhalsMaterializer(PandasMaterializer):
    REGISTER_NAME = "narwhals"
    REGISTER_INPUTS: Sequence[str] = ("narwhals.dataframe.DataFrame",)

    @override
    def _init(self) -> None:
        self.__data_context = LazyArrowTableProxy(self.data)

    @override  # type: ignore
    @property
    def data_context(self):
        return self.__data_context


class LazyArrowTableProxy(Mapping):
    def __init__(self, table: pyarrow.Table):
        self.table = table
        self.column_names = set(self.table.column_names)
        self._cache: Dict[str, pandas.Series] = {}
        self.index = pandas.RangeIndex(len(table))

    def __contains__(self, value: Any) -> Any:
        breakpoint()
        return value in self.column_names

    def __getitem__(self, key: str) -> Any:
        breakpoint()
        if key not in self.column_names:
            raise KeyError(key)
        if key not in self._cache:
            self._cache[key] = self.table.column(key).to_pandas()
        return self._cache[key]

    def __iter__(self) -> Iterator[str]:
        breakpoint()
        return iter(self.column_names)

    def __len__(self) -> int:
        breakpoint()
        return len(self.column_names)
