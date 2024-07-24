from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Sequence

import pandas
import narwhals as nw
from interface_meta import override


from .pandas import PandasMaterializer

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow


class ArrowMaterializer(PandasMaterializer):

    REGISTER_NAME: str = "arrow"
    REGISTER_INPUTS: Sequence[str] = ("narwhals.dataframe.DataFrame",)
    REGISTER_OUTPUTS: Sequence[str] = ("narwhals",)

    @override
    def _init(self) -> None:
        self.__data_context = LazyArrowTableProxy(self.data)

    @override
    def _is_categorical(self, values: Any) -> bool:
        if isinstance(values, nw.Series):
            return values.dtype in {nw.Categorical, nw.Enum, nw.String}
        return super()._is_categorical(values)

    @override  # type: ignore
    @property
    def data_context(self):
        return self.__data_context


class LazyArrowTableProxy:
    def __init__(self, table: nw.DataFrame):
        self.table = table
        self.column_names = set(self.table.columns)
        self._cache: Dict[str, nw.Series] = {}

    def __contains__(self, value: Any) -> Any:
        return value in self.column_names

    def __getitem__(self, key: str) -> Any:
        if key not in self.column_names:
            raise KeyError(key)
        if key not in self._cache:
            self._cache[key] = self.table[key]#.to_pandas()
        return self._cache[key]
