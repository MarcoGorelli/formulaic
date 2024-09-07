from __future__ import annotations

import functools
import itertools
from typing import TYPE_CHECKING, Any, Dict, List, Sequence, Set, Tuple, cast

import numpy
import pandas
import scipy.sparse as spsparse
from interface_meta import override

from formulaic.utils.cast import as_columns
from formulaic.utils.null_handling import drop_rows as drop_nulls
from formulaic.utils.null_handling import find_nulls

from .base import FormulaMaterializer
from .types import NAAction
import narwhals as nw

if TYPE_CHECKING:  # pragma: no cover
    from formulaic.model_spec import ModelSpec


class PandasMaterializer(FormulaMaterializer):
    # REGISTER_NAME = "pandas"
    REGISTER_NAME = "narwhals"
    # REGISTER_INPUTS: Sequence[str] = ("pandas.core.frame.DataFrame", "pandas.DataFrame")
    REGISTER_INPUTS: Sequence[str] = ("narwhals.dataframe.DataFrame",)
    REGISTER_OUTPUTS: Sequence[str] = ("pandas", "numpy", "sparse")

    @override
    def _is_categorical(self, values: Any) -> bool:
        values = nw.from_native(values, strict=False, allow_series=True)
        if isinstance(values, (nw.Series)):
            return values.dtype in (nw.Object, nw.String, nw.Categorical, nw.Enum)
        if isinstance(getattr(values, 'dtype', None), pandas.CategoricalDtype):
            return True
        return super()._is_categorical(values)

    @override
    def _check_for_nulls(
        self, name: str, values: Any, na_action: NAAction, drop_rows: Set[int]
    ) -> None:
        if na_action is NAAction.IGNORE:
            return

        try:
            null_indices = find_nulls(nw.from_native(values, allow_series=True, strict=False))

            if na_action is NAAction.RAISE:
                if null_indices:
                    raise ValueError(f"`{name}` contains null values after evaluation.")

            elif na_action is NAAction.DROP:
                drop_rows.update(null_indices)

            else:
                raise ValueError(
                    f"Do not know how to interpret `na_action` = {repr(na_action)}."
                )  # pragma: no cover; this is currently impossible to reach
        except ValueError as e:
            raise ValueError(
                f"Error encountered while checking for nulls in `{name}`: {e}"
            ) from e

    @override
    def _encode_constant(
        self,
        value: Any,
        metadata: Any,
        encoder_state: Dict[str, Any],
        spec: ModelSpec,
        drop_rows: Sequence[int],
    ) -> Any:
        nrows = self.nrows - len(drop_rows)
        if spec.output == "sparse":
            return spsparse.csc_matrix(numpy.array([value] * nrows).reshape((nrows, 1)))
        series = value * numpy.ones(nrows)
        return series

    @override
    def _encode_numerical(
        self,
        values: Any,
        metadata: Any,
        encoder_state: Dict[str, Any],
        spec: ModelSpec,
        drop_rows: Sequence[int],
    ) -> Any:
        if drop_rows:
            values = drop_nulls(nw.from_native(values, allow_series=True, strict=False), indices=drop_rows)
        if spec.output == "sparse":
            return spsparse.csc_matrix(
                numpy.array(values).reshape((values.shape[0], 1))
            )
        return values

    @override
    def _encode_categorical(
        self,
        values: Any,
        metadata: Any,
        encoder_state: Dict[str, Any],
        spec: ModelSpec,
        drop_rows: Sequence[int],
        reduced_rank: bool = False,
    ) -> Any:
        # Even though we could reduce rank here, we do not, so that the same
        # encoding can be cached for both reduced and unreduced rank. The
        # rank will be reduced in the _encode_evaled_factor method.
        from formulaic.transforms import encode_contrasts

        if drop_rows:
            values = nw.to_native(drop_nulls(nw.from_native(values, allow_series=True, strict=False), indices=drop_rows), strict=False)
        return as_columns(
            encode_contrasts(
                values,
                reduced_rank=False,
                _metadata=metadata,
                _state=encoder_state,
                _spec=spec,
            )
        )

    @override
    def _get_columns_for_term(
        self, factors: List[Dict[str, Any]], spec: ModelSpec, scale: float = 1
    ) -> Dict[str, Any]:
        out = {}

        names = [
            ":".join(reversed(product))
            for product in itertools.product(*reversed(factors))
        ]

        # Pre-multiply factors with only one set of values (improves performance)
        solo_factors = {}
        indices = []
        for i, factor in enumerate(factors):
            if len(factor) == 1:
                solo_factors.update(factor)
                indices.append(i)
        if solo_factors:
            for index in reversed(indices):
                factors.pop(index)
            if spec.output == "sparse":
                factors.append(
                    {
                        ":".join(solo_factors): functools.reduce(
                            spsparse.csc_matrix.multiply, solo_factors.values()
                        )
                    }
                )
            else:
                factors.append(
                    {
                        ":".join(solo_factors): functools.reduce(
                            numpy.multiply,
                            (numpy.asanyarray(p) for p in solo_factors.values()),
                        )
                    }
                )

        for i, reversed_product in enumerate(
            itertools.product(*(factor.items() for factor in reversed(factors)))
        ):
            if spec.output == "sparse":
                out[names[i]] = scale * functools.reduce(
                    spsparse.csc_matrix.multiply,
                    (nw.to_native(p[1], strict=False) for p in reversed(reversed_product)),
                )
            else:
                out[names[i]] = scale * functools.reduce(
                    numpy.multiply,
                    (numpy.array(p[1]) for p in reversed(reversed_product)),
                )
        return out

    @override
    def _combine_columns(
        self, cols: Sequence[Tuple[str, Any]], spec: ModelSpec, drop_rows: Sequence[int]
    ) -> pandas.DataFrame:
        # If we are outputing a pandas DataFrame, explicitly override index
        # in case transforms/etc have lost track of it.
        if spec.output == "pandas":
            pandas_index = cast(pandas.DataFrame, self.data_context).index
            if drop_rows:
                pandas_index = pandas_index.drop(
                    cast(pandas.DataFrame, self.data_context).index[drop_rows]
                )

        # Special case no columns to empty csc_matrix, array, or DataFrame
        if not cols:
            values = numpy.empty((self.data.shape[0], 0))
            if spec.output == "sparse":
                return spsparse.csc_matrix(values)
            if spec.output == "numpy":
                return values
            return pandas.DataFrame(index=pandas_index)

        # Otherwise, concatenate columns into model matrix
        if spec.output == "sparse":
            return spsparse.hstack([nw.to_native(col[1], strict=False) for col in cols])
        if spec.output == "numpy":
            return numpy.stack([col[1] for col in cols], axis=1)
        if spec.output == 'narwhals':
            native_namespace = nw.get_native_namespace(nw.from_native(self.data))
            return nw.to_native(
                nw.from_dict({col[0]: col[1] for col in cols}, native_namespace=native_namespace)
            )
        return pandas.DataFrame(
            {col[0]: col[1] for col in cols},
            index=pandas_index,
            copy=False,
        )
