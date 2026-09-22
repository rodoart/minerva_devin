"""Constructores de columnas de peso de aristas y helpers del mapa `weights`."""
###############################################################################
# WEIGHT FUNCTIONS
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Dict, Union


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import Column
from pyspark.sql.functions import col, lit, create_map, when


def column_weight(column_name:str) -> Column:
    """Peso directo de una columna de las aristas."""
    return col(column_name)


def composed_weight(*column_names:str) -> Column:
    """Peso compuesto: media simple de varias columnas."""
    return sum(col(name) for name in column_names) / len(column_names)


def ratio_weight(numerator:str, denominator:str) -> Column:
    """Peso como cociente de dos columnas (0 si el denominador no es positivo)."""
    return when(col(denominator) > 0, col(numerator) / col(denominator)).otherwise(lit(0.0))


def build_weights_map(weight_columns:Dict[str, Column], alias:str = "weights") -> Column:
    """Construye la columna `weights` (mapa nombre->peso) para las aristas."""
    return create_map(*[x for k, v in weight_columns.items() for x in (lit(k), v)]).alias(alias)


def get_weight(weights_column:Union[str, Column], weight_type:str) -> Column:
    """Extrae un peso concreto del mapa `weights` de las aristas."""
    if isinstance(weights_column, str):
        weights_column = col(weights_column)
    return weights_column.getItem(weight_type)


# Catálogo de funciones de peso estándar, seleccionables por nombre desde los
# job configs (ver `build_weight_columns`).
STANDARD_WEIGHT_FUNCTIONS = {
    "column": column_weight,      # peso directo de una columna
    "composed": composed_weight,  # media simple de varias columnas
    "ratio": ratio_weight,        # cociente numerador/denominador (0 si den <= 0)
}


def build_weight_columns(
    weight_specs:Dict[str, Union[tuple, dict]],
    functions:Dict[str, callable] = STANDARD_WEIGHT_FUNCTIONS,
) -> Dict[str, Column]:
    """Resuelve una especificación declarativa de pesos en columnas.

    Cada entrada del spec es `nombre_peso -> spec`, donde spec puede ser:
      - tupla ("func", arg1, arg2, ...) -> functions["func"](arg1, arg2, ...)
      - dict {"function": "func", "args": [a1, a2, ...]}

    Args:
        weight_specs: especificación declarativa (configurable en el job config).
        functions: catálogo nombre_función -> callable(*args) -> Column.

    Returns:
        Dict nombre_peso -> Column, apto para `build_weights_map`.

    Raises:
        KeyError: si la función nombrada no existe en el catálogo.
        TypeError: si el spec no es tupla ni dict.
    """
    weight_columns:Dict[str, Column] = {}
    for weight_name, spec in weight_specs.items():
        if isinstance(spec, dict):
            func_name, args = spec["function"], list(spec.get("args", []))
        elif isinstance(spec, tuple):
            func_name, args = spec[0], list(spec[1:])
        else:
            raise TypeError(
                f"Invalid weight spec for '{weight_name}': {spec!r}")
        weight_columns[weight_name] = functions[func_name](*args)
    return weight_columns
