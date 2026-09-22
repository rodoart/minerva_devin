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
