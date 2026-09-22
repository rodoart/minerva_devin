"""Funciones de imputación de nulos y dispatcher `apply_missing_treatment`."""
##########################################################################
# MISSING TREATMENT FUNCTIONS
##########################################################################

# ------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------
from typing import List, Union, Dict, Any, Callable

# ------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (coalesce, col,
    lit, mean as spark_mean
)


def fill_missings_with_value(df: DataFrame, columns: List[str], fill_value: Any) -> DataFrame:
    """Rellena los valores nulos de las columnas especificadas con un valor fijo.
    """
    selection = [coalesce(col(column), lit(fill_value)).alias(column) if column in columns else column for column in df.columns]
    return df.select(*selection)


def fill_missings_with_mean(df: DataFrame, columns: List[str]) -> DataFrame:
    """Rellena los valores nulos de las columnas especificadas con la media de cada columna.
    """
    means = df.select([spark_mean(col(column)).alias(column) for column in columns]).collect()[0].asDict()
    selection = [coalesce(col(column), lit(means[column])).alias(column) if column in columns else column for column in df.columns]
    return df.select(*selection)

def fill_missings_with_mean_without_ignoring_null_counts(df: DataFrame, columns: List[str]) -> DataFrame:
    """Rellena los valores nulos de las columnas especificadas con la media de cada columna,
    """
    means = df.select([spark_mean(coalesce(col(column),lit(0))).alias(column) for column in columns]).collect()[0].asDict()
    selection = [coalesce(col(column), lit(means[column])).alias(column) if column in columns else column for column in df.columns]
    return df.select(*selection)


MISSING_TREATMENT_FUNCTION_RELATIONS = {
    "mean": fill_missings_with_mean,
    "mean_with_nulls": fill_missings_with_mean_without_ignoring_null_counts,
    "value": fill_missings_with_value
}


def apply_missing_treatment(
    df: DataFrame,
    column_treatment_dict:Dict[str, Union[str, Callable, List[Any]]]
) -> DataFrame:
    """Aplica un tratamiento de valores nulos a las columnas especificadas.

    Formatos aceptados por columna:
      - "mean" | "mean_with_nulls" | "value" (requiere el valor extra)
      - [<tipo>] o [<tipo>, *args] p.ej. ["value", 0]
      - callable(df) -> DataFrame
    """
    for column, treatment in column_treatment_dict.items():
        if isinstance(treatment, list) and len(treatment) >=2:
            treatment_type = treatment[0]
            args_ = treatment[1:]
            if treatment_type in MISSING_TREATMENT_FUNCTION_RELATIONS:
                df = MISSING_TREATMENT_FUNCTION_RELATIONS[treatment_type](df, [column], *args_)
            else:
                raise ValueError(f"Unsupported missing treatment '{treatment_type}' for column '{column}'.")

            #
        elif isinstance(treatment, str) or (isinstance(treatment, list) and len(treatment) == 1):
            if isinstance(treatment, list):
                treatment = treatment[0]
            if treatment in MISSING_TREATMENT_FUNCTION_RELATIONS:
                df = MISSING_TREATMENT_FUNCTION_RELATIONS[treatment](df, [column])
            else:
                raise ValueError(f"Unsupported missing treatment '{treatment}' for column '{column}'.")
        elif callable(treatment):
            df = treatment(df)
        else:
            raise ValueError(f"Invalid treatment type for column '{column}': {type(treatment)}. Must be str or callable.")
    return df
