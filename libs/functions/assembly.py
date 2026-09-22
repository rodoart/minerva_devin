"""Funciones de ensamblado: agregaciones por feature, explode de arrays y `VectorAssembler`."""
###############################################################################
# ASSEMBLY FUNCTIONS
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import List, Dict, Optional, Union


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (col, lit, explode,
    mean as spark_mean, min as spark_min, max as spark_max, sum as spark_sum,
    first as spark_first, countDistinct
)
from pyspark.ml.feature import VectorAssembler


def weighted_mean(column:str, weight:Column) -> Column:
    """Media ponderada de `column` usando `weight` (los nulos no aportan)."""
    return (spark_sum(col(column) * weight) / spark_sum(weight)).alias(column)


def distinct_count(column:str, weight:Column) -> Column:
    """Número de valores distintos (p.ej. cuántos nodos aportan a un numcliente)."""
    return countDistinct(col(column)).alias(f"distinct_{column}")


ASSEMBLY_AGGREGATION_FUNCTIONS = {
    "mean": lambda column, weight: spark_mean(col(column)).alias(column),
    "min": lambda column, weight: spark_min(col(column)).alias(column),
    "max": lambda column, weight: spark_max(col(column)).alias(column),
    "sum": lambda column, weight: spark_sum(col(column)).alias(column),
    "first": lambda column, weight: spark_first(col(column)).alias(column),
    "distinct_count": lambda column, weight: countDistinct(col(column)).alias(f"distinct_{column}"),
    "weighted_mean": weighted_mean,
}


def build_aggregation_expressions(
    feature_columns:List[str],
    aggregation:Union[str, Dict[str, str]] = "weighted_mean",
    weight:Optional[Column] = None
) -> List[Column]:
    """Construye las expresiones de agregación para cada columna de feature.

    `aggregation` puede ser un string aplicado a todas las columnas o un dict
    {"default": "...", "<columna>": "..."} con overrides por feature.
    """
    if weight is None:
        weight = lit(1.0)
    if isinstance(aggregation, str):
        aggregation = {"default": aggregation}
    #
    expressions:List[Column] = []
    for column in feature_columns:
        agg_name = aggregation.get(column, aggregation.get("default", "weighted_mean"))
        if agg_name not in ASSEMBLY_AGGREGATION_FUNCTIONS:
            raise ValueError(f"Unsupported assembly aggregation '{agg_name}' for column '{column}'.")
        expressions.append(ASSEMBLY_AGGREGATION_FUNCTIONS[agg_name](column, weight))
    return expressions


def explode_array_column(df:DataFrame, column:str, explode_into:Optional[str] = None) -> DataFrame:
    """Explode una columna de tipo array manteniendo el resto de columnas."""
    if explode_into is None:
        explode_into = column
    return df.withColumn(explode_into, explode(col(column)))


def get_feature_columns(
    df:DataFrame,
    exclude_columns:Optional[List[str]] = None,
    exclude_prefixes:Optional[List[str]] = None
) -> List[str]:
    """Devuelve las columnas candidatas a feature (todas menos las excluidas)."""
    exclude_columns = exclude_columns or []
    exclude_prefixes = exclude_prefixes or []
    return [
        c for c in df.columns
        if c not in exclude_columns and not any(c.startswith(prefix) for prefix in exclude_prefixes)
    ]


def assemble_vector(
    df:DataFrame,
    feature_columns:List[str],
    output_column:str = "features",
    handle_invalid:str = "keep"
) -> DataFrame:
    """Aplica `pyspark.ml.feature.VectorAssembler` sobre las columnas dadas."""
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol=output_column,
        handleInvalid=handle_invalid
    )
    return assembler.transform(df)
