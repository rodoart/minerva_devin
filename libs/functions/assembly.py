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
    first as spark_first, countDistinct, stddev as spark_std,
    percentile_approx, count, when, coalesce, sqrt, pmod,
    hash as spark_hash
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
    "median": lambda column, weight: percentile_approx(col(column), 0.5).alias(column),
    "std": lambda column, weight: spark_std(col(column)).alias(column),
    "min": lambda column, weight: spark_min(col(column)).alias(column),
    "max": lambda column, weight: spark_max(col(column)).alias(column),
    "sum": lambda column, weight: spark_sum(col(column)).alias(column),
    "first": lambda column, weight: spark_first(col(column)).alias(column),
    "distinct_count": lambda column, weight: countDistinct(col(column)).alias(f"distinct_{column}"),
    "weighted_mean": weighted_mean,
}


def build_aggregation_expressions(
    feature_columns:List[str],
    aggregation:Union[str, List[str], Dict[str, Union[str, List[str]]]] = "weighted_mean",
    weight:Optional[Column] = None
) -> List[Column]:
    """Construye las expresiones de agregación para cada columna de feature.

    `aggregation` puede ser:
      - un string aplicado a todas las columnas,
      - una lista de strings aplicada a todas las columnas (una expresión por
        función, alias `{funcion}_{columna}`),
      - un dict {"default": str|lista, "<columna>": str|lista} con overrides
        por feature.

    Con una sola función por columna el alias es el nombre de la columna
    (back-compat); con varias, cada expresión se aliasa `{funcion}_{columna}`.
    """
    if weight is None:
        weight = lit(1.0)
    if isinstance(aggregation, (str, list)):
        aggregation = {"default": aggregation}
    #
    expressions:List[Column] = []
    for column in feature_columns:
        agg_spec = aggregation.get(column, aggregation.get("default", "weighted_mean"))
        agg_names = [agg_spec] if isinstance(agg_spec, str) else list(agg_spec)
        for agg_name in agg_names:
            if agg_name not in ASSEMBLY_AGGREGATION_FUNCTIONS:
                raise ValueError(f"Unsupported assembly aggregation '{agg_name}' for column '{column}'.")
            expression = ASSEMBLY_AGGREGATION_FUNCTIONS[agg_name](column, weight)
            if len(agg_names) > 1:
                expression = expression.alias(f"{agg_name}_{column}")
            expressions.append(expression)
    return expressions


# Funciones de ensamblado con combinador exacto a partir de parciales por
# (llave, sal): mean/sum/min/max/std (momentos) y weighted_mean (Σv·w / Σw).
# "median", "first", "distinct_count" y customs NO son combinables.
COMBINABLE_ASSEMBLY_STATS = {
    "mean", "std", "min", "max", "sum", "weighted_mean", "count"}


def _resolve_column_aggs(
    feature_columns:List[str],
    aggregation:Union[str, List[str], Dict[str, Union[str, List[str]]]],
) -> Dict[str, List[str]]:
    """Normaliza `aggregation` a {columna: [funciones]} (misma resolución que
    `build_aggregation_expressions`)."""
    if isinstance(aggregation, (str, list)):
        aggregation = {"default": aggregation}
    resolved:Dict[str, List[str]] = {}
    for column in feature_columns:
        agg_spec = aggregation.get(
            column, aggregation.get("default", "weighted_mean"))
        resolved[column] = (
            [agg_spec] if isinstance(agg_spec, str) else list(agg_spec))
    return resolved


def _assembly_output_name(agg_name:str, column:str, agg_names:List[str]) -> str:
    """Alias de salida (convención de `build_aggregation_expressions`)."""
    return column if len(agg_names) == 1 else f"{agg_name}_{column}"


def salted_assembly_groupby(
    df:DataFrame,
    group_column:str,
    feature_columns:List[str],
    aggregation:Union[str, List[str], Dict[str, Union[str, List[str]]]],
    weight:Column,
    salt_buckets:int,
    id_column:Optional[str] = "id",
) -> DataFrame:
    """`groupBy(group_column)` de features agregadas en dos etapas con sal.

    Robusto a llaves gigantes (p.ej. un `numcliente`/`cta` con miles de nodos):
    la etapa 1 agrega parciales por (llave, sal) — la sal deriva de
    `id_column` — y la etapa 2 los combina por llave.

    - Stats combinables (`COMBINABLE_ASSEMBLY_STATS`: mean/sum/min/max/std/
      weighted_mean) van por la vía salteada con combinadores exactos.
    - El resto (median, first, distinct_count, ...) se calcula por la vía
      directa `groupBy` y se une por la llave.
    - `countDistinct(id_column)` se emite como `node_count` por deduplicación
      (distinct(llave, id) -> count), también en dos etapas.

    Devuelve una fila por `group_column` con los mismos nombres de columna
    que produciría `build_aggregation_expressions` + `node_count`.
    """
    resolved = _resolve_column_aggs(feature_columns, aggregation)
    for column, names in resolved.items():
        for name in names:
            if name not in ASSEMBLY_AGGREGATION_FUNCTIONS:
                raise ValueError(
                    f"Unsupported assembly aggregation '{name}' for column "
                    f"'{column}'.")
    combinable = {c: [n for n in names if n in COMBINABLE_ASSEMBLY_STATS]
        for c, names in resolved.items()}
    combinable = {c: names for c, names in combinable.items() if names}
    plain = {c: [n for n in names if n not in COMBINABLE_ASSEMBLY_STATS]
        for c, names in resolved.items()}
    plain = {c: names for c, names in plain.items() if names}
    #
    frames:List[DataFrame] = []
    if combinable:
        salted = df.withColumn(
            "__asalt", pmod(spark_hash(col(id_column)), lit(salt_buckets))
        ).withColumn("__aw", weight)
        partial_aggs:List[Column] = [count(lit(1)).alias("__size")]
        # Denominador de weighted_mean: Σw sobre TODAS las filas del grupo —
        # incluidas las de valor nulo (misma semántica que `weighted_mean`).
        if any("weighted_mean" in names for names in combinable.values()):
            partial_aggs.append(spark_sum(col("__aw")).alias("__w"))
        for column, names in combinable.items():
            value = col(column)
            partial_aggs += [
                count(value).alias(f"__n_{column}"),
                spark_sum(value).alias(f"__s_{column}"),
            ]
            if any(n in names for n in ("std",)):
                partial_aggs.append(
                    spark_sum(value * value).alias(f"__s2_{column}"))
            if "min" in names:
                partial_aggs.append(spark_min(value).alias(f"__min_{column}"))
            if "max" in names:
                partial_aggs.append(spark_max(value).alias(f"__max_{column}"))
            if "weighted_mean" in names:
                partial_aggs.append(
                    spark_sum(value * col("__aw")).alias(f"__sw_{column}"))
        partials = salted.groupBy(group_column, "__asalt").agg(*partial_aggs)
        #
        merges:List[Column] = [spark_sum("__size").alias("__size")]
        if "__w" in partials.columns:
            merges.append(spark_sum("__w").alias("__w"))
        for column in combinable:
            merges += [spark_sum(f"__n_{column}").alias(f"__n_{column}"),
                       spark_sum(f"__s_{column}").alias(f"__s_{column}")]
            if f"__s2_{column}" in partials.columns:
                merges.append(spark_sum(f"__s2_{column}").alias(f"__s2_{column}"))
            if f"__min_{column}" in partials.columns:
                merges.append(spark_min(f"__min_{column}").alias(f"__min_{column}"))
            if f"__max_{column}" in partials.columns:
                merges.append(spark_max(f"__max_{column}").alias(f"__max_{column}"))
            if f"__sw_{column}" in partials.columns:
                merges.append(
                    spark_sum(f"__sw_{column}").alias(f"__sw_{column}"))
        grouped = partials.groupBy(group_column).agg(*merges)
        #
        finals:List[Column] = [col(group_column)]
        for column, names in combinable.items():
            for name in names:
                out = _assembly_output_name(name, column, resolved[column])
                if name == "count":
                    finals.append(col(f"__n_{column}").alias(out))
                elif name == "sum":
                    finals.append(col(f"__s_{column}").alias(out))
                elif name == "min":
                    finals.append(col(f"__min_{column}").alias(out))
                elif name == "max":
                    finals.append(col(f"__max_{column}").alias(out))
                elif name == "mean":
                    finals.append(
                        (col(f"__s_{column}") / col(f"__n_{column}"))
                        .alias(out))
                elif name == "std":
                    variance = ((col(f"__s2_{column}")
                        - col(f"__s_{column}")**2 / col(f"__n_{column}"))
                        / (col(f"__n_{column}") - 1))
                    finals.append(
                        coalesce(sqrt(variance), lit(0.0)).alias(out))
                elif name == "weighted_mean":
                    finals.append(
                        (col(f"__sw_{column}") / col("__w")).alias(out))
        frames.append(grouped.select(*finals))
    #
    if plain:
        plain_aggs:List[Column] = []
        for column, names in plain.items():
            for name in names:
                expression = ASSEMBLY_AGGREGATION_FUNCTIONS[name](
                    column, weight)
                plain_aggs.append(
                    expression.alias(
                        _assembly_output_name(name, column, resolved[column])))
        frames.append(df.groupBy(group_column).agg(*plain_aggs))
    #
    # node_count: nº de ids distintos por llave, vía distinct en dos etapas.
    if id_column is not None:
        node_counts = (df.select(group_column, id_column).distinct()
            .groupBy(group_column)
            .agg(count(lit(1)).alias("node_count")))
        frames.append(node_counts)
    #
    result = frames[0]
    for frame in frames[1:]:
        result = result.join(frame, on=group_column, how="outer")
    return result


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
