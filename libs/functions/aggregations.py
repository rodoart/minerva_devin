"""Librería de funciones estándar de agregación para pasos group-by.

Provee un catálogo reutilizable de agregaciones (`standard_group_by_features`),
columnas auxiliares de peso por recencia (`recency_auxiliary_columns`) y la
resolución de nombres declarativos "{func}_{variable}" a expresiones `Column`
(`resolve_group_by_expressions`). Los configs seleccionan las agregaciones por
nombre, de modo que el conjunto aplicado queda configurable por job.
"""
###############################################################################
# WEIGHT FUNCTIONS
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Callable, Dict, List, Union


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import Column, Window
from pyspark.sql.functions import (col, min as spark_min,
    max as spark_max, mean as spark_mean, stddev as spark_std,
    sum as spark_sum, count, countDistinct, last, first, coalesce, lit
)


# Nombres de las columnas auxiliares de peso por recencia: se crean antes del
# groupBy y se descartan del resultado final.
RECENCY_MAX_COLUMN = "_max_tfrom_days"      # antigüedad máxima de la ventana
RECENCY_WEIGHT_COLUMN = "_recency_weight"   # peso de recencia por fila


def recency_auxiliary_columns(
    tfrom_column:str = "tfrom_days",
    max_column:str = RECENCY_MAX_COLUMN,
    weight_column:str = RECENCY_WEIGHT_COLUMN,
) -> Dict[str, Column]:
    """Columnas auxiliares para agregaciones ponderadas por recencia.

    `max_column` es el máximo de `tfrom_column` en toda la ventana (la txn más
    antigua) y `weight_column` vale 1 en la más antigua y max+1 en la más
    reciente: las agregaciones `weighted_*` dan más peso a lo reciente.

    Args:
        tfrom_column: columna de antigüedad (días desde la txn al corte).
        max_column: nombre de la columna auxiliar con el máximo.
        weight_column: nombre de la columna auxiliar con el peso.

    Returns:
        Dict nombre_columna_auxiliar -> Column (se usa con
        `standard_group_by_txn(auxiliary_columns=...)`).
    """
    global_window = Window.partitionBy()  # sin claves: agrega sobre todo el df
    return {
        max_column: spark_max(col(tfrom_column)).over(global_window),
        weight_column: (col(max_column) - col(tfrom_column) + lit(1)),
    }


def standard_group_by_features(
    weight_column:str = RECENCY_WEIGHT_COLUMN,
) -> Dict[str, Callable[[str], Column]]:
    """Catálogo de funciones de agregación estándar para group-by.

    Cada entrada se invoca como `func(nombre_variable) -> Column`; el resultado
    se suele renombrar `"{func_name}_{variable}"` (ver
    `resolve_group_by_expressions`).

    Args:
        weight_column: columna de peso usada por las funciones `weighted_*`
            (típicamente la generada por `recency_auxiliary_columns`).

    Returns:
        Dict nombre_función -> callable(variable) -> Column.
    """
    return {
        "min": spark_min,                                     # mínimo de la variable en el grupo
        "max": spark_max,                                     # máximo de la variable en el grupo
        "mean": lambda c: spark_mean(col(c)),                 # media simple
        "weighted_mean": lambda c: spark_sum(col(c)*col(weight_column))/spark_sum(col(weight_column)),  # media ponderada por recencia
        "weighted_sum": lambda c: spark_sum(col(c)*col(weight_column)),   # suma ponderada por recencia
        "std": lambda c: coalesce(spark_std(col(c)), lit(0.0)),           # desviación estándar (0.0 sin varianza)
        "count": count,                                       # nº de registros del grupo
        "weighted_count": lambda c: spark_sum(col(weight_column)),        # recuento ponderado (suma de pesos)
        "countDistinct": countDistinct,                       # nº de valores distintos
        "sum": spark_sum,                                     # suma simple
        "last": lambda c: last(col(c), ignorenulls=True),     # último valor no nulo del grupo
        "first": lambda c: first(col(c), ignorenulls=True),   # primer valor no nulo del grupo
        "curt": lambda c: spark_sum(col(c)**3)/spark_sum(col(c)**2)**(3/2),  # momento de orden 3 normalizado (aprox. curtosis)
        "skew": lambda c: spark_sum(col(c)**3)/spark_sum(col(c)**2)**(3/2),  # momento de orden 3 normalizado (aprox. asimetría)
        "so": lambda c: spark_sum(col(c)**4)/spark_sum(col(c)**2)**(4/2),    # momento de orden 4 normalizado (aprox. outlier-ness)
    }


def select_group_by_features(
    enabled:List[str],
    weight_column:str = RECENCY_WEIGHT_COLUMN,
) -> Dict[str, Callable[[str], Column]]:
    """Subconjunto configurable del catálogo estándar de agregaciones.

    Args:
        enabled: nombres de funciones habilitadas (clave del job config).
        weight_column: columna de peso para las `weighted_*`.

    Returns:
        Dict nombre -> callable limitado a `enabled`.

    Raises:
        KeyError: si algún nombre no existe en el catálogo estándar.
    """
    catalog = standard_group_by_features(weight_column)
    unknown = [name for name in enabled if name not in catalog]
    if unknown:
        raise KeyError(f"Unknown group by features: {unknown}")
    return {name: catalog[name] for name in enabled}


def resolve_group_by_expressions(
    aggregation_names:List[str],
    features:Dict[str, Callable[[str], Column]],
) -> List[Column]:
    """Resuelve nombres declarativos "{func}_{variable}" a Column de agregación.

    Para cada nombre busca el prefijo de función más largo presente en
    `features` y aplica `features[func](variable)`; el resultado se renombra al
    nombre declarado.

    Args:
        aggregation_names: lista de nombres "{func}_{variable}" (p.ej.
            "weighted_mean_oper_mto").
        features: catálogo nombre_función -> callable(variable) -> Column.

    Returns:
        Lista de expresiones `Column` ya aliasadas, en el orden de entrada.

    Raises:
        ValueError: si un nombre no casa con ninguna función del catálogo.
    """
    aggregations:List[Column] = []
    func_names = sorted(features, key=len, reverse=True)  # prefijo más largo primero
    for agg_name in aggregation_names:
        for func_name in func_names:
            if agg_name.startswith(f"{func_name}_"):
                variable = agg_name[len(func_name) + 1:]
                aggregations.append(
                    features[func_name](variable).alias(agg_name))
                break
        else:
            raise ValueError(f"Unknown txn aggregation: {agg_name}")
    return aggregations
