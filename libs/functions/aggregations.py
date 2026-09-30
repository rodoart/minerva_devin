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
from typing import Callable, Dict, List, Tuple, Union


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import (col, min as spark_min,
    max as spark_max, mean as spark_mean, stddev as spark_std,
    sum as spark_sum, count, countDistinct, last, first, coalesce, lit,
    when, datediff, to_date, sqrt
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


def split_group_by_name(
    agg_name:str,
    feature_names:Union[List[str], Dict[str, Callable]],
) -> Tuple[str, str]:
    """Separa un nombre "{func}_{variable}" en (función, variable).

    Busca el prefijo de función más largo presente en `feature_names`
    (o en las claves del catálogo si se pasa un dict).

    Args:
        agg_name: nombre declarado (p.ej. "weighted_mean_oper_mto").
        feature_names: nombres de funciones habilitadas o catálogo
            nombre -> callable (se usan las claves).

    Returns:
        Tupla (func_name, variable).

    Raises:
        ValueError: si el nombre no casa con ninguna función.
    """
    func_names = sorted(feature_names, key=len, reverse=True)  # prefijo más largo primero
    for func_name in func_names:
        if agg_name.startswith(f"{func_name}_"):
            return func_name, agg_name[len(func_name) + 1:]
    raise ValueError(f"Unknown txn aggregation: {agg_name}")


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
    for agg_name in aggregation_names:
        func_name, variable = split_group_by_name(agg_name, features)
        aggregations.append(features[func_name](variable).alias(agg_name))
    return aggregations


# ------------------------------------------------------------------------------
# Agregación incremental mensual
# ------------------------------------------------------------------------------
#
# Los group-by de toda la ventana (12 meses) pueden descomponerse en
# agregaciones parciales por mes que luego se combinan. Esto permite
# materializar las particiones mensuales una sola vez y, en corridas
# posteriores, computar únicamente los meses que falten
# (ver `libs.framework.ensure_monthly_partitions`).
#
# La variable de antigüedad (p.ej. `tfrom_days`) NO es invariante entre
# corridas: depende de la fecha de referencia del vintage. Por eso las
# particiones mensuales guardan `time_column` relativa al fin de su propio
# mes (`t_rel_days`) y el merge le suma el desfase
# `datediff(reference_date, month_partition)` de cada partición, que es
# constante dentro de la partición.
#
# Los pesos de recencia w_v = G - t_v + 1 usan G = máximo GLOBAL de la
# ventana (como `recency_auxiliary_columns`). Las agregaciones ponderadas
# se expanden en términos combinables:
#   weighted_sum   = Σv·w = (G+1)·Σv* - Σv·t        (v* restringido a t válido)
#   weighted_count = Σw   = (G+1)·count_t - Σt
#   weighted_mean  = weighted_sum / weighted_count

MONTHLY_TIME_COLUMN = "t_rel_days"        # días desde la txn al fin de SU mes
MONTHLY_PARTITION_COLUMN = "month_partition"  # fin de mes de la txn (partición)
_MONTH_OFFSET_COLUMN = "_month_offset"    # datediff(reference, month_partition)

# Funciones sin combinador exacto a partir de agregados parciales mensuales
# (countDistinct requeriría estructuras tipo HLL; last/first dependen de orden).
NON_MERGEABLE_GROUP_BY_FUNCTIONS = {"countDistinct", "last", "first"}

# Funciones que requieren momentos (Σx²..Σx⁴) de la variable, que las
# particiones mensuales solo guardan para variables de valor, no para la
# variable temporal: aplicadas a `time_variable` tampoco son combinables.
_MOMENT_GROUP_BY_FUNCTIONS = {"std", "curt", "skew", "so"}


def monthly_partial_group_by_aggregations(
    value_columns:List[str],
    time_column:str = MONTHLY_TIME_COLUMN,
    date_column:str = "information_date",
) -> List[Column]:
    """Agregaciones parciales por (grupo, mes), combinables por min/max/sum.

    Para cada variable de valor `v` se guardan:
      - ``min_v``, ``max_v``, ``sum_v``, ``count_v``
      - ``sum2_v``, ``sum3_v``, ``sum4_v``: momentos para so/curt/skew/std
      - ``wsum_v``: Σv restringida a filas con `time_column` válida
        (elegible para ponderar por recencia)
      - ``vtsum_v``: Σ(v·`time_column`)
    Para la variable temporal se guardan ``min``, ``max``, ``sum`` y
    ``count``, más el máximo de `date_column`.

    Args:
        value_columns: variables de valor (p.ej. ["oper_mto"]).
        time_column: columna de antigüedad relativa al fin del mes
            (la genera `standard_group_by_txn_monthly`).
        date_column: columna de fecha de información (su máximo se conserva).

    Returns:
        Lista de expresiones `Column` aliasadas para el groupBy mensual.
    """
    aggregations:List[Column] = []
    for value_column in value_columns:
        value = col(value_column)
        time = col(time_column)
        aggregations += [
            spark_min(value).alias(f"min_{value_column}"),
            spark_max(value).alias(f"max_{value_column}"),
            spark_sum(value).alias(f"sum_{value_column}"),
            count(value).alias(f"count_{value_column}"),
            spark_sum(value**2).alias(f"sum2_{value_column}"),
            spark_sum(value**3).alias(f"sum3_{value_column}"),
            spark_sum(value**4).alias(f"sum4_{value_column}"),
            spark_sum(when(time.isNotNull(), value)).alias(f"wsum_{value_column}"),
            spark_sum(value*time).alias(f"vtsum_{value_column}"),
        ]
    aggregations += [
        spark_min(col(time_column)).alias(f"min_{time_column}"),
        spark_max(col(time_column)).alias(f"max_{time_column}"),
        spark_sum(col(time_column)).alias(f"sum_{time_column}"),
        count(col(time_column)).alias(f"count_{time_column}"),
        spark_max(col(date_column)).alias(date_column),
    ]
    return aggregations


def _merged_group_by_expression(
    func_name:str,
    variable:str,
    time_variable:str,
    weight_base:Column,
) -> Column:
    """Expresión final de una agregación sobre los agregados mensuales.

    Opera sobre las columnas intermedias producidas por el group-by de
    `merge_monthly_group_by` (``min_/max_/count_{var}``,
    ``_sum_/_sum2_/_sum3_/_sum4_/_wsum_/_vtsum_{var}`` y
    ``_sum_/count_{time_variable}``).

    Args:
        func_name: función del catálogo (min, max, weighted_sum, so, ...).
        variable: variable agregada (p.ej. "oper_mto").
        time_variable: nombre de la variable temporal en las agregaciones
            declaradas (p.ej. "tfrom_days").
        weight_base: ``lit(G + 1)`` con G = máximo global de la variable
            temporal en la ventana (o lit(None) si está vacía).
    """
    if func_name in ("min", "max", "count"):
        return col(f"{func_name}_{variable}")
    if func_name == "sum":
        return col(f"_sum_{variable}")
    if func_name == "mean":
        return col(f"_sum_{variable}")/col(f"count_{variable}")
    if func_name == "std":
        # desviación estándar muestral desde los momentos: (Σx²-(Σx)²/N)/(N-1)
        variance = ((col(f"_sum2_{variable}")
            - col(f"_sum_{variable}")**2/col(f"count_{variable}"))
            / (col(f"count_{variable}") - 1))
        return coalesce(sqrt(variance), lit(0.0))
    if func_name in ("curt", "skew"):
        return col(f"_sum3_{variable}")/col(f"_sum2_{variable}")**1.5
    if func_name == "so":
        return col(f"_sum4_{variable}")/col(f"_sum2_{variable}")**2
    if func_name == "weighted_mean":
        return ((weight_base*col(f"_wsum_{variable}") - col(f"_vtsum_{variable}"))
            / (weight_base*col(f"count_{time_variable}") - col(f"_sum_{time_variable}")))
    if func_name == "weighted_sum":
        return weight_base*col(f"_wsum_{variable}") - col(f"_vtsum_{variable}")
    if func_name == "weighted_count":
        return weight_base*col(f"count_{time_variable}") - col(f"_sum_{time_variable}")
    raise ValueError(f"Aggregation '{func_name}' has no monthly combiner")


def merge_monthly_group_by(
    monthly_df:DataFrame,
    txn_id_columns:List[str],
    aggregation_names:List[str],
    feature_names:Union[List[str], Dict[str, Callable]],
    reference_date:str,
    time_variable:str = "tfrom_days",
    time_column:str = MONTHLY_TIME_COLUMN,
    month_column:str = MONTHLY_PARTITION_COLUMN,
    date_column:str = "information_date",
) -> DataFrame:
    """Combina los agregados parciales mensuales en la agregación de la ventana.

    Replica el resultado de un group-by completo sobre las transacciones
    crudas, pero a partir de las particiones mensuales parciales
    (`monthly_partial_group_by_aggregations`):

      - min/max/sum/count se recombinan por min/max/sum/sum
      - mean = Σ(sum)/Σ(count); so/curt/skew/std desde los momentos Σv²..Σv⁴
      - la antigüedad absoluta se recupera como t_rel + desfase del mes
      - weighted_* se recomponen con G = máximo global de la ventana
        (ver bloque de comentarios del módulo)

    Args:
        monthly_df: particiones mensuales parciales (una fila por grupo-mes).
        txn_id_columns: columnas de agrupación final (p.ej. ["id_src","id_dst"]).
        aggregation_names: nombres "{func}_{variable}" declarados en config.
        feature_names: funciones habilitadas (o catálogo; se usan las claves)
            para resolver los prefijos de `aggregation_names`.
        reference_date: fecha de referencia de la antigüedad (la misma que
            usó `calculate_tfroms`, p.ej. "2025-08-31").
        time_variable: nombre de la variable temporal en las agregaciones
            declaradas (p.ej. "tfrom_days").
        time_column: columna de antigüedad relativa en las particiones.
        month_column: columna-partición de mes (fin de mes) en las particiones.
        date_column: columna de fecha de información.

    Returns:
        DataFrame con `txn_id_columns` + las agregaciones pedidas +
        `date_column` (máximo).

    Raises:
        ValueError: si alguna agregación pedida no tiene combinador mensual
            (countDistinct/last/first, o weighted_* sobre la variable temporal).
    """
    resolved:List[Tuple[str, str, str]] = []
    value_columns:List[str] = []
    for agg_name in aggregation_names:
        func_name, variable = split_group_by_name(agg_name, feature_names)
        if func_name in NON_MERGEABLE_GROUP_BY_FUNCTIONS:
            raise ValueError(
                f"Aggregation '{agg_name}' ('{func_name}') has no exact "
                "monthly combiner")
        if variable == time_variable and (
                func_name.startswith("weighted")
                or func_name in _MOMENT_GROUP_BY_FUNCTIONS):
            raise ValueError(
                f"Aggregation '{agg_name}' ('{func_name}' over the time "
                "variable) has no monthly combiner")
        resolved.append((agg_name, func_name, variable))
        if variable != time_variable and variable not in value_columns:
            value_columns.append(variable)
    #
    # Desfase en días del fin de cada mes-partición a la referencia:
    # tfrom_days = t_rel + desfase (constante dentro de cada partición).
    df = monthly_df.withColumn(
        _MONTH_OFFSET_COLUMN,
        datediff(to_date(lit(reference_date)), col(month_column)))
    #
    # Antigüedad máxima global de la ventana (base de los pesos recenciales).
    global_max_t = df.agg(
        spark_max(col(f"max_{time_column}") + col(_MONTH_OFFSET_COLUMN))
    ).first()[0]
    weight_base = lit(global_max_t + 1) if global_max_t is not None else lit(None)
    #
    # Combinación por grupo sobre los parciales mensuales.
    edge_aggregations:List[Column] = []
    for variable in value_columns:
        edge_aggregations += [
            spark_min(f"min_{variable}").alias(f"min_{variable}"),
            spark_max(f"max_{variable}").alias(f"max_{variable}"),
            spark_sum(f"sum_{variable}").alias(f"_sum_{variable}"),
            spark_sum(f"count_{variable}").alias(f"count_{variable}"),
            spark_sum(f"sum2_{variable}").alias(f"_sum2_{variable}"),
            spark_sum(f"sum3_{variable}").alias(f"_sum3_{variable}"),
            spark_sum(f"sum4_{variable}").alias(f"_sum4_{variable}"),
            spark_sum(f"wsum_{variable}").alias(f"_wsum_{variable}"),
            spark_sum(col(f"vtsum_{variable}")
                + col(_MONTH_OFFSET_COLUMN)*col(f"wsum_{variable}")
            ).alias(f"_vtsum_{variable}"),
        ]
    edge_aggregations += [
        spark_min(col(f"min_{time_column}") + col(_MONTH_OFFSET_COLUMN)
            ).alias(f"min_{time_variable}"),
        spark_max(col(f"max_{time_column}") + col(_MONTH_OFFSET_COLUMN)
            ).alias(f"max_{time_variable}"),
        spark_sum(col(f"sum_{time_column}")
            + col(_MONTH_OFFSET_COLUMN)*col(f"count_{time_column}")
        ).alias(f"_sum_{time_variable}"),
        spark_sum(f"count_{time_column}").alias(f"count_{time_variable}"),
        spark_max(col(date_column)).alias(date_column),
    ]
    grouped = df.groupBy(*txn_id_columns).agg(*edge_aggregations)
    #
    finals = [col(column) for column in txn_id_columns]
    for agg_name, func_name, variable in resolved:
        finals.append(
            _merged_group_by_expression(
                func_name, variable, time_variable, weight_base)
            .alias(agg_name))
    finals.append(col(date_column))
    return grouped.select(*finals)
