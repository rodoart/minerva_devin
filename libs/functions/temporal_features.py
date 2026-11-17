"""Features temporales (prefijo `tmp_`) calculadas desde las transacciones.

Dos familias:

1. **Por nodo**: antigüedad y recencia de la actividad, ritmo de
   transacciones por ventanas (1/7/30 días) y velocidad relativa, perfil
   horario (nocturnidad) y de semana (fin de semana), burstiness de los
   tiempos entre llegadas, receptores únicos recientes, pass-through 24h
   (fracción del monto entrante que vuelve a salir en <= 24h) y proporción
   de montos justo por debajo de umbrales de reporte.

2. **Por par (a, x)**: z-score del monto medio de la relación contra el
   historial del emisor, último monto sobre la mediana histórica del par y
   monto medio del par sobre la mediana del emisor.

Convenciones:
- Las funciones esperan columnas `src`, `dst`, `ts` (timestamp del evento),
  `oper_mto` (monto) y `tfrom_days` (días desde el evento hasta la fecha de
  referencia); `with_event_timestamp` construye `ts` a partir de
  `information_date` + `hora_oper`.
- "Actividad del nodo" = sus transacciones como EMISOR y como RECEPTOR
  (formato largo) para antigüedad/recencia/ritmo/horario; las de dirección
  (receptores únicos, pass-through) se calculan en el sentido del flujo.
- La antigüedad de la RELACIÓN a-x NO se calcula aquí: ya existe en las
  aristas del grafo como `max_tfrom_days` (edges_and_nodes) — sería
  redundante.
"""
###############################################################################
# General
###############################################################################
from typing import Iterable, Tuple

###############################################################################
# Pyspark
###############################################################################
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (coalesce, col,
    count as spark_count, countDistinct, dayofweek,
    hour, lag, lit, max as spark_max, mean as spark_mean,
    min as spark_min, percentile_approx, stddev as spark_std,
    struct, sum as spark_sum, to_timestamp, lpad, concat, unix_timestamp,
    when)

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)


###############################################################################
# HELPERS
###############################################################################

def with_event_timestamp(
    txns: DataFrame,
    date_column: str = "information_date",
    time_column: str = "hora_oper",
    date_format: str = "yyyyMMdd",
    output_column: str = "ts",
) -> DataFrame:
    """Añade `ts`: timestamp del evento = fecha de información + hora HHmmss."""
    return txns.withColumn(
        output_column,
        to_timestamp(
            concat(col(date_column).cast("string"),
                   lpad(col(time_column).cast("string"), 6, "0")),
            f"{date_format}HHmmss"))


def _long_format(txns: DataFrame) -> DataFrame:
    """Formato largo (id, ts, oper_mto, tfrom_days): cada transacción aparece
    una vez por lado (emisor y receptor)."""
    base = txns.select(
        col("src").alias("id"), "ts", "oper_mto", "tfrom_days")
    other = txns.select(
        col("dst").alias("id"), "ts", "oper_mto", "tfrom_days")
    return base.unionByName(other)


###############################################################################
# NODE TEMPORAL FEATURES
###############################################################################

def node_temporal_features(
    txns: DataFrame,
    window_days: Tuple[int, ...] = (1, 7, 30),
    report_thresholds: Iterable[float] = (),
    below_threshold_band: float = 0.9,
    night_start_hour: int = 22,
    night_end_hour: int = 6,
) -> DataFrame:
    """Features temporales de actividad por nodo (una fila por id).

    - `tmp_txn_count` y `tmp_txn_count_<w>d` por ventana de `window_days`.
    - `tmp_velocity_<w1>d_<w2>d`: ritmo diario de la ventana corta sobre el de
      la larga ((c_w1/w1)/(c_w2/w2)); > 1 = aceleración reciente.
    - `tmp_account_age_days`: antigüedad de la cuenta (máx tfrom_days).
    - `tmp_days_since_last_txn`: días desde la última transacción.
    - `tmp_pct_night`: fracción de transacciones entre `night_start_hour` y
      `night_end_hour` (horario nocturno, cruza medianoche).
    - `tmp_pct_weekend`: fracción en sábado/domingo.
    - `tmp_pct_below_threshold`: fracción de montos en
      [band*U, U) para algún umbral de reporte U (estructuración/smurfing).
    """
    long = _long_format(txns)
    is_night = (hour(col("ts")) >= lit(night_start_hour)) | \
               (hour(col("ts")) < lit(night_end_hour))
    is_weekend = dayofweek(col("ts")).isin(1, 7)   # Spark: 1=dom, 7=sáb
    thresholds = sorted(report_thresholds)
    if thresholds:
        is_below = None
        for threshold in thresholds:
            condition = ((col("oper_mto") >= lit(below_threshold_band * threshold))
                & (col("oper_mto") < lit(threshold)))
            is_night_cond = condition if is_below is None else (is_below | condition)
            is_below = is_night_cond
    else:
        is_below = lit(False)

    annotated = (long
        .withColumn("_night", is_night.cast("int"))
        .withColumn("_weekend", is_weekend.cast("int"))
        .withColumn("_below", is_below.cast("int")))

    aggs = [
        spark_count("*").alias("tmp_txn_count"),
        spark_max("tfrom_days").alias("tmp_account_age_days"),
        spark_min("tfrom_days").alias("tmp_days_since_last_txn"),
        spark_mean("_night").alias("tmp_pct_night"),
        spark_mean("_weekend").alias("tmp_pct_weekend"),
        spark_mean("_below").alias("tmp_pct_below_threshold"),
    ]
    for window in window_days:
        aggs.append(
            spark_sum(when(col("tfrom_days") <= lit(window), 1).otherwise(0))
                .alias(f"tmp_txn_count_{window}d"))
    result = annotated.groupBy("id").agg(*aggs)

    # Velocidad: ritmo diario de cada ventana corta contra la más larga
    longest = max(window_days)
    for window in window_days:
        if window == longest:
            continue
        result = result.withColumn(
            f"tmp_velocity_{window}d_{longest}d",
            when(col(f"tmp_txn_count_{longest}d") > 0,
                 (col(f"tmp_txn_count_{window}d") / lit(float(window)))
                 / (col(f"tmp_txn_count_{longest}d") / lit(float(longest))))
            .otherwise(lit(None).cast("double")))
    return result


def node_burstiness(
    txns: DataFrame,
    min_txn: int = 3,
) -> DataFrame:
    """Burstiness de los tiempos entre llegadas: (σ−μ)/(σ+μ) por nodo.

    En (-1, 1): 1 = ráfagas (cola pesada de esperas largas), 0 = aleatorio
    (Poisson), -1 = perfectamente periódico. Null si el nodo tiene menos de
    `min_txn` transacciones (no hay gaps suficientes).
    """
    long = _long_format(txns)
    window = Window.partitionBy("id").orderBy("ts")
    gaps = (long
        .withColumn("_gap",
            unix_timestamp("ts")
            - unix_timestamp(lag("ts").over(window)))
        .filter(col("_gap").isNotNull()))
    return (gaps
        .groupBy("id")
        .agg(spark_count("*").alias("_n_gaps"),
             spark_mean("_gap").alias("_mean_gap"),
             spark_std("_gap").alias("_std_gap"))
        .withColumn("tmp_burstiness",
            when((col("_n_gaps") >= lit(min_txn - 1))
                 & ((col("_std_gap") + col("_mean_gap")) > 0),
                 (col("_std_gap") - col("_mean_gap"))
                 / (col("_std_gap") + col("_mean_gap")))
            .otherwise(lit(None).cast("double")))
        .select("id", "tmp_burstiness"))


def node_unique_receivers(
    txns: DataFrame,
    window_days: int = 7,
) -> DataFrame:
    """Receptores únicos por emisor en la ventana: `tmp_unique_receivers_<w>d`.

    Un emisor que de repente dispersa a muchos receptores nuevos es señal de
    smurfing; la ventana corta capta el comportamiento reciente.
    """
    return (txns
        .filter(col("tfrom_days") <= lit(window_days))
        .groupBy(col("src").alias("id"))
        .agg(countDistinct("dst").alias(f"tmp_unique_receivers_{window_days}d")))


def node_pass_through_24h(txns: DataFrame) -> DataFrame:
    """Pass-through 24h por nodo RECEPTOR: fracción del monto entrante que
    vuelve a salir del nodo en <= 24h (`tmp_pass_through_24h`).

    Detecta cuentas puente (entra y sale rápido): para cada entrada se busca
    al menos una salida del mismo nodo en [t_in, t_in + 24h]; la fracción es
    sobre montos. Null si el nodo no recibe. También emite
    `tmp_flow_out_in_ratio` = monto total emitido / recibido en toda la
    ventana (barato; ~1 en cuentas de paso).
    """
    inflows = txns.select(
        col("dst").alias("id"),
        col("ts").alias("t_in"),
        col("oper_mto").alias("amt_in"))
    outflow_times = txns.select(
        col("src").alias("id"), col("ts").alias("t_out")).distinct()
    matched = (inflows
        .join(outflow_times, on="id")
        .filter((col("t_out") >= col("t_in"))
            & (unix_timestamp("t_out") - unix_timestamp("t_in")
               <= lit(24 * 3600)))
        .select("id", "t_in").distinct())
    pass_through = (inflows
        .join(matched.withColumn("_matched", lit(1)), on=["id", "t_in"],
            how="left")
        .groupBy("id")
        .agg(spark_sum("amt_in").alias("_in_tot"),
             spark_sum(when(col("_matched") == 1, col("amt_in"))
                 .otherwise(lit(0.0))).alias("_in_matched"))
        .withColumn("tmp_pass_through_24h",
            when(col("_in_tot") > 0, col("_in_matched") / col("_in_tot"))
            .otherwise(lit(None).cast("double")))
        .select("id", "tmp_pass_through_24h"))

    out_totals = (txns.groupBy(col("src").alias("id"))
        .agg(spark_sum("oper_mto").alias("_out_tot")))
    in_totals = (txns.groupBy(col("dst").alias("id"))
        .agg(spark_sum("oper_mto").alias("_in_tot2")))
    ratio = (out_totals.join(in_totals, on="id", how="outer")
        .withColumn("tmp_flow_out_in_ratio",
            when(col("_in_tot2") > 0,
                 coalesce(col("_out_tot"), lit(0.0)) / col("_in_tot2"))
            .otherwise(lit(None).cast("double")))
        .select("id", "tmp_flow_out_in_ratio"))
    return pass_through.join(ratio, on="id", how="outer")


###############################################################################
# PAIR TEMPORAL FEATURES
###############################################################################

def pair_amount_features(txns: DataFrame) -> DataFrame:
    """Features de monto por par (a, x) contra el historial del emisor.

    - `tmp_amt_zscore_ax`: (monto medio de a->x − monto medio de a) /
      std de a. Mide cuán atípica es la relación para el emisor (null si el
      emisor no tiene varianza).
    - `tmp_last_amt_over_median_ax`: último monto de a->x sobre la mediana
      histórica del par (spike reciente de la relación).
    - `tmp_mean_amt_over_median_a`: monto medio del par sobre la mediana del
      emisor (relación de montos altos para el emisor).
    """
    pair_stats = (txns
        .groupBy(col("src").alias("a"), col("dst").alias("x"))
        .agg(spark_mean("oper_mto").alias("_mean_ax"),
             percentile_approx("oper_mto", 0.5).alias("_median_ax"),
             spark_max(struct("ts", "oper_mto")).alias("_last_struct"))
        .withColumn("_last_ax", col("_last_struct")["oper_mto"])
        .drop("_last_struct"))
    sender_stats = (txns
        .groupBy(col("src").alias("a"))
        .agg(spark_mean("oper_mto").alias("_mean_a"),
             spark_std("oper_mto").alias("_std_a"),
             percentile_approx("oper_mto", 0.5).alias("_median_a")))
    return (pair_stats
        .join(sender_stats, on="a", how="left")
        .withColumn("tmp_amt_zscore_ax",
            when(col("_std_a") > 0,
                 (col("_mean_ax") - col("_mean_a")) / col("_std_a"))
            .otherwise(lit(None).cast("double")))
        .withColumn("tmp_last_amt_over_median_ax",
            when(col("_median_ax") > 0, col("_last_ax") / col("_median_ax"))
            .otherwise(lit(None).cast("double")))
        .withColumn("tmp_mean_amt_over_median_a",
            when(col("_median_a") > 0, col("_mean_ax") / col("_median_a"))
            .otherwise(lit(None).cast("double")))
        .select("a", "x", "tmp_amt_zscore_ax", "tmp_last_amt_over_median_ax",
            "tmp_mean_amt_over_median_a"))


###############################################################################
# ORQUESTADOR
###############################################################################

def temporal_node_features(
    txns: DataFrame,
    window_days: Tuple[int, ...] = (1, 7, 30),
    report_thresholds: Iterable[float] = (),
    below_threshold_band: float = 0.9,
    min_txn_for_burstiness: int = 3,
    unique_receivers_window: int = 7,
) -> DataFrame:
    """Une todas las features temporales de nodo en una fila por id."""
    result = node_temporal_features(txns,
        window_days=window_days,
        report_thresholds=report_thresholds,
        below_threshold_band=below_threshold_band)
    for frame in (
        node_burstiness(txns, min_txn=min_txn_for_burstiness),
        node_unique_receivers(txns, window_days=unique_receivers_window),
        node_pass_through_24h(txns),
    ):
        result = result.join(frame, on="id", how="left")
    return result
