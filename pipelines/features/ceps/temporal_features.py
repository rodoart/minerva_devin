
###############################################################################
# TEMPORAL FEATURES
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Dict, Any

# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import col

# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
import libs.functions.temporal_features as ltf
import libs.functions.pair_features as lpf

import config.job as cj
import config.features.ceps.temporal_features as ctf

from pipelines.features.ceps.cluster_features import (
    StandardClusterFeaturesStep, StandardClusterFeaturesSubStep)

import libs.framework as ppf

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)


###############################################################################
# Ceps
###############################################################################


class CepsTemporalFeaturesStep(StandardClusterFeaturesStep):
    """Step CEPS de features temporales (prefijo `tmp_`) desde las transacciones.

    Acepta `variant` ("rfc" por defecto): lee el `missing_treatment` de la
    variante (los ids de nodo ya resueltos por esa variante) y escribe sus
    salidas namespaced.
    """
    STEP_NAME_PREFIX = "Standard_Temporal_Features_Step"

    def __init__(self, *args, variant: str = "rfc", **kwargs) -> None:
        self.variant = variant
        super().__init__(*args, **kwargs)
        if variant != "rfc":
            self.step_name = f"{self.step_name}_{variant}"
        #
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta la sub-etapa CEPS temporal y recoge su salida."""
        return ppf.run_substep_and_collect(
            self, self.ceps_temporal_features_substep,
            "ceps_temporal_features_substep")
        #
    @ppf.cached_property
    def ceps_temporal_features_substep(self) -> "CepsTemporalFeaturesSubStep":
        """Sub-etapa CEPS de features temporales de nodo y de par."""
        return CepsTemporalFeaturesSubStep(
            self, previous_step=self.previous_step[0])


class CepsTemporalFeaturesSubStep(StandardClusterFeaturesSubStep):
    """Calcula las features temporales: una fila por id y una por par (a, x).
    """
    def step_action(self) -> Dict[str, Any]:
        """Materializa las tres salidas y las recoge en output_parameters."""
        return ppf.collect_step_output(self, {
            "temporal_node_features": self.temporal_node_features_output,
            "temporal_pair_features": self.temporal_pair_features_output,
            "temporal_pair_node": self.temporal_pair_node_output,
        }, "temporal_features")
        #
    # ------------------------------------------------------------------
    # Insumo normalizado
    # ------------------------------------------------------------------
    @ppf.cached_property
    def txns(self) -> DataFrame:
        """Transacciones normalizadas a (src, dst, ts, oper_mto, tfrom_days).

        `ts` se construye de information_date + hora_oper (HHmmss); los ids
        src/dst son los del grafo de la variante (id_src/id_dst de
        missing_treatment).
        """
        txns = self.standard_load_parquet_or_table("missing_treatment")
        return ltf.with_event_timestamp(
            txns, date_format=cj.DATE_STANDARD_SPARK_FORMAT).select(
                col("id_src").alias("src"),
                col("id_dst").alias("dst"),
                "ts", "oper_mto", "tfrom_days")
        #
    # ------------------------------------------------------------------
    # Features
    # ------------------------------------------------------------------
    @ppf.cached_property
    def temporal_node_features(self) -> DataFrame:
        """Una fila por id con todas las familias tmp_ de nodo."""
        return ltf.temporal_node_features(self.txns,
            window_days=ctf.WINDOW_DAYS,
            report_thresholds=ctf.REPORT_THRESHOLDS,
            below_threshold_band=ctf.BELOW_THRESHOLD_BAND,
            min_txn_for_burstiness=ctf.MIN_TXN_FOR_BURSTINESS,
            unique_receivers_window=ctf.UNIQUE_RECEIVERS_WINDOW)
        #
    @ppf.cached_property
    def temporal_pair_features(self) -> DataFrame:
        """Una fila por par (a, x) con las features de monto tmp_*."""
        return ltf.pair_amount_features(self.txns)
        #
    @ppf.cached_property
    def temporal_pair_node(self) -> DataFrame:
        """Features temporales de par agregadas a nivel nodo X."""
        return lpf.aggregate_pair_features_to_node(
            self.temporal_pair_features, node_role="x", stats=("max", "mean"),
            prefixes=("tmp_",))
        #
    # ------------------------------------------------------------------
    # Salidas persistidas
    # ------------------------------------------------------------------
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="temporal_node_features")
    def temporal_node_features_output(self) -> DataFrame:
        """Salida `temporal_node_features` (parquet plano, fila por `id`)."""
        return self.temporal_node_features
        #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="temporal_pair_features")
    def temporal_pair_features_output(self) -> DataFrame:
        """Salida `temporal_pair_features` (parquet plano, fila por par)."""
        return self.temporal_pair_features
        #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="temporal_pair_node")
    def temporal_pair_node_output(self) -> DataFrame:
        """Salida `temporal_pair_node` (parquet plano, fila por `id`)."""
        return self.temporal_pair_node
