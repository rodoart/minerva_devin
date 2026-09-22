
##########################################################################
# LIBRARIES
##########################################################################

# ------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------
from typing import Dict, Any

# ------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (col, when, lit
)
from pyspark.sql.types import  StringType
# ------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------
import libs.framework as ppf
from libs.functions.aggregations import resolve_group_by_expressions
import config.graph_making.ceps.group_by as ccgb
import pipelines.graph_making.group_by as p_gm_gb

##########################################################################
# CLASSES
##########################################################################


class CepsGroupByStep(p_gm_gb.StandardGroupByStep):
    """Step orquestador del group-by del grafo CEPS (por arista y por nodo)."""
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta el substep de agregación (`ceps_group_by_step`)."""
        return ppf.run_substep_and_collect(self, self.ceps_group_by_step, "ceps_group_by_step")
        #
    @ppf.cached_property
    def ceps_group_by_step(self) -> "CepsGroupBySubStep":
        """Substep de agregación CEPS (lazy, cacheado) con dependencia del step previo."""
        return CepsGroupBySubStep(self, previous_step=self.previous_step[0])


class CepsGroupBySubStep(p_gm_gb.StandardGroupBySubStep):
    """Substep que agrega las transacciones CEP por arista (`group_by_txn`) y por nodo (`group_by_id`)."""
    def step_action(self) -> Dict[str, Any]:
        """Materializa y colecta `group_by_txn` y `group_by_id` (ambos se
        recargan o recomputan de forma independiente)."""
        return ppf.collect_step_output(
            self, {"group_by_txn": self.group_by_txn, "group_by_id": self.group_by_id},
            "group_by_id")
        #
    @ppf.cached_property
    def missing_treatment(self) -> DataFrame:
        """Recarga la salida `missing_treatment` del step de *special treatment*."""
        return self.get_previous_step("missing_treatment")
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="group_by_txn")
    def group_by_txn(self) -> DataFrame:
        """Agrega las transacciones tratadas por arista con `GROUP_TXN_AGGREGATIONS` de config."""
        missing_treatment: DataFrame = self.missing_treatment
        # GROUP_TXN_AGGREGATIONS son nombres "{func}_{variable}" -> Column
        # vía GROUP_BY_TXN_FEATURES (prefijo de función más largo primero).
        aggregations = resolve_group_by_expressions(
            ccgb.GROUP_TXN_AGGREGATIONS, ccgb.GROUP_BY_TXN_FEATURES)
        #
        group_by_txn:DataFrame = (self.standard_group_by_txn(
            input_df=missing_treatment,
            aggregations=aggregations,
            txn_id_columns=ccgb.GROUP_BY_TXN_GROUPING_VARS,
            auxiliary_columns=ccgb.GROUP_BY_AUXILIARY_COLS
            )
        )
        #
        return group_by_txn
        #
    @ppf.cached_property
    def txn_src(self) -> DataFrame:
        """Proyección del lado ordenante; `numcliente` solo se conserva si `cve_tipo_orden` == "E"."""
        missing_treatment: DataFrame = self.missing_treatment
        return (self.standard_txn_src_split(
            input_df=missing_treatment,
            id_columns=ccgb.ID_SRC_COLUMNS,
            unique_columns=ccgb.SRC_COLUMNS,
            common_columns=ccgb.COMMON_COLUMNS,
            suffix="_src"
            )
            # custom imputation for ceps
            .withColumn("numcliente",  when((col("cve_tipo_orden") == "E"),  col("numcliente")).otherwise(lit(None).cast(StringType())))
        )
        #
    @ppf.cached_property
    def txn_dst(self) -> DataFrame:
        """Proyección del lado beneficiario; `numcliente` solo se conserva si `cve_tipo_orden` == "R"."""
        missing_treatment: DataFrame = self.missing_treatment
        return (self.standard_txn_dst_split(
            input_df=missing_treatment,
            id_columns=ccgb.ID_DST_COLUMNS,
            unique_columns=ccgb.DST_COLUMNS,
            common_columns=ccgb.COMMON_COLUMNS,
            suffix="_dst"
            )
            .withColumn("numcliente",  when((col("cve_tipo_orden") == "R"),  col("numcliente")).otherwise(lit(None).cast(StringType())))
        )
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="group_by_id")
    def group_by_id(self) -> DataFrame:
        """Agrega la unión de `txn_src` y `txn_dst` por `id` para formar los nodos.

        Materializa también `group_by_txn`, del que dependen las aristas.
        """
        txn_src: DataFrame = self.txn_src
        txn_dst: DataFrame = self.txn_dst
        _:DataFrame = self.group_by_txn
        group_by_id: DataFrame = (self.standard_group_by_id(
            input_src_df=txn_src,
            input_dst_df=txn_dst,
            aggregations=ccgb.GROUP_ID_AGGREGATIONS,
            id_columns="id",
            )
        )
        return group_by_id
