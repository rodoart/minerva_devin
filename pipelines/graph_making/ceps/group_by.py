
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

from pyspark.sql.functions import (col, to_date, when, lit, concat_ws,
    date_format
)
from pyspark.sql.types import  StringType
# ------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------
import libs.framework as ppf
import config.graph_making.ceps.group_by as ccgb
import pipelines.graph_making.group_by as p_gm_gb

from importlib import reload

reload(ccgb)
reload(p_gm_gb)

##########################################################################
# CLASSES
##########################################################################


class CepsGroupByStep(p_gm_gb.StandardGroupByStep):
    def step_action(self) -> Dict[str, Any]:
        return ppf.run_substep_and_collect(self, self.ceps_group_by_step, "ceps_group_by_step")
        #
    @ppf.cached_property
    def ceps_group_by_step(self) -> "CepsGroupBySubStep":
        """
        """
        return CepsGroupBySubStep(self, previous_step=self.previous_step[0])


class CepsGroupBySubStep(p_gm_gb.StandardGroupBySubStep):
    def step_action(self) -> Dict[str, Any]:
        return ppf.collect_step_output(self, self.group_by_id, "group_by_id")
        #
    @ppf.cached_property
    def missing_treatment(self) -> DataFrame:
        return self.get_previous_step("missing_treatment")
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="group_by_txn")
    def group_by_txn(self) -> DataFrame:
        missing_treatment: DataFrame = self.missing_treatment
        group_by_txn:DataFrame = (self.standard_group_by_txn(
            input_df=missing_treatment,
            aggregations=ccgb.GROUP_TXN_AGGREGATIONS,
            txn_id_columns=ccgb.GROUP_BY_TXN_GROUPING_VARS,
            auxiliary_columns=ccgb.GROUP_BY_AUXILIARY_COLS
            )
        )
        #
        return group_by_txn
        #
    @ppf.cached_property
    def txn_src(self) -> DataFrame:
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
