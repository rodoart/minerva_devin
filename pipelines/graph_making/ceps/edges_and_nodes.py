
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
import config.graph_making.ceps.edges_and_nodes as ccgen


import pipelines.graph_making.edges_and_nodes as p_gm_en


from importlib import reload

for module in [ccgen, ccgb, p_gm_en]:
    reload(module)

##########################################################################
# CLASSES
##########################################################################


class CepsEdgesAndNodesStep(p_gm_en.StandardEdgesAndNodesStep):
    def step_action(self) -> Dict[str, Any]:
        return ppf.run_substep_and_collect(self, self.ceps_edges_and_nodes_step, "ceps_edges_and_nodes_step")
        #
    @ppf.cached_property
    def ceps_edges_and_nodes_step(self) -> "CepsEdgesAndNodesSubStep":
        """
        """
        return CepsEdgesAndNodesSubStep(self, previous_step=self.previous_step[0])


class CepsEdgesAndNodesSubStep(p_gm_en.StandardEdgesAndNodesSubStep):
    def step_action(self) -> Dict[str, Any]:
        return ppf.collect_step_output(self, self.edges_and_nodes_id, "edges_and_nodes_id")
        #
    @ppf.cached_property
    def group_by_id(self) -> DataFrame:
        return self.get_previous_step("group_by_id")
        #
    @ppf.cached_property
    def group_by_txn(self) -> DataFrame:
        return self.get_previous_step("group_by_txn")
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="edges")
    def edges(self) -> DataFrame:
        group_by_txn: DataFrame = self.group_by_txn
        edges: DataFrame = (self.standard_edges(
            input_df=group_by_txn,
            edges_vars=ccgen.EDGES_VARS,
            weight_columns=ccgen.WEIGHT_COLUMNS,
            txn_id_columns=ccgb.GROUP_BY_TXN_GROUPING_VARS
            )
        )
        #
        return edges
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="nodes")
    def nodes(self) -> DataFrame:
        _:DataFrame = self.edges
        group_by_id: DataFrame = self.group_by_id
        nodes: DataFrame = (self.standard_nodes(
            input_df=group_by_id,
            node_vars=ccgen.NODE_VARS,
            node_id_column=ccgen.NODES_ID_COLUMNS
            )
        )
        #
        return nodes
