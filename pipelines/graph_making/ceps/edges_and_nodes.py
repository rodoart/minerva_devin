
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

# ------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------
import libs.framework as ppf
import config.graph_making.ceps.group_by as ccgb
import config.graph_making.ceps.edges_and_nodes as ccgen


import pipelines.graph_making.edges_and_nodes as p_gm_en

##########################################################################
# CLASSES
##########################################################################


class CepsEdgesAndNodesStep(p_gm_en.StandardEdgesAndNodesStep):
    """Step orquestador de la construcción de aristas y nodos del grafo CEPS."""
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta el substep de aristas y nodos (`ceps_edges_and_nodes_step`)."""
        return ppf.run_substep_and_collect(self, self.ceps_edges_and_nodes_step, "ceps_edges_and_nodes_step")
        #
    @ppf.cached_property
    def ceps_edges_and_nodes_step(self) -> "CepsEdgesAndNodesSubStep":
        """Substep de aristas y nodos CEPS (lazy, cacheado) con dependencia del step previo."""
        return CepsEdgesAndNodesSubStep(self, previous_step=self.previous_step[0])


class CepsEdgesAndNodesSubStep(p_gm_en.StandardEdgesAndNodesSubStep):
    """Substep que materializa las aristas (`edges`) y los nodos (`nodes`) del grafo CEPS."""
    def step_action(self) -> Dict[str, Any]:
        """Materializa y colecta `edges` y `nodes` bajo la clave `edges_and_nodes_id`."""
        return ppf.collect_step_output(
            self, {"edges": self.edges, "nodes": self.nodes}, "edges_and_nodes_id")
        #
    @ppf.cached_property
    def group_by_id(self) -> DataFrame:
        """Recarga la salida `group_by_id` del step de group-by."""
        return self.get_previous_step("group_by_id")
        #
    @ppf.cached_property
    def group_by_txn(self) -> DataFrame:
        """Recarga la salida `group_by_txn` del step de group-by."""
        return self.get_previous_step("group_by_txn")
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="edges")
    def edges(self) -> DataFrame:
        """Aristas del grafo: variables de `EDGES_VARS` + mapa de pesos sobre `group_by_txn`."""
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
        """Nodos del grafo: variables de `NODE_VARS` sobre `group_by_id` (materializa `edges` primero)."""
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
