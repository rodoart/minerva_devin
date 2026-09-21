##########################################################################
# LIBRARIES
##########################################################################

# ----------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------
from calendar import c
from typing import Dict, Any, List

# ----------------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (col, to_date, when, lit, concat_ws,
    date_format
)
from pyspark.sql.types import StringType

from graphframes import GraphFrame
# ----------------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------------
import libs.framework as ppf
import config.graph_making.ceps.group_by as ccgb
import config.graph_making.ceps.edges_and_nodes as ccgen
import config.features.ceps.graph_features as cfgf


import pipelines.features.graph_features as p_f_cgf

from libs.data_engineering_toolbox.path import HivePath


from importlib import reload

for module in [ccgen, ccgb, p_f_cgf, cfgf]:
    reload(module)

##########################################################################
# CLASSES
##########################################################################

class CepsGraphFeaturesStep(p_f_cgf.StandardGraphFeaturesStep):
    def step_action(self) -> Dict[str, Any]:
        return ppf.run_substep_and_collect(self, self.ceps_edges_and_nodes_step, "ceps_edges_and_nodes_step")
    #
    @ppf.cached_property
    def ceps_graph_features_un_weighted_step(self) -> "CepsGraphFeaturesUnWeightedStep":
        """
        """
        return CepsGraphFeaturesUnWeightedStep(self, previous_step=self.previous_step[0])
    @ppf.cached_property
    def ceps_graph_features_weighted_step(self) -> "CepsGraphFeaturesWeightedStep":
        """
        """
        return CepsGraphFeaturesWeightedStep(self, previous_step=self.previous_step[0])


class CepsGraphFeaturesUnWeightedStep(p_f_cgf.StandardGraphFeaturesUnWeightedStep):
    def step_action(self) -> Dict[str, Any]:
        return ppf.collect_step_output(self, self.run_un_weighted_features, "run_un_weighted_features")
    #
    @ppf.cached_property
    def edges(self) -> DataFrame:
        return self.standard_load_parquet_or_table("edges")
    #
    @ppf.cached_property
    def nodes(self) -> DataFrame:
        return self.standard_load_parquet_or_table("nodes")
    #
    @ppf.cached_property
    def run_un_weighted_features(self) -> List[Dict[str,Any]]:
        """
        Run all the selected features and return a dictionary with the results.
        """
        return self.run_selected_features(  # <- método base (no la property)
            all_prefixes=cfgf.GRAPH_ALL_FEATURE_PREFIXES,
            features_to_run=cfgf.GRAPH_CENTRALITY_FEATURES,
            output_config_dict=cfgf.output,
            current_prefix="",
            column_renames=cfgf.GRAPH_RENAMES,
            edge_final_columns=["src", "dst"],
        )


class CepsGraphFeaturesWeightedStep(p_f_cgf.StandardGraphFeaturesWeightedStep):
    def step_action(self) -> Dict[str, Any]:
        return ppf.collect_step_output(self, self.run_weighted_features, "run_weighted_features")
    #
    @ppf.cached_property
    def edges(self) -> DataFrame:
        return self.standard_load_parquet_or_table("edges")
    #
    @ppf.cached_property
    def nodes(self) -> DataFrame:
        return self.standard_load_parquet_or_table("nodes")
    #
    @ppf.cached_property
    def run_weighted_features(self) -> List[Dict[str,Any]]:
        """
        Run all the selected features and return a dictionary with the results.
        """
        return self.run_selected_features(
            all_prefixes=cfgf.GRAPH_ALL_FEATURE_PREFIXES,
            features_to_run=cfgf.GRAPH_CENTRALITY_FEATURES,
            output_config_dict=cfgf.output,
            current_prefix="weighted",
            column_renames=cfgf.GRAPH_RENAMES,
            edge_final_columns=["src", "dst", "weight"]
        )
        #
