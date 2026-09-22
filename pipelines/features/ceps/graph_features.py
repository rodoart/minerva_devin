##########################################################################
# LIBRARIES
##########################################################################

# ----------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------
from typing import Dict, Any, List

# ----------------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------------
from pyspark.sql import DataFrame

# ----------------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------------
import libs.framework as ppf
import config.features.ceps.graph_features as cfgf

import pipelines.features.graph_features as p_f_cgf

##########################################################################
# CLASSES
##########################################################################

class CepsGraphFeaturesStep(p_f_cgf.StandardGraphFeaturesStep):
    """Step CEPS de features de grafo: ejecuta las variantes unweighted y weighted.
    """
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta las sub-etapas unweighted y weighted y recoge sus salidas."""
        ppf.run_substep_and_collect(self, self.ceps_graph_features_un_weighted_step, "ceps_graph_features_un_weighted_step")
        return ppf.run_substep_and_collect(self, self.ceps_graph_features_weighted_step, "ceps_graph_features_weighted_step")
    #
    @ppf.cached_property
    def ceps_graph_features_un_weighted_step(self) -> "CepsGraphFeaturesUnWeightedStep":
        """Sub-etapa de features de grafo no ponderadas.
        """
        return CepsGraphFeaturesUnWeightedStep(self, previous_step=self.previous_step[0])
    @ppf.cached_property
    def ceps_graph_features_weighted_step(self) -> "CepsGraphFeaturesWeightedStep":
        """Sub-etapa de features de grafo ponderadas.
        """
        return CepsGraphFeaturesWeightedStep(self, previous_step=self.previous_step[0])


class CepsGraphFeaturesUnWeightedStep(p_f_cgf.StandardGraphFeaturesUnWeightedStep):
    """Sub-etapa CEPS: features de centralidad sin peso sobre el grafo de transferencias.
    """
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta las features no ponderadas seleccionadas y recoge sus salidas."""
        return ppf.collect_step_output(self, self.run_un_weighted_features, "run_un_weighted_features")
    #
    @ppf.cached_property
    def edges(self) -> DataFrame:
        """Aristas del grafo (tabla `edges` particionada)."""
        return self.standard_load_parquet_or_table("edges")
    #
    @ppf.cached_property
    def nodes(self) -> DataFrame:
        """Nodos del grafo (tabla `nodes` particionada)."""
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
    """Sub-etapa CEPS: features ponderadas por el peso de las aristas.
    """
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta las features ponderadas seleccionadas y recoge sus salidas."""
        return ppf.collect_step_output(self, self.run_weighted_features, "run_weighted_features")
    #
    @ppf.cached_property
    def edges(self) -> DataFrame:
        """Aristas del grafo (tabla `edges` particionada)."""
        return self.standard_load_parquet_or_table("edges")
    #
    @ppf.cached_property
    def nodes(self) -> DataFrame:
        """Nodos del grafo (tabla `nodes` particionada)."""
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
