
###############################################################################
# RISK FEATURES
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Dict, Any, List

# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame
from graphframes import GraphFrame

from pyspark.sql.functions import col, lit, min as spark_min

# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath

import libs.functions.pair_features as lpf

import config.features.ceps.graph_features as cfgf
import config.features.ceps.risk_features as crf

from pipelines.features.ceps.cluster_features import (
    StandardClusterFeaturesStep, StandardClusterFeaturesSubStep)

import libs.framework as ppf

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)


###############################################################################
# Ceps
###############################################################################


class CepsRiskFeaturesStep(StandardClusterFeaturesStep):
    """Step CEPS de features de riesgo de nodo y de par (A, X).

    Acepta `variant` ("rfc" por defecto): los insumos (aristas, nodos con
    target, scores de contagio) se derivan de la config namespaced de la
    variante, de modo que cada grafo produce sus propias salidas.
    """
    STEP_NAME_PREFIX = "Standard_Risk_Features_Step"

    def __init__(self, *args, variant: str = "rfc", **kwargs) -> None:
        self.variant = variant
        super().__init__(*args, **kwargs)
        if variant != "rfc":
            self.step_name = f"{self.step_name}_{variant}"
        #
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta la sub-etapa CEPS de risk features y recoge su salida."""
        return ppf.run_substep_and_collect(
            self, self.ceps_risk_features_substep, "ceps_risk_features_substep")
        #
    @ppf.cached_property
    def ceps_risk_features_substep(self) -> "CepsRiskFeaturesSubStep":
        """Sub-etapa CEPS de risk features de nodo y de par."""
        return CepsRiskFeaturesSubStep(self, previous_step=self.previous_step[0])


class CepsRiskFeaturesSubStep(StandardClusterFeaturesSubStep):
    """Calcula las features risk_* por nodo y pair_* por par candidato.

    Semillas (cada una produce su propia familia de columnas con sufijo):
    - duras: columnas `RISK_HARD_SEED_COLUMNS` de nodes_join_target_lovelace
      (el target original, "antes" de la propagación);
    - post-propagación: `contagion_<w>` para `RISK_CONTAGION_WEIGHTS`
      ("después" de la propagación).
    """
    def step_action(self) -> Dict[str, Any]:
        """Materializa las tres salidas y las recoge en output_parameters."""
        return ppf.collect_step_output(self, {
            "risk_node_features": self.risk_node_features_output,
            "pair_features": self.pair_features_output,
            "pair_features_node": self.pair_features_node_output,
        }, "risk_features")
        #
    # ------------------------------------------------------------------
    # Insumos
    # ------------------------------------------------------------------
    @ppf.cached_property
    def nodes_join_target(self) -> DataFrame:
        """Nodos + etiquetas target_lovelace (tabla particionada)."""
        return self.standard_load_parquet_or_table("nodes_join_target_lovelace")
        #
    @ppf.cached_property
    def edges(self) -> DataFrame:
        """Aristas del grafo (tabla particionada), con renombres del grafo."""
        edges = self.standard_load_parquet_or_table("edges")
        for old_name, new_name in cfgf.GRAPH_RENAMES.items():
            if old_name in edges.columns:
                edges = edges.withColumnRenamed(old_name, new_name)
        return edges
        #
    @ppf.cached_property
    def contagion_df(self) -> DataFrame:
        """Scores de contagio por nodo (merge_schema de target_propagation)."""
        return self.feature_dataframe(
            self.sqlContext,
            self.input_hive["target_propagation"]["table_or_hdfs"])
        #
    @ppf.cached_property
    def graph(self) -> GraphFrame:
        """GraphFrame (nodos de nodes_join_target, aristas src/dst)."""
        self.define_checkpoint(
            checkpoint_hdfs=self.output_hive["checkpoint"]["table_or_hdfs"])
        return GraphFrame(
            self.nodes_join_target.select("id"),
            self.edges.select("src", "dst"))
        #
    # ------------------------------------------------------------------
    # Semillas
    # ------------------------------------------------------------------
    @ppf.cached_property
    def seed_frames(self) -> Dict[str, DataFrame]:
        """{sufijo: DataFrame(id, seed_score)} para todas las semillas."""
        seeds = {}
        nodes = self.nodes_join_target
        for column in crf.RISK_HARD_SEED_COLUMNS:
            if column not in nodes.columns:
                logger.warning(
                    "Semilla dura %r ausente en nodes_join_target; se omite",
                    column)
                continue
            seeds[column] = (nodes
                .select("id", col(column).cast("double").alias("seed_score"))
                .fillna(0.0, subset=["seed_score"]))
        if crf.RISK_CONTAGION_WEIGHTS:
            contagion = self.contagion_df
            for weight in crf.RISK_CONTAGION_WEIGHTS:
                candidates = sorted(c for c in contagion.columns
                    if c.startswith(f"contagion_{weight}"))
                if not candidates:
                    logger.warning(
                        "Sin columna contagion_%s* en target_propagation; "
                        "se omite la semilla", weight)
                    continue
                if len(candidates) > 1:
                    logger.warning(
                        "Varias columnas contagion_%s* %s; se usa %s",
                        weight, candidates, candidates[0])
                seeds[f"contagion_{weight}"] = (contagion
                    .select("id", col(candidates[0]).cast("double")
                        .alias("seed_score"))
                    .fillna(0.0, subset=["seed_score"]))
        return seeds
        #
    def _seed_ids(self, suffix: str, seed_df: DataFrame) -> List[str]:
        """Ids semilla: score por encima del umbral de su tipo de semilla."""
        threshold = crf.RISK_CONTAGION_SEED_THRESHOLD \
            if suffix.startswith("contagion_") else crf.RISK_HARD_SEED_THRESHOLD
        return [row["id"] for row in seed_df
            .filter(col("seed_score") > threshold)
            .select("id").distinct().collect()]
        #
    # ------------------------------------------------------------------
    # Features de nodo (risk_*)
    # ------------------------------------------------------------------
    @ppf.cached_property
    def risk_node_features(self) -> DataFrame:
        """Una fila por id con todas las familias risk_*_<semilla> unidas."""
        result = self.nodes_join_target.select("id")
        for suffix, seed_df in self.seed_frames.items():
            seed_ids = self._seed_ids(suffix, seed_df)
            logger.info("Semilla %s: %d ids sobre umbral", suffix, len(seed_ids))
            frames = [
                lpf.seed_neighbor_stats(self.edges, seed_df, suffix),
                lpf.seed_two_hop_counts(self.edges, seed_ids, suffix),
                lpf.seed_flow_ratio(self.edges, seed_ids, suffix,
                    weight_column=crf.RISK_FLOW_WEIGHT_COLUMN),
                lpf.seed_personalized_pagerank(self.graph, seed_ids, suffix,
                    max_iter=crf.PPR_MAX_ITER,
                    reset_prob=crf.PPR_RESET_PROB,
                    max_seeds=crf.PPR_MAX_SEEDS),
            ]
            for direction in crf.RISK_DISTANCE_DIRECTIONS:
                frames.append(lpf.seed_distance(self.graph, seed_ids,
                    f"{direction}_{suffix}", direction=direction))
            for frame in frames:
                result = result.join(frame, on="id", how="left")
        return result
        #
    # ------------------------------------------------------------------
    # Features de par (pair_*)
    # ------------------------------------------------------------------
    @ppf.cached_property
    def pair_candidates(self) -> DataFrame:
        """Pares candidatos (a, x, hops): a semilla, x a <= PAIR_MAX_HOPS."""
        pairs = None
        for suffix in crf.PAIR_CANDIDATE_SEED_SUFFIXES:
            if suffix not in self.seed_frames:
                logger.warning(
                    "Semilla de pares %r no disponible; se omite", suffix)
                continue
            seed_ids = self._seed_ids(suffix, self.seed_frames[suffix])
            found = lpf.candidate_pairs_within_hops(
                self.edges, seed_ids, max_hops=crf.PAIR_MAX_HOPS)
            pairs = found if pairs is None else pairs.unionByName(found)
        if pairs is None:
            logger.warning("Sin semillas de pares: pair_features vacío")
            return (self.edges.limit(0)
                .select(col("src").alias("a"), col("dst").alias("x"))
                .withColumn("hops", lit(None).cast("int")))
        return pairs.groupBy("a", "x").agg(spark_min("hops").alias("hops"))
        #
    @ppf.cached_property
    def pair_features(self) -> DataFrame:
        """Proximidades topológicas + distancia mínima de cada par (a, x)."""
        pairs = self.pair_candidates
        features = lpf.pair_proximity_features(self.edges, pairs)
        seed_ids = pairs.select("a").distinct()
        distances = lpf.pair_min_distance(self.graph, pairs,
            [row["a"] for row in seed_ids.collect()])
        return features.join(distances, on=["a", "x"], how="left")
        #
    @ppf.cached_property
    def pair_features_node(self) -> DataFrame:
        """Features de par agregadas a nivel nodo X: pair_<f>_<stat> por id."""
        return lpf.aggregate_pair_features_to_node(
            self.pair_features, node_role="x",
            stats=crf.PAIR_NODE_AGGREGATE_STATS)
        #
    # ------------------------------------------------------------------
    # Salidas persistidas
    # ------------------------------------------------------------------
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="risk_node_features")
    def risk_node_features_output(self) -> DataFrame:
        """Salida `risk_node_features` (parquet plano, una fila por `id`)."""
        return self.risk_node_features
        #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="pair_features")
    def pair_features_output(self) -> DataFrame:
        """Salida `pair_features` (parquet plano, una fila por `(a, x)`)."""
        return self.pair_features
        #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="pair_features_node")
    def pair_features_node_output(self) -> DataFrame:
        """Salida `pair_features_node` (parquet plano, una fila por `id`)."""
        return self.pair_features_node
