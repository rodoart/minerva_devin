
###############################################################################
# GRAPH FEATURES
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from tabnanny import check
from typing import List, Union, Dict, Any,  Callable, Optional
import re
import unicodedata


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import Column, DataFrame

from pyspark.sql.functions import (coalesce, col, to_date,
    unix_timestamp, lit,  floor,  months_between, to_timestamp, mean as spark_mean,
    min as spark_min, max as spark_max, sum as spark_sum, count as spark_count,
    countDistinct, stddev as spark_std
)

from graphframes import GraphFrame
# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from data_engineering_toolbox.path import HivePath
from libs.framework.utils import sanitize_name

import libs.framework as ppf
import config.job as cj

from importlib import reload
reload(cj)
###############################################################################
# FUNCTIONS
###############################################################################
###############################################################################
# Process
###############################################################################




class SubStep(ppf.Step):
    """Clase base para las sub-etapas del pipeline `CepsRfcNomRankingStep`, ..."""
    def __init__(self, parent: "StandardGraphFeaturesStep", **kwargs) -> None:
        super().__init__(parent, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)
        #
    def get_graph(
        self,
        edges_df:DataFrame,
        nodes_df:DataFrame,
        checkpoint_hdfs:HivePath,
        weight: Optional[str] = None,
        column_renames: Optional[Dict[str,str]] = None,
        edge_final_columns: List[str] = ["src", "dst", "weight"],
        node_final_columns: List[str] = ["id"],
        *args, **kwargs
    ) -> GraphFrame:
        def make_graph(*args, **kwargs) -> GraphFrame:
            edges_df:DataFrame = kwargs.get("edges_df")
            nodes_df:DataFrame = kwargs.get("nodes_df")
            #
            if column_renames is not None:
                for old_name, new_name in column_renames.items():
                    if old_name in edges_df.columns:
                        edges_df = edges_df.withColumnRenamed(old_name, new_name)
                    if old_name in nodes_df.columns:
                        nodes_df = nodes_df.withColumnRenamed(old_name, new_name)
            #
            if weight is not None and "weights" in edges_df.columns:
                edges_df = edges_df.withColumn("weight", col("weights").getItem(weight))
            #
            self.define_checkpoint(checkpoint_hdfs=checkpoint_hdfs)
            return GraphFrame(
                nodes_df.select(*node_final_columns),
                edges_df.select(*edge_final_columns)
            )
        #
        return self.get_cached_decorated_table_or_parquet_property(
            method=make_graph,
            property_name=f"graph_{sanitize_name(value=weight) or 'default'}",
            edges_df=edges_df,
            nodes_df=nodes_df
        )
        #
    def feature(self, feature_name: str, feature_parent_hdfs: HivePath, **kwargs) -> DataFrame:
        """Devuelve (y cachea) el resultado de `feature_name` con estos kwargs, ..."""
        # kwargs de infraestructura + kwargs NO serializables (no van al path/nombre)
        not_naming_kwargs = ["current_feature", "feature_name", "feature_parent_hdfs",
                             "checkpoint_hdfs", "column_renames", "edge_final_columns",
                             "aggregations"]   # <- objetos Column: NO al path
        # Solo escalares simples definen identidad/path.
        naming_items = [
            (sanitize_name(k), sanitize_name(v)) for k, v in kwargs.items()
            if k not in not_naming_kwargs and isinstance(v, (str, int, float, bool))
        ]
        subdir = "/".join(f"{k}={v}" for k, v in naming_items)
        feature_path = feature_parent_hdfs.joinpath(subdir)
        # checkpoint_hdfs = kwargs.pop("checkpoint_hdfs")
        # column_renames = kwargs.pop("column_renames")
        # edge_final_columns = kwargs.pop("edge_final_columns")
        #
        name_suffix = "_".join(f"{k}_{v}" for k, v in naming_items)
        name = f"feature_{feature_name}" + (f"_{name_suffix}" if name_suffix else "")
        #
        def make_graph_and_feature(*args, **kwargs) -> DataFrame:
            graph = self.get_graph(edges_df=self.edges, nodes_df=self.nodes,**kwargs)
            feature_method = getattr(self, f"{feature_name}_ft")
            return feature_method(graph, **kwargs)
        #
        print(f"Defining feature property: {name} at {feature_path}")
        return self.get_cached_decorated_table_or_parquet_property(
            method=make_graph_and_feature,
            property_name=name,
            path=feature_path,
            **kwargs
        )
        #
    def define_checkpoint(self, checkpoint_hdfs:HivePath) -> None:
        self.sqlContext.sparkContext.setCheckpointDir("hdfs://"+str(checkpoint_hdfs))
        #
    def run_selected_features(self,
        all_prefixes:List[str],
        features_to_run:List[Dict[str,Any]],
        output_config_dict:Dict[str, Dict[str,Union[str,HivePath]]],
        column_renames:Dict[str,str],
        edge_final_columns:List[str],
        current_prefix:Optional[str] = None,
    ) -> List[Dict[str,Any]]:
        """
        Run all the selected features and return a dictionary with the results.
        """
        features_to_run_list = [list(f.keys())[0] for f in features_to_run]
        if current_prefix is None:
            current_prefix = ""
        #
        if current_prefix == "all":
            current_features_to_run = list(set(features_to_run_list))
        elif current_prefix == "":
            current_features_to_run = list(set([f for f in features_to_run_list if not any(f.startswith(prefix) for prefix in all_prefixes)]))
        else:
            #
            current_features_to_run = list(set([f for f in features_to_run_list if f.startswith(current_prefix)]))
        result: List[Dict[str, Any]] = []
        for one_feature_config_dict in features_to_run:
            feature_name = list(one_feature_config_dict.keys())[0]
            feature_kwargs = one_feature_config_dict[feature_name]
            #
            # Solo procesar las features seleccionadas por el prefijo actual.
            if feature_name not in current_features_to_run:
                continue
            #
            feature_parent_hdfs = HivePath(str(output_config_dict[feature_name]["table_or_hdfs"]))
            checkpoint_hdfs = str(output_config_dict["checkpoint"]["table_or_hdfs"])
            #
            common_kwargs = dict(
                feature_name=feature_name,
                feature_parent_hdfs=feature_parent_hdfs,
                checkpoint_hdfs=checkpoint_hdfs,
                column_renames=column_renames,
                edge_final_columns=edge_final_columns,
            )
            if feature_kwargs is None:
                last_feature = self.feature(**common_kwargs)
            else:
                #
                last_feature = self.feature(**common_kwargs, **feature_kwargs)
            #
            # Copia superficial para NO mutar el config global (cfgf.GRAPH_CENTRALITY_FEATURES).
            result_entry = {feature_name: dict(feature_kwargs or {})}
            result_entry[feature_name]["df"] = last_feature
            result.append(result_entry)
        #
        return result




class StandardGraphFeaturesStep(ppf.Step):
    """
    """
    def __init__(self,
        date_treatment: Dict[str,str],
        input_hive:Dict[str, HivePath],
        output_hive:Dict[str,HivePath],
        cohort:str,
        is_dynamic: bool = True,
        **kwargs
    ) -> None:
        self.date_treatment = date_treatment
        self.is_dynamic = is_dynamic
        self.cohort = cohort
        super_class_kwargs = ppf.build_step_init_kwargs(
            date_treatment=date_treatment,
            input_hive=input_hive,
            output_hive=output_hive,
            step_name_prefix="Graph_Features_Step",
            extra_kwargs=kwargs,
        )
        super().__init__(**super_class_kwargs)
        #
    def standard_load_parquet_or_table(self, config_dict_key, input_or_output:str = "input") -> DataFrame:
        """Carga estándar de una tabla/parquet particionado con validación de historial. ..."""
        config_dict = self.input_hive if input_or_output == "input" else self.output_hive
        return ppf.standard_load_parquet_or_table(
            config_dict=config_dict,
            config_dict_key=config_dict_key,
            current_date=self.input_parameters['vintage_date'],
            session=self.sqlContext,
        )
        #
    # For testting purposes only
    @ppf.cached_property
    def standard_graph_features_un_weighted_step(self) -> "StandardGraphFeaturesUnWeightedStep":
        """
        """
        return StandardGraphFeaturesUnWeightedStep(self)


class StandardGraphFeaturesUnWeightedStep(SubStep):
    #
    @staticmethod
    def pagerank_ft(
        graph:GraphFrame,
        **kwargs
    ) -> DataFrame:
        max_iter = kwargs.get("max_iter", 10)
        reset_prob = kwargs.get("reset_prob", 0.15)
        return graph.pageRank(maxIter=max_iter, resetProbability=reset_prob).vertices
    #
    @staticmethod
    def degrees_ft(
        graph:GraphFrame,
        **kwargs
    ) -> DataFrame:
        # in degrees
        in_degrees_df = graph.inDegrees.withColumnRenamed("inDegree", "in_degree")
        out_degrees_df = graph.outDegrees.withColumnRenamed("outDegree", "out_degree")
        degrees_df = (
            in_degrees_df.join(out_degrees_df, on="id", how="outer")
            .withColumn("in_degree", coalesce(col("in_degree"), lit(0)))
            .withColumn("out_degree", coalesce(col("out_degree"), lit(0)))
            .withColumn("total_degree", col("in_degree") + col("out_degree"))
        )
        return degrees_df
    #
    @staticmethod
    def components_ft(
        graph:GraphFrame,
        **kwargs
    ) -> DataFrame:
        return graph.connectedComponents().withColumnRenamed("component", "component_id")
    #
    @staticmethod
    def triangle_count_ft(
        graph:GraphFrame,
        **kwargs
    ) -> DataFrame:
        return graph.triangleCount().withColumnRenamed("count", "triangle_count")
    #
    # def k_core_ft(self, graph:GraphFrame, k:int) -> DataFrame:
    #     return graph.kCore(k).withColumnRenamed("core", f"k_core_{k}")


class StandardGraphFeaturesWeightedStep(SubStep):
    """Sub-etapa de features de grafo **ponderadas** (weighted). ..."""
    @staticmethod
    def weighted_pagerank_ft(
        graph: GraphFrame,
        max_iter: int = 10,
        reset_prob: float = 0.15,
        **kwargs
    ) -> DataFrame:
        """PageRank ponderado por el peso de las aristas. ..."""
        return (
            graph
            .pageRank(maxIter=max_iter, resetProbability=reset_prob)
            .vertices
            .withColumnRenamed("pagerank", "weighted_pagerank")
        )
    #
    @staticmethod
    def weighted_degrees_ft(
        graph: GraphFrame,
        **kwargs
    ) -> DataFrame:
        """Fuerza (strength) ponderada: suma de pesos entrantes/salientes por nodo. ..."""
        edges = graph.edges
        #
        in_strength = (
            edges.groupBy(col("dst").alias("id"))
            .agg(spark_sum("weight").alias("in_strength"))
        )
        out_strength = (
            edges.groupBy(col("src").alias("id"))
            .agg(spark_sum("weight").alias("out_strength"))
        )
        return (
            in_strength.join(out_strength, on="id", how="outer")
            .withColumn("in_strength", coalesce(col("in_strength"), lit(0.0)))
            .withColumn("out_strength", coalesce(col("out_strength"), lit(0.0)))
            .withColumn("total_strength", col("in_strength") + col("out_strength"))
        )
    #
    @staticmethod
    def weighted_edge_stats_ft(
        graph: GraphFrame,
        aggregations:Optional[List[Column]] = None,
        **kwargs
    ) -> DataFrame:
        """Estadísticas de peso de aristas incidentes por nodo (in + out). ..."""
        if aggregations is None:
            standard_features = {
                "min": spark_min,
                "max": spark_max,
                "mean": lambda c: spark_mean(col(c)),
                "std": lambda c: coalesce(spark_std(col(c)), lit(0.0)),
                "count": spark_count,
                "sum": spark_sum,
            }
            aggregations = [func(variable).alias(f"{func_name}_{variable}") for variable in ["weight"] for func_name, func in standard_features.items()]
        #
        edges = graph.edges
        incident = (
            edges.select(col("src").alias("id"), col("weight"))
            .union(edges.select(col("dst").alias("id"), col("weight")))
        )
        return (
            incident.groupBy("id")
            .agg(
                *aggregations
            )
        )
    #
    @staticmethod
    def weighted_components_ft(
        graph: GraphFrame,
        **kwargs
    ) -> DataFrame:
        """Componentes conexas enriquecidas con el peso total de cada componente. ..."""
        components = (
            graph.connectedComponents()
        )
        #
        # Peso total por componente: unir cada arista a la componente de su src.
        src_comp = components.select(
            col("id").alias("src"),
            col("component"),
        )
        comp_weight = (
            graph.edges.join(src_comp, on="src", how="inner")
            .groupBy("component")
            .agg(spark_sum("weight").alias("component_weight"))
        )
        return components.join(comp_weight, on="component", how="left")
    #
    @staticmethod
    def weighted_triangle_count_ft(
        graph: GraphFrame,
        *args,
        **kwargs
    ) -> DataFrame:
        """Conteo de triángulos por nodo (estructural, no ponderado) + fuerza. ..."""
        triangles = (
            graph.triangleCount()
            .withColumnRenamed("count", "triangle_count")
        )
        strength = StandardGraphFeaturesWeightedStep.weighted_degrees_ft(graph).select("id", "total_strength")
        return triangles.join(strength, on="id", how="left")
        #
