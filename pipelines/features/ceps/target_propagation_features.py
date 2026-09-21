
###############################################################################
# SPECIAL TREATMENT
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Callable, List, Dict, Any, Optional, Union, Dict


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame, Column
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM


from pyspark.sql.functions import (col,to_date, lit, date_format,
    max as spark_max, create_map, explode, sum as spark_sum, coalesce, when, greatest)
from pyspark.sql.types import StringType
# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from data_engineering_toolbox.path import HivePath
from libs.framework.utils import sanitize_name

import pipelines.features.graph_features as p_f_fg


import libs.framework as ppf
import config.job as cj

from importlib import reload
reload(cj)
###############################################################################
# Process
###############################################################################


class SubStep(ppf.Step):
    """
    """
    def __init__(self, parent: "StandardTargetPropagationFeaturesStep", *args, **kwargs) -> None:
        graph_features_substep = p_f_fg.SubStep(parent, *args, **kwargs)
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)
        self.get_graph: Callable[...,GraphFrame] = graph_features_substep.get_graph
        self.feature: Callable[...,DataFrame] = graph_features_substep.feature
        self.define_checkpoint: Callable[...,None] = graph_features_substep.define_checkpoint
        #
        #




class StandardTargetPropagationFeaturesStep(ppf.Step):
    """
    """
    def __init__(self,
        date_treatment: Dict[str,str],
        input_hive:Dict[str, HivePath],
        output_hive:Dict[str,HivePath],
        cohort:str,
        is_dynamic: bool = True,
        *args, **kwargs
    ) -> None:
        self.date_treatment = date_treatment
        self.is_dynamic = is_dynamic
        self.cohort = cohort
        super_class_kwargs = ppf.build_step_init_kwargs(
            date_treatment=date_treatment,
            input_hive=input_hive,
            output_hive=output_hive,
            step_name_prefix="Standard_Target_Propagation_Features_Step",
            extra_kwargs=kwargs,
        )
        super().__init__(*args, **super_class_kwargs)
        #
    def standard_load_parquet_or_table(self, config_dict_key, input_or_output:str = "input") -> DataFrame:
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
    def standard_groupby_step(self) -> "StandardTargetPropagationFeaturesSubStep":
        return StandardJoinGraphSubStep(self)


class StandardJoinGraphSubStep(SubStep):
    # previous_step
    def get_input(self,
        config_dict_key:str,
        input_or_output:str = "input",
        *args, **kwargs) -> DataFrame:
        property_path = (self.input_hive[config_dict_key]["table_or_hdfs"]
            if input_or_output == "input"
            else self.output_hive[config_dict_key]["table_or_hdfs"])
        property_path_str = str(property_path)   # <- congela el str AQUI
        #
        def simple_load(*args, **kwargs) -> DataFrame:
            return self.sqlContext.read.parquet(property_path_str)   # <- siempre str
        #
        return self.get_cached_decorated_table_or_parquet_property(
            path=property_path,
            input_or_output=input_or_output,      # <- respeta el argumento, no hardcode "input"
            method=simple_load,
        )
        #
    def get_multiple_inputs(
        self,
        keys:List[str]
    ) -> Dict[str, DataFrame]:
        return {key: self.get_input(key) for key in keys}
    #
    @staticmethod
    def standard_target_group_by_id(
        group_by_id:DataFrame,
        target:DataFrame,
        column:str,
        function:Callable[..., Column]=spark_max
    ) -> DataFrame:
        target_columns = (target
            .groupBy(column)
            .agg(function(col("target")).alias("target"))
        )
        return (group_by_id
            .withColumn(column, explode(column))
            .join(other=target_columns, on=column, how="left")
            .groupBy("id")
            .agg(function("target").alias(f"target_agg_{column}"))
        )
    #
    @staticmethod
    def standard_join_target(
        nodes:DataFrame,
        target_group_by_id:DataFrame,
        new_column_suffix:str
    ) -> DataFrame:
        return (nodes
            .join(other=target_group_by_id, on="id", how="left")
            .withColumnRenamed("target", f"target_{new_column_suffix}")
        )


class StandardTargetPropagationSubStep(SubStep, p_f_fg.SubStep):
    def __init__(self, parent: "StandardTargetPropagationFeaturesStep", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)
        #
    @property
    def standard_join_graph_substep(self) -> StandardJoinGraphSubStep:
        return StandardJoinGraphSubStep(self)
    #
    @staticmethod
    def standard_get_degree(
        graph:GraphFrame, # with  weight

    ) -> DataFrame:
        #
        raw_edges:DataFrame = graph.edges
        #
        deg = (
            raw_edges.select(col("src").alias("node"), "weight")
            .union(raw_edges.select(col("dst").alias("node"), "weight"))
            .groupBy("node")
            .agg(spark_sum("weight").alias("deg_sum"))
        )
        return deg
    #
    @staticmethod
    def standard_weight_normalization(
        graph:GraphFrame,
        degree:DataFrame
    ) -> DataFrame:
        raw_edges:DataFrame = graph.edges
        return (
            raw_edges
            # normaliza cada dirección por el grado del RECEPTOR
            .join(degree.withColumnRenamed("node", "dst").withColumnRenamed("deg_sum", "deg_dst"), on="dst", how="left")
            .join(degree.withColumnRenamed("node", "src").withColumnRenamed("deg_sum", "deg_src"), on="src", how="left")
            .withColumn(  # mensaje src->dst: normalizado por el grado de dst (receptor)
                "w_src_to_dst",
                when(col("deg_dst") > 0, col("weight") / col("deg_dst")).otherwise(lit(0.0)),
            )
            .withColumn(  # mensaje dst->src: normalizado por el grado de src (receptor)
                "w_dst_to_src",
                when(col("deg_src") > 0, col("weight") / col("deg_src")).otherwise(lit(0.0)),
            )
            .select("src", "dst", "w_src_to_dst", "w_dst_to_src")
        )
    #
    @staticmethod
    def standard_propagate_target(
        graph: GraphFrame,
        edges_norm:DataFrame,
        target_column:str,
        final_output_column_name:str="contagion_score",
        keep_seed_floor:bool=True,
        max_iter:int = 3,
        alpha:float = 0.15
    ) -> DataFrame:
        """Propaga el score del target por el grafo mediante difusión iterativa de mensajes ponderados."""
        #
        # ------------------------------------------------------------------------------
        # 2) Inicializar score = semilla flotante.
        # ------------------------------------------------------------------------------
        nodes = graph.vertices
        nodes = nodes.withColumn(
            final_output_column_name, coalesce(col(target_column).cast("double"), lit(0.0))
        )
        if keep_seed_floor:
            nodes = nodes.withColumn(
                "seed_score", col(final_output_column_name)
            )
        #
        g = GraphFrame(nodes, edges_norm)
        #
        #
        # ------------------------------------------------------------------------------
        # 3) Difusión iterativa: promedio ponderado entrante.
        # ------------------------------------------------------------------------------
        for _ in range(max_iter):
            # mensaje = score_origen * peso_normalizado_de_esa_direccion
            msg_to_dst = AM.src[final_output_column_name] * AM.edge["w_src_to_dst"]
            msg_to_src = AM.dst[final_output_column_name] * AM.edge["w_dst_to_src"]
            #
            # como los pesos ya están normalizados por nodo, SUM = promedio ponderado
            agg = g.aggregateMessages(
                spark_sum(AM.msg).alias("incoming_score"),
                sendToDst=msg_to_dst,
                sendToSrc=msg_to_src,
            )
            #
            new_nodes = (
                g.vertices.join(agg, on="id", how="left")
                .withColumn(
                    "incoming_score",
                    coalesce(col("incoming_score"), lit(0.0)),
                )
                # amortiguación: mezcla propio + entrante
                .withColumn(
                    final_output_column_name,
                    (lit(1.0 - alpha) * col(final_output_column_name))
                    + (lit(alpha) * col("incoming_score")),
                )
                .drop("incoming_score")
            )
            #
            # el score nunca cae por debajo de la semilla original (opcional)
            if keep_seed_floor:
                new_nodes = new_nodes.withColumn(
                    final_output_column_name,
                    greatest(col(final_output_column_name), col("seed_score")),
                )
            #
            g = GraphFrame(AM.getCachedDataFrame(new_nodes), g.edges)
        #
        return (g.vertices.drop("seed_score") if keep_seed_floor else g.vertices
            .select("id", final_output_column_name, *[
                c for c in graph.vertices.columns if c not in ("id", final_output_column_name)
            ]))
        #
    def get_edges_norm(self,
        # edges_df:DataFrame,
        # nodes_df:DataFrame,
        weight_type:str,
        parent_hdfs:HivePath,
        # checkpoint_hdfs:HivePath,
        # column_renames: Optional[Dict[str,str]] = None,
        # edge_final_columns: List[str] = ["src", "dst", "weight"],
        # node_final_columns: List[str] = ["id", "target_lovelace"],
        **kwargs
    ) -> DataFrame:
        naming_kwargs = ["weight_type"]
        property_base_name = "edges_norm"
        #
        def make_edges_norm(*args, **kwargs) -> DataFrame:
            # Parameters for graph
            graph = self.get_graph(
                # edges_df=edges_df,
                # nodes_df=nodes_df,
                # checkpoint_hdfs=checkpoint_hdfs,
                # column_renames=column_renames,
                # edge_final_columns=edge_final_columns,
                # node_final_columns=node_final_columns,
                **kwargs)
            degree = self.standard_get_degree(graph)
            edges_norm = self.standard_weight_normalization(graph, degree)
            return edges_norm
        #
        all_kwargs = {
            "weight_type":weight_type,
            "parent_hdfs":parent_hdfs,
            **kwargs
        }
        naming_items = [
            (sanitize_name(k), sanitize_name(v)) for k, v in all_kwargs.items()
            if k in naming_kwargs and isinstance(v, (str, int, float, bool))
        ]
        #
        subdir = "/".join(f"{k}={v}" for k, v in naming_items)
        property_path = parent_hdfs.joinpath(subdir)
        print(f"property_path = {property_path}")
        #
        name_suffix = "_".join(f"{k}_{v}" for k, v in naming_items)
        property_name = property_base_name + (f"_{name_suffix}" if name_suffix else "")
        #
        return self.get_cached_decorated_table_or_parquet_property(
            method=make_edges_norm,
            path=property_path,
            property_name=property_name,
            weight=weight_type,
            input_or_output="output",
            # edges_df=edges_df,
            # nodes_df=nodes_df,
            **kwargs
        )
        #
    def propagate_target(self,
        weight_name:str
    ) -> DataFrame:
        # kwargs de infraestructura + kwargs NO serializables (no van al path/nombre)
        not_naming_kwargs = ["parent_hdfs"]
        # Solo escalares simples definen identidad/path.
        naming_items = [
            (sanitize_name(k), sanitize_name(v)) for k, v in kwargs.items()
            if k not in not_naming_kwargs and isinstance(v, (str, int, float, bool))
        ]
        subdir = "/".join(f"{k}={v}" for k, v in naming_items)
        property_path = parent_hdfs.joinpath(subdir)
        #
        name_suffix = "_".join(f"{k}_{v}" for k, v in naming_items)
        name = property_name + (f"_{name_suffix}" if name_suffix else "")
        #
        def make_graph_and_property(*args, **kwargs) -> DataFrame:
            graph = self.get_graph(edges_df=self.edges, nodes_df=self.nodes,**kwargs)
            property_method = getattr(self, f"{property_name}_ft")
            return property_method(graph, **kwargs)
        #
        print(f"Defining property property: {name} at {property_path}")
        return self.get_cached_decorated_table_or_parquet_property(
            method=make_graph_and_property,
            property_name=name,
            path=property_path,
            **kwargs
        )
