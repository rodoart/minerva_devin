
###############################################################################
# GRAPH FEATURES
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
import inspect
from typing import List, Union, Dict, Any, Optional


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import Column, DataFrame

from graphframes import GraphFrame
# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath
from libs.framework.utils import sanitize_property_name, sanitize_path_name

import libs.functions.features as lff
import libs.functions.weights as lfw
import libs.framework as ppf

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)
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
        """Construye (y cachea) el `GraphFrame` a partir de aristas y nodos.

        Aplica `column_renames`, deriva la columna `weight` desde `weights`
        según el tipo de peso pedido y fija el directorio de checkpoint.
        El grafo se cachea como propiedad `graph_<weight>` (o `graph_default`).

        Args:
            edges_df: DataFrame de aristas (con `src`, `dst` y pesos).
            nodes_df: DataFrame de nodos (con `id`).
            checkpoint_hdfs: directorio HDFS para checkpoints de GraphFrames.
            weight: tipo de peso a extraer de la columna `weights` (opcional).
            column_renames: renombrados previos sobre aristas/nodos (opcional).
            edge_final_columns: columnas finales del DataFrame de aristas.
            node_final_columns: columnas finales del DataFrame de nodos.

        Returns:
            GraphFrame con las columnas finales seleccionadas.
        """
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
                edges_df = edges_df.withColumn("weight", lfw.get_weight("weights", weight))
            #
            self.define_checkpoint(checkpoint_hdfs=checkpoint_hdfs)
            return GraphFrame(
                nodes_df.select(*node_final_columns),
                edges_df.select(*edge_final_columns)
            )
        #
        return self.get_cached_decorated_table_or_parquet_property(
            method=make_graph,
            property_name=f"graph_{sanitize_property_name(value=weight) or 'default'}",
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
            (sanitize_path_name(k), sanitize_path_name(v)) for k, v in kwargs.items()
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
            # Excluir los kwargs de get_graph: el resto son params de la feature
            get_graph_params = inspect.signature(self.get_graph).parameters
            feature_kwargs = {k: v for k, v in kwargs.items()
                              if k not in get_graph_params}
            return feature_method(graph, **feature_kwargs)
        #
        logger.info("Defining feature property: %s at %s", name, feature_path)
        return self.get_cached_decorated_table_or_parquet_property(
            method=make_graph_and_feature,
            property_name=name,
            path=feature_path,
            input_or_output="output",
            **kwargs
        )
        #
    def define_checkpoint(self, checkpoint_hdfs:HivePath) -> None:
        """Fija el directorio de checkpoint de Spark en HDFS."""
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
    """Etapa estándar de cálculo de features de grafo sobre aristas/nodos.

    Orquesta las sub-etapas de features no ponderadas y ponderadas.
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
        """Sub-etapa de features de grafo no ponderadas (solo para tests)."""
        return StandardGraphFeaturesUnWeightedStep(self)


class StandardGraphFeaturesUnWeightedStep(SubStep):
    """Sub-etapa de features de grafo **no ponderadas** (unweighted)."""
    #
    @staticmethod
    def pagerank_ft(
        graph:GraphFrame,
        **kwargs
    ) -> DataFrame:
        """PageRank estándar por nodo."""
        return lff.pagerank(graph, **kwargs)
    #
    @staticmethod
    def degrees_ft(
        graph:GraphFrame,
        **kwargs
    ) -> DataFrame:
        """Grado de entrada, salida y total por nodo."""
        return lff.degrees(graph)
    #
    @staticmethod
    def components_ft(
        graph:GraphFrame,
        **kwargs
    ) -> DataFrame:
        """Componente conexa a la que pertenece cada nodo."""
        return lff.components(graph)
    #
    @staticmethod
    def triangle_count_ft(
        graph:GraphFrame,
        **kwargs
    ) -> DataFrame:
        """Número de triángulos en los que participa cada nodo."""
        return lff.triangle_count(graph)
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
        return lff.weighted_pagerank(graph, max_iter=max_iter, reset_prob=reset_prob)
    #
    @staticmethod
    def weighted_degrees_ft(
        graph: GraphFrame,
        **kwargs
    ) -> DataFrame:
        """Fuerza (strength) ponderada: suma de pesos entrantes/salientes por nodo. ..."""
        return lff.weighted_degrees(graph)
    #
    @staticmethod
    def weighted_edge_stats_ft(
        graph: GraphFrame,
        aggregations:Optional[List[Column]] = None,
        **kwargs
    ) -> DataFrame:
        """Estadísticas de peso de aristas incidentes por nodo (in + out). ..."""
        return lff.weighted_edge_stats(graph, aggregations=aggregations)
    #
    @staticmethod
    def weighted_components_ft(
        graph: GraphFrame,
        **kwargs
    ) -> DataFrame:
        """Componentes conexas enriquecidas con el peso total de cada componente. ..."""
        return lff.weighted_components(graph)
    #
    @staticmethod
    def weighted_triangle_count_ft(
        graph: GraphFrame,
        *args,
        **kwargs
    ) -> DataFrame:
        """Conteo de triángulos por nodo (estructural, no ponderado) + fuerza. ..."""
        return lff.weighted_triangle_count(graph)
        #
