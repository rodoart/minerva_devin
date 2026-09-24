
###############################################################################
# SPECIAL TREATMENT
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Callable, List, Dict, Any


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame, Column
from graphframes import GraphFrame


from pyspark.sql.functions import (col,
    max as spark_max)
# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath
from libs.framework.utils import sanitize_path_name
from libs.functions.missing_treatment import apply_missing_treatment

import libs.functions.features as lff
import pipelines.features.graph_features as p_f_fg

import config.features.ceps.graph_features as cfgf
import config.features.ceps.target_propagation_features as cfcf

import libs.framework as ppf

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)
###############################################################################
# Process
###############################################################################


class SubStep(ppf.Step):
    """Clase base para las sub-etapas de propagación del target.

    Reutiliza `get_graph`, `feature` y `define_checkpoint` del SubStep de
    features de grafo (`pipelines.features.graph_features`).
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
    """Etapa estándar de features de propagación del target por el grafo.
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
        """Carga estándar de una tabla/parquet particionado con validación de historial.
        """
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
    def standard_groupby_step(self) -> "StandardJoinGraphSubStep":
        """Sub-etapa de unión del target a los nodos (solo para tests)."""
        return StandardJoinGraphSubStep(self)


class StandardJoinGraphSubStep(SubStep):
    """Sub-etapa base: carga de inputs y unión del target agregado a los nodos.
    """
    # previous_step
    def get_input(self,
        config_dict_key:str,
        input_or_output:str = "input",
        *args, **kwargs) -> DataFrame:
        """Carga simple (parquet plano) desde input_hive/output_hive."""
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
        """Carga varias claves de `input_hive` a la vez; devuelve {key: DataFrame}."""
        return {key: self.get_input(key) for key in keys}
    #
    @staticmethod
    def standard_target_group_by_id(
        group_by_id:DataFrame,
        target:DataFrame,
        column:str,
        function:Callable[..., Column]=spark_max
    ) -> DataFrame:
        """Agrega el target por cada valor de `column` (array) asociado a cada id de nodo."""
        return lff.target_group_by_id(group_by_id, target, column, function)
    #
    @staticmethod
    def standard_join_target(
        nodes:DataFrame,
        target_group_by_id:DataFrame,
        new_column_suffix:str,
        target_column:str = "target"
    ) -> DataFrame:
        """Une la agregación de target a los nodos renombrándola `target_{suffix}`."""
        return lff.join_target(nodes, target_group_by_id, new_column_suffix, target_column)


class StandardTargetPropagationSubStep(SubStep, p_f_fg.SubStep):
    """Sub-etapa base de propagación: grado, normalización de pesos y difusión del target.
    """
    def __init__(self, parent: "StandardTargetPropagationFeaturesStep", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)
        #
    @property
    def standard_join_graph_substep(self) -> StandardJoinGraphSubStep:
        """Acceso a los helpers de carga/unión del substep de join."""
        return StandardJoinGraphSubStep(self)
    #
    @staticmethod
    def standard_get_degree(
        graph:GraphFrame, # with  weight

    ) -> DataFrame:
        """Grado ponderado por nodo: suma de `weight` de aristas incidentes (in+out)."""
        return lff.get_degree(graph)
    #
    @staticmethod
    def standard_weight_normalization(
        graph:GraphFrame,
        degree:DataFrame
    ) -> DataFrame:
        """Aristas con pesos normalizados por el grado del receptor en cada dirección."""
        return lff.weight_normalization(graph, degree)
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
        return lff.propagate_target(
            graph=graph,
            edges_norm=edges_norm,
            target_column=target_column,
            final_output_column_name=final_output_column_name,
            keep_seed_floor=keep_seed_floor,
            max_iter=max_iter,
            alpha=alpha,
        )
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
        """Aristas con pesos normalizados (`w_src_to_dst`/`w_dst_to_src`), cacheadas.

        Construye el grafo con `get_graph`, calcula el grado ponderado y
        normaliza los pesos; el resultado se cachea bajo
        `parent_hdfs/weight_type=<weight_type>` como `edges_norm_<weight_type>`.

        Args:
            weight_type: tipo de peso a usar para las aristas del grafo.
            parent_hdfs: directorio padre donde cachear el resultado.

        Returns:
            DataFrame de aristas normalizadas (src, dst, w_src_to_dst, w_dst_to_src).
        """
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
            (sanitize_path_name(k), sanitize_path_name(v)) for k, v in all_kwargs.items()
            if k in naming_kwargs and isinstance(v, (str, int, float, bool))
        ]
        #
        subdir = "/".join(f"{k}={v}" for k, v in naming_items)
        # edges_norm va a un directorio HERMANO de target_propagation: si se
        # escribe bajo parent_hdfs/weight_type=... el padre mezcla niveles de
        # partición (weight_type=X vs weight_type=X/target_column=...) y la
        # lectura merge_schema del assembler explota.
        property_path = parent_hdfs.parent.joinpath("edges_norm").joinpath(subdir)
        logger.info("property_path = %s", property_path)
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
        weight_type:str,
        parent_hdfs:HivePath,
        target_column:str = "target",
        max_iter:int = 3,
        alpha:float = 0.15,
        keep_seed_floor:bool = True,
        **kwargs
    ) -> DataFrame:
        """Propaga el target por el grafo y cachea el score de contagio.

        Obtiene las aristas normalizadas (`get_edges_norm`), construye el grafo
        con `weight=weight_type` y ejecuta la difusión iterativa; escribe bajo
        `target_propagation/<params subdir>` (parent_hdfs + subdirectorio
        parametrizado por weight_type/target_column/max_iter/alpha/
        keep_seed_floor) y nombra la columna de salida `contagion_<weight_type>`.

        Args:
            weight_type: tipo de peso de las aristas para la propagación.
            parent_hdfs: directorio padre del output `target_propagation`.
            target_column: columna semilla del target en los nodos.
            max_iter: iteraciones de difusión de mensajes.
            alpha: factor de amortiguación (mezcla propio/entrante).
            keep_seed_floor: si el score nunca cae por debajo de la semilla.

        Returns:
            DataFrame de nodos con la columna `contagion_<weight_type>`.
        """
        naming_kwargs = ["weight_type", "target_column", "max_iter", "alpha", "keep_seed_floor"]
        property_base_name = "target_propagation"
        #
        def make_propagation(*args, **kwargs) -> DataFrame:
            edges_norm = self.get_edges_norm(
                weight_type=weight_type,
                parent_hdfs=parent_hdfs,
                **kwargs
            )
            graph = self.get_graph(weight=weight_type, **kwargs)
            return self.standard_propagate_target(
                graph=graph,
                edges_norm=edges_norm,
                target_column=target_column,
                final_output_column_name=f"contagion_{weight_type}",
                keep_seed_floor=keep_seed_floor,
                max_iter=max_iter,
                alpha=alpha,
            )
        #
        all_kwargs = {
            "weight_type":weight_type,
            "target_column":target_column,
            "max_iter":max_iter,
            "alpha":alpha,
            "keep_seed_floor":keep_seed_floor,
            **kwargs
        }
        naming_items = [
            (sanitize_path_name(k), sanitize_path_name(v)) for k, v in all_kwargs.items()
            if k in naming_kwargs and isinstance(v, (str, int, float, bool))
        ]
        #
        subdir = "/".join(f"{k}={v}" for k, v in naming_items)
        property_path = parent_hdfs.joinpath(subdir)
        logger.info("property_path = %s", property_path)
        #
        name_suffix = "_".join(f"{k}_{v}" for k, v in naming_items)
        property_name = property_base_name + (f"_{name_suffix}" if name_suffix else "")
        #
        return self.get_cached_decorated_table_or_parquet_property(
            method=make_propagation,
            path=property_path,
            property_name=property_name,
            input_or_output="output",
            **kwargs
        )


###############################################################################
# Ceps + Lovelace
###############################################################################


class CepsTargetPropagationFeaturesStep(StandardTargetPropagationFeaturesStep):
    """Step concreto: join del target Lovelace a los nodos CEPS + features de contagio."""
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta el join del target Lovelace y la propagación, recogiendo salidas."""
        ppf.run_substep_and_collect(self, self.lovelace_ceps_join_graph_step, "lovelace_ceps_join_graph_step")
        return ppf.run_substep_and_collect(self, self.lovelace_ceps_propagation_step, "lovelace_ceps_propagation_step")
        #
    @ppf.cached_property
    def lovelace_ceps_join_graph_step(self) -> "LovelaceCepsJoinGraphSubStep":
        """Sub-etapa de unión del target Lovelace a los nodos del grafo."""
        return LovelaceCepsJoinGraphSubStep(self, previous_step=self.previous_step[0])
        #
    @ppf.cached_property
    def lovelace_ceps_propagation_step(self) -> "LovelaceCepsTargetPropagationSubStep":
        """Sub-etapa de propagación del target Lovelace por el grafo."""
        return LovelaceCepsTargetPropagationSubStep(self, previous_step=self.previous_step[0])


class LovelaceCepsJoinGraphSubStep(StandardJoinGraphSubStep):
    """Une el target Lovelace (por cta y por numcliente) a los nodos del grafo."""
    def step_action(self) -> Dict[str, Any]:
        """Materializa `nodes_join_target` y recoge su salida."""
        return ppf.collect_step_output(self, self.nodes_join_target, "nodes_join_target")
        #
    @ppf.cached_property
    def group_by_id(self) -> DataFrame:
        """Tabla de correspondencia nodo -> ids de agregación (cta, numcliente)."""
        return self.get_input("group_by_id")
        #
    @ppf.cached_property
    def nodes(self) -> DataFrame:
        """Nodos del grafo (parquet plano)."""
        return self.get_input("nodes")
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="nodes_join_target_lovelace")
    def nodes_join_target(self) -> DataFrame:
        """Nodos del grafo enriquecidos con el target Lovelace agregado por modo.

        Para cada modo de `TARGETS["lovelace"]["modes"]` (cta, numcliente) se
        agrega el target sobre `group_by_id` y se une a `nodes`; después se
        combinan las columnas `target_lovelace_*` en `target_lovelace` con
        `TARGET_SELECTION_FUNCTION`.
        """
        nodes = self.nodes
        #
        for mode_name, mode in cfcf.TARGETS["lovelace"]["modes"].items():
            target = self.get_input(mode["input_key"])
            target_group_by_id = self.standard_target_group_by_id(
                self.group_by_id, target, mode["suffix"], mode["aggregation_function"]
            )
            #
            agg_column = f"target_agg_{mode['suffix']}"
            if mode.get("missing_treatment"):
                target_group_by_id = apply_missing_treatment(
                    target_group_by_id, {agg_column: mode["missing_treatment"]}
                )
            #
            nodes = self.standard_join_target(
                nodes, target_group_by_id, f"lovelace_{mode['suffix']}",
                target_column=agg_column
            )
        #
        target_columns = [
            f"target_lovelace_{mode['suffix']}"
            for mode in cfcf.TARGETS["lovelace"]["modes"].values()
        ]
        nodes = nodes.withColumn(
            "target_lovelace",
            cfcf.TARGET_SELECTION_FUNCTION(*[col(c) for c in target_columns])
        )
        return nodes


class LovelaceCepsTargetPropagationSubStep(StandardTargetPropagationSubStep):
    """Calcula las features de contagio del target Lovelace por cada weight_type."""
    def step_action(self) -> Dict[str, Any]:
        """Materializa `propagation_features` y recoge su salida."""
        return ppf.collect_step_output(self, self.propagation_features, "propagation_features")
        #
    @ppf.cached_property
    def nodes_join_target(self) -> DataFrame:
        """Nodos con el target Lovelace ya unido (salida del substep de join)."""
        return self.parent.lovelace_ceps_join_graph_step.nodes_join_target[0]
        #
    @ppf.cached_property
    def edges(self) -> DataFrame:
        """Aristas del grafo (parquet plano)."""
        return self.standard_join_graph_substep.get_input("edges")
        #
    @ppf.cached_property
    def propagation_features(self) -> List[Dict[str, Any]]:
        """Ejecuta las features de contagio configuradas y devuelve sus resultados."""
        parent_hdfs = HivePath(str(self.output_hive["target_propagation"]["table_or_hdfs"]))
        checkpoint_hdfs = str(self.output_hive["checkpoint"]["table_or_hdfs"])
        #
        results:List[Dict[str, Any]] = []
        for feature_config in cfcf.PROPAGATION_FEATURES:
            feature_name = list(feature_config.keys())[0]
            feature_kwargs = feature_config[feature_name]
            #
            df = self.propagate_target(
                parent_hdfs=parent_hdfs,
                target_column="target_lovelace",
                edges_df=self.edges,
                nodes_df=self.nodes_join_target,
                checkpoint_hdfs=checkpoint_hdfs,
                column_renames=cfgf.GRAPH_RENAMES,
                edge_final_columns=["src", "dst", "weight"],
                node_final_columns=["id", "target_lovelace"],
                **feature_kwargs
            )
            results.append({feature_name: {**feature_kwargs, "df": df}})
        #
        return results
