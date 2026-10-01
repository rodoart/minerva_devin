
###############################################################################
# CLUSTER FEATURES
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import List, Dict, Any, Union


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame
from graphframes import GraphFrame

from pyspark.sql.functions import col
from pyspark.sql.types import NumericType
# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath
from libs.framework.utils import sanitize_column_name

import libs.functions.features as lff
import pipelines.graph_making.special_treatment as p_gm_sp


import libs.framework as ppf

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)
###############################################################################
# Process
###############################################################################


class SubStep(ppf.Step):
    """Clase base para las sub-etapas de features de clustering."""
    def __init__(self, parent: "StandardClusterFeaturesStep", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)
        #
    def define_checkpoint(self, checkpoint_hdfs:HivePath) -> None:
        """Fija el directorio de checkpoint de Spark en HDFS."""
        self.sqlContext.sparkContext.setCheckpointDir("hdfs://"+str(checkpoint_hdfs))
        #


class StandardClusterFeaturesStep(ppf.Step):
    """Etapa estándar de features de clustering: stats intra-grupo por nodo.
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
            step_name_prefix="Standard_Cluster_Features_Step",
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
    def standard_cluster_features_substep(self) -> "StandardClusterFeaturesSubStep":
        """Sub-etapa de stats intra-grupo (solo para tests)."""
        return StandardClusterFeaturesSubStep(self)


class StandardClusterFeaturesSubStep(SubStep):
    """Sub-etapa base de clustering: enriquecimiento de nodos + stats por grupo.
    """
    @staticmethod
    def feature_dataframe(session, path:Union[str, HivePath]) -> DataFrame:
        """Lee un parquet de features a nivel `id`.

        Si el directorio padre contiene subdirs por parámetros (p.ej.
        `weighted_pagerank/weight=count_txn`), une las variantes por `id`
        renombrando las columnas con el sufijo derivado de los segmentos de
        partición (misma convención que `get_merge_schema_input`); si es un
        parquet plano, lo devuelve tal cual.
        """
        base = HivePath(str(path))
        df = None
        for leaf in base.listparquets(recursive=True):
            sub_df = (session.read
                .option("mergeSchema", "true")
                .parquet(str(leaf)))
            suffix_parts = leaf.relative_to(base).parts
            if suffix_parts:
                suffix = "_".join(sanitize_column_name(p) for p in suffix_parts)
                sub_df = sub_df.select(
                    "id",
                    *[sub_df[c].alias(f"{c}_{suffix}")
                      for c in sub_df.columns if c != "id"])
            df = sub_df if df is None else df.join(sub_df, "id")
        return df
        #
    @staticmethod
    def subcluster(
        nodes_df:DataFrame,
        edges_df:DataFrame,
        method:str = "scc",
        max_iter:int = 10
    ) -> DataFrame:
        """Sub-partición del grafo en grupos dirigidos: una fila por `id`.

        - `"scc"`: `stronglyConnectedComponents` (determinista).
        - `"label_propagation"`: `labelPropagation` (NO determinista).

        Requiere que el llamador haya fijado `sparkContext.setCheckpointDir`.
        """
        if method not in ("scc", "label_propagation"):
            raise ValueError(f"Método de subcluster desconocido: {method!r}")
        if method == "label_propagation":
            logger.warning(
                "label_propagation NO es determinista: los subclusters pueden "
                "cambiar entre ejecuciones con los mismos datos")
        graph = GraphFrame(nodes_df.select("id"), edges_df.select("src", "dst"))
        result = (graph.stronglyConnectedComponents(maxIter=max_iter)
            if method == "scc"
            else graph.labelPropagation(maxIter=max_iter))
        label_col = "component" if method == "scc" else "label"
        return result.select("id", col(label_col).alias("scc"))
        #
    @staticmethod
    def standard_cluster_group_stats(
        nodes:DataFrame,
        group_column:str,
        aggregate_columns:List[str],
        stats:Dict = None,
        prefix:str = "cluster"
    ) -> DataFrame:
        """Estadísticos intra-grupo por nodo (delega en `lff.cluster_group_stats`)."""
        return lff.cluster_group_stats(
            nodes=nodes,
            group_column=group_column,
            aggregate_columns=aggregate_columns,
            stats=stats,
            prefix=prefix,
        )
        #
