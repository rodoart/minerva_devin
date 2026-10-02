
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

from pyspark.sql.functions import col, broadcast
from pyspark.sql.types import NumericType
# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath
from libs.framework.utils import sanitize_column_name

import libs.functions.aggregations as lfa
import libs.functions.features as lff
import pipelines.graph_making.special_treatment as p_gm_sp

import config.features.ceps.graph_features as cfgf
import config.features.ceps.cluster_features as cclf

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
    def staged_cluster_group_stats(self,
        df:DataFrame,
        group_column:str,
        aggregate_columns:List[str],
        stats:Dict,
        prefix:str = "cluster",
    ) -> DataFrame:
        """Stats intra-grupo por nodo, en etapas persistidas y robusto a skew.

        Estrategia (la distribución de `component_id`/`scc` es muy asimétrica:
        una componente gigante concentraría casi todo el shuffle en un
        reducer):

        1. Las columnas se agregan en chunks de `STATS_COLUMN_CHUNK`.
        2. Los stats combinables (count/sum/min/max/mean/std) van por la
           vía salteada en dos etapas (`lfa.salted_partial_stats` +
           `lfa.merge_salted_stats`): un grupo gigante se reparte entre
           `SALT_BUCKETS` reducers.
        3. Los stats no combinables (median/percentile_approx...) van por
           `lfa.plain_group_stats` dentro del mismo chunk.
        4. Cada chunk se materializa como parquet intermedio bajo
           `cluster_stats_parts/group_column=<g>/chunk=<i>_<hash>`: si el
           proceso muere solo se recomputan los chunks sin parquet. La huella
           incluye columnas y stats: cambios de config invalidan el caché.
        5. Las partes se unen por `group_column` y el resultado (una fila por
           grupo) se une a los nodos con broadcast si es razonablemente
           pequeño (`STATS_BROADCAST_MAX_GROUPS`) — evita el shuffle skewed
           del join sobre la clave asimétrica.
        """
        parts_base = HivePath(str(
            self.output_hive["cluster_stats_parts"]["table_or_hdfs"]))
        combinable, plain_stats = lfa.split_stats_by_combinable(stats)
        size_column = f"{prefix}_{group_column}_size"
        chunk_size = max(1, cclf.STATS_COLUMN_CHUNK)
        chunks = [aggregate_columns[i:i + chunk_size]
            for i in range(0, len(aggregate_columns), chunk_size)]
        #
        part_frames:List[DataFrame] = []
        size_emitted = False
        for index, chunk in enumerate(chunks):
            chunk_stat_names = combinable + list(plain_stats)
            digest = lfa.stats_chunk_key(group_column, chunk, chunk_stat_names)
            part_path = parts_base.joinpath(
                f"group_column={group_column}",
                f"chunk={index:03d}_{digest}")
            emit_size = not size_emitted
            #
            def make_chunk(*args, _chunk=chunk, _emit=emit_size, **kwargs) -> DataFrame:
                frames:List[DataFrame] = []
                column_prefix = f"{prefix}_{group_column}_"
                if combinable:
                    frames.append(lfa.merge_salted_stats(
                        lfa.salted_partial_stats(
                            df, group_column, _chunk,
                            salt_buckets=cclf.SALT_BUCKETS),
                        group_column, _chunk, combinable,
                        size_column=size_column if _emit else None,
                        column_prefix=column_prefix))
                if plain_stats:
                    frames.append(lfa.plain_group_stats(
                        df, group_column, _chunk, plain_stats,
                        size_column=(size_column
                            if _emit and not combinable else None),
                        column_prefix=column_prefix))
                result = frames[0]
                for frame in frames[1:]:
                    result = result.join(frame, on=group_column)
                return result
            #
            part_frames.append(
                self.get_cached_decorated_table_or_parquet_property(
                    method=make_chunk,
                    path=part_path,
                    property_name=(
                        f"cluster_stats_part_{group_column}_{index}_{digest}"),
                    input_or_output="output"))
            size_emitted = True
        #
        if not part_frames:     # sin columnas agregables: solo tamaños de grupo
            grouped = df.groupBy(group_column).agg(
                {"*": "count"}).withColumnRenamed(
                    "count(1)", size_column)
        else:
            grouped = part_frames[0]
            for part in part_frames[1:]:
                grouped = grouped.join(part, on=group_column)
        #
        # Broadcast solo si los grupos caben razonablemente en memoria del
        # driver/executors (limit barato: las partes ya están en parquet).
        max_groups = getattr(cclf, "STATS_BROADCAST_MAX_GROUPS", 200000)
        if grouped.limit(max_groups + 1).count() <= max_groups:
            grouped = broadcast(grouped)
        return (df.select("id", group_column)
            .join(grouped, on=group_column, how="left"))
        #


###############################################################################
# Ceps
###############################################################################


class CepsClusterFeaturesStep(StandardClusterFeaturesStep):
    """Step CEPS de features de clustering: stats intra-grupo por nodo."""
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta la sub-etapa CEPS de clustering y recoge su salida."""
        return ppf.run_substep_and_collect(
            self, self.ceps_cluster_features_substep, "ceps_cluster_features_substep")
        #
    @ppf.cached_property
    def ceps_cluster_features_substep(self) -> "CepsClusterFeaturesSubStep":
        """Sub-etapa CEPS de stats intra-grupo por nodo."""
        return CepsClusterFeaturesSubStep(self, previous_step=self.previous_step[0])


class CepsClusterFeaturesSubStep(StandardClusterFeaturesSubStep):
    """Enriquece los nodos con features de grafo y calcula stats por grupo
    (`component_id` + sub-partición dirigida `scc`).
    """
    def step_action(self) -> Dict[str, Any]:
        """Materializa `cluster_stats` y recoge su salida."""
        if cclf.SUBCLUSTER_ENABLED:
            logger.warning(
                "SUBCLUSTER_ENABLED=True: este step ejecutará la sub-partición "
                "dirigida '%s' (GraphFrames), la parte más pesada de "
                "cluster_features. Pon SUBCLUSTER_ENABLED=False en la config "
                "para agrupar solo por component_id.", cclf.SUBCLUSTER_METHOD)
        return ppf.collect_step_output(
            self, self.cluster_stats_output, "cluster_stats_output")
        #
    @ppf.cached_property
    def nodes_join_target(self) -> DataFrame:
        """Nodos + etiquetas target_lovelace (tabla particionada)."""
        return self.standard_load_parquet_or_table("nodes_join_target_lovelace")
        #
    @ppf.cached_property
    def edges(self) -> DataFrame:
        """Aristas del grafo (tabla particionada)."""
        return self.standard_load_parquet_or_table("edges")
        #
    @ppf.cached_property
    def components_df(self) -> DataFrame:
        """Componente conexa por nodo (parquet de la feature `components`)."""
        return self.feature_dataframe(
            self.sqlContext,
            self.input_hive["components"]["table_or_hdfs"])
        #
    @ppf.cached_property
    def subcluster_df(self) -> DataFrame:
        """Sub-partición dirigida por nodo (columna `scc`), persistida.

        El SCC de GraphFrames usa checkpoints internos no reanudables: este
        parquet es el punto de reanudación real. Si ya existe se recarga y el
        algoritmo de grafo no vuelve a ejecutarse. Se escribe bajo
        `subcluster_df/method=<m>_max_iter=<n>`: cambiar la config no recarga
        un parquet con el algoritmo equivocado, y el borrado del dir base sigue
        cubriendo todas las variantes.
        """
        base = HivePath(str(
            self.output_hive["subcluster_df"]["table_or_hdfs"]))
        variant = (f"method={cclf.SUBCLUSTER_METHOD}"
            f"_max_iter={cclf.SUBCLUSTER_MAX_ITER}")
        return self.get_cached_decorated_table_or_parquet_property(
            method=self._compute_subcluster_df,
            path=base.joinpath(variant),
            property_name=f"subcluster_df_{variant}",
            input_or_output="output")
        #
    def _compute_subcluster_df(self) -> DataFrame:
        """Cuerpo de `subcluster_df`: corre el algoritmo de sub-partición."""
        self.define_checkpoint(
            checkpoint_hdfs=self.output_hive["checkpoint"]["table_or_hdfs"])
        edges = self.edges
        for old_name, new_name in cfgf.GRAPH_RENAMES.items():
            if old_name in edges.columns:
                edges = edges.withColumnRenamed(old_name, new_name)
        return self.subcluster(
            nodes_df=self.nodes_join_target.select("id"),
            edges_df=edges,
            method=cclf.SUBCLUSTER_METHOD,
            max_iter=cclf.SUBCLUSTER_MAX_ITER,
        )
        #
    def _nodes_enriched_mode(self) -> str:
        """Variante de `nodes_enriched` según la config que altera su contenido.

        Incluye light/full, si el subcluster está activo y una huella de las
        fuentes de features unidas — evita recargar un parquet intermedio con
        contenido obsoleto tras un cambio de configuración.
        """
        mode = "light" if getattr(cclf, "CLUSTER_LIGHT_MODE", False) else "full"
        if not cclf.SUBCLUSTER_ENABLED:
            mode += "_noscc"
        sources = sorted(self._active_feature_sources().keys())
        return f"{mode}_{lfa.stats_chunk_key('src', sources, [])}"
        #
    @staticmethod
    def _active_feature_sources() -> Dict[str, HivePath]:
        """Fuentes de features de grafo que se unen a los nodos enriquecidos.

        En `CLUSTER_LIGHT_MODE` solo se unen `LIGHT_FEATURE_SOURCES` (p.ej.
        contagion): las fuentes pesadas (pagerank, degrees...) se saltan por
        completo — el join de todos los parquets es gran parte del coste.
        """
        sources = cclf.GRAPH_FEATURE_SOURCES
        if getattr(cclf, "CLUSTER_LIGHT_MODE", False):
            keep = getattr(cclf, "LIGHT_FEATURE_SOURCES", [])
            sources = {k: v for k, v in sources.items() if k in keep}
        return sources
        #
    @ppf.cached_property
    def nodes_enriched(self) -> DataFrame:
        """Nodos + targets + antigüedad + features de grafo + columnas de grupo.

        Materializado como parquet intermedio: el join de todas las fuentes es
        pesado y su linaje incluye el SCC; al persistirlo, los stats por grupo
        leen de disco en vez de recomputar el grafo. Se escribe bajo
        `nodes_enriched/mode=<variante>` (ver `_nodes_enriched_mode`).
        """
        base = HivePath(str(
            self.output_hive["nodes_enriched"]["table_or_hdfs"]))
        mode = self._nodes_enriched_mode()
        return self.get_cached_decorated_table_or_parquet_property(
            method=self._compute_nodes_enriched,
            path=base.joinpath(f"mode={mode}"),
            property_name=f"nodes_enriched_{mode}",
            input_or_output="output")
        #
    def _compute_nodes_enriched(self) -> DataFrame:
        """Cuerpo de `nodes_enriched`: join de todas las fuentes por `id`."""
        df = self.nodes_join_target
        if "information_date" in df.columns:
            df = p_gm_sp.calculate_daily_tfrom(
                df=df,
                current_date=str(self.input_parameters["vintage_date"]),
                date_column="information_date",
            )
        for path in self._active_feature_sources().values():
            df = df.join(
                self.feature_dataframe(self.sqlContext, path),
                on="id", how="left")
        df = df.join(self.components_df, on="id", how="left")
        if cclf.SUBCLUSTER_ENABLED:
            df = df.join(self.subcluster_df, on="id", how="left")
        return df
        #
    @ppf.cached_property
    def cluster_stats(self) -> DataFrame:
        """Una fila por `id` con los stats de cada grupo al que pertenece."""
        df = self.nodes_enriched
        aggregate_columns = [
            field.name for field in df.schema.fields
            if isinstance(field.dataType, NumericType)
            and field.name not in cclf.AGGREGATE_EXCLUDE_COLUMNS
        ]
        # Aligeramiento: en modo ligero (o con AGGREGATE_INCLUDE_PREFIXES) solo
        # se agregan las columnas de los prefijos elegidos.
        include_prefixes = getattr(cclf, "AGGREGATE_INCLUDE_PREFIXES", None)
        if getattr(cclf, "CLUSTER_LIGHT_MODE", False):
            include_prefixes = cclf.LIGHT_AGGREGATE_PREFIXES
        if include_prefixes:
            aggregate_columns = [
                c for c in aggregate_columns
                if any(c.startswith(p) or c.endswith(p)
                    for p in include_prefixes)]
            logger.info("Columnas agregadas tras filtro de aligeramiento: %s",
                aggregate_columns)
        stats = cclf.CLUSTER_STATS
        if getattr(cclf, "CLUSTER_LIGHT_MODE", False):
            stats = {k: v for k, v in stats.items()
                if k in cclf.LIGHT_STATS}
            logger.info("CLUSTER_LIGHT_MODE activo: stats %s", list(stats))
        logger.info("Columnas agregadas por grupo: %s", aggregate_columns)
        # Guarda: la target y la target propagada (y los pesos) deben entrar
        # en los estadísticos de grupo; si ningún prefijo obligatorio tiene
        # columnas agregables se avisa en lugar de fallar.
        for prefix in getattr(cclf, "REQUIRED_STATS_PREFIXES", []):
            if not any(c.startswith(prefix) or c.endswith(prefix)
                    for c in aggregate_columns):
                logger.warning(
                    "Ninguna columna agregable coincide con el prefijo "
                    "obligatorio %r; el grupo no tendrá stats de esa variable",
                    prefix)
        # Solo se agrupa por columnas realmente presentes: con
        # SUBCLUSTER_ENABLED=False no existe "scc" y el step sigue solo con
        # "component_id".
        group_columns = [c for c in cclf.GROUP_COLUMNS if c in df.columns]
        missing_groups = [c for c in cclf.GROUP_COLUMNS if c not in df.columns]
        if missing_groups:
            logger.warning(
                "Columnas de grupo ausentes en nodes_enriched: %s "
                "(SUBCLUSTER_ENABLED=%s); se omiten en los stats",
                missing_groups, cclf.SUBCLUSTER_ENABLED)
        result = df.select("id")
        for group_column in group_columns:
            result = result.join(
                self.staged_cluster_group_stats(
                    df, group_column, aggregate_columns, stats),
                on="id", how="left")
        return result
        #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="cluster_stats")
    def cluster_stats_output(self) -> DataFrame:
        """Salida `cluster_stats` (parquet plano, una fila por `id`)."""
        return self.cluster_stats
        #
