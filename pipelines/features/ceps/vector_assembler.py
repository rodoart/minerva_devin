
###############################################################################
# CEPS VECTOR ASSEMBLER
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Dict, Any, List


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import col, countDistinct, max as spark_max
# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from libs.functions.assembly import (build_aggregation_expressions,
    explode_array_column, get_feature_columns, assemble_vector,
    salted_assembly_groupby
)
from libs.functions.missing_treatment import apply_missing_treatment

import pipelines.features.vector_assembler as p_f_va
import pipelines.graph_making.special_treatment as p_gm_sp

import config.features.ceps.vector_assembler as cvas

import libs.framework as ppf

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)
###############################################################################
# CLASSES
###############################################################################


class CepsVectorAssemblerStep(p_f_va.StandardVectorAssemblerStep):
    """Step final: features a nivel numcliente + VectorAssembler sobre un pivote.
    """
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta la sub-etapa de ensamblado Lovelace-CEPS y recoge su salida."""
        return ppf.run_substep_and_collect(self, self.lovelace_ceps_assembler_step, "lovelace_ceps_assembler_step")
        #
    @ppf.cached_property
    def lovelace_ceps_assembler_step(self) -> "LovelaceCepsAssemblerSubStep":
        """Sub-etapa de ensamblado del vector `features` a nivel numcliente."""
        return LovelaceCepsAssemblerSubStep(self, previous_step=self.previous_step[0])


class LovelaceCepsAssemblerSubStep(p_f_va.StandardVectorAssemblerSubStep):
    """Recopila todas las features de nodo, las agrega a cada nivel configurado
    (`AGGREGATION_LEVELS`: numcliente, cta, ...) y ensambla el vector final
    sobre el pivote externo de cada nivel.
    """
    def step_action(self) -> Dict[str, Any]:
        """Materializa `{nivel}_features_vector` para cada nivel y recoge salidas."""
        outputs = {
            f"{level}_features_vector": self.level_features_vector_property(level)
            for level in cvas.AGGREGATION_LEVELS
        }
        return ppf.collect_step_output(self, outputs, "features_vector")
        #
    def level_pivot(self, level:str) -> DataFrame:
        """Pivote externo del nivel: lista de llaves para las que generar vectores."""
        spec = cvas.PIVOT_BY_LEVEL[level]
        input_config = self.input_hive[spec["input_key"]]
        pivot_column = input_config.get("pivot_column", spec.get("column", level))
        return (self.get_input(spec["input_key"])
            .select(col(pivot_column).cast("string").alias(level))
            .distinct())
        #
    @ppf.cached_property
    def pivot(self) -> DataFrame:
        """Pivote externo: lista de numclientes para los que generar vectores."""
        return self.level_pivot("numcliente")
        #
    @ppf.cached_property
    def nodes(self) -> DataFrame:
        """Nodos del grafo (tabla `nodes` particionada)."""
        return self.get_partitioned_input("nodes")
        #
    @staticmethod
    def node_array_columns() -> List[str]:
        """Columnas array de `nodes` usadas como llave de agregación por nivel."""
        return [cvas.NODE_ID_ARRAY_COLUMNS[level]
            for level in cvas.AGGREGATION_LEVELS
            if level in cvas.NODE_ID_ARRAY_COLUMNS]
        #
    @ppf.cached_property
    def feature_tables(self) -> Dict[str, DataFrame]:
        """Todas las tablas de features a nivel nodo (`id`).

        Modos soportados por fuente (config `FEATURE_SOURCES`):
          - "simple": parquet plano.
          - "partitioned": clave de input_hive con mis_date/process_date.
          - "merge_schema": parquet padre con subdirs por parámetros; las filas
            se colapsan a una por `id` (cada variante aporta sus columnas).

        Flags por fuente: `"enabled": False` la desactiva; `"optional": True`
        (default) convierte errores de carga en warning y sigue sin ella.
        """
        tables:Dict[str, DataFrame] = {}
        # Dedupe: la primera fuente que aporta una columna gana (evita
        # referencias ambiguas al unir tablas con columnas homónimas).
        seen_columns = set(self.nodes.columns)
        for name, source in cvas.FEATURE_SOURCES.items():
            if source.get("enabled", True) is False:
                logger.info("Fuente de features %s desactivada (enabled=False)", name)
                continue
            try:
                if source["mode"] == "partitioned":
                    df = self.get_partitioned_input(source["key"])
                elif source["mode"] == "merge_schema":
                    df = self.get_merge_schema_input(source["path"])
                else:
                    df = self.get_input(source["key"]) if "key" in source else \
                        self.sqlContext.read.parquet(str(source["path"]))
                if df is None:
                    raise ValueError("sin datos (0 parquets encontrados)")
                feature_columns = [
                    c for c in get_feature_columns(
                        df,
                        exclude_columns=["id", "information_date"]
                            + self.node_array_columns()
                            + cvas.EXCLUDE_COLUMNS,
                    )
                    if c not in seen_columns
                ]
            except Exception as exc:
                if source.get("optional", True):
                    logger.warning(
                        "Fuente de features %s no disponible (%s); se omite",
                        name, exc)
                    continue
                raise
            #
            seen_columns.update(feature_columns)
            if source["mode"] == "merge_schema":
                df = (df.groupBy("id")
                    .agg(*[spark_max(c).alias(c) for c in feature_columns]))
            else:
                df = df.select("id", *feature_columns)
            tables[name] = df
        return tables
        #
    @ppf.cached_property
    def nodes_with_features(self) -> DataFrame:
        """Nodos + todas las features unidas por `id` + tfrom_days para el peso."""
        weight_columns = [c for c in cvas.NODE_WEIGHT_COLUMNS + ["information_date"]
            if c in self.nodes.columns]
        array_columns = [c for c in self.node_array_columns()
            if c in self.nodes.columns]
        nodes = self.nodes.select(
            "id", *array_columns, *list(dict.fromkeys(weight_columns))
        )
        for table in self.feature_tables.values():
            nodes = nodes.join(table, on="id", how="left")
        #
        return p_gm_sp.calculate_daily_tfrom(
            df=nodes,
            current_date=str(self.input_parameters["vintage_date"]),
            date_column="information_date",
        )
        #
    @ppf.cached_property
    def feature_columns(self) -> List[str]:
        """Columnas agregadas por nivel (incluye `target_*` como etiqueta)."""
        return get_feature_columns(
            self.nodes_with_features,
            exclude_columns=["id", "information_date", "tfrom_days"]
                + self.node_array_columns()
                + cvas.EXCLUDE_COLUMNS + cvas.NODE_WEIGHT_COLUMNS,
        )
        #
    def vector_columns(self, level_features_df:DataFrame, level:str) -> List[str]:
        """Columnas de entrada del VectorAssembler del nivel (sin etiquetas `target_*`)."""
        return [c for c in level_features_df.columns
            if c != level
            and not any(c.startswith(prefix) for prefix in cvas.EXCLUDE_PREFIXES)]
        #
    def _suffixed(self, df:DataFrame, key_column:str) -> DataFrame:
        """Renombra todas las columnas (menos la llave) añadiendo `VARIABLE_SUFFIX`."""
        suffix = cvas.VARIABLE_SUFFIX or ""
        if not suffix:
            return df
        return df.select(
            key_column,
            *[col(c).alias(f"{c}{suffix}") for c in df.columns if c != key_column])
        #
    def _compute_level_features(self, level:str) -> DataFrame:
        """Agrega las features de nodo al nivel `level` (llave + variables).

        Explode el array de ids del nivel sobre los nodos, aplica las
        agregaciones de `FEATURE_AGGREGATION` (acepta varias funciones por
        feature -> columnas `{funcion}_{feature}`) ponderadas por
        `AGGREGATION_WEIGHT`, añade `node_count` y el `VARIABLE_SUFFIX`.
        """
        array_column = cvas.NODE_ID_ARRAY_COLUMNS[level]
        exploded = explode_array_column(
            self.nodes_with_features,
            column=array_column,
        ).withColumn(level, col(array_column).cast("string"))
        #
        salt_buckets = getattr(cvas, "ASSEMBLY_SALT_BUCKETS", 0)
        if salt_buckets and salt_buckets > 1:
            # Dos etapas con sal: una llave gigante (cliente/cuenta con miles
            # de nodos) no concentra todo el shuffle en un reducer.
            aggregated = salted_assembly_groupby(
                exploded,
                group_column=level,
                feature_columns=self.feature_columns,
                aggregation=cvas.FEATURE_AGGREGATION,
                weight=cvas.AGGREGATION_WEIGHT,
                salt_buckets=salt_buckets,
                id_column="id",
            )
        else:
            aggregations = build_aggregation_expressions(
                self.feature_columns,
                aggregation=cvas.FEATURE_AGGREGATION,
                weight=cvas.AGGREGATION_WEIGHT,
            )
            aggregated = (exploded
                .groupBy(level)
                .agg(*(aggregations + [countDistinct("id").alias("node_count")])))
        return self._suffixed(aggregated, level)
        #
    def level_features_property(self, level:str):
        """Tabla `{level}_features` persistida (reload-or-recompute); devuelve
        `(df, load_kwargs)` como el decorador particionado."""
        return self.get_cached_decorated_table_or_parquet_property(
            config_dict_key=f"{level}_features",
            input_or_output="output",
            method=lambda *a, **k: self._compute_level_features(level),
            property_name=f"{level}_features",
        )
        #
    def level_features(self, level:str) -> DataFrame:
        """DataFrame de `{level}_features` (llave + todas las variables)."""
        return self.level_features_property(level)[0]
        #
    def level_features_vector_property(self, level:str):
        """Tabla final `{level}_features_vector`: SOLO llave + columna vector.

        Devuelve `(df, load_kwargs)` como el decorador particionado.
        """
        def compute(*args, **kwargs) -> DataFrame:
            features_df = self.level_features(level)
            vector_columns = self.vector_columns(features_df, level)
            df = (self.level_pivot(level)
                .join(features_df, on=level, how="left"))
            #
            if cvas.FILL_NULLS_VALUE is not None:
                df = apply_missing_treatment(
                    df, {c: ["value", cvas.FILL_NULLS_VALUE] for c in vector_columns}
                )
            #
            return (assemble_vector(
                df,
                feature_columns=vector_columns,
                output_column=cvas.VECTOR_OUTPUT_COLUMN,
                handle_invalid=cvas.HANDLE_INVALID,
            )
            .select(level, cvas.VECTOR_OUTPUT_COLUMN))
        #
        return self.get_cached_decorated_table_or_parquet_property(
            config_dict_key=f"{level}_features_vector",
            input_or_output="output",
            method=compute,
            property_name=f"{level}_features_vector",
        )
        #
    def level_features_vector(self, level:str) -> DataFrame:
        """DataFrame final del nivel: llave + `features` (vector)."""
        return self.level_features_vector_property(level)[0]
        #
    # ------------------------------------------------------------------
    # Aliases back-compat a nivel numcliente
    # ------------------------------------------------------------------
    @ppf.cached_property
    def numcliente_features(self) -> tuple:
        """Features agregadas a nivel numcliente `(df, load_kwargs)`."""
        return self.level_features_property("numcliente")
        #
    @ppf.cached_property
    def numcliente_features_vector(self) -> tuple:
        """Vector ensamblado a nivel numcliente `(df, load_kwargs)`."""
        return self.level_features_vector_property("numcliente")
