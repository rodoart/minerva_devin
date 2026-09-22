
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
    explode_array_column, get_feature_columns, assemble_vector
)
from libs.functions.missing_treatment import apply_missing_treatment

import pipelines.features.vector_assembler as p_f_va
import pipelines.graph_making.special_treatment as p_gm_sp

import config.features.ceps.vector_assembler as cvas

import libs.framework as ppf
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
    """Recopila todas las features de nodo, las agrega a numcliente y ensambla
    el vector final sobre el pivote externo.
    """
    def step_action(self) -> Dict[str, Any]:
        """Materializa `numcliente_features_vector` y recoge su salida."""
        return ppf.collect_step_output(self, self.numcliente_features_vector, "numcliente_features_vector")
        #
    @ppf.cached_property
    def pivot(self) -> DataFrame:
        """Pivote externo: lista de numclientes para los que generar vectores."""
        pivot_column = self.input_hive["pivot"].get("pivot_column", cvas.PIVOT_COLUMN)
        return (self.get_input("pivot")
            .select(col(pivot_column).cast("string").alias("numcliente"))
            .distinct())
        #
    @ppf.cached_property
    def nodes(self) -> DataFrame:
        """Nodos del grafo (tabla `nodes` particionada)."""
        return self.get_partitioned_input("nodes")
        #
    @ppf.cached_property
    def feature_tables(self) -> Dict[str, DataFrame]:
        """Todas las tablas de features a nivel nodo (`id`).

        Modos soportados por fuente (config `FEATURE_SOURCES`):
          - "simple": parquet plano.
          - "partitioned": clave de input_hive con mis_date/process_date.
          - "merge_schema": parquet padre con subdirs por parámetros; las filas
            se colapsan a una por `id` (cada variante aporta sus columnas).
        """
        tables:Dict[str, DataFrame] = {}
        # Dedupe: la primera fuente que aporta una columna gana (evita
        # referencias ambiguas al unir tablas con columnas homónimas).
        seen_columns = set(self.nodes.columns)
        for name, source in cvas.FEATURE_SOURCES.items():
            if source["mode"] == "partitioned":
                df = self.get_partitioned_input(source["key"])
            elif source["mode"] == "merge_schema":
                df = self.get_merge_schema_input(source["path"])
            else:
                df = self.get_input(source["key"]) if "key" in source else \
                    self.sqlContext.read.parquet(str(source["path"]))
            #
            feature_columns = [
                c for c in get_feature_columns(
                    df,
                    exclude_columns=["id", "information_date", cvas.NODE_ID_ARRAY_COLUMN]
                        + cvas.EXCLUDE_COLUMNS,
                )
                if c not in seen_columns
            ]
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
        nodes = self.nodes.select(
            "id", cvas.NODE_ID_ARRAY_COLUMN, *list(dict.fromkeys(weight_columns))
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
        """Columnas agregadas a numcliente (incluye `target_*` como etiqueta)."""
        return get_feature_columns(
            self.nodes_with_features,
            exclude_columns=["id", cvas.NODE_ID_ARRAY_COLUMN, "information_date",
                "tfrom_days"] + cvas.EXCLUDE_COLUMNS + cvas.NODE_WEIGHT_COLUMNS,
        )
        #
    @ppf.cached_property
    def vector_columns(self) -> List[str]:
        """Columnas de entrada del VectorAssembler (sin etiquetas `target_*`)."""
        return [c for c in self.feature_columns + ["node_count"]
            if not any(c.startswith(prefix) for prefix in cvas.EXCLUDE_PREFIXES)]
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="numcliente_features")
    def numcliente_features(self) -> DataFrame:
        """Features agregadas a nivel numcliente (media ponderada por defecto).

        Explode el array `numcliente` de los nodos y agrega cada feature con la
        estadística configurada (`FEATURE_AGGREGATION`) usando
        `AGGREGATION_WEIGHT` como peso (monto x decaimiento por antigüedad).
        """
        exploded = explode_array_column(
            self.nodes_with_features,
            column=cvas.NODE_ID_ARRAY_COLUMN,
        ).withColumn("numcliente", col("numcliente").cast("string"))
        #
        aggregations = build_aggregation_expressions(
            self.feature_columns,
            aggregation=cvas.FEATURE_AGGREGATION,
            weight=cvas.AGGREGATION_WEIGHT,
        )
        return (exploded
            .groupBy("numcliente")
            .agg(*(aggregations + [countDistinct("id").alias("node_count")])))
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="numcliente_features_vector")
    def numcliente_features_vector(self) -> DataFrame:
        """Pivote + features a nivel numcliente + columna `features` (vector)."""
        df = (self.pivot
            .join(self.numcliente_features[0], on="numcliente", how="left"))
        #
        if cvas.FILL_NULLS_VALUE is not None:
            df = apply_missing_treatment(
                df, {c: ["value", cvas.FILL_NULLS_VALUE] for c in self.vector_columns}
            )
        #
        return assemble_vector(
            df,
            feature_columns=self.vector_columns,
            output_column=cvas.VECTOR_OUTPUT_COLUMN,
            handle_invalid=cvas.HANDLE_INVALID,
        )
