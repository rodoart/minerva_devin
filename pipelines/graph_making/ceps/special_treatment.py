
##########################################################################
# LIBRARIES
##########################################################################

# ------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------
from typing import Dict, Any

# ------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (col, to_date, when, lit, concat_ws,
    date_format, row_number
)
from pyspark.sql.types import  StringType
from pyspark.sql.window import Window
# ------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------
import libs.framework as ppf
import pipelines.graph_making.special_treatment as p_gm_sp
import config.graph_making.ceps.special_treatment as cgmcst
import config.job as cj

##########################################################################
# FUNCTIONS
##########################################################################


def join_mapeo_cta_numcliente(df: DataFrame, mapeo: DataFrame) -> DataFrame:
    """Imputa `numcliente_src`/`numcliente_dst` uniendo el mapeo cta -> numcliente.

    El mapeo (una fila por `cta`, ya deduplicado) se une dos veces: por
    `cta_src` y por `cta_dst`. Las cuentas sin mapeo quedan con numcliente
    nulo en ese lado (left join): el nodo del grafo cta existe igualmente,
    solo no aporta cliente al agregado por nivel.
    """
    return (df
        .join(mapeo.select(
                col("cta").alias("cta_src"),
                col("numcliente").alias("numcliente_src")),
            on="cta_src", how="left")
        .join(mapeo.select(
                col("cta").alias("cta_dst"),
                col("numcliente").alias("numcliente_dst")),
            on="cta_dst", how="left"))


def resolve_cta_node_ids(df: DataFrame) -> DataFrame:
    """Resuelve los ids de nodo de la variante cta: la cuenta en crudo es el id.

    A diferencia de la variante rfc (id = RFC/CURP imputado), aquí el nodo es
    la propia cuenta (`cta_src`/`cta_dst`), con fallback al nombre
    (`nom_<nom>`) cuando la cuenta falta; si tampoco hay nombre, la
    transacción se descarta (`to_drop_*`). El RFC no interviene como id.
    """
    return (df
        .withColumn("id_src",
            when(col("cta_src").isNotNull(), col("cta_src").cast(StringType()))
            .when(col("nom_src").isNotNull(), concat_ws("_", lit("nom"), col("nom_src")))
            .otherwise(lit(None).cast(StringType())))
        .withColumn("to_drop_ord", when(col("id_src").isNull(), 1).otherwise(0))
        .withColumn("id_dst",
            when(col("cta_dst").isNotNull(), col("cta_dst").cast(StringType()))
            .when(col("nom_dst").isNotNull(), concat_ws("_", lit("nom"), col("nom_dst")))
            .otherwise(lit(None).cast(StringType())))
        .withColumn("to_drop_ben", when(col("id_dst").isNull(), 1).otherwise(0))
        .filter((col("to_drop_ord") == 0) & (col("to_drop_ben") == 0)))


def dedupe_mapeo_cta_numcliente(mapeo: DataFrame) -> DataFrame:
    """Deduplica el mapeo cta -> numcliente leído de la ventana completa.

    Cada partición mensual del mapeo trae un candidato por cuenta; al leer
    varios vintages puede haber varios. Gana la partición más reciente
    (`mis_date` yyyyMM desc) y, a empate, el candidato con más transacciones
    y más reciente (mismo criterio que construyó cada partición).
    """
    window = Window.partitionBy("cta").orderBy(
        col("mis_date").desc(),
        col("txn_count").desc(),
        col("max_fec_informacion").desc(),
        col("numcliente").asc(),
    )
    return (mapeo
        .withColumn("_rank", row_number().over(window))
        .filter(col("_rank") == 1)
        .select("cta", "numcliente"))


##########################################################################
# CLASSES
##########################################################################


class CepsSpecialTreatment(p_gm_sp.StandardSpecialTreatment):
    """Step de *special treatment* para el grafo CEPS.

    Acepta `variant` ("rfc" por defecto, conserva el comportamiento
    histórico; "cta" resuelve los nodos por cuenta e imputa el numcliente
    vía `mapeo_cta_numcliente`). La variante solo cambia la resolución de
    ids del substep y las rutas (namespaced en config); el resto de la
    cadena es compartido.
    """
    def __init__(self, *args, variant: str = "rfc", **kwargs) -> None:
        self.variant = variant
        super().__init__(*args, **kwargs)
        if variant != "rfc":
            # step_name único por variante: los logs y output_parameters no
            # confunden las dos cadenas (mismo prefijo + vintage por defecto).
            self.step_name = f"{self.step_name}_{variant}"
        #
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta el substep de extracción y colecta su salida.
        """
        return ppf.run_substep_and_collect(self, self.ceps_extract_step, "ceps_extract_step")
        #
    @ppf.cached_property
    def ceps_extract_step(self) -> "CepsExtractSubStep":
        """Substep de extracción CEPS (instanciado y cacheado).
        """
        return CepsExtractSubStep(self)


class CepsExtractSubStep(p_gm_sp.StandardExtractSubStep):
    """Substep que prepara los nodos del grafo CEPS con *missing treatment*.
    """
    def step_action(self) -> Dict[str, Any]:
        """Colecta el DataFrame tratado bajo la clave ""missing_treatment"".
        """
        return ppf.collect_step_output(self, self.missing_treatment, "missing_treatment")
        #
    @ppf.cached_property
    def s264_ceps_flattened_ranks(self) -> DataFrame:
        """Historial CEP aplanado y rankeado, con *tfroms* calculados.
        """
        input_table_historic: DataFrame = (self.input_table_historic("s264_ceps_flattened_rank_rfc_by_cta_cases_replace")
            .transform(lambda df_: self.calculate_tfroms(df_, "s264_ceps_flattened_rank_rfc_by_cta_cases_replace"))
        )
        return input_table_historic
        #
    @ppf.cached_property
    def mapeo_cta_numcliente(self) -> DataFrame:
        """Mapeo cta -> numcliente deduplicado (una fila por cuenta).

        Solo existe en la variante cta (clave del input de config); se lee la
        ventana completa del grafo y gana la partición más reciente.
        """
        return dedupe_mapeo_cta_numcliente(
            self.input_table_historic("mapeo_cta_numcliente"))
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="missing_treatment")
    def missing_treatment(self) -> DataFrame:
        """Aplica el tratamiento de faltantes y resuelve los ids de los nodos.
        """
        variant = getattr(self, "variant", None) \
            or getattr(getattr(self, "parent", None), "variant", "rfc")
        s264_ceps_flattened_ranks: DataFrame = self.s264_ceps_flattened_ranks
        missing_treatment = (
            s264_ceps_flattened_ranks
            .transform(lambda df_: self.standard_missing_treatment(df_,  cgmcst.STANDARD_MISSING_TREATMENT_VARS))
            # Remove 0 values from nom_src
            .withColumn("nom_src", when(col("nom_src") == 0, None).otherwise(col("nom_src")))
            .withColumn("numcliente", when(col("numcliente") == 0, None).otherwise(col("numcliente")))
            .withColumn("numcliente", when(col("numcliente") == 1, None).otherwise(col("numcliente")))
        )
        #
        if variant == "cta":
            # CUSTOM MISSING TREATMENT (variante cta): el nodo es la cuenta;
            # el numcliente se imputa por lado con el mapeo y viaja como
            # numcliente_src/numcliente_dst hasta el group_by (donde pasa a
            # ser el `numcliente` del nodo; ver ccgb.column_spec).
            missing_treatment = (missing_treatment
                .transform(lambda df_: join_mapeo_cta_numcliente(df_, self.mapeo_cta_numcliente))
                .transform(resolve_cta_node_ids)
            )
        else:
            # CUSTOM MISSING TREATMENT (variante rfc, histórica): el nodo es
            # el RFC/CURP imputado, con fallback a numcliente / cta_ / nom_.
            missing_treatment = (
                missing_treatment
                # ordenante
                .withColumn("id_src",
                    when((col("id_src").isNull()) & (col("cve_tipo_orden") == "E") & (col("numcliente").isNotNull()), col("numcliente"))
                    .when((col("id_src").isNull()) & ((col("cta_src").isNotNull())), concat_ws("_", lit("cta"), col("cta_src")))
                    .when((col("id_src").isNull()) & ((col("nom_src").isNotNull())), concat_ws("_", lit("nom"), col("nom_src")))
                    .otherwise(col("id_src"))
                )
                .withColumn("to_drop_ord", when((col("id_src").isNull()) & ((col("cta_src").isNull()) & (col("id_src").isNull()) & (col("nom_src").isNull())), 1).otherwise(0))
                # beneficiario
                .withColumn("id_dst",
                    when((col("id_dst").isNull()) & (col("cve_tipo_orden") == "R") & col("numcliente").isNotNull(), col("numcliente"))
                    .when((col("id_dst").isNull()) & ((col("cta_dst").isNotNull())), concat_ws("_", lit("cta"), col("cta_dst")))
                    .when((col("id_dst").isNull()) & ((col("nom_dst").isNotNull())), concat_ws("_", lit("nom"), col("nom_dst")))
                    .otherwise(col("id_dst"))
                )
                .withColumn("to_drop_ben", when((col("id_dst").isNull()) & ((col("cta_dst").isNull()) & (col("id_dst").isNull()) & (col("nom_dst").isNull())), 1).otherwise(0))
                # DROP
                .filter((col("to_drop_ord") == 0) & (col("to_drop_ben") == 0))
            )
        #
        missing_treatment = (
            missing_treatment
            # Date treatment
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col("information_date"), cj.DATE_STANDARD_SPARK_FORMAT), cj.DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )
        #
        return missing_treatment
