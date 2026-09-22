
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
    date_format
)
from pyspark.sql.types import  StringType
# ------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------
import libs.framework as ppf
import pipelines.graph_making.special_treatment as p_gm_sp
import config.graph_making.ceps.special_treatment as cgmcst
import config.job as cj

##########################################################################
# CLASSES
##########################################################################


class CepsSpecialTreatment(p_gm_sp.StandardSpecialTreatment):
    """Step de *special treatment* para el grafo CEPS.
    """
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
            .transform(self.calculate_tfroms)
        )
        return input_table_historic
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="missing_treatment")
    def missing_treatment(self) -> DataFrame:
        """Aplica el tratamiento de faltantes y resuelve los ids de los nodos.
        """
        s264_ceps_flattened_ranks: DataFrame = self.s264_ceps_flattened_ranks
        missing_treatment = (
            s264_ceps_flattened_ranks
            .transform(lambda df_: self.standard_missing_treatment(df_,  cgmcst.STANDARD_MISSING_TREATMENT_VARS))
            # Remove 0 values from nom_src
            .withColumn("nom_src", when(col("nom_src") == 0, None).otherwise(col("nom_src")))
            .withColumn("numcliente", when(col("numcliente") == 0, None).otherwise(col("numcliente")))
            .withColumn("numcliente", when(col("numcliente") == 1, None).otherwise(col("numcliente")))
            #
            # CUSTOM MISSING TREATMENT
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
            #
            # Date treatment
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col("information_date"), cj.DATE_STANDARD_SPARK_FORMAT), cj.DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )
        #
        return missing_treatment
