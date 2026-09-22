########################################################################################################################
# Libraries
########################################################################################################################

# ----------------------------------------------------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------------------------------------------------
import logging
from typing import List, Union, Dict
from threading import Lock

write_lock = Lock()

# ----------------------------------------------------------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (col, when, lit)
from pyspark.sql.types import StringType
# ----------------------------------------------------------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath

import libs.framework as ppf

logger = logging.getLogger(__name__)

########################################################################################################################
# FUNCTIONS
########################################################################################################################

########################################################################################################################
# PROCESS
########################################################################################################################


class CepsTxnReplacementStep(ppf.Step):
    """Step orquestador del reemplazo de RFC/CURP faltantes o inválidos en transacciones CEP.

    Usa los casos de reemplazo rankeados por `CepsRfcNomRankingStep` (por cuenta
    y por nombre) para rellenar `rfc_curp`/`rfc_curp_kind` de ordenante y
    beneficiario, y persiste el resultado en
    `rfc_curp_analysis_s264_ceps_replaced`.
    """
    def __init__(self,
        date_treatment: Dict[str,str],
        input_hive:Dict[str, HivePath],
        output_hive:Dict[str,HivePath],
        cohort: str,
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
            step_name_prefix="Ceps_Txn_Replacement_Step",
            extra_kwargs=kwargs,
        )
        super().__init__(*args, **super_class_kwargs)
    #
    def step_action(self) -> Dict[str, dict]:
        """Colecta el DataFrame final con RFC/CURP reemplazados (`rfc_curp_analysis_s264_ceps_replaced`)."""
        return ppf.collect_step_output(self, self.rfc_curp_analysis_s264_ceps_replaced, "rfc_curp_analysis_s264_ceps_replaced")
    #
    def standard_load_parquet_or_table(self, config_dict_key, input_or_output:str = "input") -> DataFrame:
        """Carga estándar de una tabla/parquet particionado con validación de historial.

        Args:
            config_dict_key: Clave de la tabla dentro de `input_hive`/`output_hive`.
            input_or_output: "input" lee de `input_hive`; otro valor usa `output_hive`.

        Returns:
            DataFrame cargado para la fecha `vintage_date` de los parámetros de entrada.
        """
        config_dict = self.input_hive if input_or_output == "input" else self.output_hive
        return ppf.standard_load_parquet_or_table(
            config_dict=config_dict,
            config_dict_key=config_dict_key,
            current_date=self.input_parameters['vintage_date'],
            session=self.sqlContext,
        )
    #
    # @ppf.dynamic_unpartitioned_parquet(path_key="s264_ceps_flattened_rank_rfc_by_cta_cases_replace")
    @ppf.cached_property
    def s264_ceps_flattened_rank_rfc_by_cta_cases_replace(self) -> DataFrame:
        """Casos de reemplazo por cuenta (parquet no particionado del pipeline de ranking)."""
        return (self.sqlContext.read.parquet(str(self.input_hive["s264_ceps_flattened_rank_rfc_by_cta_cases_replace"]["table_or_hdfs"])))
    #
    #
    # @ppf.dynamic_unpartitioned_parquet(path_key="s264_ceps_flattened_rank_rfc_by_nom_cases_replace")
    @ppf.cached_property
    def s264_ceps_flattened_rank_rfc_by_nom_cases_replace(self) -> DataFrame:
        """Casos de reemplazo por nombre (parquet no particionado del pipeline de ranking)."""
        return (self.sqlContext.read.parquet(str(self.input_hive["s264_ceps_flattened_rank_rfc_by_nom_cases_replace"]["table_or_hdfs"])))
    #
    @ppf.cached_property
    def rfc_curp_analysis_s264_ceps(self) -> DataFrame:
        """Análisis RFC/CURP de CEPs depurado para el reemplazo.

        Elimina `tfrom` y los flags auxiliares `is_*` de ordenante/beneficiario
        y renombra `id_kind_*` a `rfc_curp_kind_*`.
        """
        return (self.standard_load_parquet_or_table("rfc_curp_analysis_s264_ceps", input_or_output="input")
            .drop("tfrom")
            .drop(*['is_curp_ord', 'is_rfc_fisica_ord', 'is_rfc_moral_ord',
                'is_rfc_fisica_sin_homoclave_ord', 'is_tc_ord', 'is_clabe_ord',
                'is_any_valid_ord', 'is_curp_ben', 'is_rfc_fisica_ben',
                'is_rfc_moral_ben', 'is_rfc_fisica_sin_homoclave_ben',
                'is_tc_ben', 'is_clabe_ben', 'is_any_valid_ben']
            )
            .withColumnRenamed("id_kind_ord", "rfc_curp_kind_ord")
            .withColumnRenamed("id_kind_ben", "rfc_curp_kind_ben")
        )
    #
    @ppf.cached_property
    def replacement_rfc_by_cta_id_ban_nom(self) -> DataFrame:
        """Reemplazo prioridad 1: mejor RFC por (cta, id_ban, nom), con nombre y banco no nulos."""
        s264_ceps_flattened_rank_rfc_by_cta_cases_replace: DataFrame = self.s264_ceps_flattened_rank_rfc_by_cta_cases_replace
        return (s264_ceps_flattened_rank_rfc_by_cta_cases_replace
            .filter(col("nom").isNotNull())
            .filter(col("id_ban").isNotNull())
            .filter(col("_rank_by_nom_id_ban") == 1)
            .drop("_rank_by_id_ban", "_rank_by_nom_id_ban", "rank_rfc_by_cta")
            .distinct()
            .withColumn("tag_rep_src", lit("1_cta_id_ban_nom"))
        )
    #
    @ppf.cached_property
    def replacement_rfc_by_cta_id_ban(self) -> DataFrame:
        """Reemplazo prioridad 2: mejor RFC por (cta, id_ban)."""
        s264_ceps_flattened_rank_rfc_by_cta_cases_replace: DataFrame = self.s264_ceps_flattened_rank_rfc_by_cta_cases_replace
        return (s264_ceps_flattened_rank_rfc_by_cta_cases_replace
            .filter(col("_rank_by_id_ban") == 1)
            .drop("_rank_by_id_ban", "_rank_by_nom_id_ban", "rank_rfc_by_cta")
            .distinct()
            .withColumn("tag_rep_src", lit("2_cta_id_ban"))
        )
    #
    @ppf.cached_property
    def replacement_rfc_by_cta(self) -> DataFrame:
        """Reemplazo prioridad 3: mejor RFC por cuenta (`cta`)."""
        s264_ceps_flattened_rank_rfc_by_cta_cases_replace: DataFrame = self.s264_ceps_flattened_rank_rfc_by_cta_cases_replace
        return (s264_ceps_flattened_rank_rfc_by_cta_cases_replace
            .filter(col("rank_rfc_by_cta") == 1)
            .distinct()     # para quedarnos con pares únicos cta, id_ban
            .drop("_rank_by_id_ban", "_rank_by_nom_id_ban", "rank_rfc_by_cta")
            .distinct()     # para quedarnos con pares únicos cta, id_ban
            .withColumn("tag_rep_src", lit("3_cta"))
        )
    #
    @ppf.cached_property
    def replacement_rfc_by_nom_id_ban(self) -> DataFrame:
        """Reemplazo prioridad 4: mejor RFC por (nom, id_ban), con nombre y banco no nulos."""
        s264_ceps_flattened_rank_rfc_by_nom_cases_replace: DataFrame = self.s264_ceps_flattened_rank_rfc_by_nom_cases_replace
        return (s264_ceps_flattened_rank_rfc_by_nom_cases_replace
            .filter(col("nom").isNotNull())
            .filter(col("id_ban").isNotNull())
            .filter(col("_rank_by_id_ban") == 1)
            .drop("_rank_by_id_ban", "rank_rfc_by_nom")
            .distinct()
            .withColumn("tag_rep_src", lit("4_nom_id_ban"))
        )
    #
    @ppf.cached_property
    def replacement_rfc_by_nom(self) -> DataFrame:
        """Reemplazo prioridad 5: mejor RFC por nombre (`nom`)."""
        s264_ceps_flattened_rank_rfc_by_nom_cases_replace: DataFrame = self.s264_ceps_flattened_rank_rfc_by_nom_cases_replace
        return (s264_ceps_flattened_rank_rfc_by_nom_cases_replace
            .filter(col("rank_rfc_by_nom") == 1)
            .distinct()     # para quedarnos con pares únicos cta, id_ban
            .drop("_rank_by_id_ban", "rank_rfc_by_nom")
            .distinct()     # para quedarnos con pares únicos cta, id_ban
            .withColumn("tag_rep_src", lit("5_nom"))
        )
    #
    @ppf.cached_property
    def replacements(self) -> Dict[str, Dict[str, Union[DataFrame, List[str]]]]:
        """Diccionario de fuentes de reemplazo con sus claves de join.

        Returns
        -------
        Dict[str, Dict[str, Union[DataFrame, List[str]]]]
            Mapa nombre_fuente -> {"df": DataFrame, "join_keys": [...]}, en
            orden de prioridad decreciente: (cta, id_ban, nom), (cta, id_ban),
            (cta), (nom, id_ban) y (nom).
        """
        replacement_rfc_by_cta_id_ban_nom:DataFrame = self.replacement_rfc_by_cta_id_ban_nom
        replacement_rfc_by_cta_id_ban:DataFrame = self.replacement_rfc_by_cta_id_ban
        replacement_rfc_by_cta:DataFrame = self.replacement_rfc_by_cta
        replacement_rfc_by_nom_id_ban:DataFrame = self.replacement_rfc_by_nom_id_ban
        replacement_rfc_by_nom:DataFrame = self.replacement_rfc_by_nom
        return {"fc_by_cta_id_ban_nom": {
            "df": replacement_rfc_by_cta_id_ban_nom,
            "join_keys": ["cta", "id_ban", "nom"]
        },
        "fc_by_cta_id_ban": {
            "df": replacement_rfc_by_cta_id_ban,
            "join_keys": ["cta", "id_ban"]
        },
        "fc_by_cta": {
            "df": replacement_rfc_by_cta,
            "join_keys": ["cta"]
        },
        "fc_by_nom_id_ban": {
            "df": replacement_rfc_by_nom_id_ban,
            "join_keys": ["nom", "id_ban"]
        },
        "fc_by_nom": {
            "df": replacement_rfc_by_nom,
            "join_keys": ["nom"]
        }
        }
    #
    def replace_rfc_with_ceps_ranked(self, input_df:DataFrame) -> DataFrame:
        """Sustituye RFC/CURP nulos o inválidos usando las fuentes de reemplazo priorizadas.

        Para cada lado de la transacción ("ben", "ord") recorre
        `self.replacements` en orden, hace left join por sus `join_keys` y
        rellena `rfc_curp_*`, `rfc_curp_kind_*`, `cta_*`, `nom_*`, `id_ban_*` y
        `tag_rep_src_*` cuando el RFC original es nulo o de tipo "invalid" y el
        candidato es válido. Cada 3 joins hace checkpoint a parquet en
        `tmp_replaced_joined_hdfs` para cortar el linaje (reutilizándolo si ya
        existe).

        Args:
            input_df: DataFrame con columnas `rfc_curp_{ben,ord}` y
                `rfc_curp_kind_{ben,ord}`.

        Returns:
            DataFrame con los RFC/CURP reemplazados y trazabilidad de la fuente
            en `tag_rep_src_ben`/`tag_rep_src_ord`.
        """
        replaced_joined_hdfs: HivePath = HivePath(str(self.output_hive["tmp_replaced_joined_hdfs"]["table_or_hdfs"]))
        replacements = self.replacements
        #
        df_replaced = (input_df
            .withColumn("tag_rep_src_ben", lit(None).cast(StringType()))
            .withColumn("tag_rep_src_ord", lit(None).cast(StringType()))
        )
        #
        skip_saves = 3
        current_save = 0
        #
        for suffix in ["ben", "ord"]:
            logger.info("Processing suffix: %s", suffix)
            for key, replacement in replacements.items():
                tmp_parquet_hdfs = replaced_joined_hdfs.joinpath(f"{key}_{suffix}.parquet")
                current_save += 1
                try:
                    if current_save % skip_saves != 0:
                        raise Exception("Force save to avoid skipping")
                    df_replaced = self.sqlContext.read.parquet(str(tmp_parquet_hdfs))
                    logger.info("Loaded cached replacement: %s", key)
                except:
                    logger.info("Processing replacement: %s", key)
                    replacement_column_renames = [col(column).alias(f"{column}_{suffix}") if column in replacement["join_keys"] else col(column).alias(f"{column}_{suffix}_rep") for column in replacement["df"].columns]
                    replacement_join_keys_renamed = [ f"{column}_{suffix}" for column in replacement["join_keys"]]
                    replacement_df = replacement["df"].select(*replacement_column_renames)
                    df_replaced = (df_replaced
                        .join(
                            replacement_df,
                            on=replacement_join_keys_renamed,
                            how="left"
                        )
                        .withColumn(f"replacement_condition_rfc_{suffix}", when(
                            (col(f"rfc_curp_{suffix}").isNull() & col(f"rfc_curp_{suffix}_rep").isNotNull()),
                            1
                        )
                        .when((col(f"rfc_curp_kind_{suffix}")=="invalid") & ((col(f"rfc_curp_kind_{suffix}_rep")!="invalid") & (col(f"rfc_curp_{suffix}_rep").isNotNull()))
                        , 1)
                        .otherwise(0)
                        )
                    )
                    if f"cta_{suffix}_rep" in df_replaced.columns:
                        df_replaced = (df_replaced
                            .withColumn(f"cta_{suffix}", when((col(f"replacement_condition_rfc_{suffix}") == 1) & (col(f"cta_{suffix}_rep").isNotNull()), col(f"cta_{suffix}_rep")).otherwise(col(f"cta_{suffix}")))
                        )
                    if f"nom_{suffix}_rep" in df_replaced.columns:
                        df_replaced = (df_replaced
                            .withColumn(f"nom_{suffix}", when((col(f"replacement_condition_rfc_{suffix}") == 1) & (col(f"nom_{suffix}_rep").isNotNull()), col(f"nom_{suffix}_rep")).otherwise(col(f"nom_{suffix}")))
                        )
                    if f"id_ban_{suffix}_rep" in df_replaced.columns:
                        df_replaced = (df_replaced
                            .withColumn(f"id_ban_{suffix}", when((col(f"replacement_condition_rfc_{suffix}") == 1) & (col(f"id_ban_{suffix}_rep").isNotNull()), col(f"id_ban_{suffix}_rep")).otherwise(col(f"id_ban_{suffix}")))
                        )
                    if f"rfc_curp_{suffix}_rep" in df_replaced.columns:
                        df_replaced = (df_replaced
                            .withColumn(f"rfc_curp_{suffix}", when(col(f"replacement_condition_rfc_{suffix}") == 1, col(f"rfc_curp_{suffix}_rep")).otherwise(col(f"rfc_curp_{suffix}")))
                        )
                    if f"rfc_curp_kind_{suffix}_rep" in df_replaced.columns:
                        df_replaced = (df_replaced
                            .withColumn(f"rfc_curp_kind_{suffix}", when(col(f"replacement_condition_rfc_{suffix}") == 1, col(f"rfc_curp_kind_{suffix}_rep")).otherwise(col(f"rfc_curp_kind_{suffix}")))
                        )
                    if f"tag_rep_src_{suffix}" in df_replaced.columns:
                        df_replaced = (df_replaced
                            .withColumn(f"tag_rep_src_{suffix}", when(col(f"replacement_condition_rfc_{suffix}") == 1, col(f"tag_rep_src_{suffix}_rep")).otherwise(col(f"tag_rep_src_{suffix}")))
                        )
                    df_replaced = df_replaced.drop(
                        *([column for column in df_replaced.columns if column.endswith("_rep")] + [f"replacement_condition_rfc_{suffix}"])
                    )
                    if current_save % skip_saves == 0:
                        (df_replaced
                            .write
                            .mode("overwrite")
                            .partitionBy("process_date", "mis_date", "fec_informacion")
                            .parquet(str(tmp_parquet_hdfs))
                        )
                        df_replaced = self.sqlContext.read.parquet(str(tmp_parquet_hdfs))
                        logger.info("Saved replacement: %s to %s", key, tmp_parquet_hdfs)
        return df_replaced
    #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="rfc_curp_analysis_s264_ceps_replaced")
    def rfc_curp_analysis_s264_ceps_replaced(self) -> DataFrame:
        """DataFrame final con los RFC/CURP reemplazados y las columnas `vintage`/`cohort`."""
        rfc_curp_analysis_s264_ceps:DataFrame = self.rfc_curp_analysis_s264_ceps
        return (
            self.replace_rfc_with_ceps_ranked(rfc_curp_analysis_s264_ceps)
            .withColumn("vintage", lit(self.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )






# def ceps_flattened_rank_rfc_by_nom_and_cta_cases(self) -> "CepsFlattenedRankRfcByNomAndCtaCases":
#     """Substep de extracción/preparación (lazy, cacheado).
#
#     Instancia `CepsExtractStep` enlazado a este step padre. Primera fase del
#     pipeline: carga cruda, limpieza, clasificación de identificadores y
#     aplanado a formato largo. Al ser `cached_property`, se crea una única vez
#     por ejecución.
#
#     Returns
#     -------
#     CepsExtractStep
#         Substep de extracción listo para materializar `s264_ceps_flattened`.
#     """
#     return CepsFlattenedRankRfcByNomAndCtaCases(self,
#         # previous_step=[self.ceps_extract_step]
#     )
#
