##########################################################################
# LIBRARIES
##########################################################################

# ----------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------
from typing import Dict, Any

# ----------------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import col, max as spark_max
# ----------------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------------
import libs.framework as ppf

import pipelines.target_propagation.special_treatment as p_tp_st

import config.target_propagation.lovelace.special_treatment as ctp_st

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)

from libs.data_engineering_toolbox.path import HivePath
##########################################################################
# CLASSES
##########################################################################

class LovelaceTargetPropagationSpecialTreatmentStep(p_tp_st.StandardTargetPropagationSpecialTreatment):
    """Tratamiento especial del target Lovelace para la propagación por el grafo.
    """
    #
    def __init__(self,
        date_treatment: Dict[str,str],
        input_hive:Dict[str, HivePath],
        output_hive:Dict[str,HivePath],
        cohort:str,
        is_dynamic: bool = True,
        *args, **kwargs
    ) -> None:
        # Delega en StandardSpecialTreatment.__init__ con SU firma
        # (él se encarga de build_step_init_kwargs internamente).
        super().__init__(
            date_treatment=date_treatment,
            input_hive=input_hive,
            output_hive=output_hive,
            cohort=cohort,
            is_dynamic=is_dynamic,
            *args, **kwargs,
        )

    def step_action(self) -> Dict[str, Any]:
        """Ejecuta la sub-etapa de extracción Lovelace y recoge su salida."""
        return ppf.run_substep_and_collect(self, self.lovelace_extract_step, "lovelace_extract_step")
    #
    @ppf.cached_property
    def lovelace_extract_step(self) -> "LovelaceExtractSubStep":
        """Sub-etapa de extracción del target Lovelace.
        """
        return LovelaceExtractSubStep(self)



class LovelaceExtractSubStep(p_tp_st.StandardTargetPropagationExtractSubStep):
    """Extrae el target Lovelace y lo agrega a nivel `cta` y `numcliente`.
    """
    def step_action(self) -> Dict[str, Any]:
        """Materializa `target_cta` (y `target_numcliente`) y recoge su salida."""
        return ppf.collect_step_output(self, self.target_cta, "target_cta")
    #
    @ppf.cached_property
    def lovelace_target(self) -> DataFrame:
        """Histórico del target Lovelace con columnas tfrom calculadas.
        """
        input_table_historic: DataFrame = (self.input_table_historic("lovelace_target")
            .transform(lambda df_: self.calculate_tfroms(df_, "lovelace_target"))
        )
        return input_table_historic
    #
    #
    @ppf.cached_property
    def target_aggregations(self) -> Dict[str, Any]:
        """Columnas del target a propagar (config `TARGET_AGGREGATIONS`), acotadas
        a las presentes en `lovelace_target`. `target` es obligatoria; el resto
        ausentes solo avisan."""
        lovelace_target: DataFrame = self.lovelace_target
        aggregations = {
            column: func
            for column, func in ctp_st.TARGET_AGGREGATIONS.items()
            if column in lovelace_target.columns
        }
        missing = [c for c in ctp_st.TARGET_AGGREGATIONS if c not in aggregations]
        for column in missing:
            logger.warning(
                "Columna de target '%s' no existe en lovelace_target; se omite",
                column)
        if "target" not in aggregations:
            raise ValueError(
                "La columna obligatoria 'target' no está en lovelace_target")
        return aggregations
    #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="target_numcliente")
    def target_numcliente(self) -> DataFrame:
        """Target Lovelace agregado a nivel `numcliente` (todas las columnas de `TARGET_AGGREGATIONS`).
        """
        lovelace_target: DataFrame = self.lovelace_target
        return (lovelace_target
            .groupBy("numcliente")
            .agg(*[func(col(column)).alias(column)
                   for column, func in self.target_aggregations.items()])
        )
    #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="target_cta")
    def target_cta(self) -> DataFrame:
        """Target Lovelace agregado a nivel `cta` (todas las columnas de `TARGET_AGGREGATIONS`).
        """
        _ = self.target_numcliente
        lovelace_target: DataFrame = self.lovelace_target
        return (lovelace_target
            .groupBy("cta")
            .agg(*[func(col(column)).alias(column)
                   for column, func in self.target_aggregations.items()])
        )
        #
