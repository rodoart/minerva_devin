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

from pyspark.sql.functions import max as spark_max
# ----------------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------------
import libs.framework as ppf

import pipelines.target_propagation.special_treatment as p_tp_st


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
            .transform(self.calculate_tfroms)
        )
        return input_table_historic
    #
    #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="target_numcliente")
    def target_numcliente(self) -> DataFrame:
        """Target Lovelace agregado a nivel `numcliente` (max de `target`).
        """
        lovelace_target: DataFrame = self.lovelace_target
        return (lovelace_target
            .groupBy("numcliente")
            .agg(spark_max("target").alias("target"))
        )
    #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="target_cta")
    def target_cta(self) -> DataFrame:
        """Target Lovelace agregado a nivel `cta` (max de `target`).
        """
        _ = self.target_numcliente
        lovelace_target: DataFrame = self.lovelace_target
        return (lovelace_target
            .groupBy("cta")
            .agg(spark_max("target").alias("target"))
        )
        #
