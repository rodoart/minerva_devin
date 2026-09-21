##########################################################################
# SPECIAL TREATMENT
##########################################################################

# ----------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------
from typing import List, Union, Dict, Any, Callable, Optional

# ----------------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (coalesce, col, to_date,
    unix_timestamp, lit, floor, months_between, to_timestamp, mean as spark_mean
)
# ----------------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------------
from data_engineering_toolbox.path import HivePath

import libs.framework as ppf
import config.job as cj
import pipelines.graph_making.special_treatment as p_gm_sp

from importlib import reload

for module in [cj, p_gm_sp]:
    reload(module)
##########################################################################
# FUNCTIONS
##########################################################################

class SubStep(ppf.Step):
    """Clase base para las sub-etapas del pipeline `CepsRfcNomRankingStep`..."""
    def __init__(self, parent: "StandardTargetPropagationSpecialTreatment", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)

class StandardTargetPropagationSpecialTreatment(p_gm_sp.StandardSpecialTreatment):
    """
    """
    def __init__(self,
        date_treatment: Dict[str, str],
        input_hive: Dict[str, HivePath],
        output_hive: Dict[str, HivePath],
        cohort: str,
        is_dynamic: bool = True,
        *args, **kwargs
    ) -> None:
        self.date_treatment = date_treatment
        self.is_dynamic = is_dynamic
        self.cohort = cohort
        super().__init__(
            date_treatment=date_treatment,
            input_hive=input_hive,
            output_hive=output_hive,
            cohort=cohort,
            is_dynamic=is_dynamic,
            *args, **kwargs,
        )
        # For testing purposes only
    @ppf.cached_property
    def standard_target_propagation_extract_step(self) -> StandardTargetPropagationExtractSubStep:
        """
        """
        return StandardTargetPropagationExtractSubStep(self)


class StandardTargetPropagationExtractSubStep(SubStep, p_gm_sp.StandardSpecialTreatmentSubStep):
    #
    #
    pass
