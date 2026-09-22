
###############################################################################
# VECTOR ASSEMBLER
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Union, Dict


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame

# ------------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath

import libs.framework as ppf
###############################################################################
# Process
###############################################################################


class SubStep(ppf.Step):
    """Clase base para las sub-etapas del ensamblado del vector de features.
    """
    def __init__(self, parent: "StandardVectorAssemblerStep", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)


class StandardVectorAssemblerStep(ppf.Step):
    """Etapa estándar de ensamblado del vector `features` a nivel numcliente.
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
            step_name_prefix="Standard_Vector_Assembler_Step",
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
    def standard_assembler_step(self) -> "StandardVectorAssemblerSubStep":
        """Sub-etapa de ensamblado del vector de features (solo para tests).
        """
        return StandardVectorAssemblerSubStep(self)


class StandardVectorAssemblerSubStep(SubStep):
    """Substep base del ensamblado: carga de inputs y helpers de features.
    """
    # previous_step
    def get_input(self,
        config_dict_key:str,
        input_or_output:str = "input",
        *args, **kwargs) -> DataFrame:
        """Carga simple (parquet plano) desde input_hive/output_hive."""
        property_path = (self.input_hive[config_dict_key]["table_or_hdfs"]
            if input_or_output == "input"
            else self.output_hive[config_dict_key]["table_or_hdfs"])
        property_path_str = str(property_path)   # <- congela el str AQUI
        #
        def simple_load(*args, **kwargs) -> DataFrame:
            return self.sqlContext.read.parquet(property_path_str)   # <- siempre str
        #
        return self.get_cached_decorated_table_or_parquet_property(
            path=property_path,
            input_or_output=input_or_output,      # <- respeta el argumento, no hardcode "input"
            method=simple_load,
        )
        #
    def get_partitioned_input(self, config_dict_key:str) -> DataFrame:
        """Carga particionada (mis_date/process_date) con lag/history del config."""
        return self.standard_load_parquet_or_table(config_dict_key)
        #
    def get_merge_schema_input(self, property_path:Union[str, HivePath]) -> DataFrame:
        """Carga un parquet padre con subdirs de esquema distinto (mergeSchema).

        Útil para outputs parametrizados por nombre de subdirectorio, como
        `target_propagation/weight_type=.../...` donde cada variante añade su
        propia columna `contagion_<weight>`. Lectura directa (sin decorador):
        el decorador leería sin mergeSchema y perdería columnas.
        """
        return (self.sqlContext.read
            .option("mergeSchema", "true")
            .parquet(str(property_path)))
        #
