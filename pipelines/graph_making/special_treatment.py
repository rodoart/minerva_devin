
##########################################################################
# SPECIAL TREATMENT
##########################################################################

# ------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------
from datetime import datetime
from typing import List, Union, Dict, Any, Callable, Optional

from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (col, to_date,
    unix_timestamp, lit, floor, months_between, to_timestamp
)
# ------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath
from libs.functions.missing_treatment import apply_missing_treatment

import libs.framework as ppf
import config.job as cj
##########################################################################
# FUNCTIONS
##########################################################################
def calculate_monthly_tfrom(
    df:DataFrame,
    current_date:str,
    date_column:str,
    date_format: Optional[str] = cj.DATE_MONTH_SPARK_FORMAT
) -> DataFrame:
    """Añade `tfrom_months`: meses completos entre `date_column` y `current_date`."""
    if date_format is None:
        date_format = cj.DATE_MONTH_SPARK_FORMAT
    #
    return df.withColumn(
        "tfrom_months",
        floor(months_between(to_date(lit(current_date), date_format), to_date(col(date_column), date_format)))
    )

def calculate_daily_tfrom(
    df:DataFrame,
    current_date:str,
    date_column:str,
    date_format: Optional[str] = cj.DATE_STANDARD_SPARK_FORMAT
) -> DataFrame:
    """Añade `tfrom_days`: días completos entre `date_column` y `current_date`."""
    if date_format is None:
        date_format = cj.DATE_STANDARD_SPARK_FORMAT
    #
    return df.withColumn(
        "tfrom_days",
        floor((unix_timestamp(to_timestamp(lit(current_date), date_format)) - unix_timestamp(to_timestamp(col(date_column), date_format))) / (3600*24))
    )


##########################################################################
# Process
##########################################################################


class SubStep(ppf.Step):
    """Clase base para las sub-etapas del pipeline `StandardSpecialTreatment`.

    Hereda los atributos del step padre (rutas Hive, tratamiento de fechas,
    cohorte, etc.) mediante `ppf.inherit_parent_step_attributes`.
    """
    def __init__(self, parent: "StandardSpecialTreatment", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)


class StandardSpecialTreatment(ppf.Step):
    """Step orquestador base del *special treatment* para la construcción del grafo.

    Expone el substep de extracción (`standard_extract_step`); las
    implementaciones concretas definen su `step_action` y el substep a disparar.
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
            step_name_prefix="Special_Treatment_Step",
            extra_kwargs=kwargs,
        )
        super().__init__(*args, **super_class_kwargs)
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
    # For testting purposes only
    @ppf.cached_property
    def standard_extract_step(self) -> "StandardExtractSubStep":
        """Substep de extracción (lazy, cacheado); solo para pruebas."""
        return StandardExtractSubStep(self)


class StandardExtractSubStep(SubStep):
    """Substep base de extracción: carga históricos, calcula `tfroms` y aplica *missing treatment*."""
    #
    def input_table_historic(self, key:str) -> DataFrame:
        """Carga una tabla histórica de entrada aplicando el `select` opcional de su config."""
        selection:List[str] = self.input_hive[key].get("select", [])
        #
        result:DataFrame = (
            self.standard_load_parquet_or_table(key)
        )
        if len(selection) > 0:
            result = result.select(*selection)
        #
        return result
        #
    def calculate_tfroms(self, df:DataFrame, key:Optional[str] = None) -> DataFrame:
        """Añade `tfrom_months` y `tfrom_days` calculados sobre `information_date`.

        La fecha de referencia es el último día del mes vintage desplazada por
        el `lag` (en meses) configurado en el input `key`, igual que en
        `make_date_interval_with_lag_months`: lag > 0 retrasa la ventana (los
        datos acaban `lag` meses antes del vintage). Sin `key` (o sin "lag" en
        su config) se usa el fin del mes vintage sin desplazar.

        Args:
            df: DataFrame con la columna `information_date`.
            key: clave de `self.input_hive` de la que tomar el "lag".
        """
        information_date_column:str = "information_date"
        lag:int = self.input_hive[key].get("lag", 0) if key is not None else 0
        reference_date:str = str((
            datetime.strptime(
                self.parent.date_treatment["last_day_of_current_month_date_str"],
                cj.DATE_STANDARD_FORMAT,
            ).date() - relativedelta(months=lag)
        ).strftime(cj.DATE_STANDARD_FORMAT))
        return (df
            .transform(lambda df_: calculate_monthly_tfrom(df=df_,  current_date=reference_date, date_column=information_date_column,
                date_format=cj.DATE_STANDARD_SPARK_FORMAT))
            .transform(lambda df_: calculate_daily_tfrom(df=df_,  current_date=reference_date, date_column=information_date_column,
                date_format=cj.DATE_STANDARD_SPARK_FORMAT))
        )
        #
    @staticmethod
    def standard_missing_treatment(
        df: DataFrame,
        column_treatment_dict:Dict[str, Union[str, Callable, List[Any]]]
    ) -> DataFrame:
        """Aplica un tratamiento de valores nulos a las columnas especificadas.

        Args:
            df: DataFrame de entrada.
            column_treatment_dict: Mapa columna -> tratamiento (literal, callable
                o lista de valores) interpretado por `apply_missing_treatment`.

        Returns:
            DataFrame con los faltantes imputados.
        """
        return apply_missing_treatment(df, column_treatment_dict)
