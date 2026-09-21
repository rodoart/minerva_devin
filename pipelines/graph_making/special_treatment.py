
##########################################################################
# SPECIAL TREATMENT
##########################################################################

# ------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------
from typing import List, Union, Dict, Any, Callable, Optional

# ------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (coalesce, col, to_date,
    unix_timestamp, lit, floor, months_between, to_timestamp, mean as spark_mean
)
# ------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------
from data_engineering_toolbox.path import HivePath

import libs.framework as ppf
import config.job as cj

from importlib import reload
reload(cj)
##########################################################################
# FUNCTIONS
##########################################################################
def calculate_monthly_tfrom(
    df:DataFrame,
    current_date:str,
    date_column:str,
    date_format: Optional[str] = cj.DATE_MONTH_SPARK_FORMAT
) -> DataFrame:
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
    if date_format is None:
        date_format = cj.DATE_STANDARD_SPARK_FORMAT
    #
    return df.withColumn(
        "tfrom_days",
        floor((unix_timestamp(to_timestamp(lit(current_date), date_format)) - unix_timestamp(to_timestamp(col(date_column), date_format))) / (3600*24))
    )

def fill_missings_with_value(df: DataFrame, columns: List[str], fill_value: Any) -> DataFrame:
    """Rellena los valores nulos de las columnas especificadas con un valor fijo.
    """
    selection = [coalesce(col(column), lit(fill_value)).alias(column) if column in columns else column for column in df.columns]
    return df.select(*selection)


def fill_missings_with_mean(df: DataFrame, columns: List[str]) -> DataFrame:
    """Rellena los valores nulos de las columnas especificadas con la media de cada columna.
    """
    means = df.select([spark_mean(col(column)).alias(column) for column in columns]).collect()[0].asDict()
    selection = [coalesce(col(column), lit(means[column])).alias(column) if column in columns else column for column in df.columns]
    return df.select(*selection)

def fill_missings_with_mean_without_ignoring_null_counts(df: DataFrame, columns: List[str]) -> DataFrame:
    """Rellena los valores nulos de las columnas especificadas con la media de cada columna,
    """
    means = df.select([spark_mean(coalesce(col(column),lit(0))).alias(column) for column in columns]).collect()[0].asDict()
    selection = [coalesce(col(column), lit(means[column])).alias(column) if column in columns else column for column in df.columns]
    return df.select(*selection)


MISSING_TREATMENT_FUNCTION_RELATIONS = {
    "mean": fill_missings_with_mean,
    "mean_with_nulls": fill_missings_with_mean_without_ignoring_null_counts,
    "value": fill_missings_with_value
}


##########################################################################
# Process
##########################################################################


class SubStep(ppf.Step):
    """Clase base para las sub-etapas del pipeline `CepsRfcNomRankingStep`.
    """
    def __init__(self, parent: "StandardSpecialTreatment", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)


class StandardSpecialTreatment(ppf.Step):
    """
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
        """
        config_dict = self.input_hive if input_or_output == "input" else self.output_hive
        return ppf.standard_load_parquet_or_table(
            config_dict_key=config_dict_key,
            current_date=self.input_parameters['vintage_date'],
            session=self.sqlContext,
        )
        #
    # For testting purposes only
    @ppf.cached_property
    def standard_extract_step(self) -> "StandardExtractSubStep":
        """
        """
        return StandardExtractSubStep(self)


class StandardExtractSubStep(SubStep):
    #
    def input_table_historic(self, key:str) -> DataFrame:
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
    def calculate_tfroms(self, df:DataFrame) -> DataFrame:
        # TODO ADD lags
        information_date_column:str = "information_date"
        return (df
            .transform(lambda df_: calculate_monthly_tfrom(df=df_,  current_date=str(self.parent.date_treatment["last_day_of_current_month_date_str"]), date_column=information_date_column))
            .transform(lambda df_: calculate_daily_tfrom(df=df_,  current_date=self.parent.date_treatment["last_day_of_current_month_date_str"], date_column=information_date_column))
        )
        #
    @staticmethod
    def standard_missing_treatment(
        df: DataFrame,
        column_treatment_dict:Dict[str, Union[str, Callable, List[Any]]]
    ) -> DataFrame:
        """Aplica un tratamiento de valores nulos a las columnas especificadas.
        """
        for column, treatment in column_treatment_dict.items():
            if isinstance(treatment, list) and len(treatment) >=2:
                treatment_type = treatment[0]
                args_ = treatment[1:]
                if treatment_type in MISSING_TREATMENT_FUNCTION_RELATIONS:
                    df = MISSING_TREATMENT_FUNCTION_RELATIONS[treatment_type](df, [column], *args_)
                else:
                    raise ValueError(f"Unsupported missing treatment '{treatment_type}' for column '{column}'.")

            #
            elif isinstance(treatment, str) or (isinstance(treatment, list) and len(treatment) == 1):
                if isinstance(treatment, list):
                    treatment = treatment[0]
                if treatment in MISSING_TREATMENT_FUNCTION_RELATIONS:
                    df = MISSING_TREATMENT_FUNCTION_RELATIONS[treatment](df, [column])
                else:
                    raise ValueError(f"Unsupported missing treatment '{treatment}' for column '{column}'.")
            elif callable(treatment):
                df = treatment(df)
            else:
                raise ValueError(f"Invalid treatment type for column '{column}': {type(treatment)}. Must be str or callable.")
        return df
