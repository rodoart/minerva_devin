
########################################################################
# SPECIAL TREATMENT
########################################################################

# ----------------------------------------------------------------------
# General
# ----------------------------------------------------------------------
from typing import List, Dict, Optional, Union


# ----------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------
from pyspark.sql import DataFrame, Column

from pyspark.sql.functions import (col, to_date, lit, date_format,
    max as spark_max)
from pyspark.sql.types import StringType
# ----------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath


import libs.framework as ppf
import config.job as cj
########################################################################
# Process
########################################################################


class SubStep(ppf.Step):
    """Clase base para las sub-etapas del pipeline `StandardGroupByStep`.

    Hereda los atributos del step padre (rutas Hive, tratamiento de fechas,
    cohorte, etc.) mediante `ppf.inherit_parent_step_attributes`.
    """
    def __init__(self, parent: "StandardGroupByStep", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)


class StandardGroupByStep(ppf.Step):
    """Step orquestador base de la agregación de transacciones para el grafo.

    Expone el substep `standard_groupby_step`; las implementaciones concretas
    definen su `step_action` y las agregaciones a aplicar.
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
            step_name_prefix="Standard_Group_By_Step",
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
    def standard_groupby_step(self) -> "StandardGroupBySubStep":
        """Substep de group-by (lazy, cacheado); solo para pruebas."""
        return StandardGroupBySubStep(self)


class StandardGroupBySubStep(SubStep):
    """Substep base de agregación: agrupa transacciones por arista y por nodo."""
    # previous_step
    def get_previous_step(self, key:str) -> DataFrame:
        """Carga la salida persistida del step previo identificada por `key`."""
        result:DataFrame = (
            self.standard_load_parquet_or_table(key)
        )
        return result
        #
    def standard_group_by_txn(self,
        input_df:DataFrame,
        aggregations:List[Column],
        auxiliary_columns:Dict[str, Column]=None,
        txn_id_columns:Optional[Union[str, List[str]]]=["id_src", "id_dst"]
    ) -> DataFrame:
        """Agrupa las transacciones por las columnas de la arista aplicando agregaciones.

        Args:
            input_df: Transacciones de entrada.
            aggregations: Expresiones `Column` de agregación; se añade
                automáticamente `max(information_date)`.
            auxiliary_columns: Columnas auxiliares creadas antes del groupBy
                (útiles dentro de las agregaciones); se eliminan del resultado.
            txn_id_columns: Columnas de agrupación de la arista
                (por defecto `["id_src", "id_dst"]`).

        Returns:
            DataFrame agregado por arista con `process_date`, `mis_date`,
            `vintage` y `cohort`.
        """
        aggregations = aggregations + [spark_max(col("information_date")).alias("information_date")]
        if txn_id_columns is None or len(txn_id_columns) == 0:
            txn_id_columns = ["id_src", "id_dst"]
        #
        if isinstance(txn_id_columns, str):
            txn_id_columns = [txn_id_columns]
        #
        if auxiliary_columns is not None and len(auxiliary_columns) > 0:
            for column_name, column_expr in auxiliary_columns.items():
                input_df = input_df.withColumn(column_name, column_expr)
        #
        result = (input_df
            .groupBy(*txn_id_columns)
            .agg(*aggregations)
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col("information_date"), cj.DATE_STANDARD_SPARK_FORMAT), cj.DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )
        if auxiliary_columns is not None and len(auxiliary_columns) > 0:
            result = result.drop(*auxiliary_columns.keys())
        return result
        #
    def standard_txn_split(self,
        input_df: DataFrame,
        id_columns: Union[str, List[str]],
        unique_columns: List[str],
        common_columns: List[str],
        suffix: str
    ) -> DataFrame:
        """Proyecta un lado de la transacción eliminando el sufijo de sus columnas.

        Args:
            input_df: Transacciones de entrada.
            id_columns: Columnas identificadoras del lado (id del nodo).
            unique_columns: Columnas exclusivas del lado (las que llevan `suffix`).
            common_columns: Columnas comunes a ambos lados (sin sufijo).
            suffix: Sufijo a eliminar de `id_columns` y `unique_columns`.

        Returns:
            DataFrame con `id_columns` + `unique_columns` renombradas sin
            sufijo + `common_columns`.
        """
        # check if unique colums not in common colums
        assert len(set(unique_columns).intersection(set(common_columns))) == 0, "Unique columns should not be in common colums"
        # check if id_colums not in unique colums
        assert len(set(id_columns).intersection(set(unique_columns))) == 0, "Id columns should not be in unique colums"
        # check if id_colums not in common colums
        assert len(set(id_columns).intersection(set(common_columns))) == 0, "Id columns should not be in common colums"
        if isinstance(id_columns, str):
            id_columns = [id_columns]
        #
        # Create a common_columns without suffix.
        unique_columns_without_suffix = [col(column).alias(column.replace(f"{suffix}", "")) if column.endswith(suffix) else column for column in unique_columns]
        id_columns_without_suffix = [col(column).alias(column.replace(f"{suffix}", "")) if column.endswith(suffix) else column for column in id_columns]
        selection = id_columns_without_suffix + unique_columns_without_suffix + common_columns
        return (input_df
            .select(*selection)
        )
        #
    def standard_txn_src_split(
        self,
        input_df: DataFrame,
        id_columns: Union[str, List[str]],
        unique_columns: List[str],
        common_columns: List[str],
        suffix: str = "_src"
    ) -> DataFrame:
        """Atajo de `standard_txn_split` para el lado origen (sufijo "_src")."""
        return self.standard_txn_split(
            input_df=input_df,
            id_columns=id_columns,
            unique_columns=unique_columns,
            common_columns=common_columns,
            suffix=suffix
        )
        #
    def standard_txn_dst_split(
        self,
        input_df: DataFrame,
        id_columns: Union[str, List[str]],
        unique_columns: List[str],
        common_columns: List[str],
        suffix: str = "_dst"
    ) -> DataFrame:
        """Atajo de `standard_txn_split` para el lado destino (sufijo "_dst")."""
        return self.standard_txn_split(
            input_df=input_df,
            id_columns=id_columns,
            unique_columns=unique_columns,
            common_columns=common_columns,
            suffix=suffix
        )
        #
    def standard_group_by_id(self,
        input_src_df: DataFrame,
        input_dst_df: DataFrame,
        aggregations: List[Column],
        id_columns: Union[str, List[str]]="id"
    ) -> DataFrame:
        """Une las proyecciones origen/destino y agrega por nodo (`id`).

        Args:
            input_src_df: Proyección del lado origen (salida de `standard_txn_src_split`).
            input_dst_df: Proyección del lado destino (salida de `standard_txn_dst_split`).
            aggregations: Expresiones `Column` de agregación.
            id_columns: Columnas de agrupación del nodo (por defecto `"id"`).

        Returns:
            DataFrame agregado por nodo con `process_date`, `mis_date`,
            `vintage` y `cohort`.
        """
        if isinstance(id_columns, str):
            id_columns = [id_columns]
        #
        input_df = input_src_df.unionByName(input_dst_df)
        return (input_df
            .groupBy(*id_columns)
            .agg(*aggregations)
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col("information_date"), cj.DATE_STANDARD_SPARK_FORMAT), cj.DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )
