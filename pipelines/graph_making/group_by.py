
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

from pyspark.sql.functions import (col, to_date, lit, date_format, last_day,
    datediff, collect_set, collect_list, flatten, array_distinct,
    max as spark_max, sum as spark_sum)
from pyspark.sql.types import StringType
# ----------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath


import libs.framework as ppf
import libs.functions.aggregations as lfa
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
    def standard_group_by_txn_monthly(self,
        input_df:DataFrame,
        value_columns:List[str],
        txn_id_columns:Optional[Union[str, List[str]]]=["id_src", "id_dst"],
        month_column:str = lfa.MONTHLY_PARTITION_COLUMN,
        time_column:str = lfa.MONTHLY_TIME_COLUMN,
        date_column:str = "information_date",
        months:Optional[List[str]]=None,
    ) -> DataFrame:
        """Agrega las transacciones por arista Y MES con parciales combinables.

        Calcula `month_column` (fin de mes de `date_column`, usado como
        partición mensual) y `time_column` (días desde la transacción al fin
        de su propio mes, antigüedad relativa que NO depende del vintage).
        Las parciales se combinan después con `merge_monthly_group_by`.

        Args:
            input_df: Transacciones de entrada.
            value_columns: variables de valor a agregar (p.ej. ["oper_mto"]).
            txn_id_columns: columnas de agrupación de la arista.
            month_column: nombre de la columna-partición de mes.
            time_column: nombre de la columna de antigüedad relativa.
            date_column: columna de fecha de información.
            months: si se indica, filtra las transacciones a esos meses
                "YYYY-MM" antes de agrupar (cómputo incremental).

        Returns:
            DataFrame con una fila por arista-mes y las parciales de
            `lfa.monthly_partial_group_by_aggregations`.
        """
        if isinstance(txn_id_columns, str):
            txn_id_columns = [txn_id_columns]
        date_col = to_date(col(date_column), cj.DATE_STANDARD_SPARK_FORMAT)
        input_df = (input_df
            .withColumn(month_column, last_day(date_col))
            .withColumn(time_column, datediff(col(month_column), date_col))
        )
        if months:
            input_df = input_df.filter(
                date_format(col(month_column), "yyyy-MM").isin(*months))
        return (input_df
            .groupBy(*txn_id_columns, month_column)
            .agg(*lfa.monthly_partial_group_by_aggregations(
                value_columns=value_columns,
                time_column=time_column,
                date_column=date_column))
        )
        #
    def standard_merge_group_by_txn_monthly(self,
        monthly_df:DataFrame,
        aggregations:List[str],
        feature_names:Union[List[str], Dict],
        reference_date:str,
        txn_id_columns:Optional[Union[str, List[str]]]=["id_src", "id_dst"],
        time_variable:str = "tfrom_days",
        month_column:str = lfa.MONTHLY_PARTITION_COLUMN,
        time_column:str = lfa.MONTHLY_TIME_COLUMN,
        date_column:str = "information_date",
    ) -> DataFrame:
        """Combina las parciales mensuales en la agregación de toda la ventana.

        Produce el mismo resultado que `standard_group_by_txn` sobre las
        transacciones crudas, pero leyendo las particiones mensuales
        materializadas. Requiere `reference_date`, la fecha de referencia de
        la antigüedad (la misma que usó `calculate_tfroms`).

        Raises:
            ValueError: si alguna agregación pedida no tiene combinador
                mensual (el llamador puede hacer fallback al group-by completo).
        """
        if isinstance(txn_id_columns, str):
            txn_id_columns = [txn_id_columns]
        merged = lfa.merge_monthly_group_by(
            monthly_df=monthly_df,
            txn_id_columns=txn_id_columns,
            aggregation_names=aggregations,
            feature_names=feature_names,
            reference_date=reference_date,
            time_variable=time_variable,
            time_column=time_column,
            month_column=month_column,
            date_column=date_column,
        )
        return (merged
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col(date_column), cj.DATE_STANDARD_SPARK_FORMAT), cj.DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )
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
        #
    def standard_group_by_id_monthly(self,
        input_src_df: DataFrame,
        input_dst_df: DataFrame,
        set_columns: List[str],
        sum_columns: List[str],
        id_columns: Union[str, List[str]]="id",
        month_column:str = lfa.MONTHLY_PARTITION_COLUMN,
        date_column:str = "information_date",
        months:Optional[List[str]]=None,
    ) -> DataFrame:
        """Agrega por nodo Y MES las proyecciones origen/destino (parciales).

        Las parciales de conjuntos (`collect_set` por mes) se unen después con
        `standard_merge_group_by_id_monthly` vía unión de arrays; las sumas se
        recombinan por suma y la fecha por máximo.

        Args:
            input_src_df: Proyección del lado origen.
            input_dst_df: Proyección del lado destino.
            set_columns: columnas a acumular como conjuntos por nodo-mes.
            sum_columns: columnas a sumar por nodo-mes.
            id_columns: columnas de agrupación del nodo.
            month_column: nombre de la columna-partición de mes.
            date_column: columna de fecha de información (su máximo se guarda).
            months: si se indica, filtra a esos meses "YYYY-MM".

        Returns:
            DataFrame con una fila por nodo-mes.
        """
        if isinstance(id_columns, str):
            id_columns = [id_columns]
        input_df = (input_src_df.unionByName(input_dst_df)
            .withColumn(month_column, last_day(to_date(col(date_column), cj.DATE_STANDARD_SPARK_FORMAT)))
        )
        if months:
            input_df = input_df.filter(
                date_format(col(month_column), "yyyy-MM").isin(*months))
        return (input_df
            .groupBy(*id_columns, month_column)
            .agg(
                *[collect_set(col(column)).alias(column) for column in set_columns],
                *[spark_sum(col(column)).alias(column) for column in sum_columns],
                spark_max(col(date_column)).alias(date_column),
            )
        )
        #
    def standard_merge_group_by_id_monthly(self,
        monthly_df: DataFrame,
        set_columns: List[str],
        sum_columns: List[str],
        id_columns: Union[str, List[str]]="id",
        date_column:str = "information_date",
    ) -> DataFrame:
        """Combina las parciales mensuales de nodo en la agregación de la ventana.

        Los conjuntos se unen con `array_distinct(flatten(collect_list))`, las
        sumas se recombinan por suma y `date_column` por máximo (equivalente
        determinista al `last` del group-by completo).
        """
        if isinstance(id_columns, str):
            id_columns = [id_columns]
        return (monthly_df
            .groupBy(*id_columns)
            .agg(
                *[array_distinct(flatten(collect_list(col(column)))).alias(column)
                    for column in set_columns],
                *[spark_sum(col(column)).alias(column) for column in sum_columns],
                spark_max(col(date_column)).alias(date_column),
            )
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col(date_column), cj.DATE_STANDARD_SPARK_FORMAT), cj.DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )
