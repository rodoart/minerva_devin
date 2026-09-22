from re import search
from typing import Union, List, Dict

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession

from libs.data_engineering_toolbox.path import HivePath



def get_column_alias(column: Column) -> str:
    """Extrae el alias declarado de una `Column` de PySpark.

    Parsea la representación interna (`Column._jc.toString()`) buscando el patrón
    `AS <alias>` que Spark genera al aplicar `.alias(...)`. Si la columna no tiene
    alias explícito, retorna su nombre/expresión tal cual.

    Parameters
    ----------
    column : Column
        Columna de la que extraer el alias (ej: `col("id_ban_ben").alias("id_ban_dst")`).

    Returns
    -------
    str
        El alias declarado (ej: "id_ban_dst"), o la expresión base si no hay alias.

    Examples
    --------
    >>> get_column_alias(col("id_ban_ben").alias("id_ban_dst"))
    'id_ban_dst'
    >>> get_column_alias(col("customer_id"))
    'customer_id'
    """
    expr = column._jc.toString()
    match = search(r" AS (\w+)$", expr)
    return match.group(1) if match else expr



def is_table_or_parquet(table_or_hdfs:Union[HivePath, str]) -> str:
    """
    Function to determine if the input is a Hive table or a Parquet file.

    Args:
        table_or_hdfs (Union[HivePath, str]): HivePath or string path to the table or HDFS location.

    Returns:
        str: Type of the input (table or hdfs).
    """
    if isinstance(table_or_hdfs, str):
        # count dots in name
        if "." in table_or_hdfs and not "/" in table_or_hdfs:
            return "table"
        else: # hive path in string format
            return "hdfs"
    else:
        return "hdfs"


def check_if_table_exists(table:str, session:SparkSession) -> bool:
    """
    Verifica si una tabla existe en la sesión de Spark.

    Args:
        table (str): El nombre de la tabla a verificar.
        session (SparkSession): La sesión de Spark en la que se ejecutará la consulta.

    Returns:
        bool: True si la tabla existe, False en caso contrario.
    """
    try:
        session.sql(f"DESCRIBE {table}")
        return True
    except:
        return False



def convert_partitions_to_spark_filter(partitions: List[Dict[str, str]]) -> str:
    """
    Convierte una lista de particiones en un filtro de PySpark.

    Args:
        partitions (List[Dict[str, str]]): Lista de particiones a convertir.

    Returns:
        str: Filtro de PySpark.
    """
    return " OR ".join(["({})".format(" AND ".join(["{} = '{}'".format(key, value) for key, value in partition.items()])) for partition in partitions])



class ColumnRenamer:
    def __init__(self, mapping_df: pd.DataFrame, name_col: str, masking_col: str) -> None:
        """Inicializa el objeto ColumnRenamer con un DataFrame de pandas que contiene la relación de nombres.

        Args:
            mapping_df (pd.DataFrame): DataFrame de pandas con las columnas de nombre y enmascaramiento.
            name_col (str): Nombre de la columna que contiene los nombres originales.
            masking_col (str): Nombre de la columna que contiene los nombres enmascarados.
        """
        self.name_to_masking = dict(zip(mapping_df[name_col], mapping_df[masking_col]))
        self.masking_to_name = dict(zip(mapping_df[masking_col], mapping_df[name_col]))

    def rename_columns(self, df: DataFrame, reverse: bool = False) -> DataFrame:
        """Renombra las columnas de un DataFrame de PySpark según la relación proporcionada.

        Args:
            df (DataFrame): DataFrame de PySpark cuyas columnas se renombrarán.
            reverse (bool): Si es True, renombra las columnas de enmascarado a nombre original. Por defecto es False.

        Returns:
            DataFrame: DataFrame de PySpark con las columnas renombradas.
        """
        if reverse:
            mapping = self.masking_to_name
        else:
            mapping = self.name_to_masking

        for old_name, new_name in mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        return df
