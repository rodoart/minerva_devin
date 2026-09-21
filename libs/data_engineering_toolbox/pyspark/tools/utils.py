
from pyspark.sql import SparkSession
from typing import Union, List, Dict

from ...path import HivePath


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
