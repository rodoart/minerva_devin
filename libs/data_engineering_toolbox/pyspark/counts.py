from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, count as ps_count, when


def count_nulls(df: DataFrame, column: str) -> int:
    """Cuenta los valores nulos en una columna de un DataFrame de PySpark.

    Args:
        df (DataFrame): El DataFrame de PySpark.
        column (str): El nombre de la columna.

    Returns:
        int: El número de valores nulos en la columna.
    """
    return df.select(column).select(
        ps_count(when(col(column).isNull() | isnan(col(column)), column)).alias("null_count")
    ).collect()[0]["null_count"]


def count_duplicates(df: DataFrame, column: str) -> int:
    """Cuenta los valores duplicados en una columna de un DataFrame de PySpark.

    Args:
        df (DataFrame): El DataFrame de PySpark.
        column (str): El nombre de la columna.

    Returns:
        int: El número de valores duplicados en la columna.
    """
    duplicate_count = df.groupBy(column).agg(ps_count(column).alias("count")).filter(col("count") > 1).agg({"count": "sum"}).collect()[0][0]
    return duplicate_count if duplicate_count is not None else 0
