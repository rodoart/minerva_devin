from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from ...pyspark.counts import count_nulls, count_duplicates


def assertColumnHasNoNulls(df: DataFrame, column: str) -> None:
    """Verifica si una columna de un DataFrame de PySpark no tiene valores nulos.

    Args:
        df (DataFrame): El DataFrame de PySpark.
        column (str): El nombre de la columna.

    Raises:
        AssertionError: Si la columna tiene valores nulos.
    """
    null_count = count_nulls(df, column)
    if null_count > 0:
        raise AssertionError(f"La columna '{column}' tiene {null_count} valores nulos.")
    print(f"La columna '{column}' no tiene valores nulos.")



def assertColumnHasNoDuplicates(df: DataFrame, column: str) -> None:
    """Verifica si una columna de un DataFrame de PySpark no tiene valores duplicados.

    Args:
        df (DataFrame): El DataFrame de PySpark.
        column (str): El nombre de la columna.

    Raises:
        AssertionError: Si la columna tiene valores duplicados.
    """
    duplicate_count = count_duplicates(df, column)
    if duplicate_count > 0:
        raise AssertionError(f"La columna '{column}' tiene {duplicate_count} valores duplicados.")
    print(f"La columna '{column}' no tiene valores duplicados.")
