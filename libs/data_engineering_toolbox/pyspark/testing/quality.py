from pyspark.sql import DataFrame
from ...pyspark.counts import count_nulls, count_duplicates

from ...context.logging import get_logger
logger = get_logger(__name__)


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
    logger.info("La columna '%s' no tiene valores nulos.", column)



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
    logger.info("La columna '%s' no tiene valores duplicados.", column)
