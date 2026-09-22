from pyspark.sql import DataFrame
from typing import Optional

from ...pyspark.compare import DifferenceDataFrame

def assertDataFrameEqual(df1: DataFrame, df2: DataFrame, id_column, tol: Optional[float] = 0.00001) -> None:
    """Compara dos DataFrames de PySpark para verificar si son iguales.

    Args:
        df1 (DataFrame): Primer DataFrame.
        df2 (DataFrame): Segundo DataFrame.
        tol (float): Tolerancia para la comparación de valores numéricos.

    Raises:
        AssertionError: Si los DataFrames no son iguales.
    """
    # Verificar que los esquemas sean iguales
    difference_object = DifferenceDataFrame(df1, df2, id_column, tol)
    if difference_object.difference().count() > 0:
        raise AssertionError("Los DataFrames son diferentes.")
    print("Los DataFrames son iguales dentro de la tolerancia especificada.")
