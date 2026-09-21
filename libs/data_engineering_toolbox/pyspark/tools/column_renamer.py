import pandas as pd
from pyspark.sql import DataFrame


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
