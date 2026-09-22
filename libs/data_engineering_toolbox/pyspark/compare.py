"""
Project: Compare DataFrames in PySpark
Developer: Rodolfo Arturo Gonzalez
Update Date: 2025-01-02
Version: 1.0

"""

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, coalesce, lit, when, abs as ps_abs
from .testing.quality import assertColumnHasNoDuplicates, assertColumnHasNoNulls
from .testing.schemas import assertSchemasEqual
from typing import Optional, Union, List, Callable, Any
from itertools import chain


def rename_columns(df: DataFrame, rename_dict: dict) -> DataFrame:
    """
    Renombra las columnas de un DataFrame de PySpark según un diccionario de mapeo.

    Parameters:
        df (DataFrame): El DataFrame de PySpark cuyas columnas se van a renombrar.
        rename_dict (dict): Un diccionario donde las claves son los nombres originales de las columnas y los valores son los nuevos nombres.

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    # Crear una lista de columnas renombradas usando select
    renamed_columns = [col(c).alias(rename_dict.get(c, c)) for c in df.columns]
    # Seleccionar las columnas renombradas
    df_renamed = df.select(*renamed_columns)
    return df_renamed


def reverse_rename_columns(df: DataFrame, rename_dict: dict) -> DataFrame:
    """
    Deshace la operación de renombrar las columnas de un DataFrame de PySpark según un diccionario de mapeo.

    Parameters:
        df (DataFrame): El DataFrame de PySpark cuyas columnas se van a renombrar.
        rename_dict (dict): Un diccionario donde las claves son los nombres originales de las columnas y los valores son los nuevos nombres.

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    # Crear diccionario inverso
    reverse_rename_dict = {v: k for k, v in rename_dict.items()}
    # Renombrar las columnas
    return rename_columns(df, reverse_rename_dict)


class DifferenceDataFrame:
    """
    Clase para comparar dos DataFrames y encontrar diferencias entre ellos.

    Atributos:
        _numerical_types (list): Tipos de datos numéricos admitidos.
        _non_numerical_types (list): Tipos de datos no numéricos admitidos.
        __non_implemented_types (list): Tipos de datos no implementados.
        tol (float): Tolerancia para comparar valores numéricos.
        id_column (str): Nombre de la columna de identificación.
        columns (list): Lista de columnas a comparar.
        numerical_columns (list): Lista de columnas numéricas a comparar.
        non_numerical_columns (list): Lista de columnas no numéricas a comparar.
        df1 (DataFrame): Primer DataFrame.
        df2 (DataFrame): Segundo DataFrame.

    Métodos:
        __init__(self, df1, df2, id_column, tol, columns=None): Inicializa la clase con dos DataFrames, una columna de identificación y una tolerancia.
        _property_setter(self, property_name, property_method=None): Establece una propiedad de la clase.
        _dataframe_property_setter(self, property_name, property_method=None): Establece una propiedad de DataFrame de la clase.
        joined(self): Propiedad que devuelve el DataFrame unido.
        difference(self): Propiedad que devuelve el DataFrame con las diferencias.
        _isinstance_column_one_string_type(df, column, type): Verifica si una columna es de un tipo específico.
        isinstance_column_string_type(df, column, types): Verifica si una columna es de uno o más tipos específicos.
        rn(col_name, suffix): Renombra una columna con un sufijo.
        rn1(col_name): Renombra una columna con el sufijo "df1".
        rn2(col_name): Renombra una columna con el sufijo "df2".
        _get_column_renamer_1(self): Obtiene la lista de columnas renombradas para el primer DataFrame.
        _get_column_renamer_2(self): Obtiene la lista de columnas renombradas para el segundo DataFrame.
        _get_joined(self): Devuelve el DataFrame unido.
        _difference_operator(self, operator): Devuelve la columna de diferencia para una columna específica.
        _get_difference(self): Devuelve el DataFrame con las diferencias.
    """

    _numerical_types = ["byte", "short", "int", "long", "float", "double", "decimal"]
    _non_numerical_types = ["string", "binary", "boolean", "date", "timestamp"]
    __non_implemented_types = ["array", "map", "struct", "null"]

    def __init__(
        self,
        df1: DataFrame,
        df2: DataFrame,
        id_column:str,
        tol: float,
        columns: Optional[Union[List[str], str]] = None,
    ) -> None:
        """
        Inicializa la clase DifferenceDataFrame.

        Args:
            df1 (DataFrame): Primer DataFrame.
            df2 (DataFrame): Segundo DataFrame.
            id_column (str): Nombre de la columna de identificación.
            tol (float): Tolerancia para comparar valores numéricos.
            columns (Optional[Union[List[str], str]]): Lista de columnas a comparar. Si es None, se comparan todas las columnas.
        """
        self.tol = tol
        self.id_column = id_column
        self.columns = columns

        # Check quality of id_column
        assertColumnHasNoDuplicates(df1, id_column)
        assertColumnHasNoNulls(df1, id_column)
        assertColumnHasNoDuplicates(df2, id_column)
        assertColumnHasNoNulls(df2, id_column)
        #
        if self.columns is None:
            self.columns = [column for column in df1.columns if not self.isinstance_column_string_type(df1, column, self.__non_implemented_types)]

        if isinstance(self.columns, str):
            self.columns = [self.columns]

        if self.id_column in self.columns:
            # Remove id_column from columns
            self.columns = [column for column in self.columns if column != self.id_column]
        else:
            self.columns = self.columns
        # Assert columns is not empty
        assert len(self.columns) > 0, "Columns list is empty or is only the id"
        # Check if columns are present in DataFrames
        missing_columns = [column for column in self.columns if column not in df1.columns]
        assert len(missing_columns) == 0, f"Columns {missing_columns} are not present in the DataFrames"
        # Check if columns have non-implemented types
        non_implemented_columns = [column for column in self.columns if self.isinstance_column_string_type(df1, column, self.__non_implemented_types)]
        assert len(non_implemented_columns) == 0, f"Columns {non_implemented_columns} have non-implemented types {self.__non_implemented_types}"

        # Check if columns have numerical or non-numerical types
        self.numerical_columns = [column for column in self.columns if self.isinstance_column_string_type(df1, column, self._numerical_types)]
        self.non_numerical_columns = [column for column in self.columns if self.isinstance_column_string_type(df1, column, self._non_numerical_types)]

        # Keep only needed columns
        self.df1 = df1.select([id_column] + self.columns)
        self.df2 = df2.select([id_column] + self.columns)

        # Check if schemas are equal
        assertSchemasEqual(self.df1.schema, self.df2.schema)
        #
    def _property_setter(self, property_name:str, property_method:Optional[Callable[[], Any]]=None) -> Any:
        """
        Establece una propiedad de la clase.

        Args:
            property_name (str): Nombre de la propiedad.
            property_method (Optional[Callable[[], Any]]): Método para obtener el valor de la propiedad.

        Returns:
            Any: Valor de la propiedad.
        """
        #
        # Get property method
        if property_method is None:
            _property_method = self.__getattribute__(f"_get_{property_name}")
        else:
            _property_method = property_method
        #
        # Check if property exists
        if hasattr(self, f"_{property_name}"):
            return self.__getattribute__(f"_{property_name}")
        else:
            self.__setattr__(f"_{property_name}", _property_method())
            return self.__getattribute__(f"_{property_name}")
        #
    def _dataframe_property_setter(self, property_name:str, property_method:Optional[Callable[[], DataFrame]]=None) -> DataFrame:
        """
        Establece una propiedad de DataFrame de la clase.

        Args:
            property_name (str): Nombre de la propiedad.
            property_method (Optional[Callable[[], DataFrame]]): Método para obtener el valor de la propiedad.

        Returns:
            DataFrame: Valor de la propiedad.
        """
        return self._property_setter(property_name, property_method)
        #
    @property
    def joined(self) -> DataFrame:
        """
        Propiedad que devuelve el DataFrame unido.

        Returns:
            DataFrame: DataFrame unido.
        """
        return self._dataframe_property_setter("joined", self._get_joined)
        #
    @property
    def difference(self) -> DataFrame:
        """
        Propiedad que devuelve el DataFrame con las diferencias.

        Returns:
            DataFrame: DataFrame con las diferencias.
        """
        return self._dataframe_property_setter("difference", self._get_difference)
        #
    @staticmethod
    def _isinstance_column_one_string_type(df: DataFrame, column: str, type:str) -> bool:
        """
        Verifica si una columna es de un tipo específico.

        Args:
            df (DataFrame): DataFrame.
            column (str): Nombre de la columna.
            type (str): Tipo de dato.

        Returns:
            bool: True si la columna es del tipo especificado, False en caso contrario.
        """
        admitted_types = ["byte", "short", "int", "long", "float", "double", "decimal", "string", "binary", "boolean", "date", "timestamp", "array", "map", "struct", "null"]
        assert type in admitted_types, f"Invalid type, it must be one of the following: {admitted_types}"
        return str(df.schema[column].dataType).lower().startswith(type)
        #
    @staticmethod
    def isinstance_column_string_type(df: DataFrame, column:str, types:Union[str, List[str]]) -> bool:
        """
        Verifica si una columna es de uno o más tipos específicos.

        Args:
            df (DataFrame): DataFrame.
            column (str): Nombre de la columna.
            types (Union[str, List[str]]): Tipo o lista de tipos de datos.

        Returns:
            bool: True si la columna es de uno de los tipos especificados, False en caso contrario.
        """
        if isinstance(types, str):
            return DifferenceDataFrame._isinstance_column_one_string_type(df, column, types)
        else:
            return any(DifferenceDataFrame._isinstance_column_one_string_type(df, column, type) for type in types)
        #
    @staticmethod
    def rn(col_name:str, suffix:str) -> str:
        """
        Renombra una columna con un sufijo.

        Args:
            col_name (str): Nombre de la columna.
            suffix (str): Sufijo.

        Returns:
            str: Nombre de la columna renombrada.
        """
        return f"{col_name}{suffix}" if suffix.startswith("_") else f"{col_name}_{suffix}"
        #
    @staticmethod
    def rn1(col_name:str) -> str:
        """
        Renombra una columna con el sufijo "df1".

        Args:
            col_name (str): Nombre de la columna.

        Returns:
            str: Nombre de la columna renombrada.
        """
        return DifferenceDataFrame.rn(col_name, "df1")
        #
    @staticmethod
    def rn2(col_name:str) -> str:
        """
        Renombra una columna con el sufijo "df2".

        Args:
            col_name (str): Nombre de la columna.

        Returns:
            str: Nombre de la columna renombrada.
        """
        return DifferenceDataFrame.rn(col_name, "df2")
        #
    def _get_column_renamer_1(self) -> List[Column]:
        """
        Obtiene la lista de columnas renombradas para el primer DataFrame.

        Returns:
            List[Column]: Lista de columnas renombradas.
        """
        return [col(self.id_column)] + [col(column).alias(self.rn1(column)) for column in self.columns]
        #
    def _get_column_renamer_2(self) -> List[Column]:
        """
        Obtiene la lista de columnas renombradas para el segundo DataFrame.

        Returns:
            List[Column]: Lista de columnas renombradas.
        """
        return [col(self.id_column)] + [col(column).alias(self.rn2(column)) for column in self.columns]
        #
    def _get_joined(self) -> DataFrame:
        """
        Devuelve el DataFrame unido.

        Returns:
            DataFrame: DataFrame unido.
        """
        result = self.df1.select(self._get_column_renamer_1()).join(self.df2.select(self._get_column_renamer_2()), on=self.id_column, how="outer")
        tuple_names = [(self.rn1(column), self.rn2(column)) for column in self.columns]
        column_reorder = [self.id_column] + list(chain(*tuple_names))
        return result.select(column_reorder)
        #
    def _difference_operator(self, column:str) -> Column:
        """
        Devuelve la columna de diferencia para una columna específica.

        Args:
            column (str): Nombre de la columna.

        Returns:
            Column: Columna de diferencia.
        """
        # Verificar si la columna es de tipo numérico
        if self.isinstance_column_string_type(self.df1, column, self._numerical_types):
            # Si es numérico, calcular la diferencia absoluta entre las dos columnas
            return ps_abs(col(self.rn1(column)) - col(self.rn2(column)))
        else:
            # Si no es numérico, manejar valores NULL y comparaciones
            return when((col(self.rn1(column)).isNull()) | (col(self.rn2(column)).isNull())
                        & ~(col(self.rn1(column)).isNull()) & (col(self.rn2(column)).isNull())
                        , lit(-99999.00)).otherwise( # NULL
                when(col(self.rn1(column)) == col(self.rn2(column))
                | (col(self.rn1(column)).isNull()) & (col(self.rn2(column)).isNull()))
                , lit(0.0)).otherwise( # Equal
                lit(-1.0) # Different
            )


    def _get_difference(self) -> DataFrame:
        """
        Devuelve el DataFrame con las diferencias.

        Returns:
            DataFrame: DataFrame con las diferencias.
        """
        result = self.joined.withColumn("number_of_different_columns", lit(0))

        for column in self.columns:
            result = result.withColumn(f"{column}_diff", self._difference_operator(column))
            result = result.withColumn(f"{column}_diff", coalesce(col(f"{column}_diff"), lit(-99999.00)))
            result = result.withColumn(f"is_{column}_equal", when((col(f"{column}_diff") >= lit(0.0)) & (col(f"{column}_diff") <= lit(self.tol)), lit(True)).otherwise(lit(False)))
            result = result.withColumn("number_of_different_columns", col("number_of_different_columns") + when(col(f"is_{column}_equal") == lit(False), lit(1)).otherwise(lit(0)))

        result = result.filter(col("number_of_different_columns") > lit(0))
        tuple_names = [(self.rn1(column), self.rn2(column), f"{column}_diff") for column in self.columns]
        column_reorder = [self.id_column] + list(chain(*tuple_names))
        return result.select(column_reorder)
