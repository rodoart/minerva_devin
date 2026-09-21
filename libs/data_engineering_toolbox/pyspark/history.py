from functools import reduce
from typing import Any
from typing import List, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DataType
from pyspark.sql.functions import coalesce, col, struct, array, lit, concat, when, expr
from pyspark.sql.column import Column

from data_engineering_toolbox.path import HivePath

################################################################################

class cached_property:
    def __init__(self, func) -> None:
        self.func = func
        self.attr_name = f"_{func.__name__}_cached"

    def __get__(self, instance, owner) -> Any:
        if not hasattr(instance, self.attr_name):
            setattr(instance, self.attr_name, self.func(instance))
        return getattr(instance, self.attr_name)



def verify_column_schema(df: DataFrame, column_name: str, expected_schema: DataType) -> bool:
    """
    Verifica si una columna en un DataFrame tiene el esquema esperado.

    Args:
        df (DataFrame): El DataFrame de PySpark que contiene la columna.
        column_name (str): El nombre de la columna a verificar.
        expected_schema (DataType): El esquema esperado de la columna (por ejemplo, ArrayType, StructType, etc.).

    Returns:
        bool: True si la columna tiene el esquema esperado, False en caso contrario.
    """
    # Obtener el esquema de la columna
    column_schema = next((field.dataType for field in df.schema.fields if field.name == column_name), None)
    # Comparar el esquema
    return column_schema == expected_schema



def all_rows_are_null_condition(columns: List[str]) -> Column:
    """
    Crea una condición que verifica si todas las columnas especificadas son nulas.

    Args:
        columns (List[str]): Una lista de nombres de columnas para verificar.

    Returns:
        Column: Una columna de PySpark que representa la condición lógica
            de que todas las columnas en la lista son nulas.
            Si la lista contiene una sola columna, verifica únicamente esa columna.
            Si la lista contiene múltiples columnas, aplica una condición AND entre ellas.
    """
    return (
        (col(columns[0]).isNull() if len(columns) == 1 else
        reduce(lambda x, y: x & col(y).isNull(), columns[1:], col(columns[0]).isNull()))
    )

def make_column_nullable(df: DataFrame, column_name: str, session:SparkSession) -> DataFrame:
    """
    Modifica un DataFrame de PySpark para que una columna específica sea nullable.

    Args:
        df (DataFrame): El DataFrame original de PySpark.
        column_name (str): El nombre de la columna que se desea hacer nullable.
        session (SparkSession): La sesión de Spark utilizada para recrear el DataFrame.

    Returns:
        DataFrame: Un nuevo DataFrame con el esquema modificado, donde la columna especificada es nullable.
    """
    # Obtén el esquema actual
    schema = df.schema

    # Modifica el esquema para que la columna sea nullable
    new_fields = [
        StructField(field.name, field.dataType, True if field.name == column_name else field.nullable)
        for field in schema.fields
    ]
    new_schema = StructType(new_fields)

    # Recrea el DataFrame con el nuevo esquema
    return session.createDataFrame(df.rdd, schema=new_schema)



def differences_in_pairs(columns: List[str], prefixes: List[str]) -> Column:
    if len(prefixes) != 2:
        raise ValueError("Debe haber exactamente dos prefijos.")

    # Generar pares de columnas para cada columna base
    column_pairs = [(f"{prefixes[0]}_{col_name}", f"{prefixes[1]}_{col_name}") for col_name in columns]

    # Crear una expresión lógica para comparar los pares de columnas
    comparison_expr = [
        when(
            (col(pair[0]).isNull() & col(pair[1]).isNotNull()) |
            (col(pair[0]).isNotNull() & col(pair[1]).isNull()) |
            (col(pair[0]) != col(pair[1])),
            lit(1)
        ).otherwise(lit(0))
        for pair in column_pairs
    ]

    # Combinar las expresiones en una sola columna usando reduce
    return reduce(lambda x, y: x + y, comparison_expr)


def align_columns(df_1: DataFrame, df_2: DataFrame, session:SparkSession, fill_value=None, ignore_columns=None) -> DataFrame:
    """
    Alinea las columnas de df_1 con las de df_2.
    - Elimina columnas adicionales en df_1 que no están en df_2 (excepto las ignoradas).
    - Agrega columnas faltantes en df_1 con un valor predeterminado basado en df_2.
    - Garantiza que las columnas de df_1 tengan los mismos tipos de datos que df_2.
    - Las columnas ignoradas se devuelven sin ser casteadas y al final del orden.
    - Asegura que df_2 no contenga columnas con nombres en ignore_columns.

    Args:
        df_1 (DataFrame): DataFrame que cambia.
        df_2 (DataFrame): DataFrame de referencia.
        fill_value: Valor con el que se rellenarán las columnas faltantes (por defecto, None).
        ignore_columns (list): Lista de columnas de df_1 que no serán casteadas ni alineadas.

    Returns:
        DataFrame: DataFrame df_1 modificado.
    """
    if ignore_columns is None:
        ignore_columns = []

    # Filtrar columnas ignoradas de df_2
    df_2_columns = [col for col in df_2.columns if col not in ignore_columns]
    df_2 = df_2.select(*df_2_columns)

    # Obtener las columnas de ambos DataFrames
    df_1_columns = set(df_1.columns)
    df_2_columns = set(df_2.columns)

    # Filtrar las columnas ignoradas en df_1
    ignored_columns = [col for col in ignore_columns if col in df_1_columns]

    # Eliminar columnas adicionales en df_1 (excepto las ignoradas)
    columns_to_keep = df_1_columns.intersection(df_2_columns)
    df_1 = df_1.select(*columns_to_keep, *ignored_columns)

    # Agregar columnas faltantes con valores predeterminados y tipos correctos
    for field in df_2.schema.fields:
        col = field.name
        col_type = field.dataType  # Tipo de dato de la columna en df_2
        if col not in df_1_columns:
            default_value = fill_value if fill_value is not None else None
            df_1 = df_1.withColumn(col, lit(default_value).cast(col_type))
        else:
            # Garantizar que las columnas existentes tengan el mismo tipo (excepto las ignoradas)
            if col not in ignore_columns:
                df_1 = df_1.withColumn(col, df_1[col].cast(col_type))
                if not next((field.nullable for field in df_1.schema.fields if field.name == col), True):
                    df_1 = make_column_nullable(df_1, col, session)

    # Asegurar el orden de las columnas según df_2 y agregar las ignoradas al final
    return df_1.select(*df_2.columns, *ignored_columns)



def check_last_step_dropped(df: DataFrame, history_column: str, ouput_column:str = "is_dropped") -> DataFrame:
    """
    Revisa si el último elemento del array en la columna `history` tiene un campo `step` que comienza con `dropped_at`.

    Args:
        df (DataFrame): DataFrame de entrada.
        history_column (str): Nombre de la columna que contiene el array de historial.

    Returns:
        DataFrame: DataFrame con una nueva columna `is_dropped` que indica si el último `step` comienza con `dropped_at`.
    """
    return df.withColumn(
        ouput_column,
        expr(f"""
            CASE
                WHEN size({history_column}) > 0 AND
                    element_at({history_column}, -1).step LIKE 'dropped_at%'
                THEN 1.0
                ELSE 0.0
            END
        """)
    )


class HistoryColumnDataFrame:
    """
    Clase para manejar el historial de columnas en un DataFrame de PySpark.

    Esta clase permite gestionar el historial de cambios en un DataFrame, incluyendo
    la adición, concatenación, eliminación y mantenimiento de registros históricos.

    Atributos privados:
        __history_column (str): Nombre de la columna de historial.
        __history_schema (ArrayType): Esquema de la columna de historial.

    Métodos:
        __init__(update, step_name, key_columns, value_columns, history_hdfs, tmp_hdfs, session):
            Inicializa la clase con los parámetros necesarios.
        check_if_contains_history_column() -> bool:
            Verifica si el DataFrame contiene la columna de historial con el esquema esperado.
        current_history() -> DataFrame:
            Obtiene el historial actual del DataFrame.
        update_history() -> DataFrame:
            Genera el historial actualizado basado en el DataFrame de entrada.
        current_history_existed() -> bool:
            Verifica si el historial actual existe en el HDFS.
        _new_history(df) -> DataFrame:
            Crea una nueva columna de historial en el DataFrame.
        _new_history_dropped(df) -> DataFrame:
            Crea una nueva columna de historial para registros eliminados.
        joined() -> DataFrame:
            Une el historial actual y el actualizado, identificando diferencias.
        added_to_history() -> DataFrame:
            Filtra y genera registros que se agregan al historial.
        concatenated_to_history() -> DataFrame:
            Filtra y concatena registros con diferencias al historial.
        dropped_to_history() -> DataFrame:
            Filtra y genera registros eliminados en el historial.
        kept_to_history() -> DataFrame:
            Filtra y mantiene registros sin diferencias en el historial.
        filled() -> DataFrame:
            Combina todos los registros procesados en un DataFrame final.
        reloaded() -> DataFrame:
            Sobrescribe el historial en el HDFS y recarga el DataFrame.

    Atributos:
        update (DataFrame): DataFrame de entrada con datos actualizados.
        key_columns (List[str]): Columnas clave para identificar registros.
        value_columns (List[str]): Columnas de valores que se comparan.
        step_name (str): Nombre del paso actual en el historial.
        hdfs (Dict[str, HivePath]): Rutas HDFS para el historial y temporal.
        session (SparkSession): Sesión de PySpark.
        complement_column (str): Nombre de la columna de complemento.
        tef_column (str): Nombre de la columna TEF.
    """
    __history_column = "history"
    __history_schema = ArrayType(
        StructType([
            StructField("step", StringType(), True),
            StructField("complement", StringType(), True),
            StructField("tef", LongType(), True)
        ]), containsNull=True
    )
    __null_value = -99999

    def __init__(
        self,
        update: DataFrame,
        step_name:str,
        key_columns: List[str],
        value_columns: List[str],
        history_hdfs: HivePath,
        tmp_hdfs: HivePath,
        session:SparkSession,
        history_schema: ArrayType = __history_schema,
    )-> None:
        """
        Clase para manejar el historial de columnas en un DataFrame de PySpark.

        Esta clase permite gestionar el historial de cambios en un DataFrame, incluyendo
        la adición, concatenación, eliminación y mantenimiento de registros históricos.

        Atributos privados:
            __history_column (str): Nombre de la columna de historial.
            __history_schema (ArrayType): Esquema de la columna de historial.

        Métodos:
            __init__(update, step_name, key_columns, value_columns, history_hdfs, tmp_hdfs, session, history_schema):
                Inicializa la clase con los parámetros necesarios.
            check_if_contains_history_column() -> bool:
                Verifica si el DataFrame contiene la columna de historial con el esquema esperado.
            current_history() -> DataFrame:
                Obtiene el historial actual del DataFrame.
            update_history() -> DataFrame:
                Genera el historial actualizado basado en el DataFrame de entrada.
            current_history_existed() -> bool:
                Verifica si el historial actual existe en el HDFS.
            _new_history(df) -> DataFrame:
                Crea una nueva columna de historial en el DataFrame.
            _new_history_dropped(df) -> DataFrame:
                Crea una nueva columna de historial para registros eliminados.
            joined() -> DataFrame:
                Une el historial actual y el actualizado, identificando diferencias.
            added_to_history() -> DataFrame:
                Filtra y genera registros que se agregan al historial.
            concatenated_to_history() -> DataFrame:
                Filtra y concatena registros con diferencias al historial.
            dropped_to_history() -> DataFrame:
                Filtra y genera registros eliminados en el historial.
            kept_to_history() -> DataFrame:
                Filtra y mantiene registros sin diferencias en el historial.
            filled() -> DataFrame:
                Combina todos los registros procesados en un DataFrame final.
            reloaded() -> DataFrame:
                Sobrescribe el historial en el HDFS y recarga el DataFrame.

        Atributos:
            update (DataFrame): DataFrame de entrada con datos actualizados.
            key_columns (List[str]): Columnas clave para identificar registros.
            value_columns (List[str]): Columnas de valores que se comparan.
            step_name (str): Nombre del paso actual en el historial.
            hdfs (Dict[str, HivePath]): Rutas HDFS para el historial y temporal.
            session (SparkSession): Sesión de PySpark.
        """
        update_selection = list(set(key_columns + value_columns))
        self.update = update.select(update_selection)
        self.key_columns = key_columns
        self.value_columns = value_columns
        self.step_name = step_name
        self.hdfs = {
            "history": history_hdfs,
            "tmp_history": tmp_hdfs
        }
        self.session = session
        self.history_schema = history_schema

    @cached_property
    def current_history(self) -> DataFrame:
        """
        Obtiene el historial actual del DataFrame desde el HDFS o crea un DataFrame vacío si no existe.

        Este método realiza las siguientes operaciones:
        1. Si el historial actual existe:
            - Mueve el historial desde la ruta HDFS principal a una ruta temporal.
            - Lee el historial desde la ruta temporal como un DataFrame de PySpark.
            - Verifica que todas las columnas de valores (`value_columns`) estén presentes en el DataFrame.
        2. Si el historial actual no existe:
            - Crea un DataFrame vacío con el esquema del historial actualizado.

        Finalmente, selecciona las columnas clave, las columnas de valores, la columna de historial/

        Returns:
            DataFrame: El DataFrame que representa el historial actual, con las columnas clave,
                columnas de valores, columna de historial.
        """
        if self.current_history_existed:
            # move to tmp path
            self.hdfs["history"].mv(self.hdfs["tmp_history"].parent, overwrite=True)
            result = self.session.read.parquet(str(self.hdfs["tmp_history"]))
            # result = self.session.read.parquet(str(self.hdfs["history"]))
            #
            # check if all columns are present
            result = result.distinct()
            if all(column in result.columns for column in self.value_columns):
                pass
            else: # add and update
                result = align_columns(result, self.update, fill_value=self.__null_value, ignore_columns=[self.__history_column], session=self.session)
                result = self.align_history_column(result, self.history_schema)
        else:
            result = (
                self.session.createDataFrame([], self.update_history.schema)
            )
        result_selection = list(set(self.key_columns + self.value_columns + [self.__history_column]))
        return result.select(result_selection)

    @cached_property
    def update_history(self) -> DataFrame:
        """
        Lee el nuevo DataFrame que se utilizará para actualizar el historial,
        y agrega las columnas `history`.

        Este método realiza las siguientes operaciones:
        2. Llama al método `_new_history` para crear una nueva columna de historial en el DataFrame.

        Returns:
            DataFrame: El DataFrame actualizado con las columnas clave, columnas de valores,
        """
        assert all(column in self.update.columns for column in self.value_columns + self.key_columns), \
            f"Las columnas {self.value_columns + self.key_columns} deben estar presentes en el DataFrame de actualización."
        result_selection = list(set(self.key_columns + self.value_columns))
        result = self.update.select(result_selection)
        result = result.dropna()
        result = self._new_history(result)
        result = result.distinct()
        return result

    @cached_property
    def current_history_existed(self) -> bool:
        """
        Verifica si el historial actual existe en el HDFS.

        Este método utiliza la ruta HDFS especificada en el atributo `hdfs["history"]`
        para comprobar si existe un archivo en formato Parquet en esa ubicación.

        Returns:
            bool: `True` si el archivo Parquet existe en la ruta del historial,
                `False` en caso contrario.
        """
        return self.hdfs["history"].is_parquet()

    def _new_history(self, df: DataFrame) -> DataFrame:
        """
        Crea una nueva columna de historial en el DataFrame.

        Este método agrega una columna de historial al DataFrame proporcionado. La columna de historial
        contiene un arreglo de estructuras con los siguientes campos:
        - `step`: El nombre del paso actual, especificado por el atributo `step_name`.
        - `complement`: El valor de la columna de complemento, convertido a tipo `StringType`.
        - `tef`: El valor de la columna TEF, convertido a tipo `LongType`.

        Además, la columna de historial se hace nullable utilizando la función `make_column_nullable`.

        Args:
            df (DataFrame): El DataFrame al que se le agregará la nueva columna de historial.

        Returns:
            DataFrame: El DataFrame con la nueva columna de historial agregada.
        """
        # Crear el diccionario history_values_relation dinámicamente
        history_values_relation = {
            field.name: col(field.name)
            for field in self.history_schema.elementType.fields
            if field.name != "step"  # Excluir el campo constante "step"
        }

        result = self.add_history_column(
            df = df,
            history_column=self.__history_column,
            step_name=self.step_name,
            history_values_relation=history_values_relation,
            history_schema=self.history_schema
        )

        result = make_column_nullable(result, self.__history_column, self.session)
        return result

    def _new_history_dropped(self, df: DataFrame) -> DataFrame:
        """
        Este método agrega una columna al DataFrame proporcionado que representa el historial
        de los registros eliminados.

        La nueva columna contiene un arreglo de estructuras con los siguientes campos:
        - `step`: Indica el paso en el que se eliminó el registro, utilizando el nombre del paso actual
        (`step_name`) con el prefijo `dropped_at_`.
        - `complement`: Un valor nulo (`None`) convertido al tipo `StringType`.
        - `tef`: Un valor nulo (`None`) convertido al tipo `LongType`.

        Además, la columna se hace nullable utilizando la función `make_column_nullable`.

        Args:
            df (DataFrame): El DataFrame al que se le agregará la nueva columna de historial para registros eliminados.

        Returns:
            DataFrame: El DataFrame con la nueva columna de historial para registros eliminados agregada.
        """
        history_values_relation = {
            field.name: lit(None)
            for field in self.history_schema.elementType.fields
            if field.name != "step"  # Excluir el campo constante "step"
        }

        result = self.add_history_column(
            df = df,
            history_column=f"dropped_{self.__history_column}",
            step_name=f"dropped_at_{self.step_name}",
            history_values_relation=history_values_relation,
            history_schema=self.history_schema
        )
        result = make_column_nullable(result, f"dropped_{self.__history_column}", self.session)
        return result

    @staticmethod
    def align_history_column(df_1: DataFrame, history_schema: ArrayType) -> DataFrame:
        """
        Alinea la columna 'history' de df_1 con el esquema proporcionado en history_schema.
        - Agrega campos faltantes con valores nulos respetando el tipo definido en history_schema.

        Args:
            df_1 (DataFrame): DataFrame que se modificará.
            history_schema (ArrayType): Esquema de la columna 'history' de referencia.

        Returns:
            DataFrame: DataFrame df_1 modificado.
        """
        # Obtener el esquema actual de la columna 'history' en df_1
        current_schema = df_1.schema[HistoryColumnDataFrame.__history_column].dataType.elementType

        # Crear una lista de los nombres de los campos actuales y los del esquema de referencia
        current_fields = {field.name: field.dataType for field in current_schema.fields}
        reference_fields = {field.name: field.dataType for field in history_schema.elementType.fields}

        # Identificar los campos faltantes en df_1
        missing_fields = {name: dtype for name, dtype in reference_fields.items() if name not in current_fields}

        # Generar una expresión para agregar los campos faltantes con valores nulos
        add_missing_fields_expr = ", ".join(
            f"x.{name} AS {name}"  if name in current_fields else f"CAST(NULL AS {dtype.simpleString()}) AS {name}"
            for name, dtype in reference_fields.items()
        )

        expression = f"TRANSFORM(history, x -> STRUCT({add_missing_fields_expr}))"

        # Reemplazar la columna 'history' con los campos alineados
        df_1 = df_1.withColumn(
            HistoryColumnDataFrame.__history_column,
            expr(expression)
        )
        return df_1

    @cached_property
    def joined(self) -> DataFrame:
        """
        Une el historial actual y el actualizado, identificando diferencias entre ellos.

        Este método combina el historial actual (`current_history`) y el historial actualizado
        (`update_history`) utilizando una unión externa (`outer join`) basada en las columnas clave.
        Además, realiza las siguientes operaciones:

        1. Renombra las columnas de valores, historial en ambos DataFrames
        para diferenciarlas (`current_` y `update_`).
        2. Agrega una columna `current_exists` al historial actual y una columna `update_exists`
        al historial actualizado, indicando si los registros existen en cada DataFrame.
        3. Realiza una unión externa entre los DataFrames utilizando las columnas clave.
        4. Rellena los valores nulos en las columnas `current_exists` y `update_exists` con `0`.
        5. Agrega una columna `has_difference` que identifica si hay diferencias entre los valores
        de las columnas en el historial actual y el actualizado.

        Returns:
            DataFrame: Un DataFrame que combina el historial actual y el actualizado, con columnas
                adicionales para indicar la existencia de registros y las diferencias entre ellos.
        """
        # renames
        unique_columns = list(set(self.value_columns + [self.__history_column]))
        current_col_renames = [col(column).alias(f"current_{column}") for column in unique_columns]
        update_col_renames = [col(column).alias(f"update_{column}") for column in unique_columns]
        #
        current = (self.current_history
            .select(self.key_columns + current_col_renames)
            .withColumn("current_exists", lit(1))
        )
        update = (self.update_history
            .select(self.key_columns + update_col_renames)
            .withColumn("update_exists", lit(1))
        )
        result = (current.join(update, on=self.key_columns, how="outer")
            .withColumn("current_exists", coalesce(col("current_exists"), lit(0)))
            .withColumn("update_exists", coalesce(col("update_exists"), lit(0)))
            .withColumn("has_difference", differences_in_pairs(self.value_columns, prefixes=["current", "update"]))
        )
        return check_last_step_dropped(result, f"current_{self.__history_column}", "current_is_dropped")

    @cached_property
    def added_to_history(self) -> DataFrame:
        """
        Filtra y genera registros que se agregan al historial.
        """
        result = (self.joined
            .filter((col(f"current_{self.__history_column}").isNull()) & (col("update_exists") == 1.0))
            .withColumn(self.__history_column, col(f"update_{self.__history_column}"))
            .drop(f"current_{self.__history_column}", f"update_{self.__history_column}", "current_is_dropped")
        )
        for column in self.value_columns:
            if column not in self.key_columns:
                result = result.withColumnRenamed(existing=f"update_{column}", new=column)
                result = result.drop(f"current_{column}", f"update_{column}")
        return result

    @cached_property
    def concatenated_to_history(self) -> DataFrame:
        """
        Filtra y concatena registros con diferencias al historial.
        """
        result = (self.joined
            .filter((col(f"current_{self.__history_column}").isNotNull()) & (col("update_exists") == 1.0) & (col("has_difference") != 0.0))
            .withColumn(self.__history_column, concat(col(f"current_{self.__history_column}"), col(f"update_{self.__history_column}")))
            .drop(f"current_{self.__history_column}", f"update_{self.__history_column}", "current_is_dropped")
        )
        for column in self.value_columns:
            if column not in self.key_columns:
                result = result.withColumnRenamed(existing=f"update_{column}", new=column)
                result = result.drop(f"current_{column}", f"update_{column}")
        return result

    @cached_property
    def dropped_to_history(self) -> DataFrame:
        """
        Filtra y genera registros eliminados en el historial.
        """
        result = (self.joined
            .filter(
                (col(f"current_{self.__history_column}").isNotNull())
                & (col("update_exists") == 0.0)
                & (col("has_difference") != 0.0)
                & (col("current_is_dropped") == 0.0)  # Asegura que no esté ya marcado como eliminado
            )
        )
        result = (self._new_history_dropped(result)
            .withColumn(self.__history_column, concat(col(f"current_{self.__history_column}"), col(f"dropped_{self.__history_column}")))
            .drop(f"current_{self.__history_column}", f"update_{self.__history_column}", f"dropped_{self.__history_column}", "current_is_dropped")
        )

        for column in self.value_columns:
            if column not in self.key_columns:
                result = result.withColumnRenamed(existing=f"update_{column}", new=column)
                result = result.drop(f"current_{column}", f"update_{column}")
        return result


    @cached_property
    def kept_to_history(self) -> DataFrame:
        """
        Filtra y mantiene registros sin diferencias en el historial.
        """
        result = (self.joined
            .filter(
                (col("has_difference") == 0.0)
                | ((col(f"current_{self.__history_column}").isNotNull())
                    & (col("update_exists") == 0.0)
                    & (col("has_difference") != 0.0)
                    & (col("current_is_dropped") == 1.0)
                )
            )

            .withColumn(self.__history_column, col(f"current_{self.__history_column}"))
            .drop(f"current_{self.__history_column}", f"update_{self.__history_column}", "current_is_dropped")
        )
        for column in self.value_columns:
            if column not in self.key_columns:
                result = result.withColumnRenamed(existing=f"update_{column}", new=column)
                result = result.drop(f"current_{column}", f"update_{column}")
        return result

    @cached_property
    def filled(self) -> DataFrame:
        """
        Combina todos los registros procesados en un DataFrame final.
        """
        column_order = self.added_to_history.columns
        return (self.added_to_history
            .union(self.concatenated_to_history.select(column_order))
            .union(self.dropped_to_history.select(column_order))
            .union(self.kept_to_history.select(column_order))
            .drop("current_exists", "update_exists", "has_difference", "current_dropped")
        )

    @cached_property
    def reloaded(self) -> DataFrame:
        """
        Sobrescribe el historial en el HDFS y recarga el DataFrame.
        """
        self.filled.write.mode("overwrite").parquet(str(self.hdfs["history"]))
        try:
            self.hdfs["tmp_history"].rmdir(recursive=True, skip_trash=True)
        except:
            pass
        return self.session.read.parquet(str(self.hdfs["history"]))


    @staticmethod
    def add_history_column(
        df: DataFrame,
        history_column:str,
        step_name:str,
        history_values_relation: Dict[str, Column],
        history_schema:ArrayType
    )-> DataFrame:
        """
        Añade una columna de tipo ArrayType a un DataFrame con un esquema definido por `history_schema`.

        La nueva columna se construye como un array que contiene un único struct. Este struct incluye un campo constante
        llamado "step" con el valor `step_name`, y otros campos definidos dinámicamente en `history_values_relation`.
        Los valores de estos campos se obtienen de las columnas del DataFrame y se castean a los tipos especificados
        en `history_schema`.

        Args:
            df (DataFrame): El DataFrame de PySpark al que se añadirá la nueva columna.
            history_column (str): El nombre de la nueva columna que se añadirá al DataFrame.
            step_name (str): El valor constante que se asignará al campo "step" en el struct.
            history_values_relation (dict): Un diccionario que mapea los nombres de los campos del esquema
                (`history_schema`) a las columnas del DataFrame. Ejemplo: {"complement": col("complement"), "tef": col("tef")}.
            history_schema (ArrayType): El esquema de la nueva columna, de tipo ArrayType, que define los nombres
                y tipos de los campos del struct. Ejemplo:
                ArrayType(
                    StructType([
                        StructField("step", StringType(), True),
                        StructField("complement", StringType(), True),
                        StructField("tef", LongType(), True)
                    ])
                )

        Returns:
            DataFrame: Un nuevo DataFrame con la columna añadida. La nueva columna será de tipo ArrayType y contendrá
                un array con un único struct que sigue el esquema definido en `history_schema`.

        Example:
            >>> from pyspark.sql import SparkSession
            >>> from pyspark.sql.functions import col
            >>> from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
            >>>
            >>> spark = SparkSession.builder.master("local").appName("Example").getOrCreate()
            >>> data = [("value1", 123), ("value2", 456)]
            >>> schema = StructType([
            ...     StructField("complement", StringType(), True),
            ...     StructField("tef", LongType(), True)
            ... ])
            >>> df = spark.createDataFrame(data, schema)
            >>>
            >>> history_column = "history"
            >>> step_name = "Step1"
            >>> history_values_relation = {
            ...     "complement": col("complement"),
            ...     "tef": col("tef")
            ... }
            >>> history_schema = ArrayType(
            ...     StructType([
            ...         StructField("step", StringType(), True),
            ...         StructField("complement", StringType(), True),
            ...         StructField("tef", LongType(), True)
            ...     ])
            ... )
            >>>
            >>> result_df = add_history_column(df, history_column, step_name, history_values_relation, history_schema)
            >>> result_df.show(truncate=False)
            +----------+----+---------------------------+
            |complement|tef |history                    |
            +----------+----+---------------------------+
            |value1    |123 |[{Step1, value1, 123}]     |
            |value2    |456 |[{Step1, value2, 456}]     |
            +----------+----+---------------------------+
        """
        # Crear una lista de expresiones para los campos del struct
        struct_fields = [lit(step_name).alias("step")]  # Campo constante "step"
        #
        for field_name, field_col in history_values_relation.items():
            # Buscar el tipo de dato del campo en el esquema
            field_type = next(
                (f.dataType for f in history_schema.elementType.fields if f.name == field_name),
                None
            )
            if field_type:
                # Añadir el campo casteado al struct
                struct_fields.append(field_col.cast(field_type).alias(field_name))
        #
        # Crear la nueva columna con el array de struct
        return df.withColumn(history_column, array(struct(*struct_fields)))
