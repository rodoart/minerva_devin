# regular
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Optional, Tuple, List, Callable, Dict
from typing import Union
import random
from functools import cached_property

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType

# custom
from ...path import HivePath
from ..tools.partitions_lags import SparkTwoPartitionMonthlyInterval
from ..tools.partitions_lags import (get_partitions_from_dataframe,
    get_partitions_from_parquet_path, get_partitions_from_hive_table,
    delete_partition_from_table, get_partition_hdfs_path)
from .utils import is_table_or_parquet, convert_partitions_to_spark_filter, check_if_table_exists

class SparkLoadTableOrParquet:
    """
    Clase para cargar datos desde una tabla de Hive o un archivo Parquet en Spark, aplicar filtros y expresiones, y registrar una tabla temporal.

    Atributos:
    ----------
    session : SparkSession
        La sesión de Spark utilizada para leer los datos.
    select : str, opcional
        La cláusula SELECT para la consulta SQL. Por defecto es "*".
    filter : str, opcional
        La cláusula WHERE para la consulta SQL. Por defecto es una cadena vacía.
    expressions : list de tuplas (str, str), opcional
        Lista de expresiones a aplicar al DataFrame resultante. Cada tupla contiene el nombre de la nueva columna y la expresión correspondiente.

    Métodos:
    --------
    register() -> str:
        Registra una tabla temporal en Spark a partir de una tabla de Hive o un archivo Parquet y devuelve el nombre de la tabla registrada.
    clean_query(text: str) -> str:
        Limpia una consulta SQL eliminando líneas vacías y espacios en blanco al inicio de cada línea.
    query() -> str:
        Genera y devuelve la consulta SQL basada en los atributos select y filter.
    _expressions_applier(df: DataFrame) -> DataFrame:
        Aplica las expresiones definidas en el atributo expressions al DataFrame proporcionado.
    df() -> DataFrame:
        Ejecuta la consulta SQL generada, aplica las expresiones y devuelve el DataFrame resultante.
    """
    def __init__(
        self,
        table_or_hdfs: Union[str, HivePath],
        session: SparkSession,
        select:Optional[str] = "*",
        filter:Optional[str] = "",
        expressions:List[Tuple[str, str]] = [],
    ) -> None:
        """
        Inicializa una instancia de SparkLoadTableOrParquet.

        Parámetros:
        -----------
        table_or_hdfs : Union[str, HivePath]
            El nombre de la tabla de Hive o la ruta del archivo Parquet.
        session : SparkSession
            La sesión de Spark utilizada para leer los datos.
        select : str, opcional
            La cláusula SELECT para la consulta SQL. Por defecto es "*".
        filter : str, opcional
            La cláusula WHERE para la consulta SQL. Por defecto es una cadena vacía.
        expressions : list de tuplas (str, str), opcional
            Lista de expresiones a aplicar al DataFrame resultante. Cada tupla contiene el nombre de la nueva columna y la expresión correspondiente.
        """
        #
        self.session = session
        self.select = select
        self.filter = filter
        self.expressions = expressions
        #
        # read table or hdfs
        self.input_type = is_table_or_parquet(table_or_hdfs)
        #
        if self.input_type == "table":
            self.table_name = table_or_hdfs
            self.hive_path = None
        elif self.input_type == "hdfs":
            if isinstance(table_or_hdfs, str):
                self.hive_path = HivePath(table_or_hdfs)
            else:
                self.hive_path = table_or_hdfs
            self.table_name = None
    #
    @cached_property
    def register(self) -> str:
        """
        Registra una tabla temporal en Spark a partir de una tabla de Hive o un archivo Parquet.

        Devuelve:
        ----------
        str:
            El nombre de la tabla registrada.
        """
        if self.table_name is not None: # table
            self.table_register = self.table_name
        elif self.hive_path is not None: # parquet
            self.table_register = "tmp_" + self.hive_path.name + "_" + "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
            df: DataFrame = self.session.read.parquet(str(self.hive_path))
            df.createOrReplaceTempView(self.table_register)
        return self.table_register
    #
    @staticmethod
    def clean_query(text: str) -> str:
        """
        Limpia una consulta SQL eliminando líneas vacías y espacios en blanco al inicio de cada línea.

        Parámetros:
        -----------
        text : str
            La consulta SQL a limpiar.

        Devuelve:
        ---------
        str:
            La consulta SQL limpia.
        """
        # Dividir el texto en líneas
        lines = text.splitlines()
        #
        # Eliminar líneas vacías y espacios en blanco al inicio de cada línea
        cleaned_lines = [line.lstrip() for line in lines if line.strip()]
        #
        # Unir las líneas de nuevo en un solo string
        cleaned_text = "\n".join(cleaned_lines)
        #
        return cleaned_text
    #
    @property
    def query(self) -> str:
        """
        Genera y devuelve la consulta SQL basada en los atributos select y filter.

        Devuelve:
        ----------
        str:
            La consulta SQL generada.
        """
        query = f"""
        SELECT
            {self.select}
        FROM
            {self.register}
        """
        if len(self.filter) > 0:
            query += f"""
            WHERE
                {self.filter}
            """
        return self.clean_query(query)
    #
    def _expressions_applier(self, df:DataFrame) -> DataFrame:
        """
        Aplica las expresiones definidas en el atributo expressions al DataFrame proporcionado.

        Parámetros:
        -----------
        df : DataFrame
            El DataFrame al que se aplicarán las expresiones.

        Devuelve:
        ----------
        DataFrame:
            El DataFrame resultante después de aplicar las expresiones.
        """
        result = df
        #
        for expression_tuple in self.expressions:
            assert len(expression_tuple) == 2, "Expression tuple must have two elements"
            new_column_name = expression_tuple[0]
            expression = expression_tuple[1]
            result = result.withColumn(new_column_name, expr(expression))
        return result
    #
    @cached_property
    def df(self) -> DataFrame:
        """
        Ejecuta la consulta SQL generada, aplica las expresiones y devuelve el DataFrame resultante.

        Devuelve:
        ----------
        DataFrame:
            El DataFrame resultante después de ejecutar la consulta SQL y aplicar las expresiones.
        """
        result = self.session.sql(self.query)
        # Apply expressions
        if len(self.expressions) > 0:
            result = self._expressions_applier(result)
        return result


class SparkLoadPartitionedTableOrParquet(SparkLoadTableOrParquet, SparkTwoPartitionMonthlyInterval):
    """
    This class is responsible for loading partitioned tables or Parquet files into a Spark DataFrame.
    It extends the functionality of SparkLoadTableOrParquet and SparkTwoPartitionMonthlyInterval classes.

    Attributes:
        table_or_hdfs (Union[str, HivePath]): Path to the table or HDFS location.
        information_date_column (str): Column name for the information date.
        current_date (date): Current date for processing.
        lag (int): Lag value for partitioning.
        history (int): History value for partitioning.
        session (SparkSession): Spark session object.
        select (Optional[str]): Columns to select. Defaults to "*".
        filter (Optional[str]): Filter condition. Defaults to "".
        expressions (List[Tuple[str, str]]): List of expressions to apply. Defaults to [].
        process_date_column (Optional[str]): Column name for the process date. Defaults to None.
        information_date_mode (Optional[str]): Mode for information date. Defaults to "each".
        process_date_mode (Optional[str]): Mode for process date. Defaults to "each".
        incremental_lag (Optional[int]): Incremental lag value. Defaults to 0.
        date_format (Optional[str]): Date format string. Defaults to "%Y-%m-%d".
    """
    def __init__(
        self,
        table_or_hdfs: Union[str, HivePath],
        information_date_column: str,
        current_date:date,
        lag:int,
        history:int,
        session: SparkSession,
        missing_months_allowed:Optional[bool]=False,
        select:Optional[str] = "*",
        filter:Optional[str] = "",
        expressions:List[Tuple[str, str]] = [],
        process_date_column: Optional[str] = None,
        information_date_mode: Optional[str] = "each",
        process_date_mode: Optional[str] = "each",
        incremental_lag:Optional[int]=0,
        date_format: Optional[str] = "%Y-%m-%d",
    ) -> None:
        """
        Constructor for the SparkLoadPartitionedTableOrParquet class.

        Args:
            table_or_hdfs (Union[str, HivePath]): Path to the table or HDFS location.
            information_date_column (str): Column name for the information date.
            current_date (date): Current date for processing.
            lag (int): Lag value for partitioning.
            history (int): History value for partitioning.
            session (SparkSession): Spark session object.
            select (Optional[str], optional): Columns to select. Defaults to "*".
            filter (Optional[str], optional): Filter condition. Defaults to "".
            expressions (List[Tuple[str, str]], optional): List of expressions to apply. Defaults to [].
            process_date_column (Optional[str], optional): Column name for the process date. Defaults to None.
            information_date_mode (Optional[str], optional): Mode for information date. Defaults to "each".
            process_date_mode (Optional[str], optional): Mode for process date. Defaults to "each".
            incremental_lag (Optional[int], optional): Incremental lag value. Defaults to 0.
            date_format (Optional[str], optional): Date format string. Defaults to "%Y-%m-%d".

        Returns:
            None
        """

        SparkTwoPartitionMonthlyInterval.__init__(
            self,
            table_or_hdfs=table_or_hdfs,
            information_date_column=information_date_column,
            current_date=current_date,
            lag=lag,
            history=history,
            session=session,
            missing_months_allowed=missing_months_allowed,
            process_date_column=process_date_column,
            incremental_lag=incremental_lag,
            information_date_mode=information_date_mode,
            process_date_mode=process_date_mode,
            date_format=date_format
        )
        init_filter = self._filter_checker(filter)
        init_select = self._select_checker(select)
        init_expressions = expressions + [self._tfrom_expression()]
        #
        SparkLoadTableOrParquet.__init__(
            self,
            table_or_hdfs,
            session,
            init_select,
            init_filter,
            init_expressions
        )
    #
    def _filter_checker(self, filter) -> str:
        """
        Checks and constructs the filter condition.

        Args:
            filter (str): Initial filter condition.

        Returns:
            str: Constructed filter condition.
        """
        init_filter = f"({self.sql_filter})"
        #
        if len(filter) > 0:
            init_filter += f" AND {filter}"
        return init_filter
    #
    def _select_checker(self, select) -> str:
        """
        Checks and constructs the select statement.

        Args:
            select (str): Initial select statement.

        Returns:
            str: Constructed select statement.
        """
        if select == "*":
            return select
        #
        result = select
        if self.information_date_column not in select:
            result += f", {self.information_date_column}"
        if self.information_date_column is not None and self.process_date_column not in select:
            result += f", {self.process_date_column}"
        return result
    #
    def _tfrom_expression(self) -> Tuple[str, str]:
        """
        Constructs the expression for the 'tfrom' column, which represents the difference in months
        between the current date and the information date. The value is positive when the current date
        is more recent than the information date and vice versa.

        Example cases:
            current_date = 2022-10-31
            information_date = 2022-10-03  -> tfrom = 0
            information_date = 2022-09-01  -> tfrom = 1
            information_date = 2022-08-01  -> tfrom = 2
            information_date = 2022-11-01  -> tfrom = -1

        Returns:
            Tuple[str, str]: Column name and expression for 'tfrom'.
        """
        current_date = self.current_date + relativedelta(months=-self._currently_incremented_lag)
        expression = f"CAST(months_between(to_date('{current_date.strftime(self.date_format)}'), to_date({self.information_date_column})) AS INT)"
        return ("tfrom", expression)

class OverwritePartitionBase:
    """
    Clase base para sobrescribir particiones en un DataFrame de Spark.

    Args:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        partition_by (Union[str, List[str]]): La columna o columnas por las que se particionará.
        path (Union[str, HivePath]): La ruta donde se almacenarán los datos particionados.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.
        get_partitions_from_path_function (Callable[[Union[str, HivePath], SparkSession], List[Dict[str, str]]]):
            Función que obtiene las particiones desde la ruta especificada.

    Attributes:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.
        path (Union[str, HivePath]): La ruta donde se almacenarán los datos particionados.
        partition_by (List[str]): Lista de columnas por las que se particionará.
        get_partitions_from_path_function (Callable[[Union[str, HivePath], SparkSession], List[Dict[str, str]]]):
            Función que obtiene las particiones desde la ruta especificada.
    """
    def __init__(
        self,
        df: DataFrame,
        partition_by:Union[str, List[str]],
        path:Union[str, HivePath],
        session:SparkSession,
        get_partitions_from_path_function: Callable[[Union[str, HivePath], SparkSession], List[Dict[str, str]]],
    ) -> None:
        self.df = df
        self.session = session
        self.path = path
        #
        if isinstance(partition_by, str):
            self.partition_by = [partition_by]
        else:
            self.partition_by = partition_by
        #
        self.get_partitions_from_path_function = get_partitions_from_path_function
    #
    @cached_property
    def exiting_partitions(self) -> List[Dict[str, str]]:
        """
        Obtiene las particiones existentes en la ruta especificada.

        Returns:
        List[Dict[str, str]]: Lista de particiones existentes.
        """
        return self.convert_partition_values_to_string(self.get_partitions_from_path_function(self.path, self.session))
    #
    @cached_property
    def partitions_to_add(self) -> List[Dict[str, str]]:
        """
        Obtiene las particiones que se deben agregar desde el DataFrame.

        Returns:
        List[Dict[str, str]]: Lista de particiones a agregar.
        """
        return self.convert_partition_values_to_string(get_partitions_from_dataframe(self.df, self.partition_by))
    #
    @cached_property
    def partitions_to_delete(self) -> List[Dict[str, str]]:
        """
        Obtiene las particiones que se deben eliminar. Identifica una partición como igual sin importar el orden.

        Returns:
        List[Dict[str, str]]: Lista de particiones a eliminar.
        """
        partitions_to_add_set = {frozenset(partition.items()) for partition in self.partitions_to_add}
        return [partition for partition in self.exiting_partitions if frozenset(partition.items()) in partitions_to_add_set]
    #
    @staticmethod
    def convert_partition_values_to_string(partitions:List[Dict[str, Union[str, date]]]) -> List[Dict[str, str]]:
        """
        Convierte los valores de las particiones a cadenas de texto.

        Args:
        partitions (List[Dict[str, Union[str, date]]]): Lista de particiones con valores de diferentes tipos.

        Returns:
        List[Dict[str, str]]: Lista de particiones con valores convertidos a cadenas de texto.
        """
        return [{key: str(value) for key, value in partition.items()} for partition in partitions]
    #

class OverwritePartitionParquet(OverwritePartitionBase):
    """
    Clase para sobrescribir particiones en un archivo Parquet en HDFS.

    Args:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        partition_by (Union[str, List[str]]): La columna o columnas por las que se particionará.
        hdfs_path (Union[str, HivePath]): La ruta HDFS donde se almacenarán los datos particionados.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.

    Attributes:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.
        partition_by (List[str]): Lista de columnas por las que se particionará.
        hdfs_path (HivePath): La ruta HDFS donde se almacenarán los datos particionados.
    """
    def __init__(
        self,
        df: DataFrame,
        partition_by:Union[str, List[str]],
        hdfs_path:Union[str, HivePath],
        session:SparkSession
    ) -> None:
        self.df = df
        self.session = session
        #
        if isinstance(partition_by, str):
            self.partition_by = [partition_by]
        else:
            self.partition_by = partition_by
        #
        if isinstance(hdfs_path, str):
            self.hdfs_path = HivePath(hdfs_path)
        else:
            self.hdfs_path = hdfs_path
        #
        super().__init__(
            df=self.df,
            partition_by=self.partition_by,
            path=self.hdfs_path,
            session=self.session,
            get_partitions_from_path_function=get_partitions_from_parquet_path
        )
    #

    def get_delete_hdfs_paths(self) -> List[HivePath]:
        """
        Obtiene las rutas HDFS de las particiones que se deben eliminar.

        Returns:
        List[HivePath]: Lista de rutas HDFS de las particiones a eliminar.
        """
        return [get_partition_hdfs_path(self.hdfs_path, partition) for partition in self.partitions_to_delete]
    #
    def delete_partitions(self) -> None:
        """
        Elimina las particiones especificadas en HDFS.
        """
        for partition_hdfs in self.get_delete_hdfs_paths():
            partition_hdfs.rmdir(recursive=True, skip_trash=True)
        return None
    #
    def write(self) -> None:
        """
        Escribe el DataFrame en HDFS, sobrescribiendo o añadiendo particiones según sea necesario.
        """
        # check if the parquet does not exist.
        if not self.hdfs_path.exists():
            self.df.write.mode("overwrite").partitionBy(*self.partition_by).parquet(str(self.hdfs_path))
        else:
            self.delete_partitions()
            self.df.write.mode("append").partitionBy(*self.partition_by).parquet(str(self.hdfs_path))
    #
    def read(self) -> DataFrame:
        """
        Lee los datos particionados desde HDFS.

        Returns:
            DataFrame: El DataFrame leído desde HDFS.
        """
        # read
        read_object = SparkLoadTableOrParquet(
            table_or_hdfs=self.hdfs_path,
            session=self.session,
            filter=convert_partitions_to_spark_filter(self.partitions_to_add)
        )
        return read_object.df



class OverwriteTwoPartitionParquet(OverwritePartitionParquet):
    """
    Clase para sobrescribir particiones en un archivo Parquet en HDFS.

    Args:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        partition_by (Union[str, List[str]]): La columna o columnas por las que se particionará.
        hdfs_path (Union[str, HivePath]): La ruta HDFS donde se almacenarán los datos particionados.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.

    Attributes:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.
        partition_by (List[str]): Lista de columnas por las que se particionará.
        hdfs_path (HivePath): La ruta HDFS donde se almacenarán los datos particionados.
    """
    def __init__(
        self,
        df: DataFrame,
        process_date_column:str,
        information_date_column:str,
        hdfs_path:Union[str, HivePath],
        session:SparkSession
    ) -> None:
        self.df = df
        self.session = session
        self.partition_by = [information_date_column, process_date_column]
        self.process_date_column = process_date_column
        self.information_date_column = information_date_column
        #
        if isinstance(hdfs_path, str):
            self.hdfs_path = HivePath(hdfs_path)
        else:
            self.hdfs_path = hdfs_path
        #
        super().__init__(
            df=self.df,
            partition_by=self.partition_by,
            hdfs_path=self.hdfs_path,
            session=self.session
        )
    #
    @property
    def partitions_to_delete(self) -> List[Dict[str, str]]:
        """
        Obtiene las particiones que se deben eliminar basándose únicamente en el campo 'information_date'.
        #
        Returns:
        List[Dict[str, str]]: Lista de particiones a eliminar.
        """
        # Extraer los valores de 'information_date' de las particiones a añadir
        information_dates_to_add = {partition[self.information_date_column] for partition in self.partitions_to_add}
        #
        # Filtrar las particiones existentes que tengan el mismo 'information_date'
        return [
            partition for partition in self.exiting_partitions
            if partition[self.information_date_column] in information_dates_to_add
        ]





class OverwritePartitionTable(OverwritePartitionBase):
    """
    Clase para sobrescribir particiones en una tabla Hive.

    Args:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        partition_by (Union[str, List[str]]): La columna o columnas por las que se particionará.
        table (str): El nombre de la tabla Hive donde se almacenarán los datos particionados.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.

    Attributes:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.
        table (str): El nombre de la tabla Hive donde se almacenarán los datos particionados.
        partition_by (List[str]): Lista de columnas por las que se particionará.
    """
    def __init__(
        self,
        df: DataFrame,
        partition_by:Union[str, List[str]],
        table:str,
        session:SparkSession
    ) -> None:
        self.df = df
        self.session = session
        self.table = table
        #
        if isinstance(partition_by, str):
            self.partition_by = [partition_by]
        else:
            self.partition_by = partition_by
        #
        super().__init__(
            df=self.df,
            partition_by=self.partition_by,
            path=self.table,
            session=self.session,
            get_partitions_from_path_function=get_partitions_from_hive_table
        )
    #
    #
    def delete_partitions(self) -> None:
        """
        Elimina las particiones especificadas en la tabla Hive.
        """
        for partition in self.partitions_to_delete:
            delete_partition_from_table(self.table, partition, self.session)
        return None
    #
    def write(self) -> None:
        """
        Escribe el DataFrame en la tabla Hive, sobrescribiendo o añadiendo particiones según sea necesario.
        """
        # check if the parquet does not exist.
        if not check_if_table_exists(self.table, self.session):
            self.df.write.mode("overwrite").partitionBy(*self.partition_by).saveAsTable(self.table)
        else:
            self.delete_partitions()
            self.df.write.mode("append").partitionBy(*self.partition_by).saveAsTable(self.table)
    #

    def read(self) -> DataFrame:
        """
        Lee los datos particionados desde la tabla Hive.

        Returns:
        DataFrame: El DataFrame leído desde la tabla Hive.
        """
        # read
        read_object = SparkLoadTableOrParquet(
            table_or_hdfs=self.table,
            session=self.session,
            filter=convert_partitions_to_spark_filter(self.partitions_to_add)
        )
        return read_object.df



class OverwriteTwoPartitionTable(OverwritePartitionTable):
    """
    Clase para sobrescribir particiones en una tabla Hive.

    Args:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        partition_by (Union[str, List[str]]): La columna o columnas por las que se particionará.
        table (str): El nombre de la tabla Hive donde se almacenarán los datos particionados.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.

    Attributes:
        df (DataFrame): El DataFrame de Spark que contiene los datos.
        session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.
        table (str): El nombre de la tabla Hive donde se almacenarán los datos particionados.
        partition_by (List[str]): Lista de columnas por las que se particionará.
    """
    def __init__(
        self,
        df: DataFrame,
        process_date_column:str,
        information_date_column:str,
        table:str,
        session:SparkSession
    ) -> None:
        self.df = df
        self.session = session
        self.table = table
        #
        self.partition_by = [information_date_column, process_date_column]
        self.process_date_column = process_date_column
        self.information_date_column = information_date_column
        #
        super().__init__(
            df=self.df,
            partition_by=self.partition_by,
            table=self.table,
            session=self.session
        )
    #
    def partitions_to_delete(self) -> List[Dict[str, str]]:
        """
        Obtiene las particiones que se deben eliminar basándose únicamente en el campo 'information_date'.
        #
        Returns:
        List[Dict[str, str]]: Lista de particiones a eliminar.
        """
        # Extraer los valores de 'information_date' de las particiones a añadir
        information_dates_to_add = {partition[self.information_date_column] for partition in self.partitions_to_add}
        #
        # Filtrar las particiones existentes que tengan el mismo 'information_date'
        return [
            partition for partition in self.exiting_partitions
            if partition[self.information_date_column] in information_dates_to_add
        ]



def overwrite_two_partition(
    df: DataFrame,
    process_date_column:str,
    information_date_column:str,
    table_or_hdfs:Union[str, HivePath],
    session:SparkSession
) -> Union[OverwriteTwoPartitionParquet, OverwriteTwoPartitionTable]:
    input_type = is_table_or_parquet(table_or_hdfs)
    #
    if input_type == "table":
        return OverwriteTwoPartitionTable(df, process_date_column, information_date_column, str(table_or_hdfs), session)
    elif input_type == "hdfs":
        return OverwriteTwoPartitionParquet( df, process_date_column, information_date_column, table_or_hdfs, session)
    else:
        raise ValueError("The input is not a table or a parquet file.")



def overwrite_partition(
    df: DataFrame,
    partition_by:Union[str, List[str]],
    table_or_hdfs:Union[str, HivePath],
    session:SparkSession
) -> Union[OverwritePartitionParquet, OverwritePartitionTable]:
    """
    Sobrescribe particiones en una tabla Hive o en un archivo Parquet en HDFS.

    Args:
    df (DataFrame): El DataFrame de Spark que contiene los datos.
    partition_by (Union[str, List[str]]): La columna o columnas por las que se particionará.
    table_or_hdfs (Union[str, HivePath]): El nombre de la tabla Hive o la ruta HDFS del archivo Parquet.
    session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.

    Returns:
    Union[OverwritePartitionParquet, OverwritePartitionTable]: Una instancia de OverwritePartitionParquet o OverwritePartitionTable.
    """
    input_type = is_table_or_parquet(table_or_hdfs)
    #
    if input_type == "table":
        return OverwritePartitionTable(df, partition_by, str(table_or_hdfs), session)
    elif input_type == "hdfs":
        return OverwritePartitionParquet( df, partition_by, table_or_hdfs, session)
    else:
        raise ValueError("The input is not a table or a parquet file.")







def save_empty_dataframe(
    schema: StructType,
    partition_by:Union[str, List[str]],
    table_or_hdfs:Union[str, HivePath],
    mode:str,
    session:SparkSession
) -> None:
    """

    Sobrescribe particiones en una tabla Hive o en un archivo Parquet en HDFS.

    Args:
    df (DataFrame): El DataFrame de Spark que contiene los datos.
    partition_by (Union[str, List[str]]): La columna o columnas por las que se particionará.
    table_or_hdfs (Union[str, HivePath]): El nombre de la tabla Hive o la ruta HDFS del archivo Parquet.
    session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.

    Returns:
    Union[OverwritePartitionParquet, OverwritePartitionTable]: Una instancia de OverwritePartitionParquet o OverwritePartitionTable.
    """
    assert mode in ["overwrite", "ignore", "error", "errorifexists"], "Mode must be one of 'overwrite', 'ignore', 'error', 'errorifexists'"
    input_type = is_table_or_parquet(table_or_hdfs)
    #
    df = session.createDataFrame([], schema)
    #
    if input_type == "table":
        table_or_hdfs = str(table_or_hdfs)
        #
        if check_if_table_exists(table_or_hdfs, session):
            if mode == "ignore":
                return
            elif mode in ["error", "errorifexists"]:
                raise ValueError(f"The table {table_or_hdfs} already exists.")
        #
        df.write.format("hive").mode("overwrite").partitionBy(partition_by).saveAsTable(table_or_hdfs)
        return
    else:
        df.write.mode(mode).partitionBy(partition_by).parquet(str(table_or_hdfs))
        return
