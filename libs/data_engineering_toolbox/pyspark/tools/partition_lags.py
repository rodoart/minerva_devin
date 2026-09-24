from copy import deepcopy
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from functools import reduce
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from typing import List, Tuple, Dict, Union, Optional
import pandas as pd
from functools import cached_property


from ...path import HivePath
from ...general.date_treatment import make_date_interval_with_lag_months
from . import is_table_or_parquet

from ...context.logging import get_logger
logger = get_logger(__name__)

def get_partitions_from_parquet_path(parquet_hdfs: Union[HivePath, str], session: Optional[SparkSession]=None) -> List[Dict[str, str]]:
    """
    Obtiene las particiones de un archivo Parquet en HDFS.

    Esta función obtiene todas las rutas relativas a la raiz del archivo Parquet,
    filtra las particiones más profundas y crea una lista de diccionarios de particiones.

    Args:
        parquet_hdfs (Union[HivePath, str]): Ruta del archivo Parquet en HDFS.

    Returns:
        List[Dict[str, str]]: Lista de diccionarios de particiones.
    """
    # Convertir la ruta a un objeto HivePath si es una cadena
    if isinstance(parquet_hdfs, str):
        parquet_hdfs = HivePath(parquet_hdfs)
    #
    # Obtener todas las rutas relativas a la raiz
    partition_hdfss = list(parquet_hdfs.listdirs(recursive=True))
    relatives = [str(partition_hdfs.relative_to(parquet_hdfs)).split("/") for partition_hdfs in partition_hdfss]
    #
    # Caso para archivos no particionados
    if relatives == []:
        return []
    #
    # Mantener solo las particiones más profundas
    max_depth = max([len(relative) for relative in relatives])
    relatives = [relative for relative in relatives if len(relative) == max_depth]
    #
    # Crear lista de diccionarios de particiones usando comprensiones
    list_of_partition_dictionaries = [
        {string_value.split("=")[0]: string_value.split("=")[1] for string_value in partition}
        for partition in relatives
    ]
    return list_of_partition_dictionaries


def get_partitions_from_hive_table(table_name: str, session: SparkSession) -> List[Dict[str, str]]:
    """
    Obtiene las particiones de un archivo Parquet en HDFS.

    Args:
    hdfs_path (HivePath): La ruta HDFS del archivo Parquet.
    session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.

    Returns:
    List[Dict[str, str]]: Lista de diccionarios con las particiones.
    """
    #
    try:
        partitions_df = session.sql(f"SHOW PARTITIONS {table_name}")
    except AnalysisException as e:
        if "SHOW PARTITIONS is not allowed on a table that is not partitioned" in str(e):
            return []
        else:
            raise e  # Re-raise the exception if it's not the specific error we're handling
    #
    # Collect the partitions as a list of strings
    partitions = partitions_df.collect()
    # Case for unpartitioned
    #
    # Process the partitions to create a list of dictionaries
    list_of_partition_dictionaries = [
        {part.split('=')[0]: part.split('=')[1] for part in partition['partition'].split('/')}
        for partition in partitions
    ]
    #
    return list_of_partition_dictionaries


def get_partitions_from_dataframe(df:DataFrame, partition_by:Union[str, List[str]]) -> List[Dict[str, str]]:
    """
    Obtiene las particiones de un DataFrame de Spark.

    Args:
    df (DataFrame): El DataFrame de Spark que contiene los datos.
    partition_by (Union[str, List[str]]): La columna o columnas por las que se particionará.

    Returns:
    List[Dict[str, str]]: Lista de diccionarios con las particiones.
    """
    # variable format
    if isinstance(partition_by, str):
        partition_by = [partition_by]
    # get partitions
    partitions = df.select(*partition_by).distinct().collect()
    partitions = [{key: str(value) for key, value in partition.asDict().items()}
        for partition in partitions]
    return partitions

def get_partition_hdfs_path(hdfs_path:HivePath, partition:Dict[str, str]) -> HivePath:
    """
    Obtiene la ruta HDFS de una partición específica.

    Args:
    hdfs_path (HivePath): La ruta base HDFS.
    partition (Dict[str, str]): Un diccionario que representa la partición.

    Returns:
    HivePath: La ruta HDFS completa de la partición.
    """
    return hdfs_path.joinpath(*[f"{key}={value}" for key, value in partition.items()])


def delete_partition_from_table(table: str, partition: Dict[str, str], session: SparkSession) -> None:
    """
    Elimina una partición específica de una tabla Hive.

    Args:
    table (str): El nombre de la tabla Hive.
    partition (Dict[str, str]): Un diccionario que representa la partición.
    session (SparkSession): La sesión de Spark en la que se ejecutarán las operaciones.

    Returns:
    None
    """

    partition_str = ", ".join([f"{key} = '{value}'" for key, value in partition.items()])
    session.sql(f"ALTER TABLE {table} DROP IF EXISTS PARTITION ({partition_str}) PURGE")
    return None

class SparkTwoPartition:
    def __init__(self,
        table_or_hdfs: Union[str, HivePath],
        information_date_column: str,
        session: SparkSession,
        process_date_column: Optional[str] = None,
        date_format: Optional[str] = "%Y-%m-%d",
    ) -> None:
        """
        Inicializa una instancia de SparkTwoPartition.
        #
        Args:

            table_or_hdfs (Union[str, HivePath]): Nombre de la tabla o ruta HDFS.
            information_date_column (str): Nombre de la columna de fecha de información.
            process_date_column (str): Nombre de la columna de fecha de proceso.
            session (SparkSession): Sesión de Spark.
            date_format (Optional[str]): Formato de fecha. Por defecto es "%Y-%m-%d".
        """
        self.session = session
        self.information_date_column = information_date_column
        self.process_date_column = process_date_column
        self.date_format = date_format
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

    @cached_property
    def real_partitions(self) -> List[Dict[str, str]]:
        """
        Obtiene las particiones reales de la tabla Hive o del archivo Parquet.
        #
        Returns:
            List[Dict[str, str]]: Lista de particiones reales.
        """
        if self.table_name:
            return get_partitions_from_hive_table(self.table_name, self.session)
        elif self.hive_path:
            return get_partitions_from_parquet_path(self.hive_path)
        else:
            return []
    #
    @cached_property
    def real_partition_column_names(self) -> List[str]:
        """
        Obtiene los nombres de las columnas de partición reales.
        #
        Returns:
            List[str]: Lista de nombres de columnas de partición reales.
        """
        if len(self.real_partitions) == 0:
            return []
        return list(self.real_partitions[0].keys())
    #
    @cached_property
    def partitions(self) -> List[Dict[str, date]]:
        """
        Obtiene las particiones.
        #
        Returns:
            List[Dict[str, date]]: Lista de particiones.
        """
        logger.debug("are_attribute_partition_columns_real: %s",
            self.are_attribute_partition_columns_real())
        if self.are_attribute_partition_columns_real():
            if self.are_partitions_columns_the_same_number():
                result = deepcopy(self.real_partitions)
                return self._format_list_of_partitions(result, self.date_format)
            else:
                result = deepcopy(self._extract_unique_dates())
                return self._format_list_of_partitions(result, self.date_format)
        else:
            result = deepcopy(self._get_simulate_partitions())
            return self._format_list_of_partitions(result, self.date_format)
    #
    def are_attribute_partition_columns_real(self) -> bool:
        """
        Verifica si las columnas de partición de atributos son reales.
        #
        Returns:
            bool: True si las columnas de partición de atributos son reales, False en caso contrario.
        """
        if self.process_date_column is None:
            columns = [self.information_date_column]
        else:
            columns = [self.information_date_column, self.process_date_column]
        return all([column in self.real_partition_column_names for column in columns])
    #
    def are_partitions_columns_the_same_number(self) -> bool:
        """
        Verifica si el número de columnas de partición es el mismo.
        #
        Returns:
            bool: True si el número de columnas de partición es el mismo, False en caso contrario.
        """
        if self.process_date_column is None:
            columns = [self.information_date_column]
        else:
            columns = [self.information_date_column, self.process_date_column]
        return len(columns) == len(self.real_partition_column_names)
    #
    def _get_simulate_partitions(self) -> List[Dict[str, str]]:
        """
        Obtiene particiones simuladas a partir de la tabla o archivo Parquet.
        #
        Returns:
            List[Dict[str, str]]: Lista de particiones simuladas.
        """
        if self.table_name is not None:
            table_register = self.table_name
        elif self.hive_path is not None:
            table_register = "tmp_" + self.hive_path.name
            df: DataFrame = self.session.read.parquet(str(self.hive_path))
            df.createOrReplaceTempView(table_register)

        if self.process_date_column is None:
            query = f"""
            SELECT DISTINCT {self.information_date_column}
            FROM {table_register}
            """
        else:
            query = f"""
            SELECT DISTINCT {self.information_date_column}, {self.process_date_column}
            FROM {table_register}
            """
        partition_colection = self.session.sql(query).collect()
        if self.process_date_column is None:
            result = [
                {self.information_date_column: str(row[self.information_date_column])}
                for row in partition_colection
            ]
        else:
            result = [
                {self.information_date_column: str(row[self.information_date_column]), self.process_date_column: str(row[self.process_date_column])}
                for row in partition_colection
            ]
        # un register table
        if not self.table_name:
            self.session.catalog.dropTempView(table_register)
            # empty cache
            df.unpersist()
        return result
    #
    def _extract_unique_dates(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Extrae los valores únicos de múltiples claves específicas en una lista de diccionarios.
        #
        Args:
            data (list): Lista de diccionarios que contienen las fechas.
            date_keys (list): Lista de claves de las fechas que se desean extraer (por ejemplo, ['information_date', 'process_date']).
        #
        Returns:
            dict: Diccionario donde las claves son las date_keys y los valores son listas de diccionarios con los valores únicos de cada clave, ordenados.
        #
        Ejemplo:
            data = [
                {'information_date': '2023-01-01', 'process_date': '2023-01-10'},
                {'information_date': '2023-01-02', 'process_date': '2023-01-20'},
                {'information_date': '2023-01-03', 'process_date': '2023-01-30'},
                {'information_date': '2023-01-03', 'process_date': '2023-01-31'},
                {'information_date': '2023-01-03', 'process_date': '2023-02-01'}
            ]
        #
            unique_dates = extract_unique_dates(data, ['information_date', 'process_date'])
            # Resultado: {
            #   'information_date': [{'information_date': '2023-01-01'}, {'information_date': '2023-01-02'}, {'information_date': '2023-01-03'}],
            #   'process_date': [{'process_date': '2023-01-10'}, {'process_date': '2023-01-20'}, {'process_date': '2023-01-30'}, {'process_date': '2023-01-31'}, {'process_date': '2023-02-01'}]
            # }
        """
        date_keys = [self.information_date_column]
        data = self.real_partitions
        unique_dates = {}
        for key in date_keys:
            unique_dates[key] = [{key: date} for date in sorted({entry[key] for entry in data})]
        return [entry for key in date_keys for entry in unique_dates[key]]
    #
    #
    def _format_list_of_partitions(self, partitions: List[Dict[str, str]], date_format: str) -> List[Dict[str, datetime]]:
        """
        Convierte los valores de los diccionarios en una lista a objetos datetime sin modificar la lista original.
        #
        Args:
            partitions (List[Dict[str, str]]): Lista de particiones a formatear.
            date_format (str): Formato de fecha.
        #
        Returns:
            List[Dict[str, datetime]]: Lista de particiones formateadas.
        """
        partitions_copy = deepcopy(partitions)  # Crear una copia profunda de la lista original
        partitions_formatted = []
        for partition in partitions_copy:
            formatted_partition = {}
            for key, value in partition.items():
                if value is not None:
                    formatted_partition[key] = datetime.strptime(value, date_format).date()
                else:
                    formatted_partition[key] = None
            partitions_formatted.append(formatted_partition)
        return partitions_formatted
    #
    def _simulate_partitions(self) -> List[Dict[str, str]]:
        """
        Simula particiones basadas en las particiones reales.
        #
        Returns:
            List[Dict[str, str]]: Lista de particiones simuladas.
        """
        simulated_partitions = []
        for partition in self.real_partitions:
            if self.process_date_column is None:
                partition_dict = {self.information_date_column: None}
            else:
                partition_dict = {self.information_date_column: None, self.process_date_column: None}
            for part in partition.items():
                key, value = part
                if self.process_date_column is None:
                    if key == self.information_date_column:
                        partition_dict[key] = value
                else:
                    if key == self.information_date_column or key == self.process_date_column:
                        partition_dict[key] = value
            simulated_partitions.append(partition_dict)
        return simulated_partitions




class SparkTwoPartitionMonthlyInterval(SparkTwoPartition):
    """
    Clase que extiende SparkTwoPartition para manejar particiones mensuales con un intervalo y un desfase (lag).
    #
    Args:
        table_or_hdfs (Union[str, HivePath]): Ruta de la tabla o HDFS.
        information_date_column (str): Nombre de la columna de fecha de información.
        current_date (date): Fecha actual.
        lag (int): Desfase en meses.
        history (int): Historia en meses.
        session (SparkSession): Sesión de Spark.
        process_date_column (Optional[str], optional): Nombre de la columna de fecha de proceso. Por defecto es None.
        information_date_mode (Optional[str], optional): Modo de la columna de fecha de información. Por defecto es "each".
        process_date_mode (Optional[str], optional): Modo de la columna de fecha de proceso. Por defecto es "each".
        date_format (Optional[str], optional): Formato de la fecha. Por defecto es "%Y-%m-%d".
    #
    Attributes:
        process_date_column (str): Nombre de la columna de fecha de proceso.
        _process_date_column_provided (bool): Indica si se proporcionó la columna de fecha de proceso.
        current_date (date): Fecha actual.
        lag (int): Desfase en meses.
        history (int): Historia en meses.
        information_date_column_mode (str): Modo de la columna de fecha de información.
        process_date_column_mode (str): Modo de la columna de fecha de proceso.
        date_interval (Tuple[date, date]): Intervalo de fechas calculado.
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
        process_date_column: Optional[str] = None,
        information_date_mode: Optional[str] = "each",
        process_date_mode: Optional[str] = "each",
        incremental_lag:Optional[int]=0,
        date_format: Optional[str] = "%Y-%m-%d",
    ) -> None:
        self.current_date = current_date
        self.lag = lag
        self.history = history
        self.incremental_lag = incremental_lag
        self._currently_incremented_lag = 0
        #
        self.information_date_column_mode = information_date_mode
        self.process_date_column_mode = process_date_mode
        self.date_interval = make_date_interval_with_lag_months(self.current_date, self.history, self.lag)
        self.missing_months_allowed = missing_months_allowed
        #
        if process_date_column is None:
            super().__init__(
                table_or_hdfs=table_or_hdfs,
                information_date_column=information_date_column,
                session=session,
                date_format=date_format
            )
            self.partitions
            self.process_date_column = "fake_process_date"
            self._process_date_column_provided = False
            self.process_date_column_mode = self.information_date_column_mode
        else:
            super().__init__(
                table_or_hdfs=table_or_hdfs,
                information_date_column=information_date_column,
                session=session,
                process_date_column=process_date_column,
                date_format=date_format
            )
            self._process_date_column_provided = True

    #
    @property
    def partitions_pd(self) -> pd.DataFrame:
        """
        Dataframe de particiones con columnas de fecha de información y fecha de proceso.
        #
        Returns:
            pd.DataFrame: DataFrame con las particiones.
        """
        partitions_pd_df = self._generate_partitions_pd_dataframe()
        partitions_pd_df = self._add_interval_column_and_extend(partitions_pd_df)
        partitions_pd_df = self.sort_pairs_by_process_date(partitions_pd_df)
        partitions_pd_df = partitions_pd_df[partitions_pd_df[self.information_date_column].apply(lambda x: x != [])]
        return partitions_pd_df
    #
    @property
    def months_in_interval(self) -> List[str]:
        """
        Lista de meses en el intervalo.
        #
        Returns:
            List[str]: Lista de meses en el intervalo.
        """
        return self._get_months_in_interval()
    #
    @property
    def sorted_pairs(self) -> List[List[Tuple[str, str]]]:
        """
        Lista de pares ordenados de fechas de información y fechas de proceso.
        #
        Returns:
            List[List[Tuple[str, str]]]: Lista de pares ordenados.
        """
        return self._get_pairs_with_modes()
    #
    @property
    def pyspark_filter(self) -> Column:
        """
        Filtro de PySpark basado en las fechas de información y fechas de proceso.
        #
        Returns:
            Column: Filtro de PySpark.
        """
        return self.generate_pyspark_filter()
    @property
    def sql_filter(self) -> str:
        """
        Filtro de SQL basado en las fechas de información y fechas de proceso.
        #
        Returns:
            str: Filtro de SQL, se usa luego del "WHERE"
        """
        return self.generate_sql_filter()
    #
    def _generate_partitions_pd_dataframe(self) -> pd.DataFrame:
        """
        Genera un DataFrame de particiones a partir de una lista de diccionarios.
        #
        Returns:
            pd.DataFrame: DataFrame de particiones.
        """
        # Convertir la lista de diccionarios en un DataFrame
        df = pd.DataFrame(self.partitions)
        #
        # Si no se proporciona process_date_column, usar "process_date" y copiar los valores de information_date_column
        if not self._process_date_column_provided:
            df[self.process_date_column] = df[self.information_date_column]
        #

        # rango de fechas
        min_date = df[self.information_date_column].min()
        max_date = df[self.information_date_column].max()
        #
        # Generar un rango de meses sin huecos
        date_range = pd.date_range(start=min_date, end=max_date, freq='MS').strftime("%Y-%m").tolist()
        #
        # Crear un DataFrame con la columna months
        result_df = pd.DataFrame(date_range, columns=['months'])
        #
        # Agregar las columnas information_date y process_date con listas vacías
        result_df[self.information_date_column] = [[] for _ in range(len(result_df))]
        result_df[self.process_date_column] = [[] for _ in range(len(result_df))]
        #
        # Rellenar las columnas information_date y process_date con los valores correspondientes
        for _, row in df.iterrows():
            month_str = row[self.information_date_column].strftime("%Y-%m")
            matching_rows = result_df[result_df['months'] == month_str]
            if not matching_rows.empty:
                idx = matching_rows.index[0]
                result_df.at[idx, self.information_date_column].append(row[self.information_date_column])
                result_df.at[idx, self.process_date_column].append(row[self.process_date_column])
        return result_df
    #
    #
    def _add_interval_column_and_extend(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        Agrega una columna de intervalo y extiende el DataFrame con meses sin huecos.
        #
        Args:
            df (pd.DataFrame): DataFrame original.
        #
        Returns:
            pd.DataFrame: DataFrame extendido.
        """
        start_date, end_date = self.date_interval
        end_date = end_date - relativedelta(days=1)
        # Convertir start_date y end_date a pd.Timestamp
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        #
        # Obtener el rango actual de meses en el DataFrame
        current_months = pd.to_datetime(df['months'])
        #
        # Determinar el nuevo rango de meses considerando el intervalo proporcionado
        min_date = min(current_months.min(), start_date)
        max_date = max(current_months.max(), end_date)
        #
        # Generar un rango de meses sin huecos desde min_date hasta max_date
        date_range = pd.date_range(start=min_date, end=max_date, freq='MS').strftime("%Y-%m").tolist()
        #
        # Crear un DataFrame con la columna months
        extended_df = pd.DataFrame(date_range, columns=['months'])
        #
        # Agregar las columnas information_date y process_date con listas vacías
        extended_df[self.information_date_column] = [[] for _ in range(len(extended_df))]
        extended_df[self.process_date_column] = [[] for _ in range(len(extended_df))]
        #
        # Rellenar las columnas information_date y process_date con los valores correspondientes del DataFrame original
        for _, row in df.iterrows():
            month_str = row['months']
            if month_str in extended_df['months'].values:
                idx = extended_df[extended_df['months'] == month_str].index[0]
                extended_df.at[idx, self.information_date_column] = row[self.information_date_column]
                extended_df.at[idx, self.process_date_column] = row[self.process_date_column]

        # Crear una nueva columna 'in_interval' con valores True o False
        extended_df['in_interval'] = extended_df['months'].apply(lambda x: start_date <= pd.to_datetime(x) <= end_date)
        #
        return extended_df
    #
    def sort_pairs_by_process_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ordona los pares de fechas de información y fechas de proceso por fecha de proceso.
        #
        Args:
            df (pd.DataFrame): DataFrame con pares de fechas.
        #
        Returns:
            pd.DataFrame: DataFrame con pares ordenados.
        """
        for index, row in df.iterrows():
            if row[self.process_date_column]:
                # Combine information_date and process_date into pairs
                pairs = list(zip(row[self.information_date_column], row[self.process_date_column]))
                # Sort pairs by process_date
                sorted_pairs = sorted(pairs, key=lambda x: x[1])
                # Unzip the sorted pairs back into separate lists
                sorted_information_date, sorted_process_date = zip(*sorted_pairs)
                # Update the DataFrame with sorted lists
                df.at[index, self.information_date_column] = list(sorted_information_date)
                df.at[index, self.process_date_column] = list(sorted_process_date)
        return df

    def _get_months_in_interval(self) -> List[str]:
        """
        Obtiene los meses que están en el intervalo.
        #
        Returns:
            List[str]: Lista de meses en el intervalo.
        """
        return self.partitions_pd[self.partitions_pd['in_interval']]['months'].tolist()
    #
    def _get_sorted_pairs_for_month(self, month: str) -> List[Tuple[str, str]]:
        """
        Obtiene los meses que están en el intervalo.
        #
        Returns:
            List[str]: Lista de meses en el intervalo.
        """
        # Filter the DataFrame to find the row corresponding to the given month
        row = self.partitions_pd[self.partitions_pd['months'] == month]
        #
        if row.empty:
            raise ValueError(f"No data available for the month: {month}")
        #
        # Get the first (and only) row from the filtered DataFrame
        row = row.iloc[0]
        #
        if not row[self.process_date_column]:
            raise ValueError(f"No process_date available for the month: {month}")
        #
        # Combine information_date and process_date into pairs
        pairs = list(zip(row[self.information_date_column], row[self.process_date_column]))
        # Sort pairs by process_date
        sorted_pairs = sorted(pairs, key=lambda x: x[1])
        return sorted_pairs
    #
    def _get_latest_process_date_for_month(self, month: str) -> List[Tuple[str, str]]:
        """
        Obtiene los pares ordenados de fechas de información y fechas de proceso para un mes dado.
        #
        Args:
            month (str): Mes en formato "YYYY-MM".
        #
        Returns:
            List[Tuple[str, str]]: Lista de pares ordenados.
        """
        sorted_pairs = self._get_sorted_pairs_for_month(month)
        # Get the latest process_date and its corresponding information_date
        return [sorted_pairs[-1]]
    #
    def _get_first_process_date_for_month(self, month: str) -> List[Tuple[str, str]]:
        """
        Obtiene la primera fecha de proceso para un mes dado.

        Args:
            month (str): Mes en formato "YYYY-MM".

        Returns:
            List[Tuple[str, str]]: Primera fecha de proceso y su fecha de información correspondiente.
        """
        sorted_pairs = self._get_sorted_pairs_for_month(month)
        # Get the first process_date and its corresponding information_date
        return [sorted_pairs[0]]
    #
    def _get_all_process_dates_for_month(self, month: str) -> List[Tuple[str, str]]:
        """
        Obtiene todas las fechas de proceso para un mes dado.

        Args:
            month (str): Mes en formato "YYYY-MM".

        Returns:
            List[Tuple[str, str]]: Todas las fechas de proceso y sus fechas de información correspondientes.
        """
        sorted_pairs = self._get_sorted_pairs_for_month(month)
        return sorted_pairs
    #
    def _get_each_process_date_for_month(self, month: str) -> List[Tuple[str, str]]:
        """
        Obtiene cada fecha de proceso para un mes dado.

        Args:
            month (str): Mes en formato "YYYY-MM".

        Returns:
            List[Tuple[str, str]]: Cada fecha de proceso y su fecha de información correspondiente.

        Raises:
            ValueError: Si hay múltiples valores para la fecha de proceso en el mes.
        """
        sorted_pairs = self._get_sorted_pairs_for_month(month)
        if len(sorted_pairs) != 1:
            raise ValueError(f"Multiple values for process_date in the month: {month}")
        return [sorted_pairs[0]]
    #
    def _get_last_month(self) -> List[str]:
        """
        Obtiene el último mes en el intervalo.

        Returns:
            List[str]: Ultimo mes en el intervalo.
        """
        return [self.months_in_interval[-1]]
    #
    def _get_first_month(self) -> List[str]:
        """
        Obtiene el primer mes en el intervalo.

        Returns:
            List[str]: Primer mes en el intervalo.
        """
        return [self.months_in_interval[0]]
    #
    def _get_all_months(self) -> List[str]:
        """
        Obtiene todos los meses en el intervalo.

        Returns:
            List[str]: Todos los meses en el intervalo.
        """
        return self.months_in_interval
    #
    def _get_pairs_with_modes_base(self) -> List[List[Tuple[str, str]]]:
        """
        Obtiene los pares de fechas de información y fechas de proceso según los modos especificados.

        Returns:
            List[List[Tuple[str, str]]]: Lista de pares de fechas según los modos.
        """
        if self.information_date_column_mode == "last":
            months = self._get_last_month()
        elif self.information_date_column_mode == "first":
            months = self._get_first_month()
        elif (self.information_date_column_mode == "all") \
            or (self.information_date_column_mode == "each"):
            months = self._get_all_months()
        else:
            raise ValueError("Invalid information_date_column_mode")
        #
        if self.process_date_column_mode == "last":
            pair_function = self._get_latest_process_date_for_month
        elif self.process_date_column_mode == "first":
            pair_function = self._get_first_process_date_for_month
        elif self.process_date_column_mode == "all":
            pair_function = self._get_all_process_dates_for_month
        elif self.process_date_column_mode == "each":
            pair_function = self._get_each_process_date_for_month
        else:
            raise ValueError("Invalid process_date_column_mode")
        #
        if self.missing_months_allowed:
            result = []
            for month in months:
                try:

                    month_pairs = pair_function(month)
                    result.append(month_pairs)
                except:
                    pass
        else:
            result = [pair_function(month) for month in months]
        return result
    def _get_pairs_with_modes(self) -> List[List[Tuple[str, str]]]:
        if self.incremental_lag == 0:
            return self._get_pairs_with_modes_base()
        else:
            while True:
                try:
                    return self._get_pairs_with_modes_base()
                except Exception:
                    if abs(self._currently_incremented_lag) == abs(self.incremental_lag):
                        break
                    self.increment_lag_by_one()
            raise RuntimeError("No se logró encontrar un intervalo sin errores en el lag incremental determinado.")
    #
    def generate_pyspark_filter(self) -> Column:
        """
        Genera un filtro de PySpark basado en las fechas de información y fechas de proceso.

        Returns:
            Column: Filtro de PySpark.
        """
        date_tuples = self.sorted_pairs
        # Flatten the list of lists of tuples into a single list of tuples
        flattened_tuples = [t for sublist in date_tuples for t in sublist]

        # Create filter conditions
        filters = []
        for t in flattened_tuples:
            if self._process_date_column_provided:
                # Create a filter using both columns
                filter_condition = (col(self.information_date_column) == t[0]) & (col(self.process_date_column) == t[1])
            else:
                # Create a filter using only the information_date_column
                filter_condition = col(self.information_date_column) == t[0]
            filters.append(filter_condition)

        # Combine all filters using OR
        if filters:
            combined_filter = reduce(lambda acc, f: acc | f, filters)
        else:
            combined_filter = None
        #
        return combined_filter
    #
    def generate_sql_filter(self) -> str:
        """
        Genera un filtro SQL basado en las fechas de información y fechas de proceso.
        #
        Returns:
            str: Filtro SQL.
        """
        date_tuples = self.sorted_pairs
        # Flatten the list of lists of tuples into a single list of tuples
        flattened_tuples = [t for sublist in date_tuples for t in sublist]
        #
        # Create filter conditions
        filters = []
        for t in flattened_tuples:
            if self._process_date_column_provided:
                # Create a filter using both columns
                filter_condition = f"{self.information_date_column} = '{t[0]}' AND {self.process_date_column} = '{t[1]}'"
            else:
                # Create a filter using only the information_date_column
                filter_condition = f"{self.information_date_column} = '{t[0]}'"
            filters.append(filter_condition)

        # Combine all filters using OR
        if filters:
            combined_filter = " OR ".join(filters)
            sql_filter = f"{combined_filter}"
        else:
            sql_filter = ""
        #
        return sql_filter
    #
    def increment_lag_by_one(self) -> None:
        if self.incremental_lag > 0:
            self.lag = self.lag + 1
            self._currently_incremented_lag = self._currently_incremented_lag + 1
        elif self.incremental_lag < 0:
            self.lag = self.lag - 1
            self._currently_incremented_lag = self._currently_incremented_lag - 1
        #
        self.date_interval = make_date_interval_with_lag_months(self.current_date, self.history, self.lag)
        logger.warning("Lag incremented, new lag value is %s", self.lag)
