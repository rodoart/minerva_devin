from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count, mean, stddev, min, max, expr
from pyspark.sql import SparkSession


def _calculate_column_statistics(df: DataFrame, column: str, stats: list, percentiles: Optional[List[float]]) -> None:
    """
    Calculate statistics for a specific column in a DataFrame.
    (Not intended to be called directly)

    Parameters:
    df (DataFrame): The input DataFrame.
    column (str): The column name for which to calculate statistics.
    percentiles (list): A list of percentiles to calculate.
    stats (list): A list to store the calculated statistics.

    Returns:
    None: The results are appended to the stats list.
    """

    col_stats = df.select(
        count(col(column)).alias('count'),
        count(when(col(column).isNull(), column)).alias('missings'),
        count(when(isnan(column), column)).alias('nan'),
        max(col(column)).alias('max'),
        min(col(column)).alias('min'),
        stddev(col(column)).alias('std'),
        mean(col(column)).alias('mean')
    ).collect()[0]

    if percentiles is not None:
        percentiles_expr = [expr(f'percentile_approx({column}, {p})').alias(f'percentile_{p}') for p in percentiles]
        percentiles_values = df.select(*percentiles_expr).collect()[0]
        col_stats_dict = {**col_stats.asDict(), **percentiles_values.asDict()}
    else:
        col_stats_dict = col_stats.asDict()

    col_stats_dict['column'] = column
    stats.append(col_stats_dict)



def calculate_statistics(
    df: DataFrame,
    columns: Union[str, List[str]],
    session: SparkSession,
    percentiles: Optional[List[float]] = [0.01, 0.05, 0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.95, 0.99],
    max_threads: Optional[int] = 10
) -> DataFrame:
    """
    Calculate statistics for multiple columns in a DataFrame using multithreading.

    Parameters:
    df (DataFrame): The input DataFrame.
    columns (Union[str, List[str]]): A list of column names for which to calculate statistics.
    session (SparkSession): The Spark session.
    percentiles (Optional[List[float]]): A list of percentiles to calculate.
    max_threads (Optional[int]): The maximum number of threads to use.

    Returns:
    DataFrame: A DataFrame containing the calculated statistics for each column.
    """
    if isinstance(columns, str):
        columns = [columns]

    stats = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(_calculate_column_statistics, df, column, stats, percentiles) for column in columns]
        for future in as_completed(futures):
            future.result()  # Wait for all threads to complete

    result = session.createDataFrame(stats)

    column_order = ['column', 'count', 'missings', 'nan', 'min', 'max', 'mean', 'std']

    if percentiles is not None:
        column_order = column_order + [f'percentile_{p}' for p in percentiles]

    result = result.select(column_order)

    return result
