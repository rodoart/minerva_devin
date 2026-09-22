from typing import Callable, Union, Optional, Any
from pyspark.sql import DataFrame, SparkSession

from pathlib import Path

from ...path.hdfs import is_successful, exists, rmdir

import string
import random



def random_string(n:Optional[int]=9) -> Any:
    # generating random strings
    res = ''.join(random.choices(string.ascii_letters, k=n))
    return res


def save_to_parquet(df:DataFrame, output_path:Union[str, Path]) -> None:
    if isinstance(output_path, Path):
        output_path = str(output_path)

    df.write\
        .mode('overwrite')\
        .parquet(output_path)




def load_or_create_df(origin:str) -> Callable[..., Callable[..., DataFrame | tuple[Any, ...] | list[Any]]]:
    def decorator(func:Callable) -> Callable[..., DataFrame | tuple[Any, ...] | list[Any]]:
        def wrap(*args, **kwargs) -> DataFrame | tuple[Any, ...] | list[Any]:
            try:
                spark = kwargs['spark']
            except:
                spark = None
            try:
                for var in args:
                    if isinstance(var, SparkSession):
                        spark = var
            except:
                pass
            #
            assert isinstance(spark, SparkSession),\
                str(type(spark))
            #
            if is_successful(origin):
                return spark.read.parquet(origin)
            elif exists(origin):
                rmdir(origin)
            #
            result = func(*args, **kwargs)
            # Result is tuple
            if isinstance(result, (tuple, list)):
                for value in result:
                    if isinstance(value, DataFrame):
                        save_to_parquet(value, origin)
                        break
            elif isinstance(result, DataFrame):
                save_to_parquet(result, origin)
            else:
                AssertionError('The result does not contain any '
                    +'DataFrame')
            #
            return result
        return wrap
    return decorator



def reloader(
    df:DataFrame,
    temp_path:str,
    session:SparkSession,
    name:Optional[str]=None
) -> DataFrame:
    #
    if not name:
        name = random_string()
    #
    if not temp_path.endswith("/"):
        temp_path = f'{temp_path}/'
    output_path = temp_path + name
    df.write.mode('overwrite').parquet(output_path)
    return session.read.parquet(output_path)


def tester(
    name:Optional[str]=None,
) -> DataFrame:
    if not name:
        name = random_string()
    return name
