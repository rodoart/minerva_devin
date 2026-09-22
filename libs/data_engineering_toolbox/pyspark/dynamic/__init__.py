from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


from subprocess import call
from typing import Union, Callable, Optional, List, Any
from pathlib import Path

from ..path import HivePath
from ..path.linux import save_to_csv


def do_one_method_list_item(object_:object, method:Union[str, dict, set])->Any:
    """
    Applies one ´method´ to an ´object_´.
    "
    Parameters
    ----------
    :object_: object
        an object with methods
    :method: dict, str, or set
        ONE ELEMENT dictionary that contains method name and attributes.
        Examples: 'read', {'parquet':{}},
            {'long':('hola', 'mundo'),{'other':'kwargs'}}}
    "
    Returns
    ----------
    Anything.
    """
    if isinstance(method, (str, set)):
        return getattr(object_, str(method))
    #
    if isinstance(method, dict):
        name = list(method.keys())[0]
        methods = list(method.values())[0]
        #
        if isinstance(methods, str):
            return getattr(object_, name)(methods)
        #
        if isinstance(methods, dict):
            return getattr(object_, name)(**methods)
        #
        if isinstance(methods, (list, tuple)):
            if len(methods)==0:
                return getattr(object_, name)()
            if len(methods)==1:
                return getattr(object_, name)(methods[0])
            if len(methods)>=2:
                last_method = methods[-1]
                if isinstance(last_method,dict):
                    kwargs = last_method
                    args = methods[:-1]
                    return getattr(object_, name)(*args, **kwargs)
                else:
                    return getattr(object_, name)(*methods)

def do_attribute_list(object_:object, attribute_list:List[Union[str, dict]])->object:
    """
    Applies multiple ´methods´ to an ´object_´ in an attribute list.
    "
    Parameters
    ----------
    :object_: object
        an object with methods
    :attribute_list: list
        This is an example of list, follow it:
        "
        method_list_example = [
            'read',
            {'seti'},
            {'parquet':{}},
            {{'parquet':{}}},
            {'absurd_but_posible':('idiotic')},
            {'absurd_but_posible':'idiotic'},
            {'absurd_but_posible':['idiotic']},
            {'long':('hola', 'mundo'),{'other':'kwargs'}}},
            {'long':{'esete':5, 'kwarg':7}}
        ]
        ...
    "
    Returns
    ----------
    Anything. In the example, the applied methods would be:
        object_.read.seti.parquet().\
        absurd_but_posible('idiotic').absurd_but_posible('idiotic')\
        .absurd_but_posible('idiotic').long('hola','mundo',other='kwargs')
        .long(esete=5, kwarg=7)
    """
    result = object_
    for attribute in attribute_list:
        result = do_one_method_list_item(result, attribute)
    return result


def dataframe_parquet_writer(
    output_hdfs: Union[str, Path],
    session: SparkSession,
    partition: Optional[str] = None,
    force_reload: Optional[bool] = False
) -> Callable[..., Callable[..., DataFrame]]:
    def decorator(func: Callable[[], DataFrame]) -> Callable[..., DataFrame]:
        def wrapper(*args, **kwargs) -> DataFrame:
            try:
                if force_reload:
                    raise Exception
                reloaded = session.read.parquet(str(output_hdfs))
                print(f'Has been already generated, just reading {output_hdfs}')
                return reloaded
            except:
                reloaded = func(*args, **kwargs)
                if partition is None:
                    (reloaded.write
                        .mode('overwrite')
                        .parquet(str(output_hdfs)))
                else:
                    (reloaded.write
                        .mode('overwrite')
                        .partitionBy(partition)
                        .parquet(str(output_hdfs)))
                reloaded = session.read.parquet(str(output_hdfs))
                print(f'Has been written at {output_hdfs}')
                return reloaded
        return wrapper
    return decorator



def dataframe_csv_writer(
    output_linux: Union[str, Path],
    session: SparkSession,
    partition: Optional[str] = None,
    force_reload: Optional[bool] = False
)-> Callable[..., Callable[..., None]]:
    def decorator(func: Callable[[], DataFrame]) -> Callable[..., None]:
        def wrapper(*args, **kwargs) -> None:
            try:
                if force_reload:
                    raise Exception
                if not output_linux.is_dir():
                    raise Exception
                print(f'Has been already generated, just reading {output_linux}')
                return None
            except:
                reloaded = func(*args, **kwargs)
                if partition is None:
                    if not output_linux.name.endswith(".csv"):
                        new_output_linux = output_linux.parent.joinpath(f"{output_linux.name}.csv")
                    else:
                        new_output_linux = output_linux
                    save_to_csv(reloaded, str(new_output_linux))
                else:
                    if output_linux.name.endswith(".csv"):
                        new_output_linux = output_linux.parent.joinpath(output_linux.name[:-4])
                    else:
                        new_output_linux = output_linux
                    for part in reloaded.select(partition).distinct().collect():
                        partition_linux = new_output_linux.joinpath(f"{partition}={part[0]}.csv")
                        save_to_csv(reloaded.filter(col(partition) == part[0]), partition_linux)
                        #
                    print(f'Has been written at {output_linux}')
                    return None
        return wrapper
    return decorator




def load_csvs_to_hive(
    csv_folder_linux:Union[str, Path],
    output_hdfs:Union[str, HivePath],
    session:SparkSession,
    tmp_csv_hdfs:Optional[Union[str, HivePath]]=None,
    partition:Optional[str]=None,
    header:Optional[bool]=True,
    force_reload:Optional[bool]=False
) -> DataFrame:
    # Assertions
    if isinstance(csv_folder_linux, str):
        csv_folder_linux = Path(csv_folder_linux)
    #
    if isinstance(output_hdfs, str):
        output_hdfs = HivePath(output_hdfs)
    #
    if tmp_csv_hdfs is None:
        tmp_csv_hdfs = HivePath(output_hdfs.parent.joinpath("tmp_" + output_hdfs.name))
    #
    if isinstance(tmp_csv_hdfs, str):
        tmp_csv_hdfs = HivePath(tmp_csv_hdfs)
    # Copy CSV file(s) from Linux to HDFS
    call(["hdfs", "dfs", "-put", str(csv_folder_linux), str(tmp_csv_hdfs)])
    #
    # Read CSV file(s) from HDFS
    if partition is not None:
        df = session.read.csv(str(tmp_csv_hdfs) + "/*.csv", header=header)
    else:
        try:
            df = session.read.csv(str(tmp_csv_hdfs) , header=header)
        except:
            try:
                df = session.read.csv(str(tmp_csv_hdfs.parent.joinpath(tmp_csv_hdfs.name + ".csv")) , header=header)
            except:
                df = session.read.csv(str(tmp_csv_hdfs) + "/*.csv", header=header)
    # Write DataFrame to HDFS as Parquet
    dynamic_hdfs_save = dataframe_parquet_writer(output_hdfs, session,partition, force_reload)(lambda: df)
    result = dynamic_hdfs_save()
    # Delete temporary CSV file from HDFS
    tmp_csv_hdfs.rmdir(recursive=True, skip_trash=True)
    return result
