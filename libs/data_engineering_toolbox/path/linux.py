"""
Project: Linux functions
Developer: Rodolfo Arturo Gonzalez
Update Date: 2023-05-18
Version: 0.1

Functions for easy interrelation between HDFS and normal Linux
partitions, and default paths.

"""
from pyspark.sql import DataFrame
from typing import Union
from pathlib import Path
from os import makedirs

def save_to_csv(df:DataFrame, output_path:Union[str, Path]) -> None:
    """
    Saves the `df` to the outpath in .csv format.
    """
    if isinstance(output_path, str):
        output_path = Path(output_path).resolve()
    #
    if not output_path.name.endswith('.csv'):
        output_path = output_path.parent.joinpath(f'{output_path.name}.csv')
    makedirs(output_path.parent, exist_ok= True)
    df.toPandas().to_csv(output_path, index=False)
