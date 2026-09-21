from typing import Any, Dict, List, Union, Optional
from pathlib import Path, PosixPath
from inflection import underscore

from typing import Callable, Tuple
import logging

from datetime import date
from functools import wraps

from pandas.core import missing
from pyspark import HiveContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException

from libs.data_engineering_toolbox.path import HivePath
from libs.data_engineering_toolbox.pyspark.tools.parquet_treatment import SparkLoadPartitionedTableOrParquet, overwrite_partition, overwrite_two_partition
from libs.framework.utils import sanitize_name



def cast_to_schema(schema: StructType, target_df:DataFrame) -> DataFrame:
    """ ... """
    for field in schema:
        column_name = field.name
        column_type = field.dataType
        if column_name in target_df.columns:
            target_df = target_df.withColumn(column_name, col(column_name).cast(column_type))
    return target_df



class cached_property:
    _MAX_RETRIES = 5
    """ ... """
    #
    def __init__(self, func: Callable[[Any], Any]) -> None:
        """ ... """
        self.func = func
        self.cache_name = f"_{func.__name__}_cache"
        #
    def __get__(self, instance: Any, owner: Any) -> Any:
        """ ... """
        if instance is None:
            return self
        if self.cache_name not in instance.__dict__:        # solo cache de INSTANCIA
            instance.__dict__[self.cache_name] = self.func(instance)
        return instance.__dict__[self.cache_name]



class Step:
    """ ... """
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.previous_step:Union[Union['Step',List['Step']], None]= kwargs.get('previous_step', None)
        self.input_linux:Dict[str, Union[str, Path, PosixPath]] = kwargs.get('input_linux', {})
        self.input_hive:Dict[str, Union[str, Path, PosixPath, HivePath]] = kwargs.get('input_hive', {})
        self.output_linux:Dict[str, Union[str, Path, PosixPath]] = kwargs.get('output_linux', {})
        self.output_hive:Dict[str, Union[str, Path, PosixPath, HivePath]] = kwargs.get('output_hive', {})
        self.input_parameters:Dict[str, Any] = kwargs.get('input_parameters', {})
        self.step_name = kwargs.get('step_name', self._make_step_name())  # Nombre del paso, si se proporciona
        self.output_parameters:Dict[str, Any] = {self.step_name:{}}
        self.sqlContext:Union[SparkSession, HiveContext] = kwargs.get('sqlContext', None)
        self.tmp_paths:List[HivePath] = []
        self._decorated_cache: Dict[str, DataFrame] = {}
    #
    @staticmethod
    def _build_decorated_loader(
        property_name:str,
        method:Union[str,Callable],
        decorator:Optional[Callable[..., Any]]=None,
        *args, **kwargs
    ) -> Callable[[Any], DataFrame]:
        decorator_key:str = kwargs["decorator_key"]
        def _loader(self) -> DataFrame:
            fn = getattr(self, method) if isinstance(method, str) else method
            return fn(*args, **kwargs)
        _loader.__name__ = property_name
        return _loader if decorator is None else decorator(decorator_key)(_loader)
    #
    #
    def get_cached_decorated_table_or_parquet_property(self, *args, **kwargs) -> DataFrame:
        # ...misma lógica de resolución de property_name / decorator / path...
        if "input_or_output" not in kwargs:
            kwargs["input_or_output"] = "input"
        config_dict = self.input_hive if kwargs["input_or_output"] == "input" else self.output_hive
        kwargs["session"] = self.sqlContext
        #
        if "path" in kwargs:
            if isinstance(kwargs["path"], str):
                kwargs["path"] = HivePath(kwargs["path"])
            if "property_name" not in kwargs:
                cleaned_path = str(kwargs["path"]).replace("=", "").replace("'", "").replace("-", "_")
                property_name = sanitize_name(cleaned_path.split("/")[-2] + "_" + cleaned_path.split("/")[-1])
            else:
                property_name = kwargs["property_name"]
            assert "method" in kwargs, "method must be provided in kwargs"
            decorator = (None if kwargs["input_or_output"] == "input"
                         else dynamic_unpartitioned_parquet_with_path)
            if decorator is not None:
                decorator_key = kwargs["path"]
        elif "config_dict_key" in kwargs:
            if "property_name" not in kwargs:
                property_name = sanitize_name(kwargs["config_dict_key"])
            else:
                property_name = kwargs["property_name"]
            #
            assert "method" in kwargs, "method must be provided in kwargs"
            kwargs["path"] = config_dict[kwargs["config_dict_key"]]["table_or_hdfs"]
            if len(kwargs["config_dict_key"]) == 2:
                decorator = dynamic_unpartitioned_parquet_with_path
            else:
                decorator = dynamic_partitioned_table_or_parquet
            decorator_key = kwargs["config_dict_key"]
        else:
            decorator = None
            if "property_name" not in kwargs:
                property_name = "default_property_name"
            else:
                property_name = kwargs["property_name"]
            decorator_key = None
        #
        kwargs["property_name"] = property_name
        kwargs["decorator"] = decorator
        kwargs["decorator_key"] = decorator_key
        print(f"Defining property: {property_name}")
        try:
            print(f"Defining path: {kwargs['path']}")
        except:
            print(f"something wrong with path")
        #
        if property_name not in self._decorated_cache:            # <- VARIABLE, no string
            loader = self._build_decorated_loader(**kwargs)
            self._decorated_cache[property_name] = loader(self)   # <- VARIABLE
        return self._decorated_cache[property_name]
    #
    def _make_step_name(self) -> str:
        """ ... """
        global_var_name = f"{underscore(self.__class__.__name__)}_counter"
        try:
            globals()[global_var_name]
        except:
            globals()[global_var_name] = 0
        #
        globals()[global_var_name] = globals().get(global_var_name, 0) + 1
        return f"{underscore(self.__class__.__name__)}_{globals()[global_var_name]}"
    #
    def __str__(self) -> str:
        return f"Step({self.step_name})"
    #
    def __repr__(self) -> str:
        return f"Step({self.step_name})"
    #
    @cached_property
    def _previous_step_output_parameters_loaded(self) -> Dict[str,Any]:
        if self.previous_step:

            if isinstance(self.previous_step, list):
                output_params = {}
                for step in self.previous_step:
                    step.execute()
                    output_params.update(step.output_parameters)
                return output_params
            else:
                self.previous_step.execute()
            return self.previous_step.output_parameters
        else:
            return {}

    def step_action(self) -> Any:
        # self.output_parameter['example'] = 2
        # return  self.output_parameters[self.step_name]
        raise NotImplementedError("Subclasses should implement this method.")
    #
    def execute(self) -> Any:
        print(f"Executing step: {self.step_name}")
        print(f"Input parameters: {self.input_parameters}")
        if self.previous_step:
            self.input_parameters.update(self._previous_step_output_parameters_loaded)
        return self.step_action()




def dynamic_unpartitioned_parquet_with_path(path: Union[str, HivePath]) -> Callable:
    """ ... """
    if isinstance(path, str):
        path = HivePath(path)
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self) -> DataFrame:
            """ ... """
            output_hive = self.output_hive
            is_dynamic = self.is_dynamic
            #
            print(f"path={path}")
            path_key = str(path).split("/")[-2] + "-" + str(path).split("/")[-1]
            try:
                if not is_dynamic:
                    raise Exception
                result = self.sqlContext.read.parquet(str(path))
                print(f"INFO: {path_key.capitalize()} data reloaded from {path}")
            except Exception as e:
                print(f"WARNING: Could not load {path_key} from {path} due to {e}. Recomputing...")
                func(self).write.mode("overwrite").parquet(str(path))
                result = self.sqlContext.read.parquet(str(path))
                print(f"INFO: {path_key.capitalize()} data saved on {path}")
            return result
        return wrapper
    return decorator




def dynamic_partitioned_table_or_parquet(path_key: str) -> Callable:
    """ ... """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self) -> Tuple[DataFrame, Dict[str, Union[str, HivePath, PosixPath, int]]]:
            """ ... """
            output_hive:dict = self.output_hive
            is_dynamic:bool = self.is_dynamic
            process_date:date = self.input_parameters["process_date"]
            vintage_date :date = self.input_parameters["vintage_date"]
            table_or_hdfs:Union[str, HivePath, PosixPath] = output_hive[path_key]["table_or_hdfs"]
            try:
                process_date_column:str = output_hive[path_key]["process_date_column"]
            except:
                process_date_column = None
            #
            try:
                schema:StructType = output_hive[path_key]["schema"]
            except:
                schema = None
            #
            try:
                lag:int = output_hive[path_key]["lag"]
            except:
                lag = 0
            #
            try:
                history:int = output_hive[path_key]["history"]
            except:
                history = 0
            #
            try:
                information_date_mode:str = output_hive[path_key]["information_date_mode"]
            except:
                information_date_mode = "each"
            #
            try:
                process_date_mode:str = output_hive[path_key]["process_date_mode"]
            except:
                process_date_mode = "last"
            try:
                missing_months_allowed:bool = output_hive[path_key]["missing_months_allowed"]
            except:
                missing_months_allowed = True
            #
            information_date_column = output_hive[path_key]["information_date_column"]
            partitions = [information_date_column]
            if process_date_column:
                partitions.append(process_date_column)
            #
            load_object_kwargs = {
                "table_or_hdfs": table_or_hdfs,
                "information_date_column": information_date_column,
                "current_date": vintage_date,
                "lag": lag,
                "history": history,
                "process_date_column": process_date_column,
                "process_date_mode": process_date_mode,
                "information_date_mode": information_date_mode,
                "missing_months_allowed": missing_months_allowed
            }
            result = None
            try:
                if not is_dynamic:
                    raise Exception
                print(f"INFO: Attempting to load {path_key} from {table_or_hdfs}")
                print(f"load_object_kwargs: {load_object_kwargs}")
                load_object = SparkLoadPartitionedTableOrParquet(
                    session=self.sqlContext, **load_object_kwargs
                )
                result = load_object.df
                print(f"INFO: {path_key.capitalize()} data reloaded from {table_or_hdfs}")
            except Exception as e:
                print(f"WARNING: Could not load {path_key} from {table_or_hdfs} due to {e}. Recomputing...")
                #
                df:DataFrame = (func(self)
                    .withColumn(information_date_column, lit(str(vintage_date)))
                )
                if process_date_column:
                    df = df.withColumn(process_date_column, lit(str(process_date)))
                #
                if schema:
                    df = cast_to_schema(schema, df)
                #
                if df.isEmpty():
                    result = df
                #
                else:
                    if process_date_column is not None and len(partitions) == 2:
                        print(f"INFO: Overwriting two partitions: {partitions} on {table_or_hdfs}")
                        overwrite_object = overwrite_two_partition(
                            df=df,
                            process_date_column=process_date_column,
                            information_date_column=information_date_column,
                            table_or_hdfs=table_or_hdfs,
                            session=self.sqlContext
                        )
                    else:
                        print(f"INFO: Overwriting partition: {partitions} on {table_or_hdfs}")
                        overwrite_object = overwrite_partition(
                            df=df,
                            partition_by=partitions,
                            table_or_hdfs=table_or_hdfs,
                            session=self.sqlContext
                        )
                    overwrite_object.write()
                    result = overwrite_object.read()
                    print(f"INFO: {path_key.capitalize()} data saved on {table_or_hdfs}")
            return result, load_object_kwargs
        return wrapper
    return decorator




def standard_load_parquet_or_table(
    config_dict: Dict[str, dict],
    config_dict_key: str,
    current_date: date,
    session: Union[HiveContext, SparkSession],
) -> DataFrame:
    """Carga estándar de una tabla/parquet particionado con validación de historial. ..."""
    process_date_column = config_dict[config_dict_key].get("process_date_column")
    process_date_mode = config_dict[config_dict_key].get("process_date_mode")
    try:
        minimum_required_history = config_dict[config_dict_key]["minimum_required_history"]
    except:
        minimum_required_history = False
    try:
        load_object = SparkLoadPartitionedTableOrParquet(
            table_or_hdfs=config_dict[config_dict_key]["table_or_hdfs"],
            information_date_column=config_dict[config_dict_key]["information_date_column"],
            current_date=current_date,
            lag=config_dict[config_dict_key]["lag"],
            history=config_dict[config_dict_key]["history"],
            session=session,
            information_date_mode=config_dict[config_dict_key]["information_date_mode"],
            process_date_column=process_date_column,
            process_date_mode=process_date_mode,
            missing_months_allowed=True
        )
        result = load_object.df
        total_months = len(load_object.months_in_interval)
    except AnalysisException as e:
        print(f"Error loading {config_dict_key} with standard_load_parquet_or_table: {e}")
        assert config_dict[config_dict_key]["schema"], f"Schema must be provided for {config_dict_key} when loading fails."
        total_months = 0
        result = session.createDataFrame([], config_dict[config_dict_key]["schema"])

    if total_months < minimum_required_history:
        raise ValueError(f"Not enough history in {config_dict_key} data")
    #
    if total_months < config_dict[config_dict_key]["history"]:
        logging.warning(f"Warning: {config_dict_key} data has less history than expected")
    #
    return result




def run_substep_and_collect(
    parent: "Step",
    substep: "Step",
    output_key: str,
) -> Dict[str, Any]:
    """Ejecuta un substep terminal y sube su salida al 'output_parameters' del padre. ..."""
    substep.step_action()
    parent.output_parameters[parent.step_name][output_key] = (
        substep.output_parameters[substep.step_name]
    )
    return parent.output_parameters

def collect_step_output(step: "Step", output: Any, output_key: str) -> Dict[str, Any]:
    """Almacena una salida ya materializada en el `output_parameters` del step. ..."""
    step.output_parameters[step.step_name][output_key] = output
    return step.output_parameters




def build_step_init_kwargs(
    date_treatment: Dict[str, str],
    input_hive: Dict[str, "HivePath"],
    output_hive: Dict[str, "HivePath"],
    step_name_prefix: str,
    extra_kwargs: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Construye los kwargs estándar para inicializar un `Step` desde `date_treatment`. ..."""
    input_parameters = {
        "vintage": date_treatment["vintage"],
        "vintage_date": date_treatment["vintage_date"],
        "current_month_date": date_treatment["current_month_date_str"],
        "process_date_str": date_treatment["process_date_str"],
        "vintage_mmyy": date_treatment["vintage"],
        "process_date": date_treatment["process_date"],
    }
    kwargs = {
        "input_parameters": input_parameters,
        "input_hive": input_hive,
        "output_hive": output_hive,
        "step_name": f"{step_name_prefix}{date_treatment['vintage']}",
    }
    if extra_kwargs:
        kwargs.update(extra_kwargs)
    return kwargs


def inherit_parent_step_attributes(substep: "Step", parent: "Step") -> None:
    """Copia (por referencia) al substep los atributos compartidos del step padre. ..."""
    substep.cohort = parent.cohort
    substep.input_parameters = parent.input_parameters
    substep.input_hive = parent.input_hive
    substep.output_hive = parent.output_hive
    substep.vintage = parent.input_parameters["vintage"]
    substep.vintage_date = parent.input_parameters["vintage_date"]
    substep.sqlContext = parent.sqlContext
    substep.is_dynamic = parent.is_dynamic
    substep.standard_load_parquet_or_table = parent.standard_load_parquet_or_table
    substep.parent = parent






def dynamic_unpartitioned_parquet(path_key: str) -> Callable:
    """ ... """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self) -> DataFrame:
            """ ... """
            output_hive = self.output_hive
            is_dynamic = self.is_dynamic
            if isinstance(output_hive[path_key], str):
                path = HivePath(output_hive[path_key])
            else:
                path = output_hive[path_key]["table_or_hdfs"]
            #
            if output_hive[path_key]["keep_or_delete"] == "delete":
                self.tmp_paths.append(path)
            try:
                if not is_dynamic:
                    raise Exception
                result = self.sqlContext.read.parquet(str(path))
                print(f"INFO: {path_key.capitalize()} data reloaded from {path}")
            except Exception as e:
                print(f"WARNING: Could not load {path_key} from {path} due to {e}. Recomputing...")
                func(self).write.mode("overwrite").parquet(str(path))
                result = self.sqlContext.read.parquet(str(path))
                print(f"INFO: {path_key.capitalize()} data saved on {path}")
            return result
        return wrapper
    return decorator
