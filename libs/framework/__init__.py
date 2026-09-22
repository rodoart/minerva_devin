"""Framework de pasos (`Step`) del ETL: ejecución encadenada, caché de
propiedades y decoradores `dynamic_*` de lectura/escritura perezosa.
"""
from typing import Any, Dict, List, Union, Optional
from pathlib import Path, PosixPath
from inflection import underscore

from typing import Callable, Tuple
import logging

from datetime import date
from functools import wraps

from pyspark import HiveContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException

from libs.data_engineering_toolbox.path import HivePath
from libs.data_engineering_toolbox.pyspark.tools.parquet_treatment import SparkLoadPartitionedTableOrParquet, overwrite_partition, overwrite_two_partition
from libs.framework.utils import sanitize_name

import time

logger = logging.getLogger(__name__)



def cast_to_schema(schema: StructType, target_df:DataFrame) -> DataFrame:
    """Castea las columnas de `target_df` a los tipos declarados en `schema`."""
    for field in schema:
        column_name = field.name
        column_type = field.dataType
        if column_name in target_df.columns:
            target_df = target_df.withColumn(column_name, col(column_name).cast(column_type))
    return target_df



class cached_property:
    """Propiedad cacheada por instancia: calcula el valor una vez y lo guarda en `self.__dict__`."""
    _MAX_RETRIES = 5
    #
    def __init__(self, func: Callable[[Any], Any]) -> None:
        """Guarda la función y fija el nombre de la clave de caché en `__dict__`."""
        self.func = func
        self.cache_name = f"_{func.__name__}_cache"
        #
    def __get__(self, instance: Any, owner: Any) -> Any:
        """Devuelve el valor cacheado; si no existe, lo calcula y lo almacena en la instancia."""
        if instance is None:
            return self
        if self.cache_name not in instance.__dict__:        # solo cache de INSTANCIA
            instance.__dict__[self.cache_name] = self.func(instance)
        return instance.__dict__[self.cache_name]



class Step:
    """Unidad de ETL: ejecuta `step_action` tras resolver `previous_step` y acumula salidas en `output_parameters`."""
    def __init__(self, *args, **kwargs) -> None:
        """Inicializa rutas de entrada/salida (linux/hive), parámetros y el nombre del paso."""
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
        """Construye el loader de la propiedad y le aplica el decorador dinámico si procede."""
        decorator_key:str = kwargs["decorator_key"]
        # Las claves de bookkeeping del framework no se reenvían al método.
        _BOOKKEEPING_KEYS = {
            "method", "decorator", "decorator_key", "property_name",
            "path", "config_dict_key", "input_or_output", "session",
        }
        fn_kwargs = {k: v for k, v in kwargs.items() if k not in _BOOKKEEPING_KEYS}
        def _loader(self) -> DataFrame:
            fn = getattr(self, method) if isinstance(method, str) else method
            return fn(*args, **fn_kwargs)
        _loader.__name__ = property_name
        return _loader if decorator is None else decorator(decorator_key)(_loader)
    #
    #
    def get_cached_decorated_table_or_parquet_property(self, *args, **kwargs) -> DataFrame:
        """Fabrica (y cachea por `property_name`) la propiedad que carga una tabla/parquet.

        Elige el decorador según `path`/`config_dict_key`/`input_or_output`:
          - `path` de entrada: sin decorador; de salida: `dynamic_unpartitioned_parquet_with_path`.
          - `config_dict_key`: `dynamic_unpartitioned_parquet` (config de 2 claves) o
            `dynamic_partitioned_table_or_parquet` (config particionada).

        Returns:
            El DataFrame cargado (o la tupla que devuelva el decorador).
        """
        if "input_or_output" not in kwargs:
            kwargs["input_or_output"] = "input"
        config_dict = self.input_hive if kwargs["input_or_output"] == "input" else self.output_hive
        kwargs["session"] = self.sqlContext
        decorator_key = None
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
            if len(config_dict[kwargs["config_dict_key"]]) == 2:
                decorator = dynamic_unpartitioned_parquet
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
        logger.info("Defining property: %s", property_name)
        try:
            logger.info("Defining path: %s", kwargs["path"])
        except:
            logger.warning("something wrong with path")
        #
        if property_name not in self._decorated_cache:            # <- VARIABLE, no string
            loader = self._build_decorated_loader(**kwargs)
            self._decorated_cache[property_name] = loader(self)   # <- VARIABLE
        return self._decorated_cache[property_name]
    #
    def _make_step_name(self) -> str:
        """Genera un nombre único `<clase>_<n>` con un contador global por clase."""
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
        """Representación corta del paso: `Step(<step_name>)`."""
        return f"Step({self.step_name})"
    #
    def __repr__(self) -> str:
        """Representación corta del paso: `Step(<step_name>)`."""
        return f"Step({self.step_name})"
    #
    @cached_property
    def _previous_step_output_parameters_loaded(self) -> Dict[str,Any]:
        """Ejecuta los `previous_step` (lista o único) y devuelve sus `output_parameters` unidos."""
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
        """Lógica del paso; las subclases deben implementarla."""
        raise NotImplementedError("Subclasses should implement this method.")
    #
    def _iter_previous_steps(self) -> List["Step"]:
        """Devuelve los previous_step como lista (vacía si no hay)."""
        if isinstance(self.previous_step, list):
            return list(self.previous_step)
        return [self.previous_step] if self.previous_step else []
    #
    def collect_tmp_paths(self, _visited:Optional[set] = None) -> List[str]:
        """Reúne recursivamente los paths intermedios borrables: `tmp_paths`
        propios + outputs marcados `keep_or_delete="delete"`, de los substeps
        cacheados y de toda la cadena de `previous_step` (deduplicado y con
        protección contra ciclos padre<->substep)."""
        if _visited is None:
            _visited = set()
        if id(self) in _visited:
            return []
        _visited.add(id(self))
        #
        paths = [str(p) for p in self.tmp_paths]
        paths += [
            str(cfg["table_or_hdfs"]) for cfg in self.output_hive.values()
            if isinstance(cfg, dict)
            and cfg.get("keep_or_delete") == "delete"
            and "information_date_column" not in cfg   # nunca borra tablas particionadas
        ]
        for value in self.__dict__.values():
            nested:List["Step"] = []
            if isinstance(value, Step):
                nested = [value]
            elif isinstance(value, (list, tuple)):
                nested = [v for v in value if isinstance(v, Step)]
            for step in nested:
                paths += step.collect_tmp_paths(_visited)
        for prev in self._iter_previous_steps():
            paths += prev.collect_tmp_paths(_visited)
        return list(dict.fromkeys(paths))
    #
    def delete_tmp_paths(self,
        skip_missing:bool = True,
        skip_trash:bool = True
    ) -> List[str]:
        """Borra los parquets intermedios registrados en `tmp_paths` de toda la
        cadena de steps (outputs marcados `keep_or_delete="delete"`).

        Pensado para ejecutarse al final del flujo (`python main.py --cleanup`).
        """
        deleted:List[str] = []
        for path_str in self.collect_tmp_paths():
            try:
                HivePath(path_str).rmdir(recursive=True, skip_trash=skip_trash)
                deleted.append(path_str)
                logger.info("Deleted intermediate parquet: %s", path_str)
            except Exception as e:
                if not skip_missing:
                    raise
                logger.warning("Could not delete %s: %s", path_str, e)
        return deleted
    #
    def execute(self) -> Any:
        """Ejecuta el paso: resuelve `previous_step`, actualiza `input_parameters` y corre `step_action`."""
        logger.info("Executing step: %s", self.step_name)
        logger.debug("Input parameters: %s", self.input_parameters)
        _start = time.time()
        if self.previous_step:
            self.input_parameters.update(self._previous_step_output_parameters_loaded)
        result = self.step_action()
        logger.info("Step finished: %s (%.1fs)", self.step_name, time.time() - _start)
        return result




def _reload_or_recompute_parquet(self, func: Callable, path: HivePath, label: str) -> DataFrame:
    """Lee `path` como parquet si `is_dynamic`; si falla, ejecuta `func` y lo guarda."""
    try:
        if not self.is_dynamic:
            raise Exception
        result = self.sqlContext.read.parquet(str(path))
        logger.info("%s data reloaded from %s", label.capitalize(), path)
    except Exception as e:
        logger.warning("Could not load %s from %s due to %s. Recomputing...", label, path, e)
        func(self).write.mode("overwrite").parquet(str(path))
        result = self.sqlContext.read.parquet(str(path))
        logger.info("%s data saved on %s", label.capitalize(), path)
    return result


def dynamic_unpartitioned_parquet_with_path(path: Union[str, HivePath]) -> Callable:
    """Decorador reload-or-recompute para parquet sin particionar con `path` explícito."""
    if isinstance(path, str):
        path = HivePath(path)
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self) -> DataFrame:
            path_key = str(path).split("/")[-2] + "-" + str(path).split("/")[-1]
            return _reload_or_recompute_parquet(self, func, path, path_key)
        return wrapper
    return decorator


def dynamic_unpartitioned_parquet(path_key: str) -> Callable:
    """Decorador reload-or-recompute para parquet sin particionar leído de `output_hive`.

    Si la config marca `keep_or_delete == "delete"`, registra el path en `tmp_paths`.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self) -> DataFrame:
            output_hive = self.output_hive
            if isinstance(output_hive[path_key], str):
                path = HivePath(output_hive[path_key])
            else:
                path = output_hive[path_key]["table_or_hdfs"]
            #
            if output_hive[path_key]["keep_or_delete"] == "delete":
                self.tmp_paths.append(path)
            return _reload_or_recompute_parquet(self, func, path, path_key)
        return wrapper
    return decorator


def _partition_load_kwargs(config: Dict[str, Any], input_parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Construye los kwargs de `SparkLoadPartitionedTableOrParquet` desde el dict de config."""
    return {
        "table_or_hdfs": config["table_or_hdfs"],
        "information_date_column": config["information_date_column"],
        "current_date": input_parameters["vintage_date"],
        "lag": config.get("lag", 0),
        "history": config.get("history", 0),
        "process_date_column": config.get("process_date_column"),
        "process_date_mode": config.get("process_date_mode", "last"),
        "information_date_mode": config.get("information_date_mode", "each"),
        "missing_months_allowed": config.get("missing_months_allowed", True),
    }


def _compute_and_overwrite_partition(
    self,
    func: Callable,
    config: Dict[str, Any],
    load_object_kwargs: Dict[str, Any],
    path_key: str,
) -> DataFrame:
    """Ejecuta `func`, añade columnas de partición y sobreescribe la partición destino."""
    table_or_hdfs = load_object_kwargs["table_or_hdfs"]
    information_date_column = load_object_kwargs["information_date_column"]
    process_date_column = load_object_kwargs["process_date_column"]
    vintage_date:date = self.input_parameters["vintage_date"]
    process_date:date = self.input_parameters["process_date"]
    schema:StructType = config.get("schema")
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
        return df
    #
    partitions = [information_date_column]
    if process_date_column:
        partitions.append(process_date_column)
    #
    if process_date_column is not None and len(partitions) == 2:
        logger.info("Overwriting two partitions: %s on %s", partitions, table_or_hdfs)
        overwrite_object = overwrite_two_partition(
            df=df,
            process_date_column=process_date_column,
            information_date_column=information_date_column,
            table_or_hdfs=table_or_hdfs,
            session=self.sqlContext
        )
    else:
        logger.info("Overwriting partition: %s on %s", partitions, table_or_hdfs)
        overwrite_object = overwrite_partition(
            df=df,
            partition_by=partitions,
            table_or_hdfs=table_or_hdfs,
            session=self.sqlContext
        )
    overwrite_object.write()
    result = overwrite_object.read()
    logger.info("%s data saved on %s", path_key.capitalize(), table_or_hdfs)
    return result


def dynamic_partitioned_table_or_parquet(path_key: str) -> Callable:
    """Decorador reload-or-recompute para tabla/parquet particionado de `output_hive`.

    Recarga la partición (information_date=vintage [, process_date]) si existe; si no,
    ejecuta la función, añade las columnas de partición y sobreescribe solo esas
    particiones. Devuelve `(DataFrame, load_object_kwargs)`.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self) -> Tuple[DataFrame, Dict[str, Union[str, HivePath, PosixPath, int]]]:
            output_hive:dict = self.output_hive
            load_object_kwargs = _partition_load_kwargs(output_hive[path_key], self.input_parameters)
            table_or_hdfs = load_object_kwargs["table_or_hdfs"]
            #
            try:
                if not self.is_dynamic:
                    raise Exception
                logger.info("Attempting to load %s from %s", path_key, table_or_hdfs)
                logger.debug("load_object_kwargs: %s", load_object_kwargs)
                load_object = SparkLoadPartitionedTableOrParquet(
                    session=self.sqlContext, **load_object_kwargs
                )
                result = load_object.df
                logger.info("%s data reloaded from %s", path_key.capitalize(), table_or_hdfs)
            except Exception as e:
                logger.warning("Could not load %s from %s due to %s. Recomputing...", path_key, table_or_hdfs, e)
                result = _compute_and_overwrite_partition(self, func, output_hive[path_key], load_object_kwargs, path_key)
            return result, load_object_kwargs
        return wrapper
    return decorator




def standard_load_parquet_or_table(
    config_dict: Dict[str, dict],
    config_dict_key: str,
    current_date: date,
    session: Union[HiveContext, SparkSession],
) -> DataFrame:
    """Carga estándar de una tabla/parquet particionado con validación de historial."""
    config = config_dict[config_dict_key]
    minimum_required_history = config.get("minimum_required_history", False)
    try:
        load_object = SparkLoadPartitionedTableOrParquet(
            table_or_hdfs=config["table_or_hdfs"],
            information_date_column=config["information_date_column"],
            current_date=current_date,
            lag=config["lag"],
            history=config["history"],
            session=session,
            information_date_mode=config["information_date_mode"],
            process_date_column=config.get("process_date_column"),
            process_date_mode=config.get("process_date_mode"),
            missing_months_allowed=True
        )
        result = load_object.df
        total_months = len(load_object.months_in_interval)
    except AnalysisException as e:
        logger.error("Error loading %s with standard_load_parquet_or_table: %s", config_dict_key, e)
        assert config.get("schema"), f"Schema must be provided for {config_dict_key} when loading fails."
        total_months = 0
        result = session.createDataFrame([], config["schema"])

    if total_months < minimum_required_history:
        raise ValueError(f"Not enough history in {config_dict_key} data")
    #
    if total_months < config["history"]:
        logging.warning(f"Warning: {config_dict_key} data has less history than expected")
    #
    return result




def run_substep_and_collect(
    parent: "Step",
    substep: "Step",
    output_key: str,
) -> Dict[str, Any]:
    """Ejecuta un substep terminal y sube su salida al 'output_parameters' del padre."""
    substep.step_action()
    parent.output_parameters[parent.step_name][output_key] = (
        substep.output_parameters[substep.step_name]
    )
    return parent.output_parameters

def collect_step_output(step: "Step", output: Any, output_key: str) -> Dict[str, Any]:
    """Almacena una salida ya materializada en el `output_parameters` del step."""
    step.output_parameters[step.step_name][output_key] = output
    return step.output_parameters




def build_step_init_kwargs(
    date_treatment: Dict[str, str],
    input_hive: Dict[str, "HivePath"],
    output_hive: Dict[str, "HivePath"],
    step_name_prefix: str,
    extra_kwargs: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Construye los kwargs estándar para inicializar un `Step` desde `date_treatment`."""
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
    """Copia (por referencia) al substep los atributos compartidos del step padre."""
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
