# spark-etl-framework

Framework PySpark reutilizable para pipelines ETL **reanudables e
incrementales** sobre Hive/HDFS (y opcionalmente GraphFrames para features de
grafo). Extraído del proyecto Minerva: contiene solo la maquinaria genérica —
sin pipelines, configs ni targets de negocio.

## Qué incluye

- **`libs/framework`** — motor de ETL:
  - `Step`/`SubStep` con encadenamiento vía `previous_step` (ejecutar el último
    step lanza toda la cadena).
  - Decoradores `dynamic_*`: cada salida se persiste como parquet/tabla
    particionada (`information_date`, `process_date`) y se **recarga** si ya
    existe → la ejecución reanuda donde quedó.
  - `ensure_monthly_partitions`: materialización incremental — computa solo
    los meses ausentes de la ventana `lag`/`history`.
  - `build_step_init_kwargs`, `run_substep_and_collect`, `collect_step_output`,
    `inherit_parent_step_attributes`, `cached_property`.
- **`libs/data_engineering_toolbox`** — utilidades de infraestructura:
  - `context/`: `SparkSessionBuilder` (sesión YARN con reintentos, o local) y
    `setup_logging`/`get_logger`/`log_method`.
  - `path/`: `HivePath`/`LinuxPath` (wrappers de `hdfs dfs` y fs local).
  - `pyspark/tools/`: lectura de tablas/parquets particionados con ventanas
    `lag`/`history` (`SparkLoadPartitionedTableOrParquet`, `partition_lags`,
    `overwrite_partition`, `overwrite_two_partition`).
  - `pyspark/`: `history` (trazabilidad de columnas), `compare`, `counts`,
    `analysis`, `dynamic`, `testing` (asserts de DataFrames/schemas).
  - `general/date_treatment/`: helpers de fechas y constantes de formato
    (`DATE_*_FORMAT`).
- **`libs/functions`** — librerías de funciones reutilizables:
  - `missing_treatment`: imputación declarativa de nulos.
  - `aggregations`: catálogo de agregaciones + parciales mensuales combinables
    (`monthly_partial_group_by_aggregations` / `merge_monthly_group_by`).
  - `features`: features de grafo (GraphFrame → DataFrame) y propagación de
    target por difusión iterativa (`propagate_target`).
  - `weights`, `assembly`: columnas de peso y ensamblado de features a vector.
- **`pipelines/`** — clases `Standard*` reutilizables (base para los steps de
  un proyecto concreto): `graph_making/` (special_treatment, group_by,
  edges_and_nodes), `features/` (graph_features, cluster_features,
  target_propagation_features, vector_assembler) y
  `target_propagation/special_treatment`.
- **`supervisor.py`** — supervisor autónomo: reinicia el proceso si muere y
  detecta sesiones zombie consultando la Spark UI (`SUPERVISOR_*` env vars).
- **`examples/`** — mini-proyecto completo de referencia (config, pipeline de
  2 steps con agregación mensual incremental y `main.py`).
- **`opt/`** — scripts operativos: `environment_vars.sh` (plantilla),
  `run-tests.sh`, `run-pyspark_terminal.sh`, `run-supervised.sh`.

## Quickstart

```bash
conda env create -f environment.yml && conda activate spark_etl_framework

# Local (sin cluster): APP_TEST_MODE=local usa tmp_path en vez de HDFS
python -m pytest tests/ -v

# Ejemplo end-to-end
python examples/main.py --local --build-only
```

En cluster: ajusta `opt/environment_vars.sh` (vars `APP_*`, `PYSPARK_*`,
`VENV_*`) y haz `source opt/environment_vars.sh`.

## Cómo funciona

Un pipeline es una cadena de `Step`s. Cada `Step` declara `input_hive`/
`output_hive` (dicts de config: ruta, columnas de partición, `lag`,
`history`, `select`, `keep_or_delete`) y delega en `SubStep`s cuyas
propiedades `@cached_property` están decoradas con
`@dynamic_partitioned_table_or_parquet(path_key=...)`: si la partición del
vintage ya existe se recarga; si no, se computa, se le estampa
`information_date`/`process_date` y se sobrescribe solo esa partición.

Para agregaciones incrementales, el patrón es:

1. Parciales por mes (`standard_group_by_txn_monthly`,
   `monthly_partial_group_by_aggregations`) persistidos en una tabla
   particionada por `month_partition`.
2. `ensure_monthly_partitions` computa solo los meses que falten en la ventana.
3. `merge_monthly_group_by` combina los parciales en el agregado completo
   (idéntico al group-by directo sobre el crudo).

Las funciones sin combinador exacto (`countDistinct`, `last`, `first`) lanzan
`ValueError` → fallback al group-by completo.

## Supervisor

```bash
SUPERVISOR_COMMAND="python examples/main.py" \
SUPERVISOR_STEP_TIMEOUT=7200 SUPERVISOR_STALL_TIMEOUT=600 \
python supervisor.py
```

Vigila el proceso hijo: lo reinicia si muere (OOM, SIGKILL) —la reanudación la
dan los parquets ya persistidos— y lo mata si un Step lleva
`SUPERVISOR_STEP_TIMEOUT` sin progreso de tareas en la Spark UI (zombie).
Wrapper: `opt/run-supervised.sh`.

## Tests

`APP_TEST_MODE=local` (default): SparkSession `local[2]` + shim del fs local
para las primitivas HDFS (fixture `local_hdfs` en `tests/conftest.py`).
`APP_TEST_MODE=cluster`: sesión YARN real y `APP_TMP_TESTS_DIR_HDFS`.

## Entorno

- PySpark 3.3.2 (+ GraphFrames si se usan las features de grafo).
- Vars: `APP_NAME`, `APP_TODAY`, `APP_LOG_LEVEL`, `PYSPARK_QUEUE`,
  `PYSPARK_PORT`, `PYSPARK_*_DIR_HDFS`, `PYSPARK_VENV_TAR_GZ_HDFS`,
  `PYSPARK_JARS_HDFS`, `SUPERVISOR_*`.
