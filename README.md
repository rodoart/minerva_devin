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
- **`opt/`** — scripts operativos: `environment_vars.sh` (selector por
  plataforma) + `environment_vars.{unix,wsl,cluster}.sh`, `run.sh`,
  `run-supervised.sh`, `run-tests.sh`, `run-pyspark_terminal.sh`,
  `setup-local-env.sh` (venv + JRE local).

## Plataformas y modos

El framework distingue dos ejes independientes:

**Plataforma** (`APP_OS_MODE`/`APP_MODE`, o autodetección):

| Modo | Qué es | Detección |
|---|---|---|
| `unix` (**default**) | Cualquier POSIX (Linux, macOS). El default: sin configuración todo funciona. | resto |
| `wsl` | WSL (Ubuntu en Windows): unix completo + detalles de interop (paths `/mnt/*`, FS nativo recomendado). | `uname -r` contiene `microsoft`/`wsl` |
| `windows` | Windows nativo: rutas con `\`, sin `killpg`, shlex no-POSIX. | `win32`/`os.name == "nt"` |

**Backend de filesystem** (por ruta o forzado con `APP_FS_MODE`/
`APP_MODE`/`APP_TEST_MODE`): `hdfs`/`cluster` → CLI `hdfs dfs`;
`windows`/`unix`/`wsl`/`local` → filesystem local. Sin envs: `hdfs://` →
HDFS; `file:`/letra de unidad → local; `/x` + CLI hdfs → HDFS, resto local.

```bash
# unix/wsl (default — autodetecta)
source opt/environment_vars.sh        # carga .wsl.sh o .unix.sh según el OS
bash opt/setup-local-env.sh           # 1ª vez: venv + deps + JRE local
bash opt/run-tests.sh                 # o: .venv/bin/python -m pytest tests/ -v

# cluster
APP_MODE=cluster source opt/environment_vars.sh
```

En WSL el setup crea `~/venvs/laika_framework` + `~/opt/jre` en el FS
nativo (el workspace puede estar en `/mnt/<drive>` y el IO sobre `/mnt`
es lento). En unix usa `<workspace>/.venv` y `<workspace>/.local/jre`.

## Quickstart

```bash
# unix/wsl (default): venv local, sin cluster
bash opt/setup-local-env.sh
.venv/bin/python -m pytest tests/ -v     # o venv equivalente
python examples/main.py --local --build-only
```

En cluster: ajusta `opt/environment_vars.cluster.sh` (vars `APP_*`,
`PYSPARK_*`, `VENV_*`) y haz `APP_MODE=cluster source opt/environment_vars.sh`.

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

## Ejecución: supervisada y no supervisada

Ambos modos cargan `opt/environment_vars.sh` (que define `APP_MAIN_SCRIPT`
—el entry point del pipeline—, `SUPERVISOR_COMMAND` y `APP_LOG_LEVEL`):

```bash
./opt/run.sh              # SIN supervisor: ejecuta APP_MAIN_SCRIPT directo
./opt/run-supervised.sh   # CON supervisor: vigila SUPERVISOR_COMMAND
```

El **supervisor** (`supervisor.py`) lanza `$SUPERVISOR_COMMAND` (default
`$PYSPARK_DRIVER_PYTHON $APP_MAIN_SCRIPT`) como subproceso y lo vigila: lo
reinicia si muere (OOM, SIGKILL) —la reanudación la dan los parquets ya
persistidos— y lo mata si un Step lleva `SUPERVISOR_STEP_TIMEOUT` sin
progreso de tareas en la Spark UI (zombie). Toda la salida del hijo se
reemite por el logger del supervisor (`supervisor.child`).

Env vars del supervisor: `SUPERVISOR_COMMAND`, `SUPERVISOR_STEP_TIMEOUT`
(7200s), `SUPERVISOR_STALL_TIMEOUT` (600s), `SUPERVISOR_WATCHDOG_INTERVAL`
(30s), `SUPERVISOR_MAX_RESTARTS` (10), `PYSPARK_PORT` (4040).

## Logging

La suite (`libs/data_engineering_toolbox/context/logging.py`) emite a
**consola y archivo** a la vez. `setup_logging()` resuelve el `.log` así:

1. `log_file=` explícito,
2. `APP_LOG_FILE` (ruta completa; `off`/`none` desactiva el archivo),
3. `LOGS_DIR_LINUX` + `APP_NAME` →
   `logs/<APP_NAME>_<YYYY-mm-dd_HH-MM-SS>.log` (lo exporta
   `environment_vars.sh`),
4. sin envs → solo consola.

El pipeline y el supervisor escriben cada uno su propio `.log` (timestamp
por proceso); el supervisor además vuelca el stdout del hijo bajo el logger
`supervisor.child`, de modo que la salida del pipeline queda también en el
`.log` del supervisor.

## Manejo de paths

Todo el manejo de rutas pasa por `libs.data_engineering_toolbox.path`:

- **`HivePath`**: `PurePosixPath` + operaciones de filesystem (`exists`,
  `mkdir`, `iterdir`, `listparquets`, `rmdir`, `mv`, `cp`, ...). Las
  funciones del módulo despachan por ruta: CLI `hdfs dfs` en el clúster,
  filesystem local en local — la misma API sirve para ambos.
- **`LocalPath`**: misma API forzando siempre el FS local — para rutas que
  son locales incluso en el clúster (los `.log` del driver, `sys.path`,
  scripts de `opt/`).
- **`UniversalPath`**: envolvente de las dos — `UniversalPath(p)` devuelve
  `HivePath` o `LocalPath` según `is_hdfs_path(p)`. Punto de entrada cuando
  el mismo código maneja rutas de ambos backends.

`is_hdfs_path` decide el backend: `hdfs://` → HDFS; `file:`/letra Windows →
local; `APP_FS_MODE`/`APP_MODE`/`APP_TEST_MODE` con
`hdfs|cluster|windows|unix|wsl|local` fuerza el modo; sin envs, autodetecta
por la presencia del CLI `hdfs`. En tests, el fixture `local_hdfs` exporta
`APP_FS_MODE=<os_mode()>` en lugar de parchear funciones.

`os_mode()` (`path.local`, re-exportado por `path`) resuelve la plataforma:
`APP_OS_MODE`/`APP_MODE` → autodetección (`win32`→windows,
kernel `microsoft`/`wsl`→wsl) → **`unix` por defecto**.

## Tests

Modos locales `unix` (**default**)/`windows`/`wsl`/`local`: SparkSession
`local[2]` + backend local del fs (fixture `local_hdfs` en
`tests/conftest.py`). `APP_TEST_MODE=cluster|hdfs`: sesión YARN real y
`APP_TMP_TESTS_DIR_HDFS`.

## Entorno

- PySpark 3.3.2 (+ GraphFrames si se usan las features de grafo).
- Vars: `APP_NAME`, `APP_TODAY`, `APP_LOG_LEVEL`, `APP_MODE`, `APP_OS_MODE`,
  `APP_FS_MODE`, `APP_TEST_MODE`, `PYSPARK_QUEUE`, `PYSPARK_PORT`,
  `PYSPARK_*_DIR_HDFS`, `PYSPARK_VENV_TAR_GZ_HDFS`, `PYSPARK_JARS_HDFS`,
  `SUPERVISOR_*`.
- WSL: `wsl --install -d Ubuntu` (requiere reinicio de Windows), luego
  dentro de Ubuntu `bash opt/setup-local-env.sh`.
