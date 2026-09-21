# Minerva — Estado actual del proyecto

Documento generado a partir de la transcripción de las 152 capturas de pantalla
(archivadas en `notebook.md/screenshots/`). Describe lo que el código hace
**hoy**, no lo que el diseño conceptual aspira a hacer.

## Qué es Minerva

Pipeline DataPull en PySpark + GraphFrames para un banco. Construye un grafo de
transferencias (SPEI/CEPS) donde los nodos son identidades (RFC/CURP preferido,
cuenta o numcliente) y las aristas son transacciones src→dst. Sobre ese grafo
calcula features (con y sin pesos) y propaga un target de fraude (Lovelace)
para generar features de contagio a nivel `numcliente`.

## Framework (`libs/`)

- **`libs/framework`** (`Step`, `SubStep`, `cached_property`, decoradores
  `dynamic_*`): cada ETL es un `Step` con `input`/`output` declarados en
  `config/`. Cada sub-etapa persiste su resultado como parquet o partición de
  tabla Hive — es lo que permite reanudar el proceso tras interrupciones del
  clúster. `run_substep_and_collect`, `collect_step_output` y
  `build_step_init_kwargs` encadenan sub-steps y recolectan outputs.
- **`libs/data_engineering_toolbox`**:
  - `path/` — `HivePath`/`LinuxPath` (wrappers de HDFS vía `hdfs dfs` y Linux).
  - `context/` — `notebook(...)`: crea la SparkSession con jars y archive del venv.
  - `pyspark/tools/partition_lags.py` — gestión de particiones y lags mensuales.
  - `pyspark/tools/parquet_treatment.py` — lectura tabla/parquet y overwrite de
    particiones (`OverwritePartitionParquet/Table`, etc.).
  - `pyspark/history.py` — `HistoryColumnDataFrame`: trazabilidad de columnas.
  - `pyspark/compare.py`, `counts.py`, `analysis.py`, `dynamic/` — utilidades de
    diff, conteos y escritura dinámica (parquet/csv, carga a Hive).
  - `general/date_treatment/` — helpers de fechas (tfrom_months/tfrom_days).
  - `pyspark/pytest/` — helpers de tests (compare, quality, schemas).

## Configuración (`config/`)

- `job.py` — parámetros del job: fechas (`RG49392_TODAY`), `MAIN_LAG_IN_MONTHS=1`,
  `COHORT="SBX"`, `vintage` (yyyymm del mes objetivo) y `sbx` con el root HDFS:
  `/data/gcprcmsbx/work/hive/gcprcmsbx_work/rg49392/minerva/ceps_history/{vintage}`.
- Un módulo de config por step define los dicts `input`/`output` (tabla Hive o
  ruta parquet, columnas, claves de join, columnas de pesos, etc.).

## Flujo del pipeline (según `run_order.py`)

Script de desarrollo (`5.1-rg49392-dev-target_propagation`). Crea la sesión
Spark con variables de entorno `RG49392_*` / `VENV_ZIP_*` / `GRAPHFRAMES_JAR`
y encadena los steps con `previous_step`:

1. **`CepsRfcNomRankingStep`** (`pipelines/ceps/rfc_nom_ranking.py`)
   Extracción/tratamiento especial de CEPS: limpieza de nombres
   (`banamex_nom_reorder`, `limpiar_acentos`, `normalizar_espacios`), flags de
   validación RFC/CURP (`add_id_validation_flags`) y ranking para elegir el
   RFC correcto por cuenta/nombre. Produce `s264_ceps*` aplanado y rankeado.
2. **`CepsTxnReplacementStep`** (`pipelines/ceps/txn_replacement.py`)
   Reemplaza los identificadores de cada txn por el RFC ganador del ranking.
3. **`CepsSpecialTreatment`** (`pipelines/graph_making/ceps/special_treatment.py`)
   Aplanamiento src/dst, `tfrom_months`/`tfrom_days`, missing treatment.
4. **`CepsGroupByStep`** (`pipelines/graph_making/ceps/group_by.py`)
   Agregados por id (src) y por txn (src-dst): montos, conteos y arrays de
   identificación (numcliente, nom, cta, id_ban, mis_date).
5. **`CepsEdgesAndNodesStep`** (`pipelines/graph_making/ceps/edges_and_nodes.py`)
   Construye `edges` (datos txn + array `weights` de tipos de peso) y `nodes`.
6. **`CepsGraphFeaturesStep`** (`pipelines/features/ceps/graph_features.py`)
   Features GraphFrames con y sin pesos: PageRank, degrees, edge stats,
   componentes, triangle count.
7. **`LovelaceTargetPropagationSpecialTreatmentStep`**
   (`pipelines/target_propagation/lovelace/special_treatment.py`)
   Special/missing treatment del target Lovelace (`ft_tmx_proba`).
8. **`CepsTargetPropagationFeaturesStep`**
   (`pipelines/features/ceps/target_propagation_features.py`)
   Join del target a los nodos (`nodes_join_target`) y propagación sobre el
   grafo (normalización + contagio).

`run_order.py` termina mostrando `nodes_join_target.show(10)` — el join de
target ya produce datos (id, arrays numcliente/nom/cta/id_ban, `target_lov…`).

## Estado actual / brechas

**Funciona (transcrito y sintácticamente válido):** los 8 steps anteriores con
sus configs. El grafo se construye y el target ya se une a los nodos.

**Incompleto en el propio código original:**
- `propagate_target` (target_propagation_features) referencia nombres
  indefinidos (`kwargs`, `parent_hdfs`, `property_name`) — WIP.
- `dynamic/functions.py` llama `save_table(...)` que no existe.
- `config/ceps/rfc_nom_ranking.py` tiene la clave
  `"s264_ceps_flattened_ranks"` duplicada en `output`; `graph_features.py`
  reasigna `WEIGHTED_EDGE_STARTS_FEATURES_AGGREGATIONS` a lista vacía.
- Imports muertos/typos preservados verbatim (`from tabnanny import check`,
  `from calendar import c`, `ceps_rfs_nom_ranking_step`, etc.).

**Pendiente según el concept design:** features de agrupación (solo el espacio)
y el VectorAssembler final a nivel `numcliente` (explode del array de nodos +
media ponderada por monto/antigüedad).

**Perdido por las fotos (no recuperable sin re-fotografiar):**
- `s264_ceps_cleaned` en `pipelines/ceps/rfc_nom_ranking.py`: método completo
  plegado en el IDE (~113 líneas) → quedó como stub `...`.
- ~60 líneas plegadas de `SubStep` en
  `pipelines/target_propagation/special_treatment.py`.
- `s264_ceps_flattened_ranks` en `graph_making/ceps/special_treatment.py`
  (~20 líneas en un gap de scroll) → reconstruido por inferencia.
- Docstrings plegados en casi todos los módulos (solo se veía la 1ª línea).
- Primeras líneas de varios archivos (imports) cortadas por el borde superior.

## Entorno

- Requiere PySpark 3.3.2 + `jars/graphframes-0.81-spark3.0-s_2.12.jar`.
- Variables de entorno: `RG49392_WORKSPACE_LINUX`, `RG49392_QUEUE`,
  `RG49392_PORT`, `RG49392_NAME`, `RG49392_TODAY`, `VENV_ZIP_LINUX`,
  `VENV_ZIP_HDFS`, `GRAPHFRAMES_JAR`.
- `pipelines/__init__.py` añade el root y `libs/` a `sys.path`.
