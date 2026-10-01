# Guía de uso de Minerva

Cómo ejecutar, configurar y extender el pipeline. Para el diseño conceptual ver
`concept_desing.md`; para la estructura del código ver `arquitectura.md`.

## 1. Requisitos

### Entorno

```bash
conda env create -f environment.yaml
conda activate minerva_devin
```

Dependencias clave (fijadas en `environment.yaml`): `spark=3.3.2.3`,
`python=3.10`, `graphframes` (pip), `pytest`, `pandas`, `regex`,
`inflection`, `python-dateutil`.

### Variables de entorno

El acceso a HDFS/YARN se configura por env vars (las valida `main.py`):

| Variable | Uso |
|---|---|
| `MINERVA_WORKSPACE_DIR_LINUX` | Ruta de trabajo local del usuario |
| `PYSPARK_QUEUE` | Cola YARN (se usa `datalabs` en `main.py`/`run_order.py`) |
| `PYSPARK_PORT` | Puerto de la Spark UI |
| `MINERVA_NAME` | Nombre de la aplicación Spark |
| `MINERVA_TODAY` | Fecha del proceso `YYYY-MM-DD` (validada por `config/__init__.py`; define `process_date` y el vintage mensual) |
| `MINERVA_VENV_TAR_GZ_LINUX` / `MINERVA_VENV_TAR_GZ_HDFS` | Zip del venv (local y su ruta HDFS `hdfs://...` para `archive`) |
| `GRAPHFRAMES_JAR` | Path del jar `graphframes-0.81-spark3.0-s2.12.jar` |
| `HADOOP_CONF_DIR` | Config de Hadoop para comandos HDFS (`HivePath.*`) |
| `PYLIB` | Opcional: zips pyspark/py4j del cluster |

El vintage se deriva de `MINERVA_TODAY` en `config/job.py` (`vintage`, `vintage_date`, `process_date`, `date_treatment`).

## 2. Ejecución

### Flujo completo (producción)

```bash
python main.py                      # ejecuta los 10 steps encadenados
python main.py --level DEBUG        # más logs
python main.py --log-file run.log   # también a fichero
python main.py --build-only         # solo construye la cadena (smoke test)
python main.py --cleanup            # ejecuta y luego borra parquets intermedios
python main.py --cleanup-only       # solo borra los parquets intermedios
```

`main.py` construye la cadena y llama a `vector_assembler_step.execute()`.
Como `Step.execute()` ejecuta recursivamente los `previous_step`, esto lanza
todo el flujo:

```
rfc_nom_ranking → txn_replacement → special_treatment → group_by
  → edges_and_nodes → graph_features → target_propagation (lovelace)
  → target_propagation_features → cluster_features → vector_assembler
```

### Desarrollo interactivo (`run_order.py`)

`run_order.py` es un script/notebook-cell que hace lo mismo pero expone los
DataFrames para inspección (`.show()`). Está pensado para ejecutarse por celdas
en un IDE/REPL contra el cluster. Cada step queda disponible como variable
(`graph_edges_and_nodes_step`, `vector_assembler_step`, ...) y sus outputs en
`step.output_parameters`.

## 3. Reanudación (premisa dinámica)

Todo está diseñado para reanudarse tras interrupciones del cluster:

- `is_dynamic=True` en cada step → los decoradores `dynamic_*` **leen el
  parquet/partición existente si está** y solo recomputan si falta.
- Los outputs **particionados** (`information_date_column`/`process_date_column`,
  típicamente `mis_date`/`process_date`) solo sobrescriben las particiones del
  vintage en curso; el resto de meses se conserva.
- Los outputs **simples** (`{"keep_or_delete": "keep"}`) se reescriben enteros
  solo si no existen. `"delete"` los marca como temporales (`tmp_paths`) para
  limpieza al final.
- Los **intermedios pesados** también se materializan con los mismos
  decoradores (p.ej. `subcluster_df`/`nodes_enriched` en cluster_features):
  los checkpoints que GraphFrames genera internamente NO son reanudables, así
  que el punto de reanudación real es el parquet persistido.
- Los outputs parametrizados por kwargs (features, propagación) se guardan en
  subdirs por parámetro: `target_propagation/weight_type=composed/alpha=0_15/...`.
  Cambiar un parámetro genera un subdir nuevo sin pisar los anteriores.

Para **forzar recomputo** de una etapa: borrar su directorio/partición en HDFS
(`hdfs dfs -rm -r <path>` o `HivePath(path).rmdir(recursive=True)`) y volver a
ejecutar.

### Limpieza de parquets intermedios

Los outputs simples marcados `"keep_or_delete": "delete"` se consideran
**intermedios** y se pueden borrar al final del flujo:

- `Step.collect_tmp_paths()` reúne recursivamente los `tmp_paths` + outputs
  marcados de toda la cadena (substeps + `previous_step`, con deduplicación).
- `Step.delete_tmp_paths()` los borra (`HivePath.rmdir(recursive=True,
  skip_trash=True)`; `skip_missing` por defecto).
- Flags: `python main.py --cleanup` (ejecuta y luego limpia) o
  `--cleanup-only` (solo limpia).
- Marcados actualmente: `edges_norm` y `checkpoint` (features y propagación).
- **Solo outputs simples** (sin `information_date_column`): las tablas
  particionadas nunca se borran, para no perder historia.

Para marcar un nuevo intermedio: `"keep_or_delete": "delete"` en su dict de
`output`.

## 4. Estructura de configuración (`config/`)

Cada etapa tiene un módulo con `input = {...}` y `output = {...}`:

### Dict de tabla/parquet **particionado** (≥3 claves)

```python
"nodes": {
    "table_or_hdfs": HivePath("/user/.../nodes"),
    "information_date_column": "mis_date",       # partición de información
    "process_date_column": "process_date",       # partición de proceso (opcional)
    "lag": 0,                                    # meses de desfase
    "history": 12,                               # meses de ventana a leer
    "information_date_mode": "each",             # each|all|first|last
    "process_date_mode": "last",
    "missing_months_allowed": True,
    # opcionales:
    "minimum_required_history": 3,               # error si hay menos meses
    "schema": StructType(...),                   # fallback si falla la lectura
}
```

- `lag`/`history` definen el intervalo de meses leído
  (`make_date_interval_with_lag_months`).
- Los `*_mode` eligen qué particiones cargar por mes (`each` = todas,
  `last` = la última por `process_date`, `first`, `all`).

### Dict **simple** (2 claves → parquet plano)

```python
"pagerank": {"table_or_hdfs": path, "keep_or_delete": "keep"}
```

### Config de inputs

Los inputs usan las mismas claves + filtros de carga (p.ej.
`lovelace_target` incluye `select`/`filter`/`column`/`missing_treatment` —
ver `config/target_propagation/lovelace/special_treatment.py`).

## 5. Etapas y qué producen

| Step | Lee | Produce |
|---|---|---|
| `CepsRfcNomRankingStep` | CEPS crudo (24m) + catálogo Banxico opcional | CEPS con `id_src`/`id_dst` (ranking rfc/nom) |
| `CepsTxnReplacementStep` | ranking + CEPS | `rfc_curp_analysis_s264_ceps_replaced` (solo últimos 3 meses de txns) |
| `CepsSpecialTreatment` | txn_replacement | `raw_flattened` (tfrom_days, hora, monto) |
| `CepsGroupByStep` | flattened | `group_by_id` (métricas + arrays id por nodo), `group_by_txn` + parciales `*_monthly` (incremental por `month_partition`) |
| `CepsEdgesAndNodesStep` | group_by | `edges` (con mapa `weights`), `nodes` |
| `CepsGraphFeaturesStep` | edges/nodes | parquets por feature (`pagerank`, `degrees`, `degree_balance`, `reciprocity`, `self_loops`, `weighted_*`...) |
| `LovelaceTargetPropagationSpecialTreatmentStep` | lovelace crudo | `target_cta`/`target_numcliente` con TODAS las columnas de `TARGET_AGGREGATIONS` (target + scores) |
| `CepsTargetPropagationFeaturesStep` | nodes/edges + target | `edges_norm`, `target_propagation` (`contagion_<weight>` y `contagion_<col>_<weight>`), `nodes_join_target_lovelace` |
| `CepsClusterFeaturesStep` | nodos enriquecidos + features | `cluster_stats` (stats intra-grupo por `component_id`/`scc`) |
| `CepsVectorAssemblerStep` | todas las features + pivotes | `{numcliente,cta}_features` y `{numcliente,cta}_features_vector` |

**Salida final**: `{nivel}_features_vector` = SOLO llave + columna vector
`features` (lista para `pyspark.ml`); `{nivel}_features` = llave + variables
agregadas `{func}_{feat}_ceps` (con `target_*` como etiquetas fuera del
vector).

## 6. Cómo extender

### Nueva función de peso (`libs/functions/weights.py`)

```python
def mi_peso() -> Column:
    return col("oper_mto") / (col("count_txn") + lit(1))
```

Registrarla en `WEIGHT_COLUMNS` de
`config/graph_making/ceps/edges_and_nodes.py` con una clave
(`"mi_peso": mi_peso()`) → usable como `weight="mi_peso"` en features
ponderadas y propagación (`WEIGHT_TYPES`).

### Nuevo missing treatment (`libs/functions/missing_treatment.py`)

```python
def fill_missings_con_mediana(df, columns): ...
MISSING_TREATMENT_FUNCTION_RELATIONS["mediana"] = fill_missings_con_mediana
```

Usable en configs como `{"col": "mediana"}` o `{"col": ["mediana", *args]}`.

### Nueva feature de grafo (`libs/functions/features.py`)

```python
def mi_feature(graph: GraphFrame, **kwargs) -> DataFrame:
    return graph.vertices.select("id", ...)
GRAPH_FEATURE_FUNCTIONS["mi_feature"] = mi_feature
```

- Exponer `mi_feature_ft` en `StandardGraphFeatures{Un,Weighted}Step`
  (`getattr(self, f"{name}_ft")` lo busca ahí).
- Añadir `output["mi_feature"]` (simple) en
  `config/features/ceps/graph_features.py` y una entrada
  `{"mi_feature": kwargs_o_None}` en `GRAPH_CENTRALITY_FEATURES`.
- Si es ponderada, su nombre debe empezar por un prefijo de
  `GRAPH_ALL_FEATURE_PREFIXES` (`"weighted"`) para correr en el step weighted.

### Nueva fuente RAW (patrón CEPS)

1. `pipelines/<fuente>/` + `config/<fuente>/`: steps de ranking/replacement
   propios.
2. `pipelines/graph_making/<fuente>/` + `config/graph_making/<fuente>/`:
   subclases `Standard*Step` con sus `standard_*` delegates (missing
   treatment, select de columnas, renombres).
3. Cablear en `main.build_pipeline`/`run_order.py`.

### Nuevo target (patrón Lovelace)

1. `config/target_propagation/<nuevo>/special_treatment.py` con su `input`
   (`select`/`filter`/`column`/`missing_treatment`) y un step
   `StandardTargetPropagationSpecialTreatmentStep` propio que produzca
   `target_cta`/`target_numcliente` (o el nivel que aplique). Usar
   `TARGET_AGGREGATIONS = {columna: funcion}` para propagar varias columnas
   a la vez (target + scores).
2. En `config/features/ceps/target_propagation_features.py`: registrar el
   target en `TARGETS` con sus `modes` — cada modo define `input_key`
   (clave de `input`/`output` del step de tratamiento), `suffix` (nombre de
   la columna `target_<suffix>`), `aggregation_function` y
   `missing_treatment`. Para propagar varias columnas usar `"columns":
   {columna: {"aggregation_function": f, "missing_treatment": [...]}}`;
   `TARGET_SELECTION_FUNCTION` (defecto `greatest`) decide el target final
   cuando hay varios modos. Las columnas extra producen
   `contagion_<col>_<weight>` automáticamente.
3. Crear los substeps de join/propagación en
   `pipelines/features/ceps/target_propagation_features.py` siguiendo el
   patrón `LovelaceCeps{JoinGraph,TargetPropagation}SubStep` y añadir las
   claves `nodes_join_target_<nuevo>`/paths correspondientes en `output`.
4. En `config/features/ceps/vector_assembler.py`: añadir la fuente en
   `FEATURE_SOURCES` (modo `"partitioned"`).

### Nuevos parámetros de propagación

Editar `config/features/ceps/target_propagation_features.py`:
`WEIGHT_TYPES` (pesos a propagar), `PROPAGATION_MAX_ITER`,
`PROPAGATION_ALPHA`, `PROPAGATION_KEEP_SEED_FLOOR`,
`PROPAGATION_FEATURES`. Cada combinación genera su propio subdir de salida.

### Parámetros del assembler

`config/features/ceps/vector_assembler.py`:

- `AGGREGATION_LEVELS` — niveles de agregación (`["numcliente", "cta"]`); cada
  uno produce `{nivel}_features` + `{nivel}_features_vector`.
- `NODE_ID_ARRAY_COLUMNS` — array de `nodes` explotado por nivel.
- `PIVOT_BY_LEVEL` — pivote externo por nivel (`input_key` + `column`).
- `AGGREGATION_WEIGHT` — Column de peso (defecto `oper_mto/(tfrom_days+1)`).
- `FEATURE_AGGREGATION` — string, lista (`{func}_{feat}` por función) o dict
  `{"default": ..., "<col>": ...}`; funciones: mean/median/std/min/max/sum/
  first/distinct_count/weighted_mean.
- `VARIABLE_SUFFIX` — sufijo de todas las variables (`"_ceps"`).
- `FILL_NULLS_VALUE` — centinela de nulos del vector (`-99999`).
- `HANDLE_INVALID`, `VECTOR_OUTPUT_COLUMN`, `EXCLUDE_COLUMNS`,
  `EXCLUDE_PREFIXES` (`target_*` queda fuera del vector).
- `FEATURE_SOURCES` — flags por fuente: `"enabled": False` desactiva;
  `"optional": True` (default) convierte errores de carga en warning.

### Desactivar una fuente de features

```python
FEATURE_SOURCES["cluster_stats"] = {
    "mode": "simple", "path": ..., "enabled": False}   # no se lee
# o dejarla opcional (default): si falta el parquet solo hay warning.
```

### Historia de txns vs. catálogos de reemplazo

`CEPS_RANKING_HISTORY_IN_MONTHS` (24, en `config/ceps/rfc_nom_ranking.py`)
controla cuánta historia CEP alimenta los catálogos;
`TXN_REPLACED_HISTORY_IN_MONTHS` (3, en `config/ceps/txn_replacement.py`)
cuánta historia transaccional conserva `rfc_curp_analysis_s264_ceps_replaced`.
Son independientes.

## 7. Tests

```bash
python -m pytest tests/                 # toda la suite
python -m pytest tests/ -m spark        # solo tests con Spark local
python -m pytest tests/ -m graphframes  # solo GraphFrames (requiere jar)
python -m pytest tests/test_weights.py -v
```

Los tests marcados `spark`/`graphframes` se saltan solos si falta la
dependencia. `conftest.py` provee la `SparkSession` local[2] y el checkpoint
dir.

## 8. Troubleshooting

| Síntoma | Causa típica |
|---|---|
| `OSError: Faltan variables de entorno` | Exportar las env vars de §1 |
| `HADOOP_CONF_DIR ... no funcionaran` (warning) | `HivePath.*` fallará: exportar `HADOOP_CONF_DIR` |
| `Not enough history in <key> data` | `minimum_required_history` > meses reales de la tabla |
| `Warning: <key> data has less history` | La tabla tiene menos meses que `history` (no fatal) |
| Feature no se ejecuta | Falta en `GRAPH_CENTRALITY_FEATURES`, en `output`, o el método `{name}_ft` |
| `GraphFrames` falla al instanciar | Falta `--jars`/env `GRAPHFRAMES_JAR` o el zip del venv en executors |
| Columnas `contagion_*` ausentes en el vector | Revisar `WEIGHT_TYPES` y que el subdir de `target_propagation/` exista para ese vintage |
