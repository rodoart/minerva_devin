# Minerva — Arquitectura (post-simplificación)

Estado del código tras la refactorización. Sin cambios de comportamiento: las
claves de tablas, paths HDFS y la API pública (`Step`, decoradores, helpers) se
conservan.

## Estructura

```
minerva_devin/
├── main.py                   # entry point: ejecuta el flujo completo con logging
├── run_order.py              # driver de desarrollo (crea SparkSession + encadena steps)
├── environment.yaml          # env conda (spark 3.3.2, py3.10, graphframes vía pip)
├── jars/graphframes-0.81-spark3.0-s_2.12.jar
├── config/                   # dicts input/output por step (única fuente de rutas)
│   ├── __init__.py           # sys.path root + validación de MINERVA_TODAY/pyspark
│   ├── job.py                # fechas, vintage, cohort=SBX, root HDFS
│   ├── ceps/                 # rfc_nom_ranking, txn_replacement
│   ├── graph_making/ceps/    # special_treatment, group_by (+ mensual), edges_and_nodes
│   ├── features/ceps/        # graph_features, target_propagation_features,
│   │                         # cluster_features, vector_assembler
│   └── target_propagation/lovelace/special_treatment.py
├── libs/
│   ├── framework/            # Step, cached_property, decoradores dynamic_*,
│   │                         # standard_load_parquet_or_table, helpers de substep,
│   │                         # ensure_monthly_partitions (incremental mensual)
│   ├── functions/            # librerías de funciones importables:
│   │   ├── missing_treatment.py  # fill_* + MISSING_TREATMENT_FUNCTION_RELATIONS
│   │   │                         # + apply_missing_treatment (dispatcher)
│   │   ├── features.py           # features de grafo + propagación de target
│   │   │                         # + STANDARD_WEIGHT_STATS + GRAPH_FEATURE_FUNCTIONS
│   │   ├── assembly.py           # agregación multi-nivel + VectorAssembler
│   │   ├── aggregations.py       # parciales mensuales combinables + merge
│   │   └── weights.py            # column/composed/ratio weight, build_weights_map
│   └── data_engineering_toolbox/
│       ├── context/          # notebook(), get_remote() — SparkSession de clúster
│       ├── path/             # HivePath (+ hdfs.py comandos, linux.py save_to_csv)
│       ├── general/date_treatment/
│       └── pyspark/
│           ├── tools/        # get_column_alias, is_table_or_parquet, ColumnRenamer,
│           │                 # partition_lags.py, parquet_treatment.py
│           ├── history.py    # HistoryColumnDataFrame
│           ├── compare.py    # DifferenceDataFrame
│           ├── counts.py, analysis.py
│           ├── dynamic/      # escritura dinámica parquet/csv + load_csvs_to_hive
│           └── testing/      # helpers de pytest (antes pyspark/pytest)
└── pipelines/                # Standard* genéricos + subclases Ceps*
    ├── ceps/                 # rfc_nom_ranking, txn_replacement (extracción CEPS)
    ├── graph_making/         # special_treatment, group_by, edges_and_nodes
    │   └── ceps/             # wrappers delgados sobre los genéricos
    ├── features/             # graph_features, vector_assembler (genéricos)
    │   └── ceps/             # graph_features, target_propagation_features,
    │                         # cluster_features, vector_assembler
    └── target_propagation/   # special_treatment (genérico)
        └── lovelace/         # special_treatment (concreto)
```

## Convenciones

- **Imports:** todo lo interno usa prefijo `libs.` / `config.` / `pipelines.`
  (absolutos desde el root). `config/__init__.py` y `pipelines/__init__.py`
  insertan el root en `sys.path`; ya no se añade `libs/` al path.
- **Patrón step:** `pipelines/<etapa>/<modulo>.py` contiene clases
  `Standard*` reutilizables; `pipelines/<etapa>/<fuente>/` (p.ej. `ceps/`)
  contiene subclases delgadas que conectan el config de esa fuente.
- **Persistencia:** `@ppf.dynamic_partitioned_table_or_parquet(path_key=...)` y
  `@ppf.dynamic_unpartitioned_parquet(path_key=...)` leen el parquet/tabla si
  existe (`is_dynamic`) y recomputan+sobreescriben la partición si no — es la
  reanudabilidad del pipeline.
- **`reload()`:** eliminado de los módulos de librería; solo vive en
  `run_order.py` (driver interactivo).

## Cambios hechos en la simplificación

- `libs/framework`: los 3 decoradores `dynamic_*` comparten ahora
  `_reload_or_recompute_parquet` y `_partition_load_kwargs` /
  `_compute_and_overwrite_partition`; los `try/except` de config opcional
  pasaron a `dict.get()`; quitados imports muertos (`pandas.core.missing`).
- `tools/utils.py` + `tools/column_renamer.py` fusionados en `tools/__init__.py`.
- `pyspark/pytest/` renombrado a `pyspark/testing/` (evita shadow del paquete pytest).
- `libs/__init__.py` creado → `libs` es paquete regular.
- ~120 imports muertos eliminados (`tabnanny`, `calendar.c`, `reload` loops,
  aliases `dtb_pt_*` sin uso, listas gigantes de `pyspark.sql.functions`).
- Docstrings plegados/ilegibles permanecen como stubs `""" ... """`.

## Bugs corregidos

| Problema | Fix |
|---|---|
| `tools/partitions_lags` (con 's') importado en `parquet_treatment.py` — el módulo es `partition_lags` | renombrado el import |
| `save_table(...)` inexistente en `dynamic/functions.py` | `save_to_parquet` |
| `standard_load_parquet_or_table` (graph_making/special_treatment) no pasaba `config_dict` (arg requerido) | añadido |
| `path/__init__.py` anotaciones `-> Self`, `-> Generator[...]`, `-> HivePath` con nombres sin importar (rompían el import) | `from __future__ import annotations` |
| Clave `"s264_ceps_flattened_ranks"` duplicada en config output | deduplicada (entradas idénticas) |
| `minimum_requiered_history` (typo) — el framework lee `minimum_required_history` | estandarizado en los 2 configs |
| `rfc_nom_raking.py` vs imports `rfc_nom_ranking` | renombrado a `rfc_nom_ranking.py` |
| `sys.path` hack doble + `print`s en cada `import config`/`pipelines` | solo root en path, sin prints |
| `3` suelto tras un `return` en rfc_nom_ranking | eliminado |

## Gaps conocidos (del material fuente, no del refactor)

- `StandardTargetPropagationExtractSubStep` (target_propagation/special_treatment.py:51) —
  la clase estaba en una región plegada del IDE; el código llama a un método
  que no existe → NameError si se ejecuta ese path.
- `CepsHistoryStep` — anotación a un padre que no existe en el repo (string,
  inofensiva).
- `s264_ceps_flattened_rank_rfc_by_cta_cases_replace` (rfc_nom_ranking:546)
  asignada sin uso — se conserva porque evaluar la property materializa la tabla.
- Regex con escapes inválidos en `path/hdfs.py` (SyntaxWarning, verbatim del original).

## Propagation features (implementado)

`pipelines/features/ceps/target_propagation_features.py` quedó completo:

- **`CepsTargetPropagationFeaturesStep`** — el step que `run_order.py`
  instancia. `step_action` ejecuta el join del target y luego la propagación.
- **`LovelaceCepsJoinGraphSubStep`** — substep `lovelace_ceps_join_graph_step`
  con la property `nodes_join_target` (particionada, key
  `nodes_join_target_lovelace`): por cada modo de `TARGETS["lovelace"]["modes"]`
  (`cta`, `numcliente`) agrega el target sobre `group_by_id`
  (`standard_target_group_by_id`), aplica el `missing_treatment`
  (`mean_with_nulls` → fillna con la media) y lo une a `nodes`
  (`standard_join_target`). Las columnas `target_lovelace_{modo}` se combinan
  en `target_lovelace` con `TARGET_SELECTION_FUNCTION` (`greatest`).
- **Multi-columna**: cada modo admite `"columns"` = `{columna: {"aggregation_function",
  "missing_treatment"}}` — propaga target Y scores en una sola pasada
  (`target_lovelace_<modo>_<columna>` por nodo, combinadas en
  `target_lovelace_<columna>`; "target" produce `target_lovelace`).
  `lff.target_group_by_id` acepta `function` como dict `{columna: callable}`.
- **`LovelaceCepsTargetPropagationSubStep`** — ejecuta `PROPAGATION_FEATURES`:
  una feature `contagion_{weight_type}` por cada tipo de peso definido en
  `config/graph_making/ceps/edges_and_nodes.py::WEIGHT_COLUMNS`
  (`weighted_mean_oper_mto`, `weighted_sum_oper_mto`, `weighted_count_txn`,
  `mean_oper_mto`, `count_txn`, `composed`) — y `contagion_<col>_<weight>`
  para cada columna extra propagada (scores). Cada una llama a
  `propagate_target` → normaliza pesos (`get_edges_norm` →
  `standard_weight_normalization`, cada dirección dividida por el grado del
  receptor) y difunde el score por `AggregateMessages` con amortiguación
  `alpha=0.15`, `max_iter=3` y suelo en la semilla (`keep_seed_floor`),
  cacheando cada variante bajo `output["target_propagation"]/<parametros>`.
- **`propagate_target`** (antes WIP con `kwargs`/`parent_hdfs`/`property_name`
  indefinidos) ahora sigue el patrón decorado de `get_edges_norm`.
- Parámetros en `config/features/ceps/target_propagation_features.py`:
  `WEIGHT_TYPES`, `PROPAGATION_MAX_ITER`, `PROPAGATION_ALPHA`,
  `PROPAGATION_KEEP_SEED_FLOOR`, `PROPAGATION_FEATURES`,
  `propagation_node_columns()`.
- En el special_treatment del target (`config/target_propagation/lovelace/
  special_treatment.py`) `TARGET_AGGREGATIONS = {columna: función}` decide
  qué columnas viajan juntas en `target_cta`/`target_numcliente`.

## Librerías de funciones (`libs/functions/`)

Tres módulos importables que centralizan la lógica que estaba embebida en los
pipelines; los `standard_*` / `*_ft` de los steps son ahora delegates delgados:

- **`missing_treatment`** — `fill_missings_with_value`,
  `fill_missings_with_mean`, `fill_missings_with_mean_without_ignoring_null_counts`,
  registro `MISSING_TREATMENT_FUNCTION_RELATIONS` (`"value"`, `"mean"`,
  `"mean_with_nulls"`) y el dispatcher `apply_missing_treatment(df, {col: tratamiento})`.
  Usado por `standard_missing_treatment` (graph_making) y por el join de target
  en propagation features.
- **`features`** — funciones puras `(GraphFrame[, ...]) -> DataFrame`:
  `pagerank`, `degrees`, `components`, `triangle_count`,
  `degree_balance` (net_degree, in_out_degree_ratio),
  `reciprocity` (reciprocal/reciprocity in/out y global, sin self-loops),
  `self_loops`, `weighted_*` (pagerank, degrees, edge_stats, degree_balance,
  components, triangle_count), `get_degree`, `weight_normalization`,
  `propagate_target` (difusión de contagio), `target_group_by_id`
  (callable o dict multi-columna), `join_target`, `cluster_group_stats`.
  Incluye `STANDARD_WEIGHT_STATS`
  (min/max/mean/std/count/countDistinct/sum/curt/skew/so) y el registro
  `GRAPH_FEATURE_FUNCTIONS` (nombre de feature → función).
- **`weights`** — `column_weight`, `composed_weight`, `ratio_weight`,
  `build_weights_map` (mapa `weights` de las aristas, usado por
  `standard_edges`), `get_weight` (extrae un peso del mapa, usado por
  `get_graph`). `WEIGHT_COLUMNS` del config se construye con estas funciones.
- **`assembly`** — `weighted_mean`, `ASSEMBLY_AGGREGATION_FUNCTIONS`
  (mean/median/std/min/max/sum/first/distinct_count/weighted_mean),
  `build_aggregation_expressions` (string, lista — columnas
  `{funcion}_{feature}` — o dict con `"default"` + overrides por columna),
  `explode_array_column`, `get_feature_columns`, `assemble_vector`
  (wrapper de `ml.VectorAssembler`).
- **`aggregations`** — `resolve_group_by_expressions`,
  `standard_group_by_features`, `recency_auxiliary_columns`,
  `monthly_partial_group_by_aggregations` (parciales combinables por mes:
  min/max/sum/count, momentos Σx²..Σx⁴, auxiliares ponderados y
  `t_rel_days` = antigüedad relativa al fin de su mes) y
  `merge_monthly_group_by` (combina las parciales mensuales en el agregado
  de toda la ventana; `countDistinct`/`first`/`last` no combinables →
  `ValueError`).

## VectorAssembler multi-nivel (numcliente + cta)

`pipelines/features/{,ceps/}vector_assembler.py` + `config/features/ceps/vector_assembler.py`:

- **`CepsVectorAssemblerStep`** / **`LovelaceCepsAssemblerSubStep`** — última
  etapa del pipeline (añadida a `run_order.py`).
- **Niveles** — `AGGREGATION_LEVELS = ["numcliente", "cta"]`; cada nivel tiene
  su columna array en `nodes` (`NODE_ID_ARRAY_COLUMNS`) y su pivote externo
  (`PIVOT_BY_LEVEL`: `pivot` para numcliente, `pivot_cta` para cta). Misma
  lógica por nivel; añadir otro nivel = entrada en esas tres estructuras.
- **`feature_tables`** — recopila las tablas de features de nodo según
  `FEATURE_SOURCES`: `"simple"` (parquet plano: pagerank, degrees, components,
  degree_balance, reciprocity, self_loops, weighted_*), `"partitioned"`
  (mis_date/process_date con lag/history: nodes, nodes_join_target_lovelace) y
  `"merge_schema"` (lee `target_propagation/` padre uniendo las columnas
  `contagion_*` de cada subdir de parámetros, colapsando a una fila por `id`).
  Flags por fuente: `"enabled": False` la desactiva; `"optional": True`
  (default) convierte errores de carga en warning y sigue sin ella — para
  desactivar fuentes de features sin romper el flujo. Dedupe de columnas: la
  primera fuente que aporta una columna gana.
- **`nodes_with_features`** — nodos (id + arrays de nivel + pesos) + todas las
  features por `id` + `tfrom_days` (via `calculate_daily_tfrom` con el
  vintage).
- **`{nivel}_features`** (particionada) — explode del array del nivel y
  agrega cada feature con `FEATURE_AGGREGATION` (acepta string, lista — una
  columna `{funcion}_{feature}` por función — o dict `"default"`/overrides)
  usando `AGGREGATION_WEIGHT` como peso (`oper_mto/(tfrom_days+1)`,
  parametrizable) + `node_count` (nodos distintos por llave). Todas las
  variables llevan `VARIABLE_SUFFIX` (`_ceps`) menos la llave. Las columnas
  `target_*` también se agregan → quedan disponibles como etiqueta.
- **`{nivel}_features_vector`** (particionada, salida final) — pivote externo
  del nivel + left join a `{nivel}_features` + fill de nulos
  (`FILL_NULLS_VALUE = -99999`) + `VectorAssembler` → **SOLO llave + columna
  `features`** (las variables sueltas se descartan con `select`); las
  etiquetas `target_*` no entran al vector (`EXCLUDE_PREFIXES`).
- Config: `GRAPH_CENTRALITY_FEATURES` añadido en
  `config/features/ceps/graph_features.py` (se referenciaba pero no existía).

## Historias independientes (ranking vs. txn_replaced)

- `CEPS_RANKING_HISTORY_IN_MONTHS = 24` (`config/ceps/rfc_nom_ranking.py`) —
  meses de historia CEP leídos para construir los catálogos de reemplazo.
- `TXN_REPLACED_HISTORY_IN_MONTHS = 3` (`config/ceps/txn_replacement.py`) —
  meses de transacciones que conserva la tabla `replaced`; el input
  `rfc_curp_analysis_s264_ceps` lee solo la partición del vintage (cada una
  ya contiene la ventana completa del ranking) y
  `limit_txn_history_window` recorta por `fec_informacion` con la misma
  aritmética que el loader (`make_date_interval_with_lag_months`).

## Group-by incremental mensual

`group_by` ahora calcula en dos fases (ver `notebook/proyecto.md` §detalle):

- `group_by_txn_monthly` / `group_by_id_monthly` — agregados parciales por
  (grupo, `month_partition`); `ensure_monthly_partitions` computa solo los
  meses que falten dentro de la ventana de 12 meses.
- `group_by_txn` / `group_by_id` — `merge_monthly_group_by` combina las
  parciales en el agregado completo (equivalente al group-by entero; verificado
  en tests).

## Cluster features (stats intra-grupo)

`pipelines/features/ceps/cluster_features.py` +
`config/features/ceps/cluster_features.py`: step intermedio entre
propagación y assembler (`CepsClusterFeaturesStep`). Enriquece los nodos con
todas las features de grafo + contagio (`GRAPH_FEATURE_SOURCES`, merge_schema)
y calcula, por cada columna de grupo (`GROUP_COLUMNS`: `component_id`, `scc`
vía `stronglyConnectedComponents`), stats de TODAS las columnas numéricas del
nodo (`CLUSTER_STATS`: count/sum/mean/median/std/min/max).

`REQUIRED_STATS_PREFIXES` (`target_`, `contagion_`, `oper_mto`, `weight`,
`_strength`) es una guarda: si ninguna columna agregable coincide con un
prefijo, se registra warning — así los stats de grupo siempre incluyen la
target, la target propagada y los pesos.

## Entry point (`main.py`)

`python main.py [--level INFO|DEBUG|...] [--log-file run.log] [--build-only]`

- Configura `logging` (stdout + fichero opcional), valida las env vars
  requeridas, crea la SparkSession con `context.notebook` (mismos parámetros
  que `run_order`) y construye la cadena de 10 steps:
  `rfc_nom_ranking → txn_replacement → special_treatment → group_by →
  edges_and_nodes → graph_features → target_propagation →
  target_propagation_features → cluster_features → vector_assembler`.
- `Step.execute()` ejecuta recursivamente los `previous_step`, así que
  `vector_assembler_step.execute()` lanza todo el flujo; cada etapa recarga su
  parquet/partición si existe (reanudable).
- Logging: el framework (`libs.framework`) y los pipelines usan
  `logging.getLogger(__name__)` — ejecución de steps con duración, cargas
  reload/recompute de los decoradores y overwrite de particiones salen por el
  logger (INFO/WARNING/ERROR); `main` captura excepciones con traceback.
- `--build-only` construye la cadena sin ejecutar (útil para smoke test de
  imports/config).

## Tests (`tests/` + `pytest.ini`)

Suite de pytest (~200 tests). Ejecución: `python -m pytest tests/`.

- **`conftest.py`** — fixtures: `spark` (SparkSession local[2], salta si no hay
  pyspark), `graphframes` (salta si el jar no carga), `checkpoint_dir`,
  `df_factory`, `local_hdfs` (shim de `libs.data_engineering_toolbox.path` al
  filesystem local para tests de particionado). Markers: `spark`, `graphframes`.
- **Puros** (corren en cualquier entorno): `test_sanitize_property_name` (utils.py
  cargado aislado vía importlib, evita importar el paquete con deps Spark),
  `test_hive_path` (path ops + `_ls`/`exists`/`is_dir` con monkeypatch),
  `test_date_treatment`, `test_main` (argparse, env vars, logging, flujo con
  mocks de spark/pipeline).
- **Marcados `spark`** (requieren pyspark): `test_framework` (Step, cached_property,
  decoradores con IO real en tmp_path, execute chain, substep helpers),
  `test_missing_treatment`, `test_weights`, `test_assembly` (multi-agregación
  mean/median/std, sufijos `{func}_{col}`), `test_tools`, `test_counts`,
  `test_partition_lags` (helpers a nivel DataFrame/path; `SparkTwoPartition*`
  no testeable sin HDFS), `test_compare`, `test_testing_quality`,
  `test_features` (parte DataFrame + `target_group_by_id` multi-columna),
  `test_monthly_group_by` (parciales mensuales, merge vs. completo,
  `ensure_monthly_partitions` incremental con `local_hdfs`),
  `test_txn_history_window` (ventana de historia transaccional + config
  independiente), `test_vector_assembler_options` (sufijo `_ceps`, niveles,
  fuentes opcionales/desactivadas, dedup), `test_cluster_features`
  (`cluster_group_stats`, `REQUIRED_STATS_PREFIXES`, helpers de config de
  propagación multi-columna).
- **Marcados `graphframes`**: pagerank, degrees, components, triangle_count,
  degree_balance, reciprocity, self_loops, weighted_* (incl.
  weighted_degree_balance), get_degree, weight_normalization,
  propagate_target, subcluster (scc/label_propagation).

### Bugs encontrados por la suite (ya corregidos)

- `HivePath.__init__` no llamaba a `super().__init__` → roto en Python 3.13
  (pathlib movió el parsing de `__new__` a `__init__`).
- `HivePath._iter_terminal_dirs` comparaba paths contra la lista de objetos
  `_PathParents` (nunca coincidía) → devolvía todos los dirs; ahora aplana
  `hive.parents` a un set de ancestros.
- `get_cached_decorated_table_or_parquet_property`: `len(config_dict_key)`
  medía el nombre de la clave, no el dict (los dicts simples tienen 2 claves);
  y elegía `dynamic_unpartitioned_parquet_with_path` (espera path) cuando el
  decorator_key es una clave → ahora `dynamic_unpartitioned_parquet`.
- `CepsGraphFeaturesStep.step_action` llamaba a `ceps_edges_and_nodes_step`
  (inexistente) → corre los substeps un/weighted.
- `libs/data_engineering_toolbox/__init__.py`: el sys.path hack PYLIB + assert
  HADOOP_CONF_DIR rompía cualquier import fuera del cluster → ahora
  condicional + warning.
- `sanitize_property_name` no colapsa `-` (solo `_`): documentado en test.

## Pendiente según el concept design

- Nada: las features de agrupación (`cluster_features`) ya están implementadas.

## Documentación del proyecto (`notebook/`)

- `concept_desing.md` — diseño conceptual original (de las capturas).
- `uso.md` — guía de uso: ejecución, config, extensión, troubleshooting.
- `features.md` — diccionario de features: definición y nombre final de cada
  columna por nivel (`id` nodo / `numcliente` / `cta` / vector).
- `estado_actual.md` — qué hace el proyecto.
- `proyecto.md` — detalle de implementación: framework, cadena de steps,
  group-by mensual incremental y mejoras de la 2ª iteración.
- `preguntas_grafo.md` — inventario exhaustivo de preguntas de grafo →
  features que las responden.
- `ceps.md` — ranking RFC/nom y reemplazo de CEPs.
- `arquitectura.md` — este documento.
- `screenshots/` — capturas fuente del código original.

## Estado

- `environment.yaml` replica las versiones del concept (`spark=3.3.2.3`,
  `protobuf=4.23.4`, pandas<2, pyarrow<11); se añadió `inflection` y
  `python-dateutil` que el código importa.
- `main.py` ejecuta el flujo completo con logging (`Step.execute` encadena
  los `previous_step`).
- Suite pytest en `tests/` (~200 tests, markers `spark`/`graphframes`).
