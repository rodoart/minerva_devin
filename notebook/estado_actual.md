# Minerva — Estado actual del proyecto

Documento generado a partir de la transcripción de las 152 capturas de pantalla
(archivadas en `notebook/`). Describe lo que el código hace
**hoy**, no lo que el diseño conceptual aspira a hacer.

## Qué es Minerva

Pipeline DataPull en PySpark + GraphFrames para un banco. Construye grafos de
transferencias (SPEI/CEPS) donde los nodos son identidades y las aristas son
transacciones src→dst. Hay **dos variantes de grafo coexistiendo**
(`config/graph_making/ceps/variants.py`): `rfc` (nodo = RFC/CURP imputado con
fallback a numcliente/cta/nom; rutas históricas) y `cta` (nodo = cuenta, con
imputación de `numcliente` vía la tabla `mapeo_cta_numcliente`; rutas
namespaced `*/ceps/cta/*`). Sobre cada grafo calcula features (con y sin
pesos, de riesgo, de par y temporales) y propaga un target de fraude
(Lovelace) para generar features de contagio a nivel `numcliente` y `cta`.

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

- `job.py` — parámetros del job: fechas (`MINERVA_TODAY`), `MAIN_LAG_IN_MONTHS=1`,
  `COHORT="SBX"`, `vintage` (yyyymm del mes objetivo) y `sbx` con el root HDFS:
  `/data/gcprcmsbx/work/hive/gcprcmsbx_work/rg49392/minerva/ceps_history/{vintage}`.
- Un módulo de config por step define los dicts `input`/`output` (tabla Hive o
  ruta parquet, columnas, claves de join, columnas de pesos, etc.).

## Flujo del pipeline (según `main.py` / `run_order.py`)

`main.py` es el entry point (driver de producción); `run_order.py` es el
driver de desarrollo. Ambos crean la sesión Spark con variables de entorno
`MINERVA_*` / `VENV_ZIP_*` / `MINERVA_GRAPHFRAMES_JAR_HDFS` y encadenan los
steps con `previous_step`. `main.py` construye **una cadena de 12 steps por
cada variante** de `ENABLED_VARIANTS` y devuelve los steps terminales
(comparten `rfc_nom_ranking`, `txn_replacement` y el special treatment del
target, que se construyen una sola vez):

1. **`CepsRfcNomRankingStep`** (`pipelines/ceps/rfc_nom_ranking.py`)
   Extracción/tratamiento especial de CEPS: limpieza de nombres
   (`banamex_nom_reorder`, `limpiar_acentos`, `normalizar_espacios`), flags de
   validación RFC/CURP (`add_id_validation_flags`), catálogo opcional Banxico
   (`bxico_rfc_curp_cat`) y ranking para elegir el RFC correcto por
   cuenta/nombre. **El catálogo se construye all-time e incremental**:
   `s264_ceps_flattened_monthly` y `s264_ceps_flattened_groupby_monthly`
   guardan parciales combinables por mes (`CEPS_ALL_TIME_HISTORY_MONTHS`) y
   cada corrida solo computa los meses ausentes. Con
   `PREFER_OLDEST_VALID_TAG=True`, a igualdad de validez gana el RFC/CURP
   observado desde hace más tiempo (`fst_fec_informacion_hora_oper`).
   Produce `s264_ceps*` aplanado y rankeado.
2. **`CepsTxnReplacementStep`** (`pipelines/ceps/txn_replacement.py`)
   Reemplaza los identificadores de cada txn por el RFC ganador del ranking.
   La tabla resultante solo conserva `TXN_REPLACED_HISTORY_IN_MONTHS` (3)
   meses de txns (`limit_txn_history_window` por `fec_informacion`).
   También produce **`mapeo_cta_numcliente`** (una fila por cuenta →
   numcliente elegido por frecuencia/recencia), reemplazable por otra tabla
   apuntando la clave homónima del input de la variante `cta`.
3. **`CepsSpecialTreatment`** (`pipelines/graph_making/ceps/special_treatment.py`)
   Aplanamiento src/dst, `tfrom_months`/`tfrom_days`, missing treatment; en la
   variante `cta` resuelve los nodos por cuenta e imputa el numcliente.
4. **`CepsGroupByStep`** (`pipelines/graph_making/ceps/group_by.py`)
   Agregados por id (src) y por txn (src-dst): montos, conteos y arrays de
   identificación (numcliente, nom, cta, id_ban, mis_date). **Incremental
   mensual**: `group_by_{txn,id}_monthly` guarda parciales combinables por
   `month_partition` (`ensure_monthly_partitions` solo computa los meses
   ausentes) y el agregado completo se obtiene con `merge_monthly_group_by`.
5. **`CepsEdgesAndNodesStep`** (`pipelines/graph_making/ceps/edges_and_nodes.py`)
   Construye `edges` (datos txn + array `weights` de tipos de peso) y `nodes`.
6. **`CepsGraphFeaturesStep`** (`pipelines/features/ceps/graph_features.py`)
   Features GraphFrames con y sin pesos: PageRank, degrees, edge stats,
   componentes, triangle count, `degree_balance`, `reciprocity`,
   `self_loops`, `weighted_degree_balance`.
7. **`LovelaceTargetPropagationSpecialTreatmentStep`**
   (`pipelines/target_propagation/lovelace/special_treatment.py`)
   Special/missing treatment del target Lovelace; agrega TODAS las columnas
   de `TARGET_AGGREGATIONS` (target + scores) a `target_cta`/`target_numcliente`.
   Compartido entre variantes (es externo al grafo).
8. **`CepsTargetPropagationFeaturesStep`**
   (`pipelines/features/ceps/target_propagation_features.py`)
   Join del target a los nodos (`nodes_join_target`, multi-columna) y
   propagación sobre el grafo (normalización + contagio:
   `contagion_<weight>` y `contagion_<col>_<weight>`). Además deriva
   **targets sintéticos** (`SYNTHETIC_TARGETS`: reglas
   top_quantile/above_threshold/top_n sobre métricas de nodo, p.ej. top-1%
   pagerank) y los propaga como semillas (`contagion_synthetic_<name>_<weight>`).
9. **`CepsRiskFeaturesStep`** (`pipelines/features/ceps/risk_features.py`)
   Features de riesgo por nodo (`risk_*_<semilla>`: fracción de vecinos
   semilla, semillas a 2 saltos, ratio de flujo, distancia a la semilla
   to/from, Personalized PageRank) con semillas duras (target) y blandas
   (scores de contagio); y features de par (A, X) (`pair_*`: vecinos comunes,
   Jaccard, Adamic-Adar, preferential attachment, reciprocidad, caminos de
   longitud 2, distancia mínima) agregadas a nivel nodo X.
10. **`CepsTemporalFeaturesStep`** (`pipelines/features/ceps/temporal_features.py`)
    Features temporales (`tmp_*`) desde `missing_treatment`: antigüedad y
    recencia, conteos por ventana 1/7/30 días y velocidades, % nocturno/fin de
    semana, burstiness, receptores únicos 7d, pass-through 24h, % bajo umbral
    de reporte; y de par: z-score de monto y razones a la mediana.
11. **`CepsClusterFeaturesStep`** (`pipelines/features/ceps/cluster_features.py`)
    Stats intra-grupo por nodo (`component_id` + sub-partición `grp_<m>` con
    `SUBCLUSTER_METHOD`: `scc` dirigido, `label_propagation` o `cc` —
    connectedComponents no dirigido, el modo ligero) sobre todas las columnas
    numéricas — incluidas target, contagio, riesgo, temporales y pesos
    (`REQUIRED_STATS_PREFIXES` lo vigila).
12. **`CepsVectorAssemblerStep`** (`pipelines/features/ceps/vector_assembler.py`)
    Ensamblado multi-nivel (`numcliente`, `cta`): `{nivel}_features` (llave +
    variables `{func}_{feat}_ceps`) y `{nivel}_features_vector` (SOLO llave +
    vector `features`). Multi-transformación: `AGGREGATION_SPECS` (lista de
    agregaciones con peso propio, todas guardadas) y `NODE_TRANSFORMS`
    (expresiones a nivel nodo pre-agregación). Fuentes opcionales
    (`optional`/`enabled`), sufijo `_ceps`, fill `-99999`.

## Estado actual / brechas

**Funciona:** los 10 steps anteriores con sus configs. El grafo se construye,
el target (y columnas adicionales como scores) se une a los nodos, se propaga
por el grafo, se calculan stats intra-grupo y el vector final se ensambla a
nivel `numcliente` y `cta`.

**Resuelto desde la transcripción original:**
- `propagate_target` implementado (difusión iterativa con `AggregateMessages`,
  `edges_norm` cacheado por `weight_type`).
- `dynamic/functions.py`: `save_table` → `save_to_parquet`.
- Clave duplicada `s264_ceps_flattened_ranks` deduplicada.
- Features de agrupación implementadas (`cluster_features` step).
- VectorAssembler final implementado (multi-nivel, multi-agregación, sufijo,
  centinela de nulos, fuentes opcionales).
- Historia de reemplazo desacoplada de la historia del grafo
  (`CEPS_RANKING_HISTORY_IN_MONTHS` vs `TXN_REPLACED_HISTORY_IN_MONTHS`).

**Queda como nota histórica (verbatim del material fuente):**
- `graph_features.py` reasigna `WEIGHTED_EDGE_STARTS_FEATURES_AGGREGATIONS`
  a lista vacía (intencional: sin lista explícita se usa todo el catálogo
  `STANDARD_WEIGHT_STATS`).

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
- Variables de entorno: `MINERVA_WORKSPACE_DIR_LINUX`, `PYSPARK_QUEUE`,
  `PYSPARK_PORT`, `MINERVA_NAME`, `MINERVA_TODAY`, `MINERVA_VENV_TAR_GZ_LINUX`,
  `MINERVA_VENV_TAR_GZ_HDFS`, `GRAPHFRAMES_JAR`.
- `pipelines/__init__.py` añade el root y `libs/` a `sys.path`.
