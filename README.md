# minerva_devin

Pipeline DataPull en PySpark + GraphFrames para generar features de grafo a
partir de transferencias CEPS (SPEI) y propagar un target de fraude (Lovelace)
hasta nivel `numcliente` **y `cta`**, listo para `pyspark.ml`
(`VectorAssembler`).

## Quickstart

```bash
conda env create -f environment.yaml && conda activate minerva_devin

# env vars requeridas (ver notebook/uso.md §1)
export MINERVA_TODAY=2025-08-31  # ... y el resto

python main.py                   # ejecuta el flujo completo (12 steps × variante)
python main.py --build-only      # smoke test de la cadena sin ejecutar
python -m pytest tests/          # tests unitarios
```

## Flujo (12 steps × variante de grafo)

```
rfc_nom_ranking ──► txn_replacement ──► special_treatment ──► group_by
  ──► edges_and_nodes ──► graph_features ──► target_propagation (lovelace)
  ──► target_propagation_features ──► risk_features ──► temporal_features
  ──► cluster_features ──► vector_assembler
```

La cadena se construye **una vez por variante de grafo**
(`config/graph_making/ceps/variants.py`): `rfc` (grafo RFC-RFC histórico,
rutas intactas) y `cta` (grafo cta-cta con imputación de `numcliente` vía la
tabla `mapeo_cta_numcliente`); ambas coexisten y comparten los steps previos
al grafo y el special treatment del target.

- **Catálogo de imputación all-time incremental**: el ranking RFC/nom se
  construye desde el principio de los tiempos con parciales mensuales
  combinables (`s264_ceps_flattened_monthly` / `..._groupby_monthly`,
  `CEPS_ALL_TIME_HISTORY_MONTHS`); cada corrida solo escribe el incremento
  del mes. Con `PREFER_OLDEST_VALID_TAG`, a igual validez gana el tag más
  antiguo.
- **Historia incremental mensual** en `group_by`: los agregados parciales se
  guardan por mes (`month_partition`) y solo se computan los meses que falten
  (`ensure_monthly_partitions`).
- **Propagación multi-columna + targets sintéticos**: target + scores viajan
  juntos por el grafo (`contagion_<col>_<weight>`); `SYNTHETIC_TARGETS` deriva
  semillas sintéticas de métricas de nodo (p.ej. top-1% pagerank) y las
  propaga igual (`contagion_synthetic_<name>_<weight>`).
- **Features de riesgo de nodo** (`risk_*_<semilla>`: vecinos semilla, 2
  saltos, ratio de flujo, distancia al fraude, Personalized PageRank) y
  **de par (A, X)** (`pair_*`: vecinos comunes, Jaccard, Adamic-Adar,
  preferential attachment, reciprocidad, caminos de longitud 2, distancia
  mínima), más **features temporales** (`tmp_*`: ráfagas, pass-through 24h,
  velocidad, bajo-umbral, ...).
- **Assembler multi-nivel y multi-transformación**: `numcliente_features_vector`
  y `cta_features_vector` (solo llave + vector), sufijo `_ceps`,
  `AGGREGATION_SPECS` (lista de agregaciones con peso propio) y
  `NODE_TRANSFORMS` (transformaciones a nivel nodo pre-agregación), centinela
  `-99999` y fuentes de features opcionales/desactivables.

## Estructura

- `main.py` — entry point con logging: valida env vars, crea la SparkSession
  remota y ejecuta la cadena completa (reanudable: cada etapa recarga su
  parquet/partición si ya existe).
- `run_order.py` — driver de desarrollo interactivo que encadena los steps.
- `config/` — dicts `input`/`output` por step (rutas Hive/HDFS, particiones,
  lags, history, schemas) + parámetros de features/propagación/assembler.
- `libs/framework` — framework de ETL reanudable (`Step`, decoradores
  `dynamic_*`, `cached_property`, `ensure_monthly_partitions`, helpers de
  substep).
- `libs/functions` — librerías importables: `missing_treatment`, `features`
  (grafo + propagación + targets sintéticos), `pair_features` (riesgo de nodo
  y proximidad de par), `temporal_features` (`tmp_*` desde transacciones),
  `weights`, `assembly`, `aggregations` (parciales mensuales combinables).
- `libs/data_engineering_toolbox` — toolbox: paths HDFS (`HivePath`), contexto
  Spark, IO de particiones (`SparkLoad*`/`SparkTwoPartition*`), history,
  compare/dynamic/testing.
- `pipelines/` — clases `Standard*` genéricas + subclases por fuente
  (`ceps/`, `lovelace/`).
- `tests/` — suite pytest (markers `spark`, `graphframes`; se saltan solos si
  falta la dependencia).
- `environment.yaml` — entorno conda (spark 3.3.2, python 3.10, graphframes).
- `notebook/` — documentación: `concept_desing.md` (diseño),
  `uso.md` (**guía de uso completa: ejecución, config, extensión,
  troubleshooting**), `features.md` (**diccionario de features: definiciones y
  nombres finales de columna**), `estado_actual.md` (qué hace el proyecto),
  `arquitectura.md` (estructura y decisiones de refactor), `proyecto.md`
  (detalle de implementación, group-by mensual incremental y mejoras),
  `preguntas_grafo.md` (inventario de preguntas de grafo → features),
  `ceps.md` (ranking RFC/nom y reemplazo de CEPs), `screenshots/`
  (capturas fuente del código).

Variables de entorno requeridas: `MINERVA_WORKSPACE_DIR_LINUX`, `PYSPARK_QUEUE`,
`PYSPARK_PORT`, `MINERVA_NAME`, `MINERVA_TODAY`, `MINERVA_VENV_TAR_GZ_LINUX`,
`MINERVA_VENV_TAR_GZ_HDFS`, `MINERVA_GRAPHFRAMES_JAR_HDFS` (+ `HADOOP_CONF_DIR`
para operaciones HDFS).
