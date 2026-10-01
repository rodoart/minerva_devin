# Minerva — Documentación detallada del proyecto

Pipeline PySpark que construye un grafo de transacciones **CEPS (SPEI)**,
calcula features de grafo por `numcliente` y propaga el target de fraude
**Lovelace** hasta alimentar un `VectorAssembler` de `pyspark.ml`.

La esencia de CEPS es **transaccional**: el grafo se nutre de los últimos
**12 meses** de transacciones (`GRAPH_TOTAL_HISTORY_IN_MONTHS = 12` en
`config/graph_making/__init__.py`). Cada corrida mensual (vintage) recalcula
la ventana completa.

---

## 1. Estructura del repositorio

```
minerva_devin/
├── main.py                  # Punto de entrada: cadena de 10 steps + ejecución
├── run_order.py             # Driver interactivo para desarrollo (encadena steps)
├── local_flow_check.py      # Smoke test local (shim HDFS -> filesystem local)
├── supervisor.py            # Orquestación/supervisión de corridas
├── environment.yml          # Env conda (spark 3.3.2.3, pyarrow, graphframes...)
├── opt/                     # Scripts bash de entorno (env vars, tests, spark)
├── config/                  # Configuración por job (input/output, rutas, aggs)
│   ├── job.py               # date_treatment, formatos de fecha, cohorte
│   ├── graph_making/        # ventana del grafo (12 meses, lag global)
│   ├── ceps/                # ranking rfc/nom, txn_replacement
│   ├── features/            # graph features, assembler, cluster
│   └── target_propagation/  # lovelace
├── libs/
│   ├── framework/           # Step, SubStep, decoradores dinámicos, carga
│   ├── data_engineering_toolbox/  # HivePath, partition_lags, parquet, fechas
│   └── functions/           # aggregations, features, weights, missing, assembly
├── pipelines/
│   ├── ceps/                # rfc_nom_ranking, txn_replacement
│   ├── graph_making/        # special_treatment, group_by, edges_and_nodes (+ceps/)
│   ├── features/            # graph features, cluster, target prop., assembler
│   └── target_propagation/  # lovelace special_treatment
├── tests/                   # Suite pytest (spark local[2] o cluster)
└── notebook/                # Documentación del proyecto (este archivo incl.)
```

---

## 2. Framework de ejecución (`libs/framework`)

### 2.1 `Step` y `SubStep`

Unidad ETL. Cada `Step`:

- Recibe `input_hive` / `output_hive` (dicts config clave -> tabla/ruta),
  `input_parameters` (vintage, `vintage_date`, `process_date`, ...) y
  `previous_step` (lista de dependencias).
- `execute()` ejecuta primero los `previous_step` (recursivo) y luego
  `step_action()`. Ejecutar el último step lanza toda la cadena.
- Los `SubStep` heredan los atributos del padre con
  `inherit_parent_step_attributes` (cohorte, rutas, `sqlContext`,
  `is_dynamic`, `standard_load_parquet_or_table`, `parent`).
- `@cached_property` (implementación propia) cachea propiedades por instancia.
- `tmp_paths` + `delete_tmp_paths()` permiten borrar parquets intermedios
  (outputs con `keep_or_delete="delete"`).

### 2.2 Particionado de salidas

Todas las salidas persistentes son parquet **particionado por dos columnas**:

- `information_date_column` — en CEPS `mis_date`; en las salidas mensuales
  nuevas `month_partition`.
- `process_date_column` — `process_date`, día en que se computó la partición.

Escritura: `overwrite_two_partition` borra las particiones existentes con la
misma fecha de información (todos sus `process_date`) y añade las nuevas.
Lectura: `SparkLoadPartitionedTableOrParquet` (en
`partition_lags.py`) reconstruye la ventana `[inicio, fin)` a partir de
`vintage_date`, `lag` e `history` y selecciona particiones con dos modos:

- `information_date_mode`: `each` (todos los meses), `first`, `last`, `all`.
- `process_date_mode`: `each`, `first`, `last` (típico: la más reciente),
  `all`.
- `missing_months_allowed=True`: los meses sin partición se omiten sin error.

### 2.3 Decoradores "reload-or-recompute"

- `dynamic_unpartitioned_parquet[_with_path]`: si el parquet existe se
  recarga; si no, se computa y se escribe.
- `dynamic_partitioned_table_or_parquet(path_key)`: intenta recargar las
  particiones de la ventana; si falla, ejecuta la función, estampa
  `mis_date=vintage` y `process_date=hoy` y sobrescribe **solo** esas
  particiones. Devuelve `(DataFrame, load_kwargs)`.
- `is_dynamic=False`: siempre recomputa (no reutiliza disco).

### 2.4 Nuevo: materialización incremental mensual

`ensure_monthly_partitions(step, path_key, compute_missing_months)`
(ver §5) implementa el patrón *"computa solo las particiones que falten"*:
enumera los meses `YYYY-MM` esperados en la ventana (`_window_months`,
misma aritmética `make_date_interval_with_lag_months` que el loader),
detecta los ya presentes vía `sorted_pairs` del loader y computa/escribe
únicamente los ausentes.

`standard_load_parquet_or_table` es la carga estándar con validación de
historia mínima (`minimum_required_history`).

---

## 3. Cadena de steps (main.py)

| # | Step | Qué hace |
|---|------|----------|
| 1 | `CepsRfcNomRankingStep` | Ranking RFC/nombre a partir de CEPs crudos |
| 2 | `CepsTxnReplacementStep` | `rfc_curp_analysis_s264_ceps` + `..._replaced`: normaliza/reemplaza ids de contraparte |
| 3 | `CepsSpecialTreatment` | `missing_treatment`: tratamiento de nulos, imputación de `numcliente`, resolución de `id_src`/`id_dst`, cálculo de `tfrom_days`/`tfrom_months` |
| 4 | `CepsGroupByStep` | **Agregado por arista (`group_by_txn`) y por nodo (`group_by_id`)** — ahora incremental mensual (§5) |
| 5 | `CepsEdgesAndNodesStep` | Aristas con pesos compuestos + nodos del grafo |
| 6 | `CepsGraphFeaturesStep` | Features GraphFrames (pagerank, degrees, componentes, triángulos, versiones weighted, propagación) |
| 7 | `LovelaceTargetPropagationSpecialTreatmentStep` | Prepara el target Lovelace |
| 8 | `CepsTargetPropagationFeaturesStep` | Propagación del target a `numcliente` |
| 9 | `CepsClusterFeaturesStep` | Features de cluster |
| 10 | `CepsVectorAssemblerStep` | Ensamblado final de vectores por `numcliente` |

`is_dynamic=True` en todos: cada etapa recarga su parquet si ya existe
(patrón reanudable). `cohort = cj.COHORT`.

### 3.1 Agregaciones del group-by (config)

- `GROUP_BY_TXN_GROUPING_VARS = ["id_src", "id_dst"]` — la arista.
- `GROUP_BY_ENABLED_FEATURES` habilita funciones del catálogo
  `standard_group_by_features` (min, max, mean, std, count, sum,
  weighted_*, countDistinct, last, first, curt, skew, so).
- `GROUP_TXN_AGGREGATIONS`: nombres declarativos `"{func}_{variable}"`
  (p.ej. `weighted_mean_oper_mto`), resueltos a `Column` por
  `resolve_group_by_expressions` (prefijo de función más largo primero).
- `GROUP_BY_AUXILIARY_COLS = recency_auxiliary_columns("tfrom_days")`:
  `_max_tfrom_days` = antigüedad máxima **global** de la ventana y
  `_recency_weight = max - t + 1` (peso 1 en la txn más antigua, max+1 en la
  más reciente). Las `weighted_*` ponderan por recencia.
- `GROUP_ID_AGGREGATIONS`: `collect_set(numcliente|nom|cta|id_ban)`,
  `last(information_date)`, `sum(oper_mto)` por nodo.
  `GROUP_ID_SET_COLUMNS`/`GROUP_ID_SUM_COLUMNS` son su equivalente
  combinable para el flujo mensual.

---

## 4. Flujo del group-by: antes vs. ahora

### 4.1 Antes

`group_by_txn` hacía `groupBy(id_src, id_dst)` sobre **toda la ventana**
de `missing_treatment` en cada corrida: recomputaba 12 meses de
transacciones todos los meses ("engorroso"). Igual para `group_by_id`.

### 4.2 Ahora (propuesta implementada)

Se añadió un paso previo de **agregados parciales mensuales** persistidos
como particiones, y el agregado total se obtiene **combinando** esas
particiones:

```
missing_treatment
      │
      ▼
group_by_txn_monthly   (una fila por arista-mes, partición month_partition=YYYY-MM-DD)
      │                ensure_monthly_partitions: solo computa meses ausentes
      ▼
group_by_txn           merge_monthly_group_by == group-by completo original
```

Análogo para nodos: `group_by_id_monthly` (una fila por nodo-mes) ->
`group_by_id`.

---

## 5. Diseño del group-by incremental mensual

### 5.1 Parciales mensuales (`monthly_partial_group_by_aggregations`)

Por cada variable de valor `v` (p.ej. `oper_mto`), una fila por
(`id_src`, `id_dst`, `month_partition`) guarda:

- `min_v`, `max_v`, `sum_v`, `count_v` — recombinables por min/max/sum/sum.
- `sum2_v`, `sum3_v`, `sum4_v` — momentos para `so`/`curt`/`skew`/`std`.
- `wsum_v` — Σv solo en filas con antigüedad válida (elegibles para peso).
- `vtsum_v` — Σ(v · t_rel).
- Sobre la antigüedad: `min/max/sum/count_t_rel_days`.
- `information_date` — máximo.

### 5.2 Antigüedad invariante al vintage (`t_rel_days`)

`tfrom_days` cambia entre corridas (depende de la fecha de referencia del
vintage), así que las particiones **no** lo guardan. En su lugar se guarda
la antigüedad **relativa al fin de su propio mes**:

```
month_partition = last_day(to_date(information_date))
t_rel_days      = datediff(month_partition, information_date)
```

`t_rel_days` es invariante: una partición mensual sirve para siempre.
En el merge, `tfrom_days = t_rel_days + datediff(reference, month_partition)`
y el desfase es constante dentro de cada partición. `reference_date`
(`CepsGroupBySubStep.tfrom_reference_date`) replica la de
`calculate_tfroms`: último día del mes vintage menos `GRAPH_GLOBAL_LAG`
meses.

### 5.3 Combinación (`merge_monthly_group_by`)

| Agregación | Combinador |
|------------|------------|
| `min`/`max` | `min`/`max` de parciales |
| `count`/`sum` | `sum` de parciales |
| `mean` | `Σ(sum)/Σ(count)` |
| `std` | `sqrt((Σx²−(Σx)²/N)/(N−1))` (muestral) |
| `curt`/`skew` | `Σx³/(Σx²)^1.5` |
| `so` | `Σx⁴/(Σx²)²` |
| `max_tfrom_days` | `max_m(max_t_rel_m + desfase_m)` |
| `weighted_sum` | `(G+1)·Σwsum − Σ(vtsum + desfase·wsum)` |
| `weighted_count` | `(G+1)·Σcount_t − Σ(sum_t + desfase·count_t)` |
| `weighted_mean` | `weighted_sum / weighted_count` |

`G` = antigüedad máxima **global** de la ventana (idéntico a
`_max_tfrom_days` del flujo completo): se calcula una sola vez sobre el df
mensual (pequeño) y se inyecta como literal.

**No combinables exactamente**: `countDistinct` (necesitaría sketches tipo
HLL), `last`/`first` (dependen de orden). `merge_monthly_group_by` lanza
`ValueError` → `group_by_txn`/`group_by_id` hacen **fallback** al group-by
completo sobre `missing_treatment` (mismo resultado, sin incremental).

### 5.4 Materialización incremental (`ensure_monthly_partitions`)

```
meses_esperados = _window_months(vintage_date, history=12, lag)
meses_presentes = particiones ya en disco (sorted_pairs del loader)
faltantes       = esperados - presentes
si faltantes:   computar SOLO esos meses -> overwrite_two_partition
devolver        ventana completa releída (process_date_mode="last")
```

- En la corrida mensual típica, la ventana se desplaza un mes y solo se
  computa **el mes nuevo**; el mes más viejo simplemente queda fuera de la
  ventana al releer (no se borra).
- `is_dynamic=False` → recomputa toda la ventana.
- Un mes sin transacciones no escribe partición (se reintentará la próxima
  corrida).
- Para **refrescar** un mes ya materializado (datos restated): borrar su
  directorio `month_partition=YYYY-MM-DD/` y la próxima corrida lo
  recomputa.
- Las particiones llevan además `process_date` (día de cómputo); la lectura
  usa el más reciente por mes, igual que el resto del framework.

### 5.5 Nodos (`group_by_id`)

Parciales por nodo-mes: `collect_set` por mes de `numcliente|nom|cta|id_ban`,
`sum(oper_mto)`, `max(information_date)`. Merge:
`array_distinct(flatten(collect_list(set)))` (unión de conjuntos), `sum`,
`max`. Nota: `max(information_date)` sustituye a `last(...)` (determinista;
`last` depende del orden de las filas).

### 5.6 Archivos tocados

- `libs/functions/aggregations.py` — `split_group_by_name`,
  `monthly_partial_group_by_aggregations`, `merge_monthly_group_by`,
  `NON_MERGEABLE_GROUP_BY_FUNCTIONS`.
- `libs/framework/__init__.py` — `_window_months`,
  `ensure_monthly_partitions`.
- `pipelines/graph_making/group_by.py` — `standard_group_by_txn_monthly`,
  `standard_merge_group_by_txn_monthly`, `standard_group_by_id_monthly`,
  `standard_merge_group_by_id_monthly`.
- `pipelines/graph_making/ceps/group_by.py` — `group_by_txn_monthly`,
  `group_by_id_monthly`, `group_by_value_columns`,
  `tfrom_reference_date`; merge + fallback en `group_by_txn`/`group_by_id`.
- `config/graph_making/ceps/group_by.py` — outputs `group_by_txn_monthly`,
  `group_by_id_monthly`; `GROUP_ID_SET_COLUMNS`, `GROUP_ID_SUM_COLUMNS`.

---

## 6. Pruebas

`tests/test_monthly_group_by.py` (nuevo):

- `_window_months`: enumeración de la ventana (con y sin lag).
- `standard_group_by_txn_monthly`: columnas y valores de las parciales
  (incl. `t_rel_days`), filtro por `months`.
- Equivalencia merge vs. group-by completo: mismos valores para todas las
  agregaciones de `GROUP_TXN_AGGREGATIONS` (aprox), con chequeo exacto de
  los `weighted_*` (pesos calculados a mano).
- Agregaciones no combinables (`countDistinct`, `last`, `first`) → `ValueError`.
- `standard_group_by_id_monthly` + merge: conjuntos/sumas/fechas por nodo.
- `ensure_monthly_partitions`: con shim de `libs.data_engineering_toolbox.path`
  al fs local, verifica que solo se computan los meses faltantes al avanzar
  el vintage, que se escriben los dirs `month_partition=.../process_date=...`
  y que `is_dynamic=False` recomputa todo.

Convención de la suite: `pytest.importorskip("pyspark")`, fixture `spark`
(local[2] o YARN según `MINERVA_TEST_MODE`), `tmp_hdfs` (tmp_path local o
HivePath en cluster), marcadores `spark`/`graphframes`. Ejecución:
`pytest tests/` con `MINERVA_TODAY` fijado (los tests lo exportan).

---

## 7. Ejecución

- `python main.py` — flujo completo (requiere env vars del cluster);
  `--build-only` construye sin ejecutar; `--cleanup[-only]` borra parquets
  intermedios (`keep_or_delete="delete"`).
- `run_order.py` — driver interactivo para desarrollo.
- `local_flow_check.py` — smoke test local con shim HDFS -> filesystem local.
- `opt/run-tests.sh` — suite en el entorno conda del cluster.

## 8. Notas y decisiones

- `missing_treatment` (y el resto de salidas preexistentes) sigue
  particionándose por vintage (`mis_date`); solo el group-by se hizo
  incremental por mes de información. La misma maquinaria
  (`ensure_monthly_partitions`) es reutilizable si se quiere llevar el
  patrón "históricos mes a mes" más arriba en la cadena.
- El merge respeta nulos: una txn con `oper_mto` nulo no cuenta en
  `count_oper_mto` ni en las ponderadas (igual que en el group-by completo).

---

## 9. Mejoras de la segunda iteración

### 9.1 Historia independiente: catálogos de reemplazo vs. tabla `txn_replaced`

- `config/ceps/rfc_nom_ranking.py`: `CEPS_RANKING_HISTORY_IN_MONTHS = 24` —
  meses de historia CEP leídos para construir los catálogos
  (`s264_ceps` input). `CEPS_MAXIMUM_HISTORY_IN_MONTHS = 3` queda como la
  ventana transaccional corta usada por los outputs.
- `config/ceps/txn_replacement.py`: `TXN_REPLACED_HISTORY_IN_MONTHS = 3` y
  el input `rfc_curp_analysis_s264_ceps` lee solo la partición del vintage
  (`history=1`) — cada partición ya contiene la ventana completa del ranking.
- `pipelines/ceps/txn_replacement.py`: `limit_txn_history_window` recorta las
  transacciones por `fec_informacion` a la ventana
  `[vintage-lag-history+1m, vintage-lag+1m)` con la misma aritmética de
  `make_date_interval_with_lag_months`. Se aplica en
  `rfc_curp_analysis_s264_ceps` antes del join de reemplazo.

Resultado: los catálogos usan 24 meses; la tabla `replaced` que alimenta el
grafo solo conserva 3 meses por partición (el grafo acumula las particiones
de los últimos vintages, como siempre).

### 9.2 Propagación multi-columna del target

- `config/target_propagation/lovelace/special_treatment.py`:
  `TARGET_AGGREGATIONS = {columna: funcion}` — todas las columnas listadas
  (target, scores, ...) viajan juntas en `target_cta`/`target_numcliente`.
- `pipelines/target_propagation/lovelace/special_treatment.py`: agrega cada
  columna con su función en vez de solo `max(target)`.
- `lff.target_group_by_id` acepta `function` como dict `{columna: callable}`:
  `target` produce `target_agg_<columna>`, el resto `target_agg_<columna>_<tcol>`.
- `TARGETS[*]["modes"][*]` admite `"columns"` multi-columna; la forma simple
  (`aggregation_function` + `missing_treatment`) sigue funcionando.
- `propagation_node_columns()` devuelve `target_lovelace` +
  `target_lovelace_<col>` por cada columna extra; `PROPAGATION_FEATURES`
  genera `contagion_<weight>` para `target` y `contagion_<col>_<weight>`
  para el resto.

### 9.3 Features globales de grafo

Nuevas en `libs/functions/features.py` (+ registry + `_ft` + config):

- `degree_balance` -> `net_degree`, `in_out_degree_ratio`
- `reciprocity` -> `reciprocal_out/in`, `reciprocity_out/in`, `reciprocity`
  (pares dirigidos únicos, sin self-loops)
- `self_loops` -> `self_loop_count` (0 si no hay)
- `weighted_degree_balance` -> `net_strength`, `in_out_strength_ratio`

Inventario completo de preguntas/features: `notebook/preguntas_grafo.md`.

### 9.4 Targets propagadas y pesos en features de agrupación

- `config/features/ceps/cluster_features.py`: `REQUIRED_STATS_PREFIXES`
  (`target_`, `contagion_`, `oper_mto`, `weight`, `_strength`) — si ninguna
  columna agregable coincide con un prefijo, `cluster_stats` registra un
  warning (guarda de que los stats de grupo incluyan target, contagio y pesos).
- `CLUSTER_STATS` incluye `median` (percentile_approx 0.5) además de
  count/sum/mean/std/min/max.

### 9.5-9.7 Vector assembler: fuentes opcionales, sufijo, niveles

`config/features/ceps/vector_assembler.py` + `pipelines/features/ceps/vector_assembler.py`:

- **Fuentes opcionales**: cada entrada de `FEATURE_SOURCES` acepta
  `"enabled": False` (se salta) y `"optional": True` (default; si la carga
  falla se registra warning y se sigue sin esa fuente; `False` propaga el error).
- **Sufijo de variables**: `VARIABLE_SUFFIX = "_ceps"` renombra TODAS las
  columnas de `{nivel}_features` menos la llave.
- **Niveles**: `AGGREGATION_LEVELS = ["numcliente", "cta"]` con
  `NODE_ID_ARRAY_COLUMNS` (array de `nodes` explotado por nivel) y
  `PIVOT_BY_LEVEL` (pivote externo por nivel: `pivot` -> numcliente,
  `pivot_cta` -> cta). Misma lógica para ambos.
- **Multi-agregación**: `FEATURE_AGGREGATION` acepta lista
  (`{"default": ["weighted_mean", "median", "std", "mean"]}`) — con varias
  funciones por feature las columnas se llaman `{funcion}_{feature}`;
  `libs/functions/assembly.py` añade `median` (percentile_approx) y `std` al
  registro `ASSEMBLY_AGGREGATION_FUNCTIONS`.
- **Nulos**: `FILL_NULLS_VALUE = -99999` se aplica a las columnas del vector
  antes del `VectorAssembler` (centinela de "no encontrado").
- **Esquema final**: `{nivel}_features` = llave + todas las variables
  agregadas (con sufijo); `{nivel}_features_vector` = SOLO llave + columna
  `features` (el select final descarta las variables sueltas).

### Tests añadidos

- `tests/test_txn_history_window.py`: `limit_txn_history_window` (ventana,
  lag, bordes inclusivo/exclusivo, passthrough) + constantes de config.
- `tests/test_features.py`: registry ampliado + `degree_balance`,
  `reciprocity` (incl. self-loops ignorados), `self_loops`,
  `weighted_degree_balance`, `target_group_by_id` multi-columna.
- `tests/test_assembly.py`: `median`/`std` en el registro, listas de
  agregaciones (`{func}_{col}`), dict con default.
- `tests/test_cluster_features.py`: warnings de `REQUIRED_STATS_PREFIXES`,
  stats de target/contagion/pesos por grupo, helpers `_mode_columns` /
  `propagation_node_columns` / naming de `PROPAGATION_FEATURES`.
- `tests/test_vector_assembler_options.py`: sufijo, niveles, pivote por
  nivel, multi-agregación con sufijo, fuentes opcionales/desactivadas,
  dedup de columnas entre fuentes.
- `tests/conftest.py`: el shim `local_hdfs` (HDFS -> fs local) se movió
  desde `test_monthly_group_by.py` para reutilizarlo.
