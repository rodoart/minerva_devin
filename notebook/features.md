# Diccionario de features de Minerva

Referencia de todas las columnas generadas por el pipeline, su definición y el
nivel en el que viven. Los nombres finales son los que quedan en los parquets
de salida (los `GRAPH_RENAMES` ya aplicados: `id_src`→`src`, `id_dst`→`dst`).

## Convenciones de nombre

| Patrón | Significado |
|---|---|
| `id` | Clave de nodo del grafo (par rfc/nom normalizado según ranking) |
| `src`, `dst` | Nodo origen/destino de una arista |
| `numcliente` | Cliente interno del banco (array en nodos, escalar tras explode) |
| `mis_date` / `process_date` | Particiones: mes de información `yyyymm` / fecha de proceso `yyyy-mm-dd` |
| `target_<fuente>` | Etiqueta combinada (p.ej. `target_lovelace`); `target_<fuente>_<modo>` por modo |
| `target_agg_<sufijo>` | Target agregado por `id` antes de unir a nodos (intermedio) |
| `contagion_<peso>` | Score propagado usando el tipo de peso `<peso>` |
| `contagion_<col>_<peso>` | Idem propagando otra columna (`target_lovelace_<col>`: scores, etc.) |
| `target_synthetic_<name>` | Etiqueta sintética generada desde una feature del grafo (`SYNTHETIC_TARGETS`) |
| `contagion_synthetic_<name>_<peso>` | Contagio de la etiqueta sintética con el peso `<peso>` |
| `risk_*` | Exposición del nodo a semillas de fraude (`risk_features`) |
| `pair_*` / `pair_<f>_<stat>` | Métrica del par (A, X) / agregada al nodo con el estadístico `<stat>` |
| `tmp_*` | Feature temporal (`temporal_features`: ventanas, velocidad, antigüedad...) |
| `cluster_grp_<m>_*` | Stats de grupo cuando el grupo es `grp_<m>` (`grp_scc`, `grp_lp`, `grp_cc` según `SUBCLUSTER_METHOD`) |
| `weighted_*` | Feature de grafo calculada sobre el peso seleccionado |
| `{stat}_weight` | Estadística `{stat}` del peso de aristas incidentes (de `STANDARD_WEIGHT_STATS`) |
| `in_*`/`out_*`/`total_*` | Métrica entrante/saliente/suma de ambas |
| `cluster_<grupo>_<stat>_<col>` | Estadística `{stat}` de `<col>` dentro del grupo `<grupo>` del nodo |
| `cluster_<grupo>_size` | Nº de miembros del grupo |
| `{func}_{feat}_ceps` | Feature agregada al nivel con la función `{func}` (sufijo `VARIABLE_SUFFIX`) |

## 1. Nodos y aristas (`edges_and_nodes`)

### `edges` (nivel txn src→dst)

| Columna | Definición |
|---|---|
| `src`, `dst` | Nodo origen y destino (renombrados desde `id_src`/`id_dst`) |
| `mean_oper_mto` | Monto medio de las txns src→dst |
| `sum_oper_mto` | Monto total acumulado src→dst |
| `max_tfrom_days` | Antigüedad máxima (días desde la operación al vintage) |
| `weighted_mean_oper_mto` | Monto medio ponderado por recencia |
| `weighted_sum_oper_mto` | Monto total ponderado por recencia |
| `count_txn` | Número de transacciones (alias de `count_oper_mto`) |
| `weighted_count_txn` | Número de txns ponderado por recencia |
| `weights` | Mapa `nombre→peso` con todas las funciones de `WEIGHT_COLUMNS` |

Claves del mapa `weights` (config `WEIGHT_COLUMNS`): `weighted_mean_oper_mto`,
`weighted_sum_oper_mto`, `weighted_count_txn`, `mean_oper_mto`, `count_txn`,
`composed` (media de `weighted_mean_oper_mto` y `mean_oper_mto`).

### `nodes` (nivel `id`)

Réplica de `group_by_id` + columnas de fecha: `id`, `numcliente` (array),
`nom` (array), `cta` (array), `id_ban` (array), `information_date`,
`oper_mto` (suma), `vintage`, `cohort`, `mis_date`, `process_date`.

## 2. Features de grafo (nivel `id`)

### Sin peso (`GRAPH_CENTRALITY_FEATURES` unweighted)

| Columna | Función | Definición |
|---|---|---|
| `pagerank` | `pagerank` | PageRank estándar (maxIter=10, reset=0.15 por defecto) |
| `in_degree` | `degrees` | Nº de aristas entrantes |
| `out_degree` | `degrees` | Nº de aristas salientes |
| `total_degree` | `degrees` | `in_degree + out_degree` |
| `component_id` | `components` | Componente conexa del nodo |
| `net_degree` | `degree_balance` | `out_degree - in_degree` (origen/destino neto de flujo) |
| `in_out_degree_ratio` | `degree_balance` | `out/in` (null si el nodo no recibe nada) |
| `reciprocal_out`/`reciprocal_in` | `reciprocity` | Nº de vecinos que también enlazan de vuelta (por dirección) |
| `reciprocity_out`/`reciprocity_in` | `reciprocity` | Fracción recíproca sobre el grado de cada dirección |
| `reciprocity` | `reciprocity` | Fracción de aristas incidentes recíprocas (global) |
| `self_loop_count` | `self_loops` | Nº de aristas src==dst del nodo (0 si no hay) |
| `triangle_count` | `triangle_count` | Nº de triángulos en los que participa |

### Ponderadas (`weight` = clave del mapa `weights`)

| Columna | Función | Definición |
|---|---|---|
| `weighted_pagerank` | `weighted_pagerank` | PageRank (con peso en aristas) |
| `in_strength` | `weighted_degrees` | Suma de pesos de aristas entrantes |
| `out_strength` | `weighted_degrees` | Suma de pesos de aristas salientes |
| `total_strength` | `weighted_degrees`/`weighted_triangle_count` | `in + out_strength` |
| `net_strength` | `weighted_degree_balance` | `out_strength - in_strength` |
| `in_out_strength_ratio` | `weighted_degree_balance` | `out/in` ponderado (null si no entra flujo) |
| `{stat}_weight` | `weighted_edge_stats` | Estadística `{stat}` sobre el peso de aristas incidentes (in+out): `min`, `max`, `mean`, `std`, `count`, `countDistinct`, `sum`, `curt`, `skew`, `so` |
| `component_weight` | `weighted_components` | Peso total de las aristas de la componente del nodo |
| `triangle_count` | `weighted_triangle_count` | Conteo estructural de triángulos + `total_strength` |

**GroupBy por nodo en dos etapas con sal**: todas las features que agregan
aristas por nodo (`degrees`, `degree_balance`, `reciprocity`, `self_loops`,
`weighted_degrees`, `weighted_edge_stats`, `weighted_degree_balance`,
`weighted_components`, `weighted_triangle_count` y `get_degree` de la
propagación) aceptan `salt_buckets` (default `EDGE_GROUPBY_SALT_BUCKETS`=64;
`<=1` = groupBy directo): los supernodos reparten sus aristas entre buckets.
Los stats `curt`/`skew`/`so` (`Σxᵏ/(Σx²)^(k/2)`) son combinables exactos vía
momentos 3-4; `countDistinct` va por la vía directa unida por `id`. El
parámetro puede ajustarse por feature desde `params` en
`GRAPH_CENTRALITY_FEATURES`/`WEIGHTED_FEATURES` (fluye como kwarg al método
`{feature}_ft`).

## 3. Propagación de target

### Intermedios

| Columna | Definición |
|---|---|
| `deg_sum` (`get_degree`) | Grado ponderado: suma de `weight` de aristas incidentes |
| `w_src_to_dst` (`edges_norm`) | `weight / deg_dst` — mensaje normalizado por grado del receptor |
| `w_dst_to_src` (`edges_norm`) | `weight / deg_src` — misma normalización en dirección inversa |
| `target_agg_<sufijo>` | Target agregado por `id` (función del modo, p.ej. `spark_max`) |
| `target_agg_<sufijo>_<col>` | Otra columna propagada (score, etc.) agregada por `id` |

### `nodes_join_target_lovelace` (nivel `id`)

| Columna | Definición |
|---|---|
| `target_lovelace_cta` | Target agregado por cuenta asociada al nodo |
| `target_lovelace_numcliente` | Target agregado por numcliente asociado al nodo |
| `target_lovelace_<modo>_<col>` | Columna extra propagada (score, ...) por cada modo |
| `target_lovelace` | Etiqueta final: `TARGET_SELECTION_FUNCTION` (`greatest`) sobre los modos |
| `target_lovelace_<col>` | Combinación de la columna extra entre modos (`greatest`) |
| `target_synthetic_<name>` | Etiqueta sintética de `SYNTHETIC_TARGETS` (p.ej. `pagerank_top`: 1 al 1% con mayor PageRank); se une a nodos como cualquier target |

### `target_propagation` (nivel `id`, un subdir por combinación de parámetros)

| Columna | Definición |
|---|---|
| `contagion_<peso>` | Score de contagio iterativo: `(1-alpha)*propio + alpha*media_ponderada_vecinos`, `max_iter` iteraciones, con suelo en la semilla si `keep_seed_floor` |
| `contagion_<col>_<peso>` | Idem usando `target_lovelace_<col>` como semilla (scores propagados) |
| `contagion_synthetic_<name>_<peso>` | Idem con la etiqueta sintética `target_synthetic_<name>` como semilla (`SYNTHETIC_WEIGHT_TYPES`) |
| `target_lovelace*` | Semilla(s) (target original y columnas extra del nodo) |

Una columna por cada (`weight_type` × columna propagada) — subdirs
`weight_type=<w>/target_column=<tc>/alpha=<a>/...` leídos con `mergeSchema`.

**Guardados intermedios del step**:

- `edges_norm/weight_type=<w>` — aristas normalizadas por grado del receptor,
  compartidas por todas las columnas propagadas con ese peso (dir hermano de
  `target_propagation`; `keep_or_delete="delete"` lo limpia al final). El
  grado se calcula con sal en dos etapas (`DEGREE_SALT_BUCKETS`) porque los
  supernodos sesgan el `groupBy`; con `<= 1` vuelve al groupBy directo.
- `target_propagation/<params>` — un parquet por combinación
  (weight_type × target_column × alpha × max_iter × keep_seed_floor); cada
  feature de contagio es su propio punto de reanudación: si el proceso muere,
  solo recomputan las combinaciones cuyo subdir falta.
- Dentro de cada difusión, `propagate_target` trunca el linaje con un
  `checkpoint()` eager por iteración al dir `checkpoint/` (los `.cache()` que
  hace GraphFrames con `AM.getCachedDataFrame` son volátiles y no truncan el
  plan — el checkpoint sí, y es durable en HDFS).

## 4. Riesgo de nodo (nivel `id`, tabla `risk_features`)

Exposición de cada nodo a las semillas de fraude (duramente marcadas
`RISK_HARD_SEED_COLUMNS = ["target_lovelace"]` con
`RISK_HARD_SEED_THRESHOLD`, o blandas vía contagio
`RISK_CONTAGION_WEIGHTS` con `RISK_CONTAGION_SEED_THRESHOLD`). `<semilla>`
es el nombre de la semilla (p.ej. `lovelace`, `composed`).

| Columna | Definición |
|---|---|
| `risk_in_sum_<semilla>` / `risk_out_sum_<semilla>` | Suma de la semilla en vecinos entrantes / salientes directos |
| `risk_in_frac_<semilla>` / `risk_out_frac_<semilla>` | Fracción de vecinos entrantes / salientes marcados como fraude |
| `risk_2hop_<semilla>` | Nº de nodos fraudulentos a 2 saltos del nodo |
| `risk_outflow_<semilla>` / `risk_inflow_<semilla>` | Monto hacia / desde nodos fraudulentos directos |
| `risk_dist_to_<semilla>` / `risk_dist_from_<semilla>` | Distancia mínima en aristas al fraude conocido más cercano, en dirección hacia él / desde él (`shortestPaths`; `from` invierte el grafo) |
| `risk_ppr_max_<semilla>` / `risk_ppr_mean_<semilla>` | Personalized PageRank máximo / medio desde las semillas fraudulentas (`PPR_MAX_ITER`, `PPR_RESET_PROB`, `PPR_MAX_SEEDS`) |

## 5. Features de par (A, X) (nivel `id`, tabla `pair_features`)

Métricas topológicas del par (A = nodo con `target`, X = vecino candidato,
máx. `PAIR_MAX_HOPS = 2` saltos de una semilla). Se calculan por par y se
agregan al nodo con estadísticos (`pair_<f>_<stat>`).

| Columna | Definición |
|---|---|
| `pair_common_neighbors` | Nº de vecinos comunes de A y X |
| `pair_jaccard` | Similitud de Jaccard de los vecindarios de A y X |
| `pair_adamic_adar` | Score Adamic-Adar del par |
| `pair_preferential_attachment` | `deg_out(A) · deg_in(X)` |
| `pair_reciprocity` | 1 si el par tiene aristas en ambas direcciones |
| `pair_paths_len2` | Nº de caminos de longitud 2 entre A y X |
| `pair_min_distance` | Distancia mínima (A, X) en el grafo |

## 6. Features temporales (nivel `id` y par, tabla `temporal_features`)

Calculadas desde las transacciones reemplazadas con timestamp. `<w>` ∈
`WINDOW_DAYS` (1, 7, 30 días).

### Nivel nodo

| Columna | Definición |
|---|---|
| `tmp_txn_count_<w>d` | Nº de transacciones del nodo en los últimos `<w>` días |
| `tmp_velocity_7d_30d` | Velocidad: txns 7d / txns 30d |
| `tmp_account_age_days` | Antigüedad de la cuenta (días desde la primera interacción) |
| `tmp_days_since_last_txn` | Días desde la última transacción del nodo |
| `tmp_pct_night` / `tmp_pct_weekend` | Proporción de txns en horario nocturno (22–06 h) / fin de semana |
| `tmp_pct_below_threshold` | Proporción de montos justo bajo umbrales de reporte (`REPORT_THRESHOLDS`, banda `BELOW_THRESHOLD_BAND`) |
| `tmp_burstiness` | Burstiness de la serie de txns (mín. `MIN_TXN_FOR_BURSTINESS`) |
| `tmp_unique_receivers_7d` | Nº de receptores únicos del emisor en 7 días |
| `tmp_pass_through_24h` | Ratio entrada/salida en 24 h (pass-through: entra y sale rápido) |
| `tmp_flow_out_in_ratio` | Ratio flujo saliente/entrante del nodo |

### Nivel par (A → X)

| Columna | Definición |
|---|---|
| `tmp_amt_zscore_ax` | Z-score del monto de la txn respecto al histórico del par A→X |
| `tmp_last_amt_over_median_ax` | Último monto / mediana histórica del par A→X |
| `tmp_mean_amt_over_median_a` | Monto medio / mediana histórica del emisor A |

## 7. Features de agrupación (nivel `id`, tabla `cluster_stats`)

| Columna | Definición |
|---|---|
| `cluster_<grupo>_size` | Nº de miembros del grupo (`grupo` ∈ `GROUP_COLUMNS`: `component_id`, `grp_<short>`) |
| `cluster_<grupo>_<stat>_<col>` | Estadística `{stat}` (`CLUSTER_STATS`: count/sum/mean/median/std/min/max) de la columna `<col>` dentro del grupo del nodo — incluye targets (`target_*`), contagio (`contagion_*`) y pesos (`oper_mto`, `*_weight`, `*_strength`); guardado por `REQUIRED_STATS_PREFIXES` |

La columna de subgrupo es `grp_<short>` según `SUBCLUSTER_METHOD`:
`grp_scc` (componente fuertemente conexa, determinista), `grp_lp`
(`label_propagation`, no determinista) o `grp_cc` (componente conexa — modo
ligero: agrupa todo lo conectado por un camino y corta donde no hay conexión
real; convive con los otros métodos eligiéndolo en config, y el prefijo
`cluster_grp_cc_*` delata el método en los features finales). Con
`SUBCLUSTER_ENABLED = False` no se corre el algoritmo de grafo: el step solo
agrupa por `component_id` y omite el subgrupo con un warning (aunque siga en
`GROUP_COLUMNS`).

**GroupBy anti-skew en dos etapas** (`SALT_BUCKETS`, por
defecto 64): un solo componente gigante concentraría todo el shuffle en un
executor. `cluster_stats` se calcula **por cada columna de grupo** y en dos
etapas, reutilizando la aritmética de momentos combinables de
`libs.functions.aggregations` (la misma del group-by mensual):

1. *Etapa sal* — `salted_partial_stats`: añade `salt = pmod(hash(id), N)` y
   agrega momentos parciales (`n`, `sum`, `sum(x²)`, min, max) por
   `(grupo, sal)`. Una partición gigante se reparte en `N` tareas.
2. *Etapa merge* — `merge_salted_stats`: combina los momentos por grupo
   (std vía varianza poblacional: `E[x²] - E[x]²`).

Cada chunk `(group_column, chunk)` se materializa en
`cluster_stats_parts/group_column=<g>/chunk=<hash>/` antes de unirse —
un fallo a mitad solo recomputa los chunks que falten. Las stats no
combinables (`median` u otras custom en `CLUSTER_STATS`) van por la vía
directa (`plain_group_stats`) con su propio parquet por chunk. Con
`SALT_BUCKETS <= 1` todo el cálculo es la vía directa.

**Modo ligero** (`CLUSTER_LIGHT_MODE = True`): las stats solo se calculan
sobre las columnas que ya trae `nodes_join_target` — no se unen las fuentes
de `GRAPH_FEATURE_SOURCES` a `nodes_enriched` (el join multi-parquet es gran
parte del coste del step).

**Reanudación del step**: los intermedios pesados se materializan como
parquets propios (`dynamic_unpartitioned_parquet`), porque los checkpoints
orgánicos de GraphFrames no son reanudables. Los paths llevan subdirs de
variante para que un cambio de config no recargue datos obsoletos:

- `subcluster_df/method=<m>_max_iter=<i>/` — SCC/label-propagation/CC
  (id, `grp_<short>`).
- `nodes_enriched/mode=<full|full_noscc|light|light_noscc>/` — join de nodos
  + targets + features de grafo activas.
- `cluster_stats_parts/...` — chunks de stats (etapa 1 y 2).
- `cluster_stats` — salida final.

Si el proceso muere, la siguiente ejecución recarga los parquets ya escritos
y no vuelve a correr el algoritmo de grafo ni el join multi-fuente.

## 8. Nivel `numcliente` y `cta` (VectorAssembler multi-nivel)

Para cada nivel en `AGGREGATION_LEVELS` (`numcliente`, `cta`):

### `{nivel}_features`

| Columna | Definición |
|---|---|
| `<nivel>` | Llave (explode del array del nivel en `nodes`) |
| `{func}_<feature>_ceps` | Cada feature de nodo agregada al nivel con `FEATURE_AGGREGATION` (lista de funciones → una columna por función; `AGGREGATION_WEIGHT = oper_mto/(tfrom_days+1)` como peso). Es el spec histórico de `AGGREGATION_SPECS`; cada spec extra aporta sus propias columnas. Todas las variables llevan `VARIABLE_SUFFIX` (`_ceps`) |
| `<spec>/<func>_<feature>_ceps` | Agregación de las specs adicionales de `AGGREGATION_SPECS` (nombre, peso, funciones y subconjunto de columnas propios), sobre las columnas ya transformadas por `NODE_TRANSFORMS` |
| `target_lovelace*_ceps` | Etiquetas agregadas igual que las features (para entrenamiento) |
| `node_count_ceps` | Nº de nodos (`id`) distintos que aportan a la llave |

El groupBy por nivel también es en dos etapas con sal
(`ASSEMBLY_SALT_BUCKETS`=32): una llave gigante (cliente/cuenta con miles de
nodos) no concentra el shuffle. `weighted_mean`/`mean`/`std`/`min`/`max`/`sum`
usan combinadores exactos (Σv·w/Σw, momentos); `median`/`first`/
`distinct_count` van por la vía directa y se unen por la llave; `node_count`
se calcula por deduplicación `(llave, id)` → count.

### `{nivel}_features_vector` (salida final para modelos)

| Columna | Definición |
|---|---|
| `<nivel>` | Llave del pivote externo (`PIVOT_BY_LEVEL`: `pivot`/`pivot_cta`) |
| `features` | `VectorAssembler` sobre todas las columnas **excepto** la llave y los prefijos `EXCLUDE_PREFIXES` (`target_*` = etiqueta, fuera del vector); nulos → `FILL_NULLS_VALUE` (`-99999`) |

La tabla final contiene **únicamente** la llave y el vector: las variables
agregadas quedan en `{nivel}_features` (penúltima tabla).

## 9. Intermedios del pipeline (para depuración)

| Tabla | Columnas clave |
|---|---|
| `raw_flattened` (special_treatment) | columnas CEPS + `tfrom_days`, `tfrom_months`, hora/monto normalizados |
| `group_by_txn` | `id_src`, `id_dst` + agregaciones txn: `min_oper_mto`, `max_oper_mto`, `mean_oper_mto`, `so_txn_number` |
| `group_by_txn_monthly` | mismas parciales combinables por (`id_src`, `id_dst`, `month_partition`) — momentos Σx²..Σx⁴, `t_rel_days` |
| `group_by_id` | `id` + arrays (`numcliente`, `nom`, `cta`, `id_ban`) + `oper_mto`, `information_date` |
| `group_by_id_monthly` | arrays/sumas parciales por (`id`, `month_partition`) |
