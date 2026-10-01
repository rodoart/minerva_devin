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

### `target_propagation` (nivel `id`, un subdir por combinación de parámetros)

| Columna | Definición |
|---|---|
| `contagion_<peso>` | Score de contagio iterativo: `(1-alpha)*propio + alpha*media_ponderada_vecinos`, `max_iter` iteraciones, con suelo en la semilla si `keep_seed_floor` |
| `contagion_<col>_<peso>` | Idem usando `target_lovelace_<col>` como semilla (scores propagados) |
| `target_lovelace*` | Semilla(s) (target original y columnas extra del nodo) |

Una columna por cada (`weight_type` × columna propagada) — subdirs
`weight_type=<w>/target_column=<tc>/alpha=<a>/...` leídos con `mergeSchema`.

**Guardados intermedios del step**:

- `edges_norm/weight_type=<w>` — aristas normalizadas por grado del receptor,
  compartidas por todas las columnas propagadas con ese peso (dir hermano de
  `target_propagation`; `keep_or_delete="delete"` lo limpia al final).
- `target_propagation/<params>` — un parquet por combinación
  (weight_type × target_column × alpha × max_iter × keep_seed_floor); cada
  feature de contagio es su propio punto de reanudación: si el proceso muere,
  solo recomputan las combinaciones cuyo subdir falta.
- Dentro de cada difusión, `propagate_target` trunca el linaje con un
  `checkpoint()` eager por iteración al dir `checkpoint/` (los `.cache()` que
  hace GraphFrames con `AM.getCachedDataFrame` son volátiles y no truncan el
  plan — el checkpoint sí, y es durable en HDFS).

## 4. Features de agrupación (nivel `id`, tabla `cluster_stats`)

| Columna | Definición |
|---|---|
| `cluster_<grupo>_size` | Nº de miembros del grupo (`grupo` ∈ `GROUP_COLUMNS`: `component_id`, `scc`) |
| `cluster_<grupo>_<stat>_<col>` | Estadística `{stat}` (`CLUSTER_STATS`: count/sum/mean/median/std/min/max) de la columna `<col>` dentro del grupo del nodo — incluye targets (`target_*`), contagio (`contagion_*`) y pesos (`oper_mto`, `*_weight`, `*_strength`); guardado por `REQUIRED_STATS_PREFIXES` |

`scc` = componente fuertemente conexa (determinista) o `label_propagation`
(no determinista) según `SUBCLUSTER_METHOD`.

**Reanudación del step**: los intermedios pesados se materializan como
parquets propios (`dynamic_unpartitioned_parquet`), porque los checkpoints
orgánicos de GraphFrames no son reanudables:

- `subcluster_df` — resultado del SCC/label-propagation (id, scc).
- `nodes_enriched` — join de nodos + targets + todas las features de grafo.
- `cluster_stats` — salida final.

Si el proceso muere, la siguiente ejecución recarga los parquets ya escritos
y no vuelve a correr el algoritmo de grafo ni el join multi-fuente.

## 5. Nivel `numcliente` y `cta` (VectorAssembler multi-nivel)

Para cada nivel en `AGGREGATION_LEVELS` (`numcliente`, `cta`):

### `{nivel}_features`

| Columna | Definición |
|---|---|
| `<nivel>` | Llave (explode del array del nivel en `nodes`) |
| `{func}_<feature>_ceps` | Cada feature de nodo agregada al nivel con `FEATURE_AGGREGATION` (lista de funciones → una columna por función; `AGGREGATION_WEIGHT = oper_mto/(tfrom_days+1)` como peso). Todas las variables llevan `VARIABLE_SUFFIX` (`_ceps`) |
| `target_lovelace*_ceps` | Etiquetas agregadas igual que las features (para entrenamiento) |
| `node_count_ceps` | Nº de nodos (`id`) distintos que aportan a la llave |

### `{nivel}_features_vector` (salida final para modelos)

| Columna | Definición |
|---|---|
| `<nivel>` | Llave del pivote externo (`PIVOT_BY_LEVEL`: `pivot`/`pivot_cta`) |
| `features` | `VectorAssembler` sobre todas las columnas **excepto** la llave y los prefijos `EXCLUDE_PREFIXES` (`target_*` = etiqueta, fuera del vector); nulos → `FILL_NULLS_VALUE` (`-99999`) |

La tabla final contiene **únicamente** la llave y el vector: las variables
agregadas quedan en `{nivel}_features` (penúltima tabla).

## 6. Intermedios del pipeline (para depuración)

| Tabla | Columnas clave |
|---|---|
| `raw_flattened` (special_treatment) | columnas CEPS + `tfrom_days`, `tfrom_months`, hora/monto normalizados |
| `group_by_txn` | `id_src`, `id_dst` + agregaciones txn: `min_oper_mto`, `max_oper_mto`, `mean_oper_mto`, `so_txn_number` |
| `group_by_txn_monthly` | mismas parciales combinables por (`id_src`, `id_dst`, `month_partition`) — momentos Σx²..Σx⁴, `t_rel_days` |
| `group_by_id` | `id` + arrays (`numcliente`, `nom`, `cta`, `id_ban`) + `oper_mto`, `information_date` |
| `group_by_id_monthly` | arrays/sumas parciales por (`id`, `month_partition`) |
