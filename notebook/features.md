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
| `weighted_*` | Feature de grafo calculada sobre el peso seleccionado |
| `{stat}_weight` | Estadística `{stat}` del peso de aristas incidentes (de `STANDARD_WEIGHT_STATS`) |
| `in_*`/`out_*`/`total_*` | Métrica entrante/saliente/suma de ambas |

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
| `triangle_count` | `triangle_count` | Nº de triángulos en los que participa |

### Ponderadas (`weight` = clave del mapa `weights`)

| Columna | Función | Definición |
|---|---|---|
| `weighted_pagerank` | `weighted_pagerank` | PageRank (con peso en aristas) |
| `in_strength` | `weighted_degrees` | Suma de pesos de aristas entrantes |
| `out_strength` | `weighted_degrees` | Suma de pesos de aristas salientes |
| `total_strength` | `weighted_degrees`/`weighted_triangle_count` | `in + out_strength` |
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

### `nodes_join_target_lovelace` (nivel `id`)

| Columna | Definición |
|---|---|
| `target_lovelace_cta` | Target agregado por cuenta asociada al nodo |
| `target_lovelace_numcliente` | Target agregado por numcliente asociado al nodo |
| `target_lovelace` | Etiqueta final: `TARGET_SELECTION_FUNCTION` (`greatest`) sobre los modos |

### `target_propagation` (nivel `id`, un subdir por combinación de parámetros)

| Columna | Definición |
|---|---|
| `contagion_<peso>` | Score de contagio iterativo: `(1-alpha)*propio + alpha*media_ponderada_vecinos`, `max_iter` iteraciones, con suelo en la semilla si `keep_seed_floor` |
| `target_lovelace` | Semilla (target original del nodo) |

Una columna por cada `weight_type` en `WEIGHT_TYPES` (subdirs
`weight_type=<w>/alpha=<a>/max_iter=<n>/...` leídos con `mergeSchema`).

## 4. Nivel `numcliente` (VectorAssembler)

### `numcliente_features`

| Columna | Definición |
|---|---|
| `numcliente` | Cliente (explode del array de los nodos) |
| `<feature>` | Cada feature de nodo agregada a numcliente con `FEATURE_AGGREGATION` (defecto `weighted_mean` con peso `AGGREGATION_WEIGHT = oper_mto/(tfrom_days+1)`) |
| `target_lovelace*` | Etiquetas agregadas igual que las features (para entrenamiento) |
| `node_count` | Nº de nodos (`id`) distintos que aportan al numcliente |

### `numcliente_features_vector` (salida final para modelos)

| Columna | Definición |
|---|---|
| `numcliente` | Cliente del pivote externo |
| `<feature>` / `node_count` / `target_lovelace*` | Columnas crudas (nulos → `FILL_NULLS_VALUE` salvo en etiquetas) |
| `features` | `VectorAssembler` sobre todas las features **excepto** prefijos `EXCLUDE_PREFIXES` (`target_*` = etiqueta, fuera del vector) |

## 5. Intermedios del pipeline (para depuración)

| Tabla | Columnas clave |
|---|---|
| `raw_flattened` (special_treatment) | columnas CEPS + `tfrom_days`, `tfrom_months`, hora/monto normalizados |
| `group_by_txn` | `id_src`, `id_dst` + agregaciones txn: `min_oper_mto`, `max_oper_mto`, `mean_oper_mto`, `so_txn_number` |
| `group_by_id` | `id` + arrays (`numcliente`, `nom`, `cta`, `id_ban`) + `oper_mto`, `information_date` |
