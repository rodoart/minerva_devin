# Preguntas que se hacen con grafos y features que las responden

Documento de diseño del módulo de features de grafo
(`pipelines/features/graph_features.py`, `libs/functions/features.py`,
`config/features/ceps/graph_features.py`).

La idea: cada familia de features responde a una clase de preguntas de
negocio sobre el comportamiento transaccional. Las columnas concretas que
produce cada función están entre paréntesis.

---

## 1. Importancia / centralidad

> ¿Qué nodos concentran el flujo de dinero de la red? ¿Quién es un "hub"?

| Pregunta | Feature | Columnas |
|---|---|---|
| ¿Qué nodos reciben más "reputación" estructural? | `pagerank` | `pagerank` |
| ¿Qué nodos reciben más reputación ponderada por monto? | `weighted_pagerank` | `weighted_pagerank` |
| ¿Cuántas contrapartes distintas envía/recibe un nodo? | `degrees` | `in_degree`, `out_degree`, `total_degree` |
| ¿Cuánto dinero mueve un nodo (in/out/total)? | `weighted_degrees` | `in_strength`, `out_strength`, `total_strength` |

## 2. Rol direccional del nodo

> ¿El nodo es origen neto de fondos (sumidero opuesto: money mule que dispersa)
> o destino neto (cuenta que concentra)?

| Pregunta | Feature | Columnas |
|---|---|---|
| ¿Envía más aristas de las que recibe? | `degree_balance` | `net_degree`, `in_out_degree_ratio` |
| ¿Mueve más monto saliente que entrante? | `weighted_degree_balance` | `net_strength`, `in_out_strength_ratio` |
| ¿Las relaciones son bidireccionales (comercio real) o unidireccionales (canalización de fondos)? | `reciprocity` | `reciprocal_out`, `reciprocal_in`, `reciprocity_out`, `reciprocity_in`, `reciprocity` |
| ¿El nodo se envía dinero a sí mismo (self-transfers)? | `self_loops` | `self_loop_count` |

## 3. Distribución de los montos por nodo

> ¿Cómo se reparte el dinero entre las aristas de un nodo? ¿Es regular
> (nómina) o irregular (rafagas de fraude)?

| Pregunta | Feature | Columnas |
|---|---|---|
| ¿Cuál es el peso mín/máx/medio de sus operaciones? | `weighted_edge_stats` | `min_weight`, `max_weight`, `mean_weight` |
| ¿Qué dispersión tienen sus montos? | `weighted_edge_stats` | `std_weight` |
| ¿Cuántas operaciones y cuántas aristas distintas? | `weighted_edge_stats` | `count_weight`, `countDistinct_weight`, `sum_weight` |
| ¿Asimetría/apuntamiento de la distribución de montos? | `weighted_edge_stats` | `skew_weight`, `curt_weight`, `so_weight` |

## 4. Pertenencia a estructuras

> ¿El nodo pertenece a un grupo aislado o a la componente gigante? ¿A un
> grupo cerrado y denso (patrón de fraude organizado)?

| Pregunta | Feature | Columnas |
|---|---|---|
| ¿A qué componente conexa pertenece? | `components` | `component_id` |
| ¿Cuál es el monto total de su componente? | `weighted_components` | `component`, `component_weight` |
| ¿En cuántos triángulos participa (densidad local)? | `triangle_count` / `weighted_triangle_count` | `triangle_count`, `total_strength` |
| ¿A qué sub-grupo dirigido (SCC) pertenece? | `subcluster` (cluster_features) | `scc` |

## 5. Estadísticos intra-grupo (cluster features)

> ¿Cómo se comporta el nodo comparado con su grupo? ¿Cuál es la media de
> fraude de su componente?

| Pregunta | Feature | Columnas |
|---|---|---|
| ¿Cuál es el tamaño de su grupo? | `cluster_group_stats` | `cluster_<grupo>_size` |
| ¿Cuál es la media/mediana/std de cada variable en su grupo? | `cluster_group_stats` | `cluster_<grupo>_<stat>_<col>` (count/sum/mean/median/std/min/max) |
| ¿Cuál es la tasa de fraude (target) y de target propagado del grupo? | `cluster_group_stats` sobre `target_*` y `contagion_*` | `cluster_*_mean_target_*`, `cluster_*_mean_contagion_*` |
| ¿Cuánto monto/weight acumula el grupo? | `cluster_group_stats` sobre `oper_mto`, `*_strength`, `*_weight` | `cluster_*_sum_oper_mto`, ... |

`REQUIRED_STATS_PREFIXES` (config) garantiza que `target_`, `contagion_`,
`oper_mto`, `weight` y `_strength` entren siempre en estos estadísticos.

## 6. Contagio / propagación del target

> ¿Qué tan cerca está el nodo de fraude conocido, aunque él no tenga etiqueta?

| Pregunta | Feature | Columnas |
|---|---|---|
| ¿Cuál es el score de contagio por cada tipo de peso? | `target_propagation` (contagion) | `contagion_<weight_type>` |
| ¿Y propagando otras señales (scores de modelos)? | `target_propagation` multi-columna | `contagion_<col>_<weight_type>` |
| ¿Cuál es la etiqueta semilla del nodo? | join de targets | `target_lovelace`, `target_lovelace_<col>` |

## 7. Identidad multi-nivel (cta / numcliente / nom / id_ban)

> La misma entidad puede aparecer en varios nodos; ¿cómo se agrega?

| Pregunta | Mecanismo | Resultado |
|---|---|---|
| ¿Qué numclientes/ctas cubre un nodo? | `group_by_id` | arrays `numcliente`, `cta`, `nom`, `id_ban` por `id` |
| ¿Features por cliente y por cuenta? | vector assembler multi-nivel | `numcliente_features_vector`, `cta_features_vector` |
| ¿Media/mediana/std ponderadas por monto x antigüedad? | `FEATURE_AGGREGATION` multi-función | `<func>_<feature>_ceps` |

---

## Preguntas frecuentes todavía no implementadas (backlog)

| Pregunta | Feature candidata |
|---|---|
| ¿Qué tan agrupado está el vecindario del nodo? (clustering coefficient) | `triangle_count / (k(k-1)/2)` — ya hay `triangle_count` |
| ¿El nodo es un cuello de botella entre grupos? | betweenness centrality (costoso) |
| ¿Distancia al fraude conocido más cercano? | shortest path a semillas |
| ¿El nodo está en el núcleo denso k del grafo? | `k_core` (ya hay output reservado en config) |
| ¿Variación temporal de su grado/monto? | features de serie temporal por mes (`*_monthly` ya disponible) |
