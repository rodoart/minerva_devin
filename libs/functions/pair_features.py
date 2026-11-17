"""Features de proximidad de par (A, X) y de riesgo de nodo sobre el grafo.

Dos familias:

1. **Features de nodo de riesgo** (`risk_*`): dado un conjunto de semillas
   (p.ej. nodos con target=1, o con score de contagio > 0), miden la
   exposición de cada nodo a ese conjunto: fracción de vecinos semilla,
   semillas a 2 saltos, ratio de flujo saliente hacia semillas, distancia al
   fraude conocido más cercano y Personalized PageRank desde las semillas.
   Todas llevan un sufijo de semilla para coexistir "antes" (target duro) y
   "después" (scores de contagio) de la propagación.

2. **Features de par (A, X)** (`pair_*`): sobre un conjunto de pares
   candidatos (típicamente X a <= 2 saltos de una semilla A) calculan las
   proximidades topológicas clásicas de link prediction: vecinos comunes,
   Jaccard, Adamic-Adar, reciprocidad del par, preferential attachment
   (deg_out(A) * deg_in(X)), nº de caminos dirigidos de longitud 2 y
   distancia mínima A->X.

Convenciones:
- CN / Jaccard / Adamic-Adar usan vecindarios NO dirigidos (in ∪ out), como
  en la literatura de link prediction.
- Preferential attachment sigue la especificación dirigida deg_out(A) *
  deg_in(X).
- Los caminos de longitud 2 y la distancia mínima son dirigidos (sentido del
  flujo A -> X).
"""
###############################################################################
# General
###############################################################################
from typing import List

###############################################################################
# Pyspark
###############################################################################
from pyspark.sql import DataFrame
from graphframes import GraphFrame

from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import (array_intersect, array_max, array_min,
    array_union, coalesce, col, collect_set, count as spark_count, explode,
    lit, log, map_values, min as spark_min, size, sum as spark_sum,
    aggregate as array_aggregate, when)

from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)


###############################################################################
# HELPERS
###############################################################################

def undirected_neighbor_sets(edges: DataFrame) -> DataFrame:
    """Vecindario NO dirigido por nodo: (id, nbr_set, deg_undirected)."""
    neighbors = (edges
        .select(col("src").alias("id"), col("dst").alias("nbr"))
        .union(edges.select(col("dst").alias("id"), col("src").alias("nbr")))
        .filter(col("id") != col("nbr"))
        .distinct())
    return (neighbors
        .groupBy("id")
        .agg(collect_set("nbr").alias("nbr_set"),
             spark_count("*").alias("deg_undirected")))


def directed_degrees(edges: DataFrame) -> DataFrame:
    """Grados dirigidos por nodo: (id, deg_in, deg_out)."""
    out_deg = (edges.groupBy(col("src").alias("id"))
        .agg(spark_count("*").alias("deg_out")))
    in_deg = (edges.groupBy(col("dst").alias("id"))
        .agg(spark_count("*").alias("deg_in")))
    return (out_deg.join(in_deg, on="id", how="outer")
        .withColumn("deg_out", coalesce(col("deg_out"), lit(0)))
        .withColumn("deg_in", coalesce(col("deg_in"), lit(0))))


def candidate_pairs_within_hops(
    edges: DataFrame,
    seed_ids: List[str],
    max_hops: int = 2,
) -> DataFrame:
    """Pares candidatos (a, x): `a` es semilla y `x` está a <= `max_hops` dirigidos.

    Devuelve (a, x, hops) con `hops` = distancia mínima dirigida a->x dentro
    del límite (1 si ya son vecinos directos, 2 si no). Sin self-pairs.
    """
    one_hop = (edges
        .filter(col("src").isin(list(seed_ids)))
        .select(col("src").alias("a"), col("dst").alias("x"))
        .filter(col("a") != col("x"))
        .withColumn("hops", lit(1)))
    pairs = one_hop
    if max_hops >= 2:
        e1 = edges.select(col("src").alias("a"), col("dst").alias("z"))
        e2 = edges.select(col("src").alias("z"), col("dst").alias("x"))
        two_hop = (e1.join(e2, on="z")
            .filter(col("a").isin(list(seed_ids)))
            .filter(col("a") != col("x"))
            .select("a", "x")
            .withColumn("hops", lit(2)))
        pairs = pairs.unionByName(two_hop)
    return pairs.groupBy("a", "x").agg(spark_min("hops").alias("hops"))


###############################################################################
# NODE RISK FEATURES (semilla -> sufijo de columna)
###############################################################################

def seed_neighbor_stats(
    edges: DataFrame,
    seeds: DataFrame,
    suffix: str,
    seed_column: str = "seed_score",
) -> DataFrame:
    """Exposición directa de cada nodo a las semillas (vecinos a 1 salto).

    `seeds`: (id, seed_score) con el score/etiqueta de cada nodo (0/1 duro o
    score continuo). Columnas de salida (una fila por nodo CON aristas):

    - `risk_in_sum_<suffix>` / `risk_out_sum_<suffix>`: suma de scores de los
      vecinos que apuntan al nodo / a los que el nodo apunta (versión soft;
      con target duro es el nº de vecinos fraudulentos).
    - `risk_in_frac_<suffix>` / `risk_out_frac_<suffix>`: fracción de vecinos
      con score > 0 (versión dura: % de in-neighbors de X marcados fraude).
    """
    seed_hard = (seeds.select("id", col(seed_column).alias("_score"))
        .withColumn("_hard", when(col("_score") > 0, 1).otherwise(0)))
    # in-neighbors del nodo (v recibe de u): stats de u agrupadas por v
    in_stats = (edges
        .join(seed_hard, edges["src"] == seed_hard["id"], "left")
        .groupBy(col("dst").alias("id"))
        .agg(spark_sum(coalesce(col("_score"), lit(0.0))).alias(f"risk_in_sum_{suffix}"),
             spark_sum(coalesce(col("_hard"), lit(0))).alias("_in_hard"),
             spark_count("*").alias("_in_deg"))
        .withColumn(f"risk_in_frac_{suffix}",
            when(col("_in_deg") > 0, col("_in_hard") / col("_in_deg"))
            .otherwise(lit(None).cast("double")))
        .drop("_in_hard", "_in_deg"))
    # out-neighbors (u apunta a v): stats de v agrupadas por u
    out_stats = (edges
        .join(seed_hard, edges["dst"] == seed_hard["id"], "left")
        .groupBy(col("src").alias("id"))
        .agg(spark_sum(coalesce(col("_score"), lit(0.0))).alias(f"risk_out_sum_{suffix}"),
             spark_sum(coalesce(col("_hard"), lit(0))).alias("_out_hard"),
             spark_count("*").alias("_out_deg"))
        .withColumn(f"risk_out_frac_{suffix}",
            when(col("_out_deg") > 0, col("_out_hard") / col("_out_deg"))
            .otherwise(lit(None).cast("double")))
        .drop("_out_hard", "_out_deg"))
    return in_stats.join(out_stats, on="id", how="outer")


def seed_two_hop_counts(
    edges: DataFrame,
    seed_ids: List[str],
    suffix: str,
) -> DataFrame:
    """Nº de nodos semilla a EXACTAMENTE 2 saltos (no dirigido) de cada nodo.

    Columna `risk_2hop_<suffix>`: semillas alcanzables en dos aristas (in ∪
    out) que no son ya vecinas directas — el vecindario de segundo orden del
    fraude.
    """
    undirected = (edges
        .select(col("src").alias("u"), col("dst").alias("v"))
        .union(edges.select(col("dst").alias("u"), col("src").alias("v")))
        .filter(col("u") != col("v"))
        .distinct())
    e1 = undirected.select(col("u").alias("id"), col("v").alias("z"))
    e2 = undirected.select(col("u").alias("z"), col("v").alias("seed2"))
    two_hop = (e1.join(e2, on="z")
        .select("id", col("seed2").alias("seed"))
        .filter(col("id") != col("seed")))
    # excluir semillas que ya son vecinas directas
    two_hop = two_hop.join(
        undirected.select(col("u").alias("id"), col("v").alias("seed")),
        on=["id", "seed"], how="left_anti")
    return (two_hop
        .filter(col("seed").isin(list(seed_ids)))
        .groupBy("id")
        .agg(spark_count("*").alias(f"risk_2hop_{suffix}")))


def seed_flow_ratio(
    edges: DataFrame,
    seed_ids: List[str],
    suffix: str,
    weight_column: str = "count_txn",
) -> DataFrame:
    """Ratio de flujo del nodo que toca semillas (tasa de fraude histórica).

    Con `weight_column` = count_txn: fracción de las transacciones EMITIDAS
    por A que van a nodos semilla (`risk_outflow_<suffix>`) y fracción de las
    RECIBIDAS que vienen de semillas (`risk_inflow_<suffix>`). Con el target
    duro como semilla, `risk_outflow_<suffix>` es la tasa histórica de A como
    emisor hacia fraude. Si la columna de peso no existe, cada arista pesa 1.
    """
    weight = (coalesce(col(weight_column), lit(1.0))
        if weight_column in edges.columns else lit(1.0))
    out_flow = (edges
        .groupBy(col("src").alias("id"))
        .agg(spark_sum(weight).alias("_out_tot"),
             spark_sum(when(col("dst").isin(list(seed_ids)), weight)
                 .otherwise(lit(0.0))).alias("_out_seed"))
        .withColumn(f"risk_outflow_{suffix}",
            when(col("_out_tot") > 0, col("_out_seed") / col("_out_tot"))
            .otherwise(lit(None).cast("double")))
        .drop("_out_tot", "_out_seed"))
    in_flow = (edges
        .groupBy(col("dst").alias("id"))
        .agg(spark_sum(weight).alias("_in_tot"),
             spark_sum(when(col("src").isin(list(seed_ids)), weight)
                 .otherwise(lit(0.0))).alias("_in_seed"))
        .withColumn(f"risk_inflow_{suffix}",
            when(col("_in_tot") > 0, col("_in_seed") / col("_in_tot"))
            .otherwise(lit(None).cast("double")))
        .drop("_in_tot", "_in_seed"))
    return out_flow.join(in_flow, on="id", how="outer")


def _reversed_graph(graph: GraphFrame) -> GraphFrame:
    """Grafo con las aristas invertidas (para distancias "desde" semillas)."""
    return GraphFrame(
        graph.vertices,
        graph.edges.select(
            col("dst").alias("src"), col("src").alias("dst")))


def seed_distance(
    graph: GraphFrame,
    seed_ids: List[str],
    suffix: str,
    direction: str = "to",
) -> DataFrame:
    """Distancia dirigida mínima entre cada nodo y la semilla más cercana.

    `shortestPaths` de GraphFrames calcula, para cada vértice v, la distancia
    DESDE v HACIA cada landmark. Con `direction="to"` la feature
    `risk_dist_<suffix>` es la distancia del nodo HACIA la semilla más
    cercana siguiendo el flujo (¿a cuántos saltos puede mandar dinero al
    fraude?); con `direction="from"` se invierten las aristas y mide la
    distancia DESDE la semilla HACIA el nodo (¿a cuántos saltos llega el
    dinero del fraude a este nodo?). null = inalcanzable.
    """
    if direction not in ("to", "from"):
        raise ValueError(f"direction inválida: {direction!r}")
    if not seed_ids:
        return graph.vertices.select("id").withColumn(
            f"risk_dist_{suffix}", lit(None).cast("int"))
    g = graph if direction == "to" else _reversed_graph(graph)
    sp = g.shortestPaths(landmarks=list(seed_ids))
    return (sp
        .select("id", col("distances").alias("_dist"))
        .withColumn(f"risk_dist_{suffix}",
            when(size(col("_dist")) > 0, array_min(map_values(col("_dist"))))
            .otherwise(lit(None).cast("int")))
        .drop("_dist"))


def seed_personalized_pagerank(
    graph: GraphFrame,
    seed_ids: List[str],
    suffix: str,
    max_iter: int = 10,
    reset_prob: float = 0.15,
    max_seeds: int = 5000,
) -> DataFrame:
    """Personalized PageRank paralelo desde las semillas.

    `parallelPersonalizedPageRank` devuelve un array de scores por nodo (uno
    por semilla, alineado con `sourceIds`); se agrega a
    `risk_ppr_max_<suffix>` y `risk_ppr_mean_<suffix>`. Con más de
    `max_seeds` semillas se usa una muestra determinista (hash).
    """
    seeds = list(seed_ids)
    if len(seeds) > max_seeds:
        logger.warning(
            "PPR: %d semillas exceden max_seeds=%d; muestra determinista",
            len(seeds), max_seeds)
        seeds = sorted(seeds, key=hash)[:max_seeds]
    if not seeds:
        return (graph.vertices.select("id")
            .withColumn(f"risk_ppr_max_{suffix}", lit(None).cast("double"))
            .withColumn(f"risk_ppr_mean_{suffix}", lit(None).cast("double")))
    ppr = graph.parallelPersonalizedPageRank(
        maxIter=max_iter, resetProbability=reset_prob, sourceIds=seeds)
    # `pageranks` es un vector ML (posiblemente disperso): se pasa a array
    # denso alineado con `sourceIds` antes de agregar.
    return (ppr.vertices
        .select("id", vector_to_array(col("pageranks")).alias("_ppr"))
        .withColumn(f"risk_ppr_max_{suffix}", array_max(col("_ppr")))
        .withColumn(f"risk_ppr_mean_{suffix}",
            array_aggregate(col("_ppr"), lit(0.0), lambda acc, x: acc + x)
                / lit(float(len(seeds))))
        .drop("_ppr"))


###############################################################################
# PAIR PROXIMITY FEATURES (pares candidatos (a, x))
###############################################################################

def pair_proximity_features(
    edges: DataFrame,
    pairs: DataFrame,
) -> DataFrame:
    """Proximidades topológicas de cada par candidato (a, x).

    Columnas de salida (una fila por (a, x)):

    - `pair_common_neighbors`: |N(a) ∩ N(x)| (no dirigido).
    - `pair_jaccard`: |N(a) ∩ N(x)| / |N(a) ∪ N(x)| (no dirigido).
    - `pair_adamic_adar`: sum_{z en N(a) ∩ N(x)} 1/log(deg(z)) (deg>1).
    - `pair_preferential_attachment`: deg_out(a) * deg_in(x) (dirigido).
    - `pair_reciprocity`: 1 si existen AMBAS direcciones a<->x.
    - `pair_paths_len2`: nº de caminos dirigidos a -> z -> x.
    """
    nbrs = undirected_neighbor_sets(edges)
    degrees = directed_degrees(edges)

    result = (pairs
        .join(nbrs, pairs["a"] == nbrs["id"], "left")
        .withColumnRenamed("nbr_set", "_nbr_a")
        .drop("id")
        .join(nbrs, pairs["x"] == nbrs["id"], "left")
        .withColumnRenamed("nbr_set", "_nbr_x")
        .drop("id")
        .withColumn("_common", size(array_intersect(col("_nbr_a"), col("_nbr_x"))))
        .withColumn("_union", size(array_union(col("_nbr_a"), col("_nbr_x"))))
        .withColumn("pair_common_neighbors", col("_common"))
        .withColumn("pair_jaccard",
            when(col("_union") > 0, col("_common") / col("_union"))
            .otherwise(lit(None).cast("double"))))

    # Adamic-Adar: explode de la intersección + grado del vecino común
    common = (result
        .select("a", "x",
            explode(array_intersect(col("_nbr_a"), col("_nbr_x"))).alias("z")))
    aa = (common
        .join(nbrs.select(col("id").alias("z"),
                col("deg_undirected").alias("_deg_z")), on="z")
        .filter(col("_deg_z") > 1)
        .groupBy("a", "x")
        .agg(spark_sum(lit(1.0) / log(col("_deg_z"))).alias("pair_adamic_adar")))
    result = (result.join(aa, on=["a", "x"], how="left")
        .withColumn("pair_adamic_adar", coalesce(col("pair_adamic_adar"), lit(0.0))))

    # Preferential attachment dirigido: deg_out(a) * deg_in(x)
    pa = (pairs
        .join(degrees.select("id", "deg_out"), pairs["a"] == degrees["id"], "left")
        .drop("id")
        .withColumnRenamed("deg_out", "_deg_out_a")
        .join(degrees.select("id", "deg_in"), pairs["x"] == degrees["id"], "left")
        .drop("id")
        .withColumnRenamed("deg_in", "_deg_in_x")
        .withColumn("pair_preferential_attachment",
            coalesce(col("_deg_out_a"), lit(0)) * coalesce(col("_deg_in_x"), lit(0)))
        .select("a", "x", "pair_preferential_attachment"))
    result = result.join(pa, on=["a", "x"], how="left")

    # Reciprocidad del par: existen a->x y x->a
    directed_pairs = edges.select("src", "dst").distinct()
    fwd = (directed_pairs
        .select(col("src").alias("a"), col("dst").alias("x"))
        .withColumn("_fwd", lit(1)))
    bwd = (directed_pairs
        .select(col("src").alias("x"), col("dst").alias("a"))
        .withColumn("_bwd", lit(1)))
    result = (result
        .join(fwd, on=["a", "x"], how="left")
        .join(bwd, on=["a", "x"], how="left")
        .withColumn("pair_reciprocity",
            when((col("_fwd") == 1) & (col("_bwd") == 1), 1).otherwise(0))
        .drop("_fwd", "_bwd"))

    # Caminos dirigidos de longitud 2: a -> z -> x
    e1 = edges.select(col("src").alias("a"), col("dst").alias("z"))
    e2 = edges.select(col("src").alias("z"), col("dst").alias("x"))
    paths2 = (e1.join(e2, on="z")
        .join(pairs.select("a", "x"), on=["a", "x"], how="inner")
        .groupBy("a", "x")
        .agg(spark_count("*").alias("pair_paths_len2")))
    result = (result.join(paths2, on=["a", "x"], how="left")
        .withColumn("pair_paths_len2", coalesce(col("pair_paths_len2"), lit(0))))

    return result.drop("_nbr_a", "_nbr_x", "_common", "_union")


def pair_min_distance(
    graph: GraphFrame,
    pairs: DataFrame,
    seed_ids: List[str],
) -> DataFrame:
    """Distancia dirigida mínima A -> X para cada par (A semilla).

    `shortestPaths` calcula distancias HACIA los landmarks, así que se corre
    sobre el grafo con aristas invertidas: el mapa {semilla: distancia} de X
    contiene entonces la distancia A -> X en el grafo original, y la feature
    es `_dist[a]` (null = X inalcanzable desde A en el sentido del flujo).
    """
    if not seed_ids:
        return pairs.withColumn("pair_min_distance", lit(None).cast("int"))
    sp = (_reversed_graph(graph).shortestPaths(landmarks=list(seed_ids))
        .select("id", col("distances").alias("_dist")))
    return (pairs
        .join(sp, pairs["x"] == sp["id"], "left")
        .drop("id")
        .withColumn("pair_min_distance", col("_dist").getItem(col("a")))
        .drop("_dist"))


def aggregate_pair_features_to_node(
    pair_features: DataFrame,
    node_role: str = "x",
    stats: tuple = ("max", "mean"),
    prefixes: tuple = ("pair_",),
) -> DataFrame:
    """Agrega las features de par a nivel nodo (por defecto al lado X).

    Cada columna con alguno de los prefijos `prefixes` (p.ej. `pair_`, `tmp_`)
    produce `<f>_<stat>` agregada sobre los pares donde el nodo juega
    `node_role` — es la forma en que las features de par entran al vector a
    nivel nodo del ensamblador.
    """
    from pyspark.sql.functions import max as spark_max, mean as spark_mean
    stat_funcs = {"max": spark_max, "mean": spark_mean}
    feature_columns = [c for c in pair_features.columns
        if c.startswith(prefixes)]
    return (pair_features
        .groupBy(col(node_role).alias("id"))
        .agg(*[stat_funcs[stat](col(c)).alias(f"{c}_{stat}")
               for c in feature_columns for stat in stats]))
