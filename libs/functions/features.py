"""Features de grafo (GraphFrame -> DataFrame): métricas no ponderadas, ponderadas
y propagación iterativa del target, más el registro `GRAPH_FEATURE_FUNCTIONS`.
"""
###############################################################################
# GRAPH FEATURE FUNCTIONS
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Callable, List, Dict, Optional, Union


# ------------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame, Column
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM


from pyspark.sql.functions import (coalesce, col,
    lit, mean as spark_mean,
    min as spark_min, max as spark_max, sum as spark_sum, count as spark_count,
    countDistinct, stddev as spark_std,
    explode, when, greatest, concat_ws, pmod, hash as spark_hash)

import libs.functions.aggregations as lfa

# Buckets de sal por defecto para los groupBy por nodo sobre aristas: los
# supernodos (hubs con millones de aristas) sesgan cualquier agregación por
# `id`. Las funciones aceptan `salt_buckets` por parámetro: None -> este
# valor; <= 1 -> groupBy directo de una etapa.
EDGE_GROUPBY_SALT_BUCKETS = 64

_EDGE_KEY_COLUMN = "__ekey"


def _resolve_salt(salt_buckets:Optional[int]) -> int:
    """Resuelve el nº de buckets de sal: None -> default del módulo."""
    return EDGE_GROUPBY_SALT_BUCKETS if salt_buckets is None else salt_buckets


def _salted_count_by_node(
    df:DataFrame,
    node_column:str,
    salt_source:str,
    salt_buckets:int,
    out_column:str,
) -> DataFrame:
    """`groupBy(node).count()` en dos etapas con sal -> (id, out_column)."""
    return (lfa.merge_salted_stats(
        lfa.salted_partial_stats(
            df, node_column, [], salt_buckets=salt_buckets,
            id_column=salt_source),
        node_column, [], [], size_column=out_column)
        .withColumnRenamed(node_column, "id"))


def _salted_sum_by_node(
    df:DataFrame,
    node_column:str,
    value_column:str,
    salt_source:str,
    salt_buckets:int,
    out_column:str,
) -> DataFrame:
    """`groupBy(node).sum(value)` en dos etapas con sal -> (id, out_column)."""
    return (lfa.merge_salted_stats(
        lfa.salted_partial_stats(
            df, node_column, [value_column], salt_buckets=salt_buckets,
            id_column=salt_source),
        node_column, [value_column], ["sum"])
        .withColumnRenamed(node_column, "id")
        .withColumnRenamed(f"sum_{value_column}", out_column))


###############################################################################
# UNWEIGHTED FEATURES
###############################################################################

def pagerank(
    graph:GraphFrame,
    max_iter:int = 10,
    reset_prob:float = 0.15
) -> DataFrame:
    """PageRank estándar por nodo."""
    return graph.pageRank(maxIter=max_iter, resetProbability=reset_prob).vertices


def degrees(
    graph:GraphFrame,
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Grado de entrada, salida y total por nodo.

    Con `salt_buckets`>1 los conteos van en dos etapas con sal (equivale a
    `inDegrees`/`outDegrees` pero sin que un supernodo concentre el shuffle);
    None -> `EDGE_GROUPBY_SALT_BUCKETS`.
    """
    buckets = _resolve_salt(salt_buckets)
    if buckets > 1:
        keyed = graph.edges.withColumn(
            _EDGE_KEY_COLUMN, concat_ws("||", col("src"), col("dst")))
        in_degrees_df = _salted_count_by_node(
            keyed, "dst", _EDGE_KEY_COLUMN, buckets, "in_degree")
        out_degrees_df = _salted_count_by_node(
            keyed, "src", _EDGE_KEY_COLUMN, buckets, "out_degree")
    else:
        in_degrees_df = graph.inDegrees.withColumnRenamed("inDegree", "in_degree")
        out_degrees_df = graph.outDegrees.withColumnRenamed("outDegree", "out_degree")
    degrees_df = (
        in_degrees_df.join(out_degrees_df, on="id", how="outer")
        .withColumn("in_degree", coalesce(col("in_degree"), lit(0)))
        .withColumn("out_degree", coalesce(col("out_degree"), lit(0)))
        .withColumn("total_degree", col("in_degree") + col("out_degree"))
    )
    return degrees_df


def components(
    graph:GraphFrame
) -> DataFrame:
    """Componente conexa a la que pertenece cada nodo."""
    return graph.connectedComponents().withColumnRenamed("component", "component_id")


def triangle_count(
    graph:GraphFrame
) -> DataFrame:
    """Número de triángulos en los que participa cada nodo."""
    return graph.triangleCount().withColumnRenamed("count", "triangle_count")


def degree_balance(
    graph:GraphFrame,
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Balance direccional del grado por nodo.

    Columnas: `net_degree` (out - in; >0 sumidero... origen neto de flujo) y
    `in_out_degree_ratio` (out/in; null cuando el nodo no recibe nada).
    Con `salt_buckets`>1 los conteos por nodo van en dos etapas con sal
    (anti-supernodos); None -> `EDGE_GROUPBY_SALT_BUCKETS`.
    """
    edges = graph.edges
    buckets = _resolve_salt(salt_buckets)
    if buckets > 1:
        keyed = edges.withColumn(
            _EDGE_KEY_COLUMN, concat_ws("||", col("src"), col("dst")))
        in_deg = _salted_count_by_node(
            keyed, "dst", _EDGE_KEY_COLUMN, buckets, "_in")
        out_deg = _salted_count_by_node(
            keyed, "src", _EDGE_KEY_COLUMN, buckets, "_out")
    else:
        in_deg = edges.groupBy(col("dst").alias("id")).agg(spark_count("*").alias("_in"))
        out_deg = edges.groupBy(col("src").alias("id")).agg(spark_count("*").alias("_out"))
    return (
        in_deg.join(out_deg, on="id", how="outer")
        .withColumn("_in", coalesce(col("_in"), lit(0)))
        .withColumn("_out", coalesce(col("_out"), lit(0)))
        .withColumn("net_degree", col("_out") - col("_in"))
        .withColumn("in_out_degree_ratio",
            when(col("_in") > 0, col("_out") / col("_in")).otherwise(lit(None).cast("double")))
        .select("id", "net_degree", "in_out_degree_ratio")
    )


def reciprocity(
    graph:GraphFrame,
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Reciprocidad por nodo sobre pares dirigidos únicos (sin self-loops).

    Una arista src->dst es recíproca si existe dst->src. Columnas:
    `reciprocal_out`/`reciprocal_in` (nº de vecinos que también enlazan de
    vuelta en cada dirección), `reciprocity_out`/`reciprocity_in`
    (fracción sobre el grado de cada dirección) y `reciprocity`
    (fracción de las aristas incidentes que son recíprocas).
    """
    pairs = (graph.edges
        .select("src", "dst")
        .distinct()
        .filter(col("src") != col("dst")))
    reversed_pairs = pairs.select(col("dst").alias("src"), col("src").alias("dst"))
    reciprocal = pairs.join(reversed_pairs, ["src", "dst"], "inner")
    #
    buckets = _resolve_salt(salt_buckets)
    if buckets > 1:
        keyed_pairs = pairs.withColumn(
            _EDGE_KEY_COLUMN, concat_ws("||", col("src"), col("dst")))
        keyed_rec = reciprocal.withColumn(
            _EDGE_KEY_COLUMN, concat_ws("||", col("src"), col("dst")))
        out_stats = _salted_count_by_node(
            keyed_pairs, "src", _EDGE_KEY_COLUMN, buckets, "_out")
        in_stats = _salted_count_by_node(
            keyed_pairs, "dst", _EDGE_KEY_COLUMN, buckets, "_in")
        rec_out = _salted_count_by_node(
            keyed_rec, "src", _EDGE_KEY_COLUMN, buckets, "reciprocal_out")
        rec_in = _salted_count_by_node(
            keyed_rec, "dst", _EDGE_KEY_COLUMN, buckets, "reciprocal_in")
    else:
        out_stats = pairs.groupBy(col("src").alias("id")).agg(spark_count("*").alias("_out"))
        in_stats = pairs.groupBy(col("dst").alias("id")).agg(spark_count("*").alias("_in"))
        rec_out = reciprocal.groupBy(col("src").alias("id")).agg(spark_count("*").alias("reciprocal_out"))
        rec_in = reciprocal.groupBy(col("dst").alias("id")).agg(spark_count("*").alias("reciprocal_in"))
    #
    result = (
        out_stats.join(in_stats, on="id", how="outer")
        .join(rec_out, on="id", how="left")
        .join(rec_in, on="id", how="left")
        .withColumn("_out", coalesce(col("_out"), lit(0)))
        .withColumn("_in", coalesce(col("_in"), lit(0)))
        .withColumn("reciprocal_out", coalesce(col("reciprocal_out"), lit(0)))
        .withColumn("reciprocal_in", coalesce(col("reciprocal_in"), lit(0)))
        .withColumn("reciprocity_out",
            when(col("_out") > 0, col("reciprocal_out") / col("_out")).otherwise(lit(None).cast("double")))
        .withColumn("reciprocity_in",
            when(col("_in") > 0, col("reciprocal_in") / col("_in")).otherwise(lit(None).cast("double")))
        .withColumn("reciprocity",
            when((col("_in") + col("_out")) > 0,
                (col("reciprocal_in") + col("reciprocal_out")) / (col("_in") + col("_out")))
            .otherwise(lit(None).cast("double")))
        .select("id", "reciprocal_out", "reciprocal_in",
            "reciprocity_out", "reciprocity_in", "reciprocity")
    )
    return result


def self_loops(
    graph:GraphFrame,
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Número de aristas src==dst por nodo (`self_loop_count`; 0 si no hay)."""
    loops_df = graph.edges.filter(col("src") == col("dst"))
    buckets = _resolve_salt(salt_buckets)
    if buckets > 1:
        loops = _salted_count_by_node(
            loops_df, "src", "dst", buckets, "self_loop_count")
    else:
        loops = (loops_df
            .groupBy(col("src").alias("id"))
            .agg(spark_count("*").alias("self_loop_count")))
    return (graph.vertices.select("id")
        .join(loops, on="id", how="left")
        .withColumn("self_loop_count", coalesce(col("self_loop_count"), lit(0))))


###############################################################################
# WEIGHTED FEATURES
###############################################################################

def weighted_pagerank(
    graph: GraphFrame,
    max_iter: int = 10,
    reset_prob: float = 0.15
) -> DataFrame:
    """PageRank ponderado por el peso de las aristas."""
    return (
        graph
        .pageRank(maxIter=max_iter, resetProbability=reset_prob)
        .vertices
        .withColumnRenamed("pagerank", "weighted_pagerank")
    )


def weighted_degrees(
    graph: GraphFrame,
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Fuerza (strength) ponderada: suma de pesos entrantes/salientes por nodo.

    Con `salt_buckets`>1 las sumas por nodo van en dos etapas con sal
    (anti-supernodos); None -> `EDGE_GROUPBY_SALT_BUCKETS`.
    """
    edges = graph.edges
    buckets = _resolve_salt(salt_buckets)
    if buckets > 1:
        keyed = edges.withColumn(
            _EDGE_KEY_COLUMN, concat_ws("||", col("src"), col("dst")))
        in_strength = _salted_sum_by_node(
            keyed, "dst", "weight", _EDGE_KEY_COLUMN, buckets, "in_strength")
        out_strength = _salted_sum_by_node(
            keyed, "src", "weight", _EDGE_KEY_COLUMN, buckets, "out_strength")
    else:
        in_strength = (
            edges.groupBy(col("dst").alias("id"))
            .agg(spark_sum("weight").alias("in_strength"))
        )
        out_strength = (
            edges.groupBy(col("src").alias("id"))
            .agg(spark_sum("weight").alias("out_strength"))
        )
    return (
        in_strength.join(out_strength, on="id", how="outer")
        .withColumn("in_strength", coalesce(col("in_strength"), lit(0.0)))
        .withColumn("out_strength", coalesce(col("out_strength"), lit(0.0)))
        .withColumn("total_strength", col("in_strength") + col("out_strength"))
    )


# Catálogo de agregaciones estándar sobre la columna de peso de las aristas.
STANDARD_WEIGHT_STATS: Dict[str, Callable[..., Column]] = {
    "min": spark_min,
    "max": spark_max,
    "mean": lambda c: spark_mean(col(c)),
    "std": lambda c: coalesce(spark_std(col(c)), lit(0.0)),
    "count": spark_count,
    "countDistinct": countDistinct,
    "sum": spark_sum,
    "curt": lambda c: spark_sum(col(c)**3)/spark_sum(col(c)**2)**(3/2),
    "skew": lambda c: spark_sum(col(c)**3)/spark_sum(col(c)**2)**(3/2),
    "so": lambda c: spark_sum(col(c)**4)/spark_sum(col(c)**2)**(4/2)
}


def weighted_edge_stats(
    graph: GraphFrame,
    aggregations:Optional[List[Column]] = None,
    stats:Optional[Dict[str, Callable[..., Column]]] = None,
    weight_column:str = "weight",
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Estadísticas de peso de aristas incidentes por nodo (in + out).

    Con `salt_buckets`>1 y un catálogo `stats` con nombre, los stats
    combinables (count/sum/min/max/mean/std y los ratios de momentos
    curt/skew/so) van en dos etapas con sal — los supernodos no concentran
    el shuffle — y el resto (countDistinct, customs) por la vía directa
    sobre el mismo frame, unidos por `id`. Con `aggregations` crudas
    (expresiones `Column` sin nombre) se usa siempre el groupBy directo:
    no hay forma de saber si son combinables.
    """
    if aggregations is None:
        if stats is None:
            stats = STANDARD_WEIGHT_STATS
        aggregations = [
            func(weight_column).alias(f"{func_name}_{weight_column}")
            for func_name, func in stats.items()
        ]
    #
    edges = graph.edges
    buckets = _resolve_salt(salt_buckets)
    if stats is not None and buckets > 1:
        keyed_edges = edges.withColumn(
            _EDGE_KEY_COLUMN, concat_ws("||", col("src"), col("dst")))
        incident_keyed = (
            keyed_edges.select(
                col("src").alias("id"), col(weight_column), _EDGE_KEY_COLUMN)
            .union(keyed_edges.select(
                col("dst").alias("id"), col(weight_column), _EDGE_KEY_COLUMN))
        )
        combinable, other = lfa.split_stats_by_combinable(stats)
        frames:List[DataFrame] = []
        if combinable:
            frames.append(lfa.merge_salted_stats(
                lfa.salted_partial_stats(
                    incident_keyed, "id", [weight_column],
                    salt_buckets=buckets, id_column=_EDGE_KEY_COLUMN,
                    max_moment=lfa.stats_required_moment(combinable)),
                "id", [weight_column], combinable))
        if other:
            frames.append(lfa.plain_group_stats(
                incident_keyed.drop(_EDGE_KEY_COLUMN),
                "id", [weight_column], other))
        result = frames[0]
        for frame in frames[1:]:
            result = result.join(frame, on="id", how="outer")
        return result
    #
    incident = (
        edges.select(col("src").alias("id"), col(weight_column))
        .union(edges.select(col("dst").alias("id"), col(weight_column)))
    )
    return (
        incident.groupBy("id")
        .agg(
            *aggregations
        )
    )


def weighted_degree_balance(
    graph: GraphFrame,
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Balance direccional de la fuerza ponderada por nodo.

    Columnas: `net_strength` (out_strength - in_strength) e
    `in_out_strength_ratio` (out/in; null cuando no entra flujo).
    """
    strength = weighted_degrees(graph, salt_buckets=salt_buckets)
    return (strength
        .withColumn("net_strength", col("out_strength") - col("in_strength"))
        .withColumn("in_out_strength_ratio",
            when(col("in_strength") > 0, col("out_strength") / col("in_strength"))
            .otherwise(lit(None).cast("double")))
        .select("id", "net_strength", "in_out_strength_ratio")
    )


def weighted_components(
    graph: GraphFrame,
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Componentes conexas enriquecidas con el peso total de cada componente."""
    components = (
        graph.connectedComponents()
    )
    #
    # Peso total por componente: unir cada arista a la componente de su src.
    src_comp = components.select(
        col("id").alias("src"),
        col("component"),
    )
    comp_edges = graph.edges.join(src_comp, on="src", how="inner")
    buckets = _resolve_salt(salt_buckets)
    if buckets > 1:
        # La componente gigante concentraría casi todo el peso en un reducer:
        # suma en dos etapas con sal por arista.
        comp_weight = (lfa.merge_salted_stats(
            lfa.salted_partial_stats(
                comp_edges.withColumn(
                    _EDGE_KEY_COLUMN,
                    concat_ws("||", col("src"), col("dst"))),
                "component", ["weight"], salt_buckets=buckets,
                id_column=_EDGE_KEY_COLUMN),
            "component", ["weight"], ["sum"])
            .withColumnRenamed("sum_weight", "component_weight"))
    else:
        comp_weight = (
            comp_edges
            .groupBy("component")
            .agg(spark_sum("weight").alias("component_weight"))
        )
    return components.join(comp_weight, on="component", how="left")


def weighted_triangle_count(
    graph: GraphFrame,
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Conteo de triángulos por nodo (estructural, no ponderado) + fuerza."""
    triangles = (
        graph.triangleCount()
        .withColumnRenamed("count", "triangle_count")
    )
    strength = weighted_degrees(
        graph, salt_buckets=salt_buckets).select("id", "total_strength")
    return triangles.join(strength, on="id", how="left")


###############################################################################
# TARGET PROPAGATION FEATURES
###############################################################################

def get_degree(
    graph:GraphFrame, # with  weight
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Grado ponderado por nodo: suma de `weight` de aristas incidentes (in+out).

    Con `salt_buckets` activa la agregación en dos etapas con sal
    (parciales por (nodo, sal de arista) -> merge por nodo): robusta a
    supernodos con millones de aristas que colgarían un único reducer.
    """
    raw_edges:DataFrame = graph.edges
    #
    if not salt_buckets:
        return (
            raw_edges.select(col("src").alias("node"), "weight")
            .union(raw_edges.select(col("dst").alias("node"), "weight"))
            .groupBy("node")
            .agg(spark_sum("weight").alias("deg_sum"))
        )
    #
    # sal por arista (src||dst): las aristas de un supernodo se reparten entre
    # buckets y el merge final solo combina `salt_buckets` parciales por nodo.
    salted_edges = raw_edges.withColumn(
        "__esalt",
        pmod(spark_hash(concat_ws("||", col("src"), col("dst"))),
            lit(salt_buckets)))
    partials = (
        salted_edges.select(col("src").alias("node"), "weight", "__esalt")
        .union(salted_edges.select(col("dst").alias("node"), "weight", "__esalt"))
        .groupBy("node", "__esalt")
        .agg(spark_sum("weight").alias("__p"))
    )
    return (partials.groupBy("node")
        .agg(spark_sum("__p").alias("deg_sum")))


def weight_normalization(
    graph:GraphFrame,
    degree:DataFrame
) -> DataFrame:
    """Aristas con pesos normalizados por el grado del receptor en cada dirección."""
    raw_edges:DataFrame = graph.edges
    return (
        raw_edges
        # normaliza cada dirección por el grado del RECEPTOR
        .join(degree.withColumnRenamed("node", "dst").withColumnRenamed("deg_sum", "deg_dst"), on="dst", how="left")
        .join(degree.withColumnRenamed("node", "src").withColumnRenamed("deg_sum", "deg_src"), on="src", how="left")
        .withColumn(  # mensaje src->dst: normalizado por el grado de dst (receptor)
            "w_src_to_dst",
            when(col("deg_dst") > 0, col("weight") / col("deg_dst")).otherwise(lit(0.0)),
        )
        .withColumn(  # mensaje dst->src: normalizado por el grado de src (receptor)
            "w_dst_to_src",
            when(col("deg_src") > 0, col("weight") / col("deg_src")).otherwise(lit(0.0)),
        )
        .select("src", "dst", "w_src_to_dst", "w_dst_to_src")
    )


def propagate_target(
    graph: GraphFrame,
    edges_norm:DataFrame,
    target_column:str,
    final_output_column_name:str="contagion_score",
    keep_seed_floor:bool=True,
    max_iter:int = 3,
    alpha:float = 0.15
) -> DataFrame:
    """Propaga el score del target por el grafo mediante difusión iterativa de mensajes ponderados."""
    #
    # ------------------------------------------------------------------------------
    # 2) Inicializar score = semilla flotante.
    # ------------------------------------------------------------------------------
    nodes = graph.vertices
    nodes = nodes.withColumn(
        final_output_column_name, coalesce(col(target_column).cast("double"), lit(0.0))
    )
    if keep_seed_floor:
        nodes = nodes.withColumn(
            "seed_score", col(final_output_column_name)
        )
    #
    g = GraphFrame(nodes, edges_norm)
    #
    #
    # ------------------------------------------------------------------------------
    # 3) Difusión iterativa: promedio ponderado entrante.
    # ------------------------------------------------------------------------------
    for _ in range(max_iter):
        # mensaje = score_origen * peso_normalizado_de_esa_direccion
        msg_to_dst = AM.src[final_output_column_name] * AM.edge["w_src_to_dst"]
        msg_to_src = AM.dst[final_output_column_name] * AM.edge["w_dst_to_src"]
        #
        # como los pesos ya están normalizados por nodo, SUM = promedio ponderado
        agg = g.aggregateMessages(
            spark_sum(AM.msg).alias("incoming_score"),
            sendToDst=msg_to_dst,
            sendToSrc=msg_to_src,
        )
        #
        new_nodes = (
            g.vertices.join(agg, on="id", how="left")
            .withColumn(
                "incoming_score",
                coalesce(col("incoming_score"), lit(0.0)),
            )
            # amortiguación: mezcla propio + entrante
            .withColumn(
                final_output_column_name,
                (lit(1.0 - alpha) * col(final_output_column_name))
                + (lit(alpha) * col("incoming_score")),
            )
            .drop("incoming_score")
        )
        #
        # el score nunca cae por debajo de la semilla original (opcional)
        if keep_seed_floor:
            new_nodes = new_nodes.withColumn(
                final_output_column_name,
                greatest(col(final_output_column_name), col("seed_score")),
            )
        #
        # Truncar el linaje entre iteraciones: AM.getCachedDataFrame solo hace
        # cache() — volátil y no trunca el plan, que crecería un
        # aggregateMessages+join por iteración. checkpoint() es eager y durable
        # (dir de checkpoint ya fijado por define_checkpoint en el pipeline);
        # localCheckpoint() trunca igual pero es local, para usos sin dir (tests).
        session = getattr(new_nodes, "sparkSession", None)
        if session is None:
            session = new_nodes.sql_ctx.sparkSession
        if session.sparkContext._jsc.sc().getCheckpointDir().isDefined():
            new_nodes = new_nodes.checkpoint()
        else:
            new_nodes = new_nodes.localCheckpoint()
        #
        g = GraphFrame(AM.getCachedDataFrame(new_nodes), g.edges)
    #
    return (g.vertices.drop("seed_score") if keep_seed_floor else g.vertices
        .select("id", final_output_column_name, *[
            c for c in graph.vertices.columns if c not in ("id", final_output_column_name)
        ]))


def target_group_by_id(
    group_by_id:DataFrame,
    target:DataFrame,
    column:str,
    function:Union[Callable[..., Column], Dict[str, Callable[..., Column]]]=spark_max
) -> DataFrame:
    """Agrega el target por cada valor de `column` (array) asociado a cada id de nodo.

    `function` puede ser:
      - un callable de agregación (back-compat): agrega la columna `target` y
        produce `target_agg_{column}`.
      - un dict {columna_target: callable}: agrega TODAS esas columnas de la
        tabla `target` (p.ej. target y scores) produciendo una columna
        `target_agg_{column}` para "target" y `target_agg_{column}_{nombre}`
        para el resto.
    """
    if not isinstance(function, dict):
        function = {"target": function}
    target_columns = (target
        .groupBy(column)
        .agg(*[func(col(tcol)).alias(tcol) for tcol, func in function.items()])
    )
    return (group_by_id
        .withColumn(column, explode(column))
        .join(other=target_columns, on=column, how="left")
        .groupBy("id")
        .agg(*[
            func(col(tcol)).alias(
                f"target_agg_{column}" if tcol == "target" else f"target_agg_{column}_{tcol}")
            for tcol, func in function.items()])
    )


def join_target(
    nodes:DataFrame,
    target_group_by_id:DataFrame,
    new_column_suffix:str,
    target_column:str = "target"
) -> DataFrame:
    """Une la agregación de target a los nodos renombrándola `target_{suffix}`."""
    return (nodes
        .join(other=target_group_by_id, on="id", how="left")
        .withColumnRenamed(target_column, f"target_{new_column_suffix}")
    )


###############################################################################
# CLUSTER FEATURES
###############################################################################

def cluster_group_stats(
    nodes:DataFrame,
    group_column:str,
    aggregate_columns:List[str],
    stats:Optional[Dict[str, Callable[..., Column]]] = None,
    prefix:str = "cluster",
    salt_buckets:Optional[int] = None,
) -> DataFrame:
    """Estadísticos intra-grupo por nodo.

    Agrega `aggregate_columns` por `group_column` y re-une por `id`, de modo que
    cada nodo queda enriquecido con las estadísticas del grupo al que pertenece.

    Columnas resultantes: `{prefix}_{group_column}_size` (nº de miembros) +
    `{prefix}_{group_column}_{stat}_{column}` por cada stat y columna agregada.

    Con `salt_buckets`>1 la agregación por grupo va en dos etapas con sal
    (`lfa.salted_partial_stats`/`merge_salted_stats` para los combinables —
    incluidos los ratios de momentos curt/skew/so — y `plain_group_stats`
    para el resto): un grupo gigante no concentra todo el shuffle.
    """
    if stats is None:
        stats = STANDARD_WEIGHT_STATS
    buckets = _resolve_salt(salt_buckets)
    size_column = f"{prefix}_{group_column}_size"
    column_prefix = f"{prefix}_{group_column}_"
    if buckets > 1:
        combinable, plain_stats = lfa.split_stats_by_combinable(stats)
        frames:List[DataFrame] = []
        if combinable:
            frames.append(lfa.merge_salted_stats(
                lfa.salted_partial_stats(
                    nodes, group_column, aggregate_columns,
                    salt_buckets=buckets, id_column="id",
                    max_moment=lfa.stats_required_moment(combinable)),
                group_column, aggregate_columns, combinable,
                size_column=size_column, column_prefix=column_prefix))
        if plain_stats:
            frames.append(lfa.plain_group_stats(
                nodes, group_column, aggregate_columns, plain_stats,
                size_column=None if combinable else size_column,
                column_prefix=column_prefix))
        grouped = frames[0]
        for frame in frames[1:]:
            grouped = grouped.join(frame, on=group_column)
    else:
        grouped = nodes.groupBy(group_column).agg(
            spark_count("*").alias(size_column),
            *[func(c).alias(f"{column_prefix}{func_name}_{c}")
              for c in aggregate_columns for func_name, func in stats.items()],
        )
    return (
        nodes.select("id", group_column)
        .join(grouped, on=group_column, how="left")
    )


###############################################################################
# REGISTRY
###############################################################################

GRAPH_FEATURE_FUNCTIONS = {
    "pagerank": pagerank,
    "degrees": degrees,
    "components": components,
    "triangle_count": triangle_count,
    "degree_balance": degree_balance,
    "reciprocity": reciprocity,
    "self_loops": self_loops,
    "weighted_pagerank": weighted_pagerank,
    "weighted_degrees": weighted_degrees,
    "weighted_edge_stats": weighted_edge_stats,
    "weighted_degree_balance": weighted_degree_balance,
    "weighted_components": weighted_components,
    "weighted_triangle_count": weighted_triangle_count,
    "target_propagation": propagate_target,
}
