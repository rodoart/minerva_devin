"""Features de grafo (GraphFrame -> DataFrame): métricas no ponderadas, ponderadas
y propagación iterativa del target, más el registro `GRAPH_FEATURE_FUNCTIONS`.
"""
###############################################################################
# GRAPH FEATURE FUNCTIONS
###############################################################################

# ------------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------------
from typing import Callable, List, Dict, Optional


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
    explode, when, greatest)

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
    graph:GraphFrame
) -> DataFrame:
    """Grado de entrada, salida y total por nodo."""
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
    graph: GraphFrame
) -> DataFrame:
    """Fuerza (strength) ponderada: suma de pesos entrantes/salientes por nodo."""
    edges = graph.edges
    #
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
    weight_column:str = "weight"
) -> DataFrame:
    """Estadísticas de peso de aristas incidentes por nodo (in + out)."""
    if aggregations is None:
        if stats is None:
            stats = STANDARD_WEIGHT_STATS
        aggregations = [
            func(weight_column).alias(f"{func_name}_{weight_column}")
            for func_name, func in stats.items()
        ]
    #
    edges = graph.edges
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


def weighted_components(
    graph: GraphFrame
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
    comp_weight = (
        graph.edges.join(src_comp, on="src", how="inner")
        .groupBy("component")
        .agg(spark_sum("weight").alias("component_weight"))
    )
    return components.join(comp_weight, on="component", how="left")


def weighted_triangle_count(
    graph: GraphFrame
) -> DataFrame:
    """Conteo de triángulos por nodo (estructural, no ponderado) + fuerza."""
    triangles = (
        graph.triangleCount()
        .withColumnRenamed("count", "triangle_count")
    )
    strength = weighted_degrees(graph).select("id", "total_strength")
    return triangles.join(strength, on="id", how="left")


###############################################################################
# TARGET PROPAGATION FEATURES
###############################################################################

def get_degree(
    graph:GraphFrame, # with  weight

) -> DataFrame:
    """Grado ponderado por nodo: suma de `weight` de aristas incidentes (in+out)."""
    raw_edges:DataFrame = graph.edges
    #
    deg = (
        raw_edges.select(col("src").alias("node"), "weight")
        .union(raw_edges.select(col("dst").alias("node"), "weight"))
        .groupBy("node")
        .agg(spark_sum("weight").alias("deg_sum"))
    )
    return deg


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
    function:Callable[..., Column]=spark_max
) -> DataFrame:
    """Agrega el target por cada valor de `column` (array) asociado a cada id de nodo."""
    target_columns = (target
        .groupBy(column)
        .agg(function(col("target")).alias("target"))
    )
    return (group_by_id
        .withColumn(column, explode(column))
        .join(other=target_columns, on=column, how="left")
        .groupBy("id")
        .agg(function("target").alias(f"target_agg_{column}"))
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
# REGISTRY
###############################################################################

GRAPH_FEATURE_FUNCTIONS = {
    "pagerank": pagerank,
    "degrees": degrees,
    "components": components,
    "triangle_count": triangle_count,
    "weighted_pagerank": weighted_pagerank,
    "weighted_degrees": weighted_degrees,
    "weighted_edge_stats": weighted_edge_stats,
    "weighted_components": weighted_components,
    "weighted_triangle_count": weighted_triangle_count,
    "target_propagation": propagate_target,
}
