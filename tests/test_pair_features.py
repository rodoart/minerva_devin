"""Tests de libs.functions.pair_features.

Grafo de referencia (dirigido, salvo indicación contraria):

    a -> b      a -> c      b -> c      c -> a
    c -> x      x -> d      d -> x

Vecindarios NO dirigidos:
    a: {b, c}      b: {a, c}    c: {a, b, x}    x: {c, d}    d: {x}

Grados dirigidos:
    deg_out: a=2, b=1, c=2, x=1, d=1
    deg_in:  a=1, b=1, c=2, x=2, d=1
"""
import math

import pytest

pytestmark = pytest.mark.spark

EDGES = [
    {"src": "a", "dst": "b", "count_txn": 3},
    {"src": "a", "dst": "c", "count_txn": 1},
    {"src": "b", "dst": "c", "count_txn": 1},
    {"src": "c", "dst": "a", "count_txn": 1},
    {"src": "c", "dst": "x", "count_txn": 2},
    {"src": "x", "dst": "d", "count_txn": 1},
    {"src": "d", "dst": "x", "count_txn": 1},
]
NODE_IDS = ["a", "b", "c", "x", "d"]


@pytest.fixture()
def edges(df_factory):
    return df_factory(EDGES)


@pytest.fixture()
def graph(graphframes, edges, checkpoint_dir):
    from graphframes import GraphFrame
    vertices = graphframes.createDataFrame([(i,) for i in NODE_IDS], ["id"])
    return GraphFrame(vertices, edges.select("src", "dst"))


def _rows(df, key="id"):
    return {r[key]: r.asDict() for r in df.collect()}


###############################################################################
# HELPERS
###############################################################################

class TestNeighborSetsAndDegrees:
    def test_undirected_neighbor_sets(self, edges):
        from libs.functions.pair_features import undirected_neighbor_sets
        rows = _rows(undirected_neighbor_sets(edges))
        assert set(rows["a"]["nbr_set"]) == {"b", "c"}
        assert set(rows["c"]["nbr_set"]) == {"a", "b", "x"}
        assert set(rows["x"]["nbr_set"]) == {"c", "d"}
        assert rows["d"]["deg_undirected"] == 1
        assert rows["c"]["deg_undirected"] == 3

    def test_directed_degrees(self, edges):
        from libs.functions.pair_features import directed_degrees
        rows = _rows(directed_degrees(edges))
        assert rows["a"]["deg_out"] == 2 and rows["a"]["deg_in"] == 1
        assert rows["x"]["deg_out"] == 1 and rows["x"]["deg_in"] == 2
        assert rows["d"]["deg_out"] == 1 and rows["d"]["deg_in"] == 1


class TestCandidatePairs:
    def test_one_hop(self, edges):
        from libs.functions.pair_features import candidate_pairs_within_hops
        pairs = candidate_pairs_within_hops(edges, ["a"], max_hops=1)
        got = {(r["a"], r["x"]): r["hops"] for r in pairs.collect()}
        assert got == {("a", "b"): 1, ("a", "c"): 1}

    def test_two_hops(self, edges):
        from libs.functions.pair_features import candidate_pairs_within_hops
        pairs = candidate_pairs_within_hops(edges, ["a"], max_hops=2)
        got = {(r["a"], r["x"]): r["hops"] for r in pairs.collect()}
        # a->c->x llega a x en 2; a->c->a se excluye (self-pair).
        assert got == {("a", "b"): 1, ("a", "c"): 1, ("a", "x"): 2}

    def test_unknown_seed_yields_empty(self, edges):
        from libs.functions.pair_features import candidate_pairs_within_hops
        pairs = candidate_pairs_within_hops(edges, ["zzz"], max_hops=2)
        assert pairs.count() == 0


###############################################################################
# NODE RISK FEATURES
###############################################################################

class TestSeedNeighborStats:
    def test_hard_seed(self, edges, df_factory):
        from libs.functions.pair_features import seed_neighbor_stats
        seeds = df_factory([{"id": "b", "seed_score": 1.0}])
        rows = _rows(seed_neighbor_stats(edges, seeds, "hard"))
        # a apunta a {b, c}: suma=1, fracción=1/2; recibe de {c}: 0.
        assert rows["a"]["risk_out_sum_hard"] == pytest.approx(1.0)
        assert rows["a"]["risk_out_frac_hard"] == pytest.approx(0.5)
        assert rows["a"]["risk_in_sum_hard"] == pytest.approx(0.0)
        assert rows["a"]["risk_in_frac_hard"] == pytest.approx(0.0)
        # c recibe de {a, b}: fracción in=1/2; apunta a {a, x}: 0.
        assert rows["c"]["risk_in_frac_hard"] == pytest.approx(0.5)
        assert rows["c"]["risk_out_sum_hard"] == pytest.approx(0.0)
        # d no recibe de nadie: solo tiene stats out (x->d es in de d).
        assert rows["d"]["risk_in_sum_hard"] == pytest.approx(0.0)

    def test_soft_seed_scores(self, edges, df_factory):
        from libs.functions.pair_features import seed_neighbor_stats
        seeds = df_factory([
            {"id": "b", "seed_score": 0.25},
            {"id": "d", "seed_score": 0.75},
        ])
        rows = _rows(seed_neighbor_stats(edges, seeds, "soft"))
        # a -> b: out_sum = 0.25; x -> d: out_sum = 0.75.
        assert rows["a"]["risk_out_sum_soft"] == pytest.approx(0.25)
        assert rows["x"]["risk_out_sum_soft"] == pytest.approx(0.75)
        # d recibe de x (no semilla): in_sum = 0.
        assert rows["d"]["risk_in_sum_soft"] == pytest.approx(0.0)


class TestSeedTwoHopCounts:
    def test_two_hop_undirected(self, edges):
        from libs.functions.pair_features import seed_two_hop_counts
        rows = _rows(seed_two_hop_counts(edges, ["d"], "f"))
        # d alcanzable a 2 saltos no dirigidos desde c (c-x-d); x es vecino
        # directo de d y no cuenta; a y b están a 3.
        assert set(rows) == {"c"}
        assert rows["c"]["risk_2hop_f"] == 1

    def test_no_seed_reachable(self, edges):
        from libs.functions.pair_features import seed_two_hop_counts
        # semilla aislada: nadie la alcanza a 2 saltos
        rows = seed_two_hop_counts(edges, ["zzz"], "f")
        assert rows.count() == 0


class TestSeedFlowRatio:
    def test_weighted_flows(self, edges):
        from libs.functions.pair_features import seed_flow_ratio
        rows = _rows(seed_flow_ratio(edges, ["b"], "f", weight_column="count_txn"))
        # a emite 3+1=4, de los cuales 3 van a b -> 0.75
        assert rows["a"]["risk_outflow_f"] == pytest.approx(0.75)
        # a recibe 1 de c (no semilla) -> 0
        assert rows["a"]["risk_inflow_f"] == pytest.approx(0.0)
        # c recibe 1(a)+1(b)=2, 1 de semilla -> 0.5
        assert rows["c"]["risk_inflow_f"] == pytest.approx(0.5)
        # c emite 3 a no-semillas -> 0
        assert rows["c"]["risk_outflow_f"] == pytest.approx(0.0)

    def test_missing_weight_column_falls_back_to_one(self, edges):
        from libs.functions.pair_features import seed_flow_ratio
        plain = edges.select("src", "dst")
        rows = _rows(seed_flow_ratio(plain, ["b"], "f"))
        # sin peso cada arista pesa 1: a emite 2, 1 a semilla -> 0.5
        assert rows["a"]["risk_outflow_f"] == pytest.approx(0.5)


@pytest.mark.graphframes
class TestSeedDistance:
    def test_direction_to_seed(self, graph):
        from libs.functions.pair_features import seed_distance
        rows = _rows(seed_distance(graph, ["b"], "f", direction="to"))
        # distancia de cada nodo HACIA b: a->b=1, c->a->b=2; x,d no llegan
        assert rows["b"]["risk_dist_f"] == 0
        assert rows["a"]["risk_dist_f"] == 1
        assert rows["c"]["risk_dist_f"] == 2
        assert rows["x"]["risk_dist_f"] is None
        assert rows["d"]["risk_dist_f"] is None

    def test_direction_from_seed(self, graph):
        from libs.functions.pair_features import seed_distance
        rows = _rows(seed_distance(graph, ["b"], "f", direction="from"))
        # desde b: b=0, c=1, a=2 (b->c->a), x=2 (b->c->x), d=3
        assert rows["b"]["risk_dist_f"] == 0
        assert rows["c"]["risk_dist_f"] == 1
        assert rows["a"]["risk_dist_f"] == 2
        assert rows["x"]["risk_dist_f"] == 2
        assert rows["d"]["risk_dist_f"] == 3

    def test_invalid_direction(self, graph):
        from libs.functions.pair_features import seed_distance
        with pytest.raises(ValueError):
            seed_distance(graph, ["b"], "f", direction="sideways")

    def test_empty_seeds_all_null(self, graph):
        from libs.functions.pair_features import seed_distance
        rows = _rows(seed_distance(graph, [], "f"))
        assert all(r["risk_dist_f"] is None for r in rows.values())


@pytest.mark.graphframes
class TestSeedPersonalizedPageRank:
    def test_seed_has_top_score(self, graph):
        from libs.functions.pair_features import seed_personalized_pagerank
        rows = _rows(seed_personalized_pagerank(
            graph, ["a"], "f", max_iter=5, reset_prob=0.15))
        assert set(rows) == set(NODE_IDS)
        best = max(rows, key=lambda k: rows[k]["risk_ppr_max_f"])
        assert best == "a"
        assert all(r["risk_ppr_mean_f"] is not None for r in rows.values())

    def test_empty_seeds_all_null(self, graph):
        from libs.functions.pair_features import seed_personalized_pagerank
        rows = _rows(seed_personalized_pagerank(graph, [], "f"))
        assert all(r["risk_ppr_max_f"] is None for r in rows.values())


###############################################################################
# PAIR PROXIMITY FEATURES
###############################################################################

class TestPairProximityFeatures:
    def test_pair_ax(self, edges, df_factory):
        from libs.functions.pair_features import pair_proximity_features
        pairs = df_factory([{"a": "a", "x": "x"}])
        rows = _rows(pair_proximity_features(edges, pairs), key="x")
        r = rows["x"]
        # N(a)={b,c}, N(x)={c,d}: CN=1, unión=3 -> jaccard=1/3
        assert r["pair_common_neighbors"] == 1
        assert r["pair_jaccard"] == pytest.approx(1.0 / 3.0)
        # AA: único vecino común c con deg 3 -> 1/log(3)
        assert r["pair_adamic_adar"] == pytest.approx(1.0 / math.log(3))
        # PA dirigido: deg_out(a)=2 * deg_in(x)=2 = 4
        assert r["pair_preferential_attachment"] == 4
        # a->x no existe -> sin reciprocidad
        assert r["pair_reciprocity"] == 0
        # camino a->c->x: 1
        assert r["pair_paths_len2"] == 1

    def test_reciprocal_pair(self, edges, df_factory):
        from libs.functions.pair_features import pair_proximity_features
        pairs = df_factory([{"a": "a", "x": "c"}])
        rows = _rows(pair_proximity_features(edges, pairs), key="x")
        r = rows["c"]
        assert r["pair_reciprocity"] == 1
        # N(a)={b,c}, N(c)={a,b,x}: CN=1 (b), unión={a,b,c,x}=4
        assert r["pair_common_neighbors"] == 1
        assert r["pair_jaccard"] == pytest.approx(0.25)
        # a->?->c: a->b->c -> 1
        assert r["pair_paths_len2"] == 1

    def test_no_common_neighbors(self, edges, df_factory):
        from libs.functions.pair_features import pair_proximity_features
        pairs = df_factory([{"a": "b", "x": "d"}])
        rows = _rows(pair_proximity_features(edges, pairs), key="x")
        r = rows["d"]
        # N(b)={a,c}, N(d)={x}: CN=0 -> jaccard=0, AA=0
        assert r["pair_common_neighbors"] == 0
        assert r["pair_jaccard"] == pytest.approx(0.0)
        assert r["pair_adamic_adar"] == pytest.approx(0.0)
        # b->c->x->d: longitud 3 -> paths_len2=0
        assert r["pair_paths_len2"] == 0


@pytest.mark.graphframes
class TestPairMinDistance:
    def test_distance_from_seed(self, graph, df_factory):
        from libs.functions.pair_features import pair_min_distance
        pairs = df_factory([{"a": "a", "x": "x"}, {"a": "a", "x": "d"}])
        rows = {r["x"]: r["pair_min_distance"]
                for r in pair_min_distance(graph, pairs, ["a"]).collect()}
        # a->c->x = 2; a->c->x->d = 3
        assert rows == {"x": 2, "d": 3}

    def test_unreachable_is_null(self, graph, df_factory):
        from libs.functions.pair_features import pair_min_distance
        pairs = df_factory([{"a": "x", "x": "b"}])
        rows = pair_min_distance(graph, pairs, ["x"]).collect()
        # x->d->x...: b inalcanzable desde x
        assert rows[0]["pair_min_distance"] is None


class TestAggregatePairFeaturesToNode:
    def test_max_mean_by_x(self, df_factory):
        from libs.functions.pair_features import aggregate_pair_features_to_node
        pairs = df_factory([
            {"a": "a", "x": "x", "pair_jaccard": 0.5, "pair_paths_len2": 2},
            {"a": "b", "x": "x", "pair_jaccard": 0.1, "pair_paths_len2": 0},
            {"a": "a", "x": "c", "pair_jaccard": 0.9, "pair_paths_len2": 1},
        ])
        rows = _rows(aggregate_pair_features_to_node(pairs, node_role="x"))
        assert rows["x"]["pair_jaccard_max"] == pytest.approx(0.5)
        assert rows["x"]["pair_jaccard_mean"] == pytest.approx(0.3)
        assert rows["x"]["pair_paths_len2_max"] == 2
        assert rows["c"]["pair_jaccard_max"] == pytest.approx(0.9)
