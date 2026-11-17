"""Tests de libs.functions.temporal_features.

Transacciones de referencia (src, dst, ts, oper_mto, tfrom_days):

    a->b  2025-08-31 03:00 (domingo, noche)   100    tfrom=0
    a->c  2025-08-30 14:00 (sábado)           200    tfrom=1
    b->c  2025-08-29 12:00 (viernes)           50    tfrom=2
    c->x  2025-08-20 12:00 (miércoles)       9000    tfrom=11
    x->d  2025-08-20 13:00                   8500    tfrom=11
    d->x  2025-08-01 12:00 (viernes)           70    tfrom=30
    x->c  2025-07-25 12:00 (viernes)           80    tfrom=37
"""
from datetime import datetime

import pytest

pytestmark = pytest.mark.spark

TXNS = [
    ("a", "b", datetime(2025, 8, 31, 3, 0), 100.0, 0),
    ("a", "c", datetime(2025, 8, 30, 14, 0), 200.0, 1),
    ("b", "c", datetime(2025, 8, 29, 12, 0), 50.0, 2),
    ("c", "x", datetime(2025, 8, 20, 12, 0), 9000.0, 11),
    ("x", "d", datetime(2025, 8, 20, 13, 0), 8500.0, 11),
    ("d", "x", datetime(2025, 8, 1, 12, 0), 70.0, 30),
    ("x", "c", datetime(2025, 7, 25, 12, 0), 80.0, 37),
]


@pytest.fixture()
def txns(df_factory):
    return df_factory(TXNS, ["src", "dst", "ts", "oper_mto", "tfrom_days"])


def _rows(df, key="id"):
    return {r[key]: r.asDict() for r in df.collect()}


class TestWithEventTimestamp:
    def test_builds_timestamp(self, df_factory):
        from libs.functions.temporal_features import with_event_timestamp
        df = df_factory([("20250831", 30000)], ["information_date", "hora_oper"])
        row = with_event_timestamp(df).collect()[0]
        assert row["ts"] == datetime(2025, 8, 31, 3, 0, 0)

    def test_pads_short_hour(self, df_factory):
        from libs.functions.temporal_features import with_event_timestamp
        df = df_factory([("20250831", 0)], ["information_date", "hora_oper"])
        row = with_event_timestamp(df).collect()[0]
        assert row["ts"] == datetime(2025, 8, 31, 0, 0, 0)


class TestNodeTemporalFeatures:
    def test_counts_age_and_recency(self, txns):
        from libs.functions.temporal_features import node_temporal_features
        rows = _rows(node_temporal_features(txns))
        # a: 2 txns (ambas como emisor), tfroms {0, 1}
        assert rows["a"]["tmp_txn_count"] == 2
        assert rows["a"]["tmp_account_age_days"] == 1
        assert rows["a"]["tmp_days_since_last_txn"] == 0
        # c: 4 txns (3 in + 1 out), tfroms {1, 2, 11, 37}
        assert rows["c"]["tmp_txn_count"] == 4
        assert rows["c"]["tmp_account_age_days"] == 37
        assert rows["c"]["tmp_days_since_last_txn"] == 1

    def test_window_counts_and_velocity(self, txns):
        from libs.functions.temporal_features import node_temporal_features
        rows = _rows(node_temporal_features(txns))
        # c: 1d=1 (tfrom 1), 7d=2, 30d=3
        assert rows["c"]["tmp_txn_count_1d"] == 1
        assert rows["c"]["tmp_txn_count_7d"] == 2
        assert rows["c"]["tmp_txn_count_30d"] == 3
        # velocidad 7d vs 30d: (2/7)/(3/30) = 20/7
        assert rows["c"]["tmp_velocity_7d_30d"] == pytest.approx(20.0 / 7.0)

    def test_night_and_weekend(self, txns):
        from libs.functions.temporal_features import node_temporal_features
        rows = _rows(node_temporal_features(txns))
        # a: 03:00 domingo + 14:00 sábado -> noche 0.5, fin de semana 1.0
        assert rows["a"]["tmp_pct_night"] == pytest.approx(0.5)
        assert rows["a"]["tmp_pct_weekend"] == pytest.approx(1.0)
        # c: todo a mediodía, 1 de 4 en fin de semana
        assert rows["c"]["tmp_pct_night"] == pytest.approx(0.0)
        assert rows["c"]["tmp_pct_weekend"] == pytest.approx(0.25)

    def test_below_threshold(self, txns):
        from libs.functions.temporal_features import node_temporal_features
        rows = _rows(node_temporal_features(
            txns, report_thresholds=[10000.0], below_threshold_band=0.9))
        # x: 4 txns, solo la de 9000 cae en [9000, 10000) -> 0.25
        assert rows["x"]["tmp_pct_below_threshold"] == pytest.approx(0.25)
        assert rows["a"]["tmp_pct_below_threshold"] == pytest.approx(0.0)

    def test_no_thresholds_gives_zero(self, txns):
        from libs.functions.temporal_features import node_temporal_features
        rows = _rows(node_temporal_features(txns, report_thresholds=[]))
        assert rows["x"]["tmp_pct_below_threshold"] == pytest.approx(0.0)


class TestNodeBurstiness:
    def test_range_and_value(self, txns):
        from libs.functions.temporal_features import node_burstiness
        rows = _rows(node_burstiness(txns))
        # x: 4 txns con gaps 604800s/1641600s/3600s -> (σ-μ)/(σ+μ) ≈ 0.0498
        assert -1.0 <= rows["x"]["tmp_burstiness"] <= 1.0
        assert rows["x"]["tmp_burstiness"] == pytest.approx(0.0498, abs=1e-3)

    def test_null_with_few_txns(self, txns):
        from libs.functions.temporal_features import node_burstiness
        rows = _rows(node_burstiness(txns, min_txn=3))
        # a solo tiene 2 txns -> 1 gap -> null
        assert rows["a"]["tmp_burstiness"] is None


class TestNodeUniqueReceivers:
    def test_window_7d(self, txns):
        from libs.functions.temporal_features import node_unique_receivers
        rows = _rows(node_unique_receivers(txns, window_days=7))
        # a envía a b (tfrom 0) y c (tfrom 1) en 7 días -> 2
        assert rows["a"]["tmp_unique_receivers_7d"] == 2
        # x: solo envía con tfrom 11 y 37 -> fuera de ventana, no aparece
        assert "x" not in rows


class TestNodePassThrough:
    def test_pass_through_24h(self, txns):
        from libs.functions.temporal_features import node_pass_through_24h
        rows = _rows(node_pass_through_24h(txns))
        # x recibe 9000 (sale a la 1h) y 70 (no sale en 24h) -> 9000/9070
        assert rows["x"]["tmp_pass_through_24h"] == pytest.approx(
            9000.0 / 9070.0)
        # c recibe 3 veces pero nunca emite tras recibir -> 0
        assert rows["c"]["tmp_pass_through_24h"] == pytest.approx(0.0)
        # a nunca recibe -> sin pass_through (null tras outer join del ratio)
        assert rows["a"]["tmp_pass_through_24h"] is None

    def test_flow_out_in_ratio(self, txns):
        from libs.functions.temporal_features import node_pass_through_24h
        rows = _rows(node_pass_through_24h(txns))
        # b: emite 50, recibe 100 -> 0.5 ; a: no recibe -> null
        assert rows["b"]["tmp_flow_out_in_ratio"] == pytest.approx(0.5)
        assert rows["a"]["tmp_flow_out_in_ratio"] is None


class TestPairAmountFeatures:
    def test_zscore_and_medians(self, txns):
        from libs.functions.temporal_features import pair_amount_features
        rows = {(r["a"], r["x"]): r.asDict()
                for r in pair_amount_features(txns).collect()}
        ab = rows[("a", "b")]
        # emisor a: mean=150, std(muestral)=70.7107 -> z=(100-150)/std
        assert ab["tmp_amt_zscore_ax"] == pytest.approx(
            -50.0 / 70.7107, abs=1e-3)
        assert ab["tmp_last_amt_over_median_ax"] == pytest.approx(1.0)
        # percentile_approx(0.5) de [100, 200] en Spark = 100 (percentil
        # discreto), así que la razón contra la mediana del emisor es 1.0
        assert ab["tmp_mean_amt_over_median_a"] == pytest.approx(1.0)
        ac = rows[("a", "c")]
        assert ac["tmp_amt_zscore_ax"] == pytest.approx(
            50.0 / 70.7107, abs=1e-3)

    def test_single_txn_sender_null_zscore(self, df_factory):
        from libs.functions.temporal_features import pair_amount_features
        df = df_factory(
            [("a", "b", datetime(2025, 8, 31, 12, 0), 100.0, 0)],
            ["src", "dst", "ts", "oper_mto", "tfrom_days"])
        row = pair_amount_features(df).collect()[0]
        # emisor con una sola transacción: std=0 -> z-score null
        assert row["tmp_amt_zscore_ax"] is None


class TestOrchestrator:
    def test_unions_all_node_families(self, txns):
        from libs.functions.temporal_features import temporal_node_features
        result = temporal_node_features(
            txns, report_thresholds=[10000.0])
        for column in ("tmp_txn_count", "tmp_pct_night", "tmp_burstiness",
                       "tmp_unique_receivers_7d", "tmp_pass_through_24h",
                       "tmp_flow_out_in_ratio"):
            assert column in result.columns, column
        rows = _rows(result)
        # x tiene todas las familias pobladas
        assert rows["x"]["tmp_pass_through_24h"] is not None
        assert rows["x"]["tmp_burstiness"] is not None
