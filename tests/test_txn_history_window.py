"""Tests para la ventana de historia transaccional independiente de `txn_replaced`.

La tabla `rfc_curp_analysis_s264_ceps_replaced` conserva solo los últimos
`TXN_REPLACED_HISTORY_IN_MONTHS` meses de transacciones (por `fec_informacion`),
mientras los catálogos de reemplazo se construyen con una ventana mucho más
profunda (`CEPS_RANKING_HISTORY_IN_MONTHS`).
"""
import os
from datetime import date

import pytest

pytest.importorskip("pyspark")

# config.job exige MINERVA_TODAY en import; los tests usan un vintage fijo.
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

from pipelines.ceps.txn_replacement import limit_txn_history_window

pytestmark = pytest.mark.spark

VINTAGE = date(2025, 8, 31)


@pytest.fixture()
def txns_df(spark):
    """Transacciones repartidas en 6 meses (marzo..agosto 2025)."""
    return spark.createDataFrame(
        [("t1", "2025-03-15"), ("t2", "2025-04-20"), ("t3", "2025-05-31"),
         ("t4", "2025-06-01"), ("t5", "2025-07-15"), ("t6", "2025-08-31")],
        ["txn_id", "fec_informacion"])


# ----------------------------------------------------------------------------
# limit_txn_history_window
# ----------------------------------------------------------------------------

class TestLimitTxnHistoryWindow:
    def test_keeps_only_last_n_months(self, txns_df):
        """history=3, lag=0 -> ventana [2025-06-01, 2025-09-01)."""
        result = limit_txn_history_window(
            txns_df, vintage_date=VINTAGE, history=3)
        kept = {r["txn_id"] for r in result.collect()}
        assert kept == {"t4", "t5", "t6"}

    def test_lag_shifts_window_back(self, txns_df):
        """history=2, lag=1 -> ventana [2025-06-01, 2025-08-01)."""
        result = limit_txn_history_window(
            txns_df, vintage_date=VINTAGE, history=2, lag=1)
        kept = {r["txn_id"] for r in result.collect()}
        assert kept == {"t4", "t5"}

    def test_boundary_days_included(self, txns_df):
        """El primer día de la ventana entra; el día de cierre (exclusive) no."""
        df = txns_df.union(txns_df.sparkSession.createDataFrame(
            [("t7", "2025-06-01"), ("t8", "2025-09-01")],
            ["txn_id", "fec_informacion"]))
        result = limit_txn_history_window(df, vintage_date=VINTAGE, history=3)
        kept = {r["txn_id"] for r in result.collect()}
        assert "t7" in kept        # inicio inclusivo
        assert "t8" not in kept    # fin exclusivo

    @pytest.mark.parametrize("history", [0, -1])
    def test_no_history_is_passthrough(self, txns_df, history):
        result = limit_txn_history_window(
            txns_df, vintage_date=VINTAGE, history=history)
        assert result.count() == txns_df.count()

    def test_none_history_is_passthrough(self, txns_df):
        result = limit_txn_history_window(
            txns_df, vintage_date=VINTAGE, history=None)
        assert result.count() == txns_df.count()

    def test_deeper_catalog_window_keeps_everything(self, txns_df):
        """Con una ventana >= historia de los datos no se pierde nada."""
        result = limit_txn_history_window(
            txns_df, vintage_date=VINTAGE, history=24)
        assert result.count() == 6


# ----------------------------------------------------------------------------
# Config: ventanas independientes ranking vs replaced
# ----------------------------------------------------------------------------

class TestIndependentHistoryConfig:
    def test_ranking_deeper_than_replaced(self):
        import config.ceps.rfc_nom_ranking as crnr
        import config.ceps.txn_replacement as cctr
        # los catálogos pueden mirar mucho más atrás que la tabla final
        assert crnr.CEPS_RANKING_HISTORY_IN_MONTHS \
            > cctr.TXN_REPLACED_HISTORY_IN_MONTHS

    def test_ranking_input_uses_ranking_history(self):
        import config.ceps.rfc_nom_ranking as crnr
        assert crnr.input["s264_ceps"]["history"] \
            == crnr.CEPS_RANKING_HISTORY_IN_MONTHS

    def test_txn_replacement_reads_only_last_vintage_partition(self):
        """Cada partición de `rfc_curp_analysis_s264_ceps` ya contiene toda la
        ventana del ranking; el step de reemplazo solo necesita la última."""
        import config.ceps.txn_replacement as cctr
        assert cctr.input["rfc_curp_analysis_s264_ceps"]["history"] == 1

    def test_replaced_output_reads_only_current_partition(self):
        import config.ceps.txn_replacement as cctr
        out = cctr.output["rfc_curp_analysis_s264_ceps_replaced"]
        assert out["history"] == 1
        assert out["information_date_column"] == "mis_date"
