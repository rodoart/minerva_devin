"""Tests de resiliencia de las recargas del framework (anti-corrupción).

Simula los escenarios que se dan en el driver de producción cuando el proceso
muere a mitad de una escritura o cuando vintages distintos comparten
ubicación:

- Parquet simple escrito a medias (sin `_SUCCESS`): la recarga debe
  recomputar, nunca leer datos parciales en silencio.
- Tabla particionada con una partición parcial (sin marcador de commit, con
  `_SUCCESS` raíz obsoleto): la recarga borra la partición corrupta y
  recomputa el vintage en curso.
- `ensure_monthly_partitions` interrumpido a mitad de un mes: solo ese mes
  se recomputa; los meses ya committeados se reusan.
- Corridas repetidas en la misma ubicación: idempotentes (no recomputan).
- Vintage VIEJO ejecutado tras uno nuevo compartiendo ubicación (tablas
  all-time de raíz fija): la ventana acota la lectura, no se toma
  información del futuro.
"""
import os
import shutil
from datetime import date

import pytest

pytest.importorskip("pyspark")

# config.job exige MINERVA_TODAY en import; los tests usan vintages fijos.
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

from pyspark.sql.functions import col

import libs.framework as ppf

pytestmark = pytest.mark.spark


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def write_partial_parquet(spark, target_dir, df):
    """Simula una escritura interrumpida: archivos part-* SIN `_SUCCESS`.

    Escribe `df` (solo columnas de datos, como en un part-file real) en un
    staging completo y copia solo el primer part-file al destino — el estado
    en el que queda un directorio cuando el proceso muere a mitad del commit
    del job.
    """
    import pathlib
    target = pathlib.Path(str(target_dir))
    staging = target.parent / (target.name + "_staging")
    df.write.mode("overwrite").parquet(str(staging))
    target.mkdir(parents=True, exist_ok=True)
    part_file = next(p for p in staging.iterdir() if p.name.startswith("part-"))
    shutil.copy(part_file, target / part_file.name)
    shutil.rmtree(staging)


def partitioned_config(path, history=3, extra=None):
    config = {"out": {
        "table_or_hdfs": str(path).replace("\\", "/"),  # posix: el loader deriva nombres de vista del path
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": history,
        "information_date_mode": "each",
        "process_date_mode": "last",
    }}
    if extra:
        config["out"].update(extra)
    return config


def make_partitioned_step(spark, path, calls, config=None, vintage="202507",
        vintage_date=date(2025, 7, 31), process_date=date(2025, 8, 5)):
    """Step con una property particionada decorada que cuenta recomputos."""

    class S(ppf.Step):
        @ppf.cached_property
        @ppf.dynamic_partitioned_table_or_parquet(path_key="out")
        def out(self):
            calls.append("compute")
            return self.sqlContext.createDataFrame([(1, "a")], ["n", "v"])

    step = S(input_hive={},
        output_hive=config or partitioned_config(path),
        input_parameters={
            "vintage": vintage,
            "vintage_date": vintage_date,
            "process_date": process_date,
        })
    step.sqlContext = spark
    step.is_dynamic = True
    return step


# ----------------------------------------------------------------------------
# Parquet simple: guarda de _SUCCESS
# ----------------------------------------------------------------------------

class TestUnpartitionedReloadGuard:
    def test_complete_write_is_reloaded(self, spark, tmp_path, local_hdfs):
        path = str(tmp_path / "simple")

        class S(ppf.Step):
            @ppf.dynamic_unpartitioned_parquet_with_path(path)
            def make(self):
                self.calls += 1
                return self.sqlContext.createDataFrame([(1, "a")], ["n", "v"])

        s = S(input_hive={}, output_hive={}, input_parameters={})
        s.sqlContext, s.is_dynamic, s.calls = spark, True, 0
        assert s.make().count() == 1
        assert s.calls == 1

        s2 = S(input_hive={}, output_hive={}, input_parameters={})
        s2.sqlContext, s2.is_dynamic, s2.calls = spark, True, 0
        reloaded = s2.make()
        assert s2.calls == 0                              # recarga, no recomputo
        assert reloaded.collect()[0]["n"] == 1

    def test_partial_write_without_success_is_recomputed(
            self, spark, tmp_path, local_hdfs):
        """Un parquet sin `_SUCCESS` es residuo de un proceso interrumpido:
        debe recomputarse aunque contenga archivos part-* legibles."""
        path = tmp_path / "simple_partial"
        write_partial_parquet(spark, path,
            spark.createDataFrame([("partial", 0.0)], ["k", "v"]))
        assert not (path / "_SUCCESS").exists()

        class S(ppf.Step):
            @ppf.dynamic_unpartitioned_parquet_with_path(str(path))
            def make(self):
                self.calls += 1
                return self.sqlContext.createDataFrame([(1, "a")], ["n", "v"])

        s = S(input_hive={}, output_hive={}, input_parameters={})
        s.sqlContext, s.is_dynamic, s.calls = spark, True, 0
        df = s.make()
        assert s.calls == 1                               # recomputó
        assert df.count() == 1 and df.collect()[0]["n"] == 1
        assert (path / "_SUCCESS").exists()               # escritura completa

    def test_windows_style_path_label_does_not_crash(
            self, spark, tmp_path, local_hdfs):
        """La etiqueta del log se deriva del path; con separadores Windows
        (`C:\\...`) no debe romperse (bug baseline de la suite local)."""
        path = str(tmp_path / "win_out").replace("/", "\\")

        class S(ppf.Step):
            @ppf.dynamic_unpartitioned_parquet_with_path(path)
            def make(self):
                return self.sqlContext.createDataFrame([(1,)], ["n"])

        s = S(input_hive={}, output_hive={}, input_parameters={})
        s.sqlContext, s.is_dynamic = spark, True
        assert s.make().count() == 1


# ----------------------------------------------------------------------------
# Tabla particionada: marcadores de commit
# ----------------------------------------------------------------------------

class TestPartitionedCommitMarkers:
    def test_write_creates_marker_and_reload_reuses(
            self, spark, tmp_path, local_hdfs):
        path = tmp_path / "part"
        calls = []
        s1 = make_partitioned_step(spark, path, calls)
        df1, _ = s1.out
        assert calls == ["compute"]
        assert df1.count() == 1
        # marcador de commit del par (vintage, process_date)
        marker = (path / "_committed" / "mis_date=2025-07-31"
            / "process_date=2025-08-05")
        assert marker.exists()

        s2 = make_partitioned_step(spark, path, calls)
        df2, _ = s2.out
        assert calls == ["compute"]                         # recarga sin recomputo
        assert df2.count() == 1

    def test_missing_marker_forces_recompute(self, spark, tmp_path, local_hdfs):
        """Partición completa pero sin marcador (legacy o interrumpida antes
        de marcar): se recomputa en lugar de confiar en ella."""
        path = tmp_path / "part"
        calls = []
        make_partitioned_step(spark, path, calls).out
        shutil.rmtree(path / "_committed")                  # borra marcadores

        s2 = make_partitioned_step(spark, path, calls)
        df2, _ = s2.out
        assert calls == ["compute", "compute"]              # recomputó
        assert df2.count() == 1
        assert (path / "_committed").exists()               # y volvió a marcar

    def test_interrupted_partition_write_is_recomputed(
            self, spark, tmp_path, local_hdfs):
        """Crash a mitad de un segundo vintage: partición parcial sin
        marcador junto a una sana; la recarga la borra y recomputa."""
        path = tmp_path / "part"
        calls = []
        make_partitioned_step(spark, path, calls).out       # jul-2025 completo

        # crash simulado de otro proceso escribiendo jun-2025: archivos
        # parciales, sin marcador de commit; el `_SUCCESS` raíz ya existía.
        write_partial_parquet(spark,
            path / "mis_date=2025-06-30" / "process_date=2025-08-05",
            spark.createDataFrame([(0, "corrupt")], ["n", "v"]))

        s2 = make_partitioned_step(spark, path, calls)
        df2, _ = s2.out
        assert calls == ["compute", "compute"]              # detectó y recomputó
        assert df2.count() == 1
        assert df2.collect()[0]["n"] == 1                   # datos del recomputo
        # la partición corrupta quedó borrada (no envenena recargas futuras)
        assert not (path / "mis_date=2025-06-30"
            / "process_date=2025-08-05").exists()

    def test_trust_legacy_partitions_disables_marker_check(
            self, spark, tmp_path, local_hdfs):
        """Escape hatch por tabla: `trust_legacy_partitions` desactiva la
        validación de marcadores (particiones históricas sin recomputo)."""
        path = tmp_path / "part"
        calls = []
        make_partitioned_step(spark, path, calls).out
        shutil.rmtree(path / "_committed")

        config = partitioned_config(path,
            extra={"trust_legacy_partitions": True})
        s = make_partitioned_step(spark, path, calls, config=config)
        df, _ = s.out
        assert calls == ["compute"]                         # confió en lo viejo
        assert df.collect()[0]["n"] == 1


# ----------------------------------------------------------------------------
# ensure_monthly_partitions: interrupción, idempotencia, vintage viejo
# ----------------------------------------------------------------------------

class TestMonthlyResilience:
    @pytest.fixture()
    def monthly_source(self, spark):
        rows = [
            ("2025-06-30", "e1", 1.0),
            ("2025-07-31", "e2", 2.0),
            ("2025-08-31", "e3", 3.0),
            ("2025-09-30", "e4", 4.0),
        ]
        return spark.createDataFrame(rows, ["month_partition", "id", "v"])

    def make_monthly_step(self, spark, path, vintage_date, process_date,
            history=4):
        step = ppf.Step(
            input_hive={},
            output_hive={"monthly": {
                "table_or_hdfs": str(path).replace("\\", "/"),  # posix: el loader deriva nombres de vista del path
                "information_date_column": "month_partition",
                "process_date_column": "process_date",
                "lag": 0,
                "history": history,
                "information_date_mode": "each",
                "process_date_mode": "last",
            }},
            input_parameters={
                "vintage": vintage_date.strftime("%Y%m"),
                "vintage_date": vintage_date,
                "process_date": process_date,
            })
        step.is_dynamic = True
        step.sqlContext = spark
        return step

    def compute_from(self, monthly_source, calls):
        def compute(months):
            calls.append(list(months))
            return monthly_source.filter(
                col("month_partition").substr(1, 7).isin(*months))
        return compute

    def test_interrupted_month_write_is_recomputed(
            self, spark, tmp_path, local_hdfs, monthly_source):
        """Crash a mitad del mes nuevo: la partición parcial sin marcador se
        recomputa; los meses committeados NO se tocan."""
        path = tmp_path / "monthly"
        calls = []
        compute = self.compute_from(monthly_source, calls)

        step_aug = self.make_monthly_step(
            spark, path, date(2025, 8, 31), date(2025, 8, 5), history=3)
        ppf.ensure_monthly_partitions(step_aug, "monthly", compute)
        assert calls == [["2025-06", "2025-07", "2025-08"]]

        # crash simulado durante la escritura de septiembre
        write_partial_parquet(spark,
            path / "month_partition=2025-09-30" / "process_date=2025-09-05",
            spark.createDataFrame([("corrupt", -1.0)], ["id", "v"]))

        step_sep = self.make_monthly_step(
            spark, path, date(2025, 9, 30), date(2025, 9, 5))
        result = ppf.ensure_monthly_partitions(step_sep, "monthly", compute)
        assert calls[-1] == ["2025-09"]                     # solo el mes corrupto
        rows = {r["id"]: r["v"] for r in result.collect()}
        assert "corrupt" not in rows                        # sin datos parciales
        assert rows["e4"] == 4.0                            # septiembre correcto
        assert rows["e1"] == 1.0                            # junio intacto

    def test_repeated_runs_same_location_are_idempotent(
            self, spark, tmp_path, local_hdfs, monthly_source):
        """Ejecutar varias veces el mismo vintage en el mismo lugar no
        recomputa ni altera el resultado."""
        path = tmp_path / "monthly"
        calls = []
        compute = self.compute_from(monthly_source, calls)

        step = self.make_monthly_step(
            spark, path, date(2025, 8, 31), date(2025, 8, 5), history=3)
        first = ppf.ensure_monthly_partitions(step, "monthly", compute)
        first_rows = sorted(
            (str(r["month_partition"]), r["id"], r["v"]) for r in first.collect())

        for _ in range(2):                                  # dos corridas más
            again = ppf.ensure_monthly_partitions(step, "monthly", compute)
            again_rows = sorted(
                (str(r["month_partition"]), r["id"], r["v"]) for r in again.collect())
            assert again_rows == first_rows
        assert calls == [["2025-06", "2025-07", "2025-08"]]  # una sola computación

    def test_older_vintage_after_newer_sees_no_future(
            self, spark, tmp_path, local_hdfs, monthly_source):
        """Vintage viejo tras uno nuevo compartiendo ubicación fija: la
        ventana [vintage-history, vintage) acota la lectura — nada de
        información del futuro, y nada que recomputar."""
        path = tmp_path / "monthly"
        calls = []
        compute = self.compute_from(monthly_source, calls)

        # 1) corre el vintage NUEVO (sep): materializa jun..sep
        step_sep = self.make_monthly_step(
            spark, path, date(2025, 9, 30), date(2025, 9, 5))
        ppf.ensure_monthly_partitions(step_sep, "monthly", compute)
        assert calls == [["2025-06", "2025-07", "2025-08", "2025-09"]]

        # 2) corre el vintage VIEJO (ago, ventana jun..ago) en el mismo lugar
        step_aug = self.make_monthly_step(
            spark, path, date(2025, 8, 31), date(2025, 8, 5), history=3)
        result = ppf.ensure_monthly_partitions(step_aug, "monthly", compute)
        assert len(calls) == 1                              # nada que recomputar
        months = {str(r["month_partition"]) for r in result.collect()}
        assert months == {"2025-06-30", "2025-07-31", "2025-08-31"}
        assert "2025-09-30" not in months                   # sin futuro

        # 3) un vintage aún más viejo cuya ventana pide un mes no materializado
        #    solo computa ESE mes; sigue sin ver septiembre
        step_jul = self.make_monthly_step(
            spark, path, date(2025, 7, 31), date(2025, 7, 5), history=3)
        result_jul = ppf.ensure_monthly_partitions(step_jul, "monthly", compute)
        assert calls[-1] == ["2025-05"]                     # mayo no existía
        assert {str(r["month_partition"]) for r in result_jul.collect()} == {
            "2025-06-30", "2025-07-31"}                     # mayo vacío: sin partición


# ----------------------------------------------------------------------------
# Config del catálogo all-time: raíz fija compartida entre vintages
# ----------------------------------------------------------------------------

class TestAllTimeCatalogLocation:
    def test_monthly_tables_use_fixed_root(self):
        """`s264_ceps_flattened_monthly` y `s264_ceps_flattened_groupby_monthly`
        viven en la raíz FIJA (sin vintage): la incrementalidad mes a mes
        exige compartir ubicación entre corridas."""
        import config.job as cj
        import config.ceps.rfc_nom_ranking as ccrbr

        fixed = str(cj.sbx["general_root_hdfs_fixed"])
        vintage_root = str(cj.sbx["general_root_hdfs"])
        assert cj.vintage in vintage_root                     # raíz con vintage
        assert cj.vintage not in fixed                        # raíz fija sin él

        for key in ("s264_ceps_flattened_monthly",
                "s264_ceps_flattened_groupby_monthly"):
            path = str(ccrbr.output[key]["table_or_hdfs"])
            assert path.startswith(fixed), key
            assert cj.vintage not in path, key

    def test_per_vintage_views_stay_under_vintage_root(self):
        """Las VISTAS por vintage (groupby, ranks, cases_replace, replaced)
        siguen bajo la raíz del vintage: un vintage viejo nunca recarga la
        visión computada con datos de su futuro."""
        import config.job as cj
        import config.ceps.rfc_nom_ranking as ccrbr

        vintage_root = str(cj.sbx["general_root_hdfs"])
        for key in ("rfc_curp_analysis_s264_ceps",
                "s264_ceps_flattened_groupby",
                "s264_ceps_flattened_ranks",
                "s264_ceps_flattened_rank_rfc_by_cta_cases_replace",
                "s264_ceps_flattened_rank_rfc_by_nom_cases_replace",
                "rfc_curp_analysis_s264_ceps_replaced"):
            path = str(ccrbr.output[key]["table_or_hdfs"])
            assert path.startswith(vintage_root), key
