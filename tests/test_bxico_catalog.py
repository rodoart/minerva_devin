"""Tests para la integración del catálogo Banxico `bxico_rfc_curp_cat`.

Cubre:
- `merge_name_parts`: construcción de `nom` sin duplicar partes ya contenidas.
- `build_ban_catalog`: catálogo nom_ban -> id_ban desde s264_ceps.
- `prepare_bxico_catalog`: dedup por cta, elección de rfc_curp, cruce de
  banco_cta y proyección al esquema aplanado con `source`.
- `SubStep.optional_input`: inputs opcionales (None si falta o falla).
- Prioridad de origen: `SOURCE_PRIORITY` en las ventanas de ranking.
"""
import os

import pytest

# config.job exige MINERVA_TODAY en import; los tests usan un vintage fijo.
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

pytest.importorskip("pyspark")

from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import StructType, StructField, StringType

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def p_rnr(spark):
    """Importa el pipeline una vez existe la SparkSession (el config crea
    objetos Window en import, que exigen contexto activo)."""
    import pipelines.ceps.rfc_nom_ranking as m
    return m


@pytest.fixture(scope="module")
def bxico_config(spark):
    """Constantes del config (import diferido: crea Windows en import)."""
    import config.ceps.rfc_nom_ranking as c
    return c


BXICO_SCHEMA = ["nombre_1", "nombre_2", "apellido_paterno", "apellido_materno",
    "curp", "rfc", "domicilio", "cta", "banco_cta", "banco_reporte",
    "fecha_de_subida"]
BXICO_STRUCT = StructType(
    [StructField(c, StringType()) for c in BXICO_SCHEMA])


def _bxico_df(spark, rows):
    return spark.createDataFrame(rows, BXICO_STRUCT)


def _bxico_row(nombre_1=None, nombre_2=None, apellido_paterno=None,
        apellido_materno=None, curp=None, rfc=None, cta=None,
        banco_cta=None, fecha_de_subida="2025-08-01"):
    return (nombre_1, nombre_2, apellido_paterno, apellido_materno,
        curp, rfc, "DOMICILIO", cta, banco_cta, "BANCO REPORTE",
        fecha_de_subida)


@pytest.fixture()
def ban_catalog(spark):
    return spark.createDataFrame(
        [("BANAMEX", "40002"), ("BBVA", "40012")],
        ["nom_ban_key", "id_ban"])


# ----------------------------------------------------------------------------
# norm: solo recorta un sufijo de una letra separado por espacio
# ----------------------------------------------------------------------------

class TestNorm:
    def _norm(self, spark, p_rnr, value):
        df = spark.createDataFrame([(value,)], ["x"])
        return df.select(p_rnr.norm("x").alias("x")).first()["x"]

    def test_banamex_suffix_stripped(self, spark, p_rnr):
        assert self._norm(spark, p_rnr, "GUPL6105211K7 S") == "GUPL6105211K7"

    def test_last_letter_preserved(self, spark, p_rnr):
        assert self._norm(spark, p_rnr, "PEREZ") == "PEREZ"

    def test_accented_last_letter_preserved(self, spark, p_rnr):
        assert self._norm(spark, p_rnr, "MARÍA") == "MARÍA"

    def test_upper_applied(self, spark, p_rnr):
        assert self._norm(spark, p_rnr, "  perez ") == "PEREZ"


# ----------------------------------------------------------------------------
# merge_name_parts
# ----------------------------------------------------------------------------

class TestMergeNameParts:
    def _nom(self, spark, p_rnr, **parts):
        cols = ["nombre_1", "nombre_2", "apellido_paterno", "apellido_materno"]
        schema = StructType([StructField(c, StringType()) for c in cols])
        df = spark.createDataFrame(
            [tuple(parts.get(c) for c in cols)], schema)
        return df.select(
            p_rnr.merge_name_parts(*cols).alias("nom")).first()["nom"]

    def test_full_name_in_nombre_1(self, spark, p_rnr):
        """Todo (nombres + apellidos) viene en nombre_1."""
        assert self._nom(spark, p_rnr,
            nombre_1="LUIS ALBERTO GUTIERREZ PEREZ") == "LUIS ALBERTO GUTIERREZ PEREZ"

    def test_nombre_2_contained_in_nombre_1(self, spark, p_rnr):
        """nombre_1 ya trae nombre_1+nombre_2: no se duplica."""
        assert self._nom(spark, p_rnr,
            nombre_1="LUIS ALBERTO", nombre_2="ALBERTO",
            apellido_paterno="GUTIERREZ") == "LUIS ALBERTO GUTIERREZ"

    def test_apellidos_all_in_apellido_paterno(self, spark, p_rnr):
        """apellido_paterno trae ambos apellidos: materno no se duplica."""
        assert self._nom(spark, p_rnr,
            nombre_1="LUIS", apellido_paterno="GUTIERREZ PEREZ",
            apellido_materno="PEREZ") == "LUIS GUTIERREZ PEREZ"

    def test_normal_concat(self, spark, p_rnr):
        assert self._nom(spark, p_rnr,
            nombre_1="LUIS", nombre_2="ALBERTO",
            apellido_paterno="GUTIERREZ", apellido_materno="PEREZ"
        ) == "LUIS ALBERTO GUTIERREZ PEREZ"

    def test_all_null_gives_null(self, spark, p_rnr):
        assert self._nom(spark, p_rnr) is None

    def test_empty_parts_skipped(self, spark, p_rnr):
        assert self._nom(spark, p_rnr,
            nombre_1="ANA", nombre_2="",
            apellido_paterno="LOPEZ") == "ANA LOPEZ"


# ----------------------------------------------------------------------------
# build_ban_catalog
# ----------------------------------------------------------------------------

class TestBuildBanCatalog:
    def test_union_ord_ben_and_normalize(self, spark, p_rnr):
        s264 = spark.createDataFrame(
            [(1, "40002", "Bánamex", "40012", "B.B.V.A"),
             (2, "40036", "SANTANDER", None, None)],
            ["id", "id_ban_ord", "nom_ban_ord", "id_ban_ben", "nom_ban_ben"])
        df = p_rnr.build_ban_catalog(s264)
        rows = {r["nom_ban_key"]: r["id_ban"] for r in df.collect()}
        assert rows["BANAMEX"] == "40002"        # acento eliminado
        assert rows["B B V A"] == "40012"        # separadores -> espacio
        assert rows["SANTANDER"] == "40036"
        assert len(rows) == 3                    # nulls filtrados


# ----------------------------------------------------------------------------
# prepare_bxico_catalog
# ----------------------------------------------------------------------------

class TestPrepareBxicoCatalog:
    def test_dedup_by_cta_keeps_latest(self, spark, p_rnr, ban_catalog):
        df = p_rnr.prepare_bxico_catalog(
            _bxico_df(spark, [
                _bxico_row(nombre_1="VIEJO", rfc="XAAA010101AAA",
                    cta="111", fecha_de_subida="2025-01-01"),
                _bxico_row(nombre_1="NUEVO", rfc="XBBB020202BBB",
                    cta="111", fecha_de_subida="2025-08-01"),
            ]), ban_catalog)
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["nom"] == "NUEVO"
        assert rows[0]["rfc_curp"] == "XBBB020202BBB"

    def test_rfc_preferred_over_curp_when_valid(self, spark, p_rnr, ban_catalog):
        df = p_rnr.prepare_bxico_catalog(
            _bxico_df(spark, [_bxico_row(
                nombre_1="ANA", curp="AAPL900615MDFLRN08",
                rfc="AAPL900615AB1", cta="222")]),
            ban_catalog)
        row = df.first()
        assert row["rfc_curp"] == "AAPL900615AB1"
        assert row["rfc_curp_kind"] == "rfc_fisica"
        assert row["is_rfc_curp_valid"] == 1

    def test_curp_used_when_rfc_invalid(self, spark, p_rnr, ban_catalog):
        df = p_rnr.prepare_bxico_catalog(
            _bxico_df(spark, [_bxico_row(
                nombre_1="ANA", curp="AAPL900615MDFLRN08",
                rfc="NO VALIDO", cta="333")]),
            ban_catalog)
        row = df.first()
        assert row["rfc_curp"] == "AAPL900615MDFLRN08"
        assert row["rfc_curp_kind"] == "curp"

    def test_banco_cta_mapped_to_id_ban(self, spark, p_rnr, ban_catalog):
        df = p_rnr.prepare_bxico_catalog(
            _bxico_df(spark, [
                _bxico_row(nombre_1="A", cta="1", banco_cta="banamex"),
                _bxico_row(nombre_1="B", cta="2", banco_cta="BANCO X"),
            ]), ban_catalog)
        rows = {r["cta"]: r["id_ban"] for r in df.collect()}
        assert rows["1"] == "40002"    # case-insensitive match
        assert rows["2"] is None       # banco desconocido -> id_ban null

    def test_source_and_schema(self, spark, p_rnr, bxico_config, ban_catalog):
        df = p_rnr.prepare_bxico_catalog(
            _bxico_df(spark, [_bxico_row(
                nombre_1="A", cta="1")]), ban_catalog)
        assert set(df.columns) == {"nom", "cta", "id_ban", "rfc_curp",
            "rfc_curp_kind", "is_rfc_curp_valid", "fec_informacion",
            "oper_mto", "source"}
        row = df.first()
        assert row["source"] == bxico_config.BXICO_SOURCE
        assert row["oper_mto"] == 0.0
        assert row["fec_informacion"] == "2025-08-01"

    def test_name_parts_cleaned_like_s264(self, spark, p_rnr, ban_catalog):
        """El mismo proceso de limpieza: acentos, separadores, null synonyms."""
        df = p_rnr.prepare_bxico_catalog(
            _bxico_df(spark, [_bxico_row(
                nombre_1="JOSÉ,MARÍA", apellido_paterno="GARCÍA-LÓPEZ",
                cta="444")]), ban_catalog)
        assert df.first()["nom"] == "JOSE MARIA GARCIA LOPEZ"


# ----------------------------------------------------------------------------
# optional_input
# ----------------------------------------------------------------------------

class TestOptionalInput:
    def _substep(self, spark, p_rnr, input_hive, loader=None):
        """SubStep mínimo sin __init__; `loader` simula standard_load_parquet_or_table."""
        substep = object.__new__(p_rnr.SubStep)
        substep.sqlContext = spark
        substep.input_hive = input_hive
        substep.input_parameters = {"vintage_date": "2025-08-31"}
        if loader is not None:
            substep.standard_load_parquet_or_table = loader
        return substep

    def test_missing_key_returns_none(self, spark, p_rnr):
        substep = self._substep(spark, p_rnr, {})
        assert substep.optional_input("bxico_rfc_curp_cat") is None

    def test_load_failure_returns_none(self, spark, p_rnr):
        def boom(*a, **k):
            raise RuntimeError("boom")
        substep = self._substep(
            spark, p_rnr, {"bxico_rfc_curp_cat": {"table_or_hdfs": "x"}},
            loader=boom)
        assert substep.optional_input("bxico_rfc_curp_cat") is None

    def test_success_returns_df(self, spark, p_rnr):
        sentinel = spark.createDataFrame([(1,)], ["a"])
        substep = self._substep(
            spark, p_rnr, {"bxico_rfc_curp_cat": {"table_or_hdfs": "x"}},
            loader=lambda *a, **k: sentinel)
        assert substep.optional_input("bxico_rfc_curp_cat") is sentinel


# ----------------------------------------------------------------------------
# Prioridad de origen en las ventanas de ranking
# ----------------------------------------------------------------------------

class TestSourcePriorityWindow:
    def _rank(self, spark, bxico_config, rows):
        cols = ["cta", "nom", "rfc_curp", "rfc_curp_ends_with_xxx",
            "RFC_CURP_KIND_PRIORITY", "SOURCE_PRIORITY", "cnt_rfc_by_cta",
            "tot_oper_mto", "lst_fec_informacion_hora_oper"]
        df = spark.createDataFrame(rows, cols)
        return (df
            .withColumn("rank",
                row_number().over(bxico_config.RFC_BY_CTA_PRIORITY_WINDOW))
            .collect())

    def test_bxico_wins_over_s264_despite_worse_criteria(self, spark, p_rnr,
            bxico_config):
        """bxico (SOURCE_PRIORITY=0) gana aunque tenga peor kind/cnt/mto."""
        rows = [
            ("c1", "ANA", "AAPL900615AB1", 0, 1, 1, 100, 9e6, "2025-08-01#10"),  # s264, mejor kind y cnt
            ("c1", "ANA", "AAPL900615MDFLRN08", 0, 2, 0, 1, 0.0, "2025-07-01#"),  # bxico, peor kind
        ]
        ranked = {r["rfc_curp"]: r["rank"]
            for r in self._rank(spark, bxico_config, rows)}
        assert ranked["AAPL900615MDFLRN08"] == 1   # bxico primero
        assert ranked["AAPL900615AB1"] == 2

    def test_null_rfc_bxico_does_not_beat_valid_s264(self, spark, p_rnr,
            bxico_config):
        """Un bxico sin RFC no gana a un s264 con RFC válido (guarda de nulos)."""
        rows = [
            ("c1", "ANA", "AAPL900615AB1", 0, 1, 1, 5, 100.0, "2025-08-01#10"),
            ("c1", "ANA", None, 0, 6, 0, 1, 0.0, "2025-07-01#"),
        ]
        ranked = [r["rfc_curp"] for r in sorted(
            self._rank(spark, bxico_config, rows), key=lambda r: r["rank"])]
        assert ranked[0] == "AAPL900615AB1"
        assert ranked[1] is None

    def test_within_bxico_other_criteria_govern(self, spark, p_rnr,
            bxico_config):
        """Entre candidatos bxico mandan los demás criterios (mejor kind gana)."""
        rows = [
            ("c1", "ANA", "AAPL900615MDFLRN08", 0, 2, 0, 1, 0.0, "2025-07-01#"),  # curp
            ("c1", "ANA", "AAPL900615AB1", 0, 1, 0, 1, 0.0, "2025-07-01#"),       # rfc_fisica
        ]
        ranked = {r["rfc_curp"]: r["rank"]
            for r in self._rank(spark, bxico_config, rows)}
        assert ranked["AAPL900615AB1"] == 1
        assert ranked["AAPL900615MDFLRN08"] == 2
