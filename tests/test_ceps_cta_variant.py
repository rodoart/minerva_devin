"""Tests de la variante cta del grafo CEPS y del mapeo cta -> numcliente.

Cubre:
- `build_mapeo_cta_numcliente`: construcción del mapeo desde la tabla
  replaced (lados E/R, ranking por nº de txns y recencia, exclusión de nulos).
- `dedupe_mapeo_cta_numcliente`: al leer varias particiones gana la más reciente.
- `resolve_cta_node_ids` / `join_mapeo_cta_numcliente`: resolución de ids de
  la variante cta (id = cuenta, fallback nom_, drop) e imputación por lado.
- Factories de config `build(variant)`: namespacing de rutas (rfc intacto),
  consistencia de la cadena y `column_spec`.
"""
import os

import pytest

pytest.importorskip("pyspark")

# config.job exige MINERVA_TODAY en import; los tests usan un vintage fijo.
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

from pyspark.sql.types import StringType, StructField, StructType

from pipelines.ceps.txn_replacement import build_mapeo_cta_numcliente
from pipelines.graph_making.ceps.special_treatment import (
    dedupe_mapeo_cta_numcliente, join_mapeo_cta_numcliente, resolve_cta_node_ids)

pytestmark = pytest.mark.spark

REPLACED_COLUMNS = ["cve_tipo_orden", "cta_ord", "cta_ben", "customer_id",
    "fec_informacion", "mis_date", "process_date"]
REPLACED_SCHEMA = StructType(
    [StructField(c, StringType()) for c in REPLACED_COLUMNS])


def _replaced(spark, rows):
    return spark.createDataFrame(rows, REPLACED_SCHEMA)


# ----------------------------------------------------------------------------
# build_mapeo_cta_numcliente
# ----------------------------------------------------------------------------

class TestBuildMapeoCtaNumcliente:
    def test_emisor_y_receptor_aportan_pares(self, spark):
        """cta_ord con 'E' y cta_ben con 'R' alimentan el mismo mapeo."""
        df = _replaced(spark, [
            ("E", "CTA1", None, "100", "2025-08-10", "202508", "2025-08-31"),
            ("R", None, "CTA1", "100", "2025-08-11", "202508", "2025-08-31"),
        ])
        result = build_mapeo_cta_numcliente(df).collect()
        assert len(result) == 1
        assert result[0]["cta"] == "CTA1"
        assert result[0]["numcliente"] == "100"
        assert result[0]["txn_count"] == 2

    def test_gana_el_numcliente_con_mas_txns(self, spark):
        df = _replaced(spark, [
            ("E", "CTA1", None, "100", "2025-08-10", "202508", "2025-08-31"),
            ("E", "CTA1", None, "100", "2025-08-11", "202508", "2025-08-31"),
            ("E", "CTA1", None, "200", "2025-08-12", "202508", "2025-08-31"),
        ])
        result = build_mapeo_cta_numcliente(df).collect()
        assert len(result) == 1
        assert result[0]["numcliente"] == "100"

    def test_empate_gana_el_mas_reciente(self, spark):
        df = _replaced(spark, [
            ("E", "CTA1", None, "100", "2025-08-01", "202508", "2025-08-31"),
            ("E", "CTA1", None, "200", "2025-08-20", "202508", "2025-08-31"),
        ])
        result = build_mapeo_cta_numcliente(df).collect()
        assert len(result) == 1
        assert result[0]["numcliente"] == "200"

    def test_nulos_excluidos(self, spark):
        """Filas sin cta o sin customer_id no producen mapeo."""
        df = _replaced(spark, [
            ("E", None, None, "100", "2025-08-10", "202508", "2025-08-31"),
            ("E", "CTA1", None, None, "2025-08-10", "202508", "2025-08-31"),
            ("R", None, None, None, "2025-08-10", "202508", "2025-08-31"),
        ])
        assert build_mapeo_cta_numcliente(df).count() == 0

    def test_lado_sin_cuental_no_cruza(self, spark):
        """Una fila 'E' no aporta su cta_ben ni una 'R' su cta_ord."""
        df = _replaced(spark, [
            ("E", "CTA1", "CTA2", "100", "2025-08-10", "202508", "2025-08-31"),
        ])
        result = {r["cta"] for r in build_mapeo_cta_numcliente(df).collect()}
        assert result == {"CTA1"}


# ----------------------------------------------------------------------------
# dedupe_mapeo_cta_numcliente
# ----------------------------------------------------------------------------

class TestDedupeMapeo:
    def test_gana_la_particion_mas_reciente(self, spark):
        mapeo = spark.createDataFrame([
            ("CTA1", "100", 5, "2025-07-15", "202507", "2025-07-31"),
            ("CTA1", "200", 1, "2025-08-15", "202508", "2025-08-31"),
        ], ["cta", "numcliente", "txn_count", "max_fec_informacion",
            "mis_date", "process_date"])
        result = dedupe_mapeo_cta_numcliente(mapeo).collect()
        assert len(result) == 1
        assert result[0]["numcliente"] == "200"

    def test_una_fila_por_cta(self, spark):
        mapeo = spark.createDataFrame([
            ("CTA1", "100", 5, "2025-07-15", "202507", "2025-07-31"),
            ("CTA1", "200", 3, "2025-07-20", "202507", "2025-07-31"),
            ("CTA2", "300", 1, "2025-07-15", "202507", "2025-07-31"),
        ], ["cta", "numcliente", "txn_count", "max_fec_informacion",
            "mis_date", "process_date"])
        result = dedupe_mapeo_cta_numcliente(mapeo)
        assert result.count() == 2
        assert {r["cta"] for r in result.collect()} == {"CTA1", "CTA2"}


# ----------------------------------------------------------------------------
# resolve_cta_node_ids / join_mapeo_cta_numcliente
# ----------------------------------------------------------------------------

class TestResolveCtaNodeIds:
    COLUMNS = ["cta_src", "cta_dst", "nom_src", "nom_dst"]
    SCHEMA = StructType([StructField(c, StringType()) for c in COLUMNS])

    def test_cta_es_el_id(self, spark):
        df = spark.createDataFrame(
            [("CTA1", "CTA2", "NOM1", "NOM2")], self.SCHEMA)
        result = resolve_cta_node_ids(df).collect()
        assert len(result) == 1
        assert result[0]["id_src"] == "CTA1"
        assert result[0]["id_dst"] == "CTA2"

    def test_fallback_a_nombre(self, spark):
        df = spark.createDataFrame(
            [(None, "CTA2", "NOM1", None)], self.SCHEMA)
        result = resolve_cta_node_ids(df).collect()
        assert len(result) == 1
        assert result[0]["id_src"] == "nom_NOM1"
        assert result[0]["id_dst"] == "CTA2"

    def test_drop_sin_cta_ni_nombre(self, spark):
        df = spark.createDataFrame([
            (None, "CTA2", None, "NOM2"),   # src sin id -> drop
            ("CTA1", None, "NOM1", None),   # dst sin id -> drop
            ("CTA1", "CTA2", None, None),   # válida
        ], self.SCHEMA)
        result = resolve_cta_node_ids(df).collect()
        assert len(result) == 1
        assert result[0]["id_src"] == "CTA1"
        assert result[0]["id_dst"] == "CTA2"


class TestJoinMapeo:
    def test_imputa_ambos_lados(self, spark):
        df = spark.createDataFrame(
            [("CTA1", "CTA2"), ("CTA3", "CTA9")], ["cta_src", "cta_dst"])
        mapeo = spark.createDataFrame(
            [("CTA1", "100"), ("CTA9", "900")], ["cta", "numcliente"])
        result = {r["cta_src"]: r for r in
            join_mapeo_cta_numcliente(df, mapeo).collect()}
        assert result["CTA1"]["numcliente_src"] == "100"
        assert result["CTA1"]["numcliente_dst"] is None
        assert result["CTA3"]["numcliente_src"] is None
        assert result["CTA3"]["numcliente_dst"] == "900"


# ----------------------------------------------------------------------------
# Factories de config: namespacing y consistencia de la cadena
# ----------------------------------------------------------------------------

class TestVariantSubpath:
    def test_rfc_conserva_rutas(self):
        from config.graph_making.ceps.variants import variant_subpath
        assert variant_subpath("graph/ceps", "rfc", "group_by") \
            == "graph/ceps/group_by"

    def test_cta_inserta_segmento(self):
        from config.graph_making.ceps.variants import variant_subpath
        assert variant_subpath("graph/ceps", "cta", "group_by") \
            == "graph/ceps/cta/group_by"

    def test_variante_desconocida_falla(self):
        from config.graph_making.ceps.variants import check_variant
        with pytest.raises(ValueError, match="desconocida"):
            check_variant("banxico")


class TestConfigFactories:
    def test_rfc_module_level_es_build_default(self):
        """El input/output de módulo (back-compat) coincide con build('rfc')."""
        import config.graph_making.ceps.special_treatment as ccspt
        import config.graph_making.ceps.group_by as ccgb
        import config.features.ceps.vector_assembler as cvas
        for module in (ccspt, ccgb, cvas):
            input_b, output_b = module.build("rfc")
            assert list(module.input) == list(input_b)
            assert list(module.output) == list(output_b)

    def test_rfc_rutas_sin_segmento_cta(self):
        import config.graph_making.ceps.group_by as ccgb
        _, output = ccgb.build("rfc")
        for spec in output.values():
            assert "/cta/" not in str(spec["table_or_hdfs"])

    def test_cta_namespaced(self):
        import config.graph_making.ceps.group_by as ccgb
        _, output = ccgb.build("cta")
        for spec in output.values():
            assert "/cta/" in str(spec["table_or_hdfs"])

    def test_cta_special_treatment_tiene_mapeo(self):
        import config.graph_making.ceps.special_treatment as ccspt
        input_rfc, _ = ccspt.build("rfc")
        input_cta, _ = ccspt.build("cta")
        assert "mapeo_cta_numcliente" not in input_rfc
        assert "mapeo_cta_numcliente" in input_cta
        # La clave del mapeo es reapuntar-a-otra-tabla friendly.
        assert "table_or_hdfs" in input_cta["mapeo_cta_numcliente"]

    def test_cadena_consistente_por_variante(self):
        """El input de cada stage es el output del stage previo (misma variante)."""
        import config.graph_making.ceps.special_treatment as ccspt
        import config.graph_making.ceps.group_by as ccgb
        import config.graph_making.ceps.edges_and_nodes as ccgen
        import config.features.ceps.graph_features as cfgf
        import config.features.ceps.target_propagation_features as cfcf
        import config.features.ceps.cluster_features as cclf
        import config.features.ceps.vector_assembler as cvas
        for variant in ("rfc", "cta"):
            _, st_out = ccspt.build(variant)
            gb_in, gb_out = ccgb.build(variant)
            ean_in, ean_out = ccgen.build(variant)
            gf_in, gf_out = cfgf.build(variant)
            tp_in, tp_out = cfcf.build(variant)
            cl_in, cl_out = cclf.build(variant)
            va_in, _ = cvas.build(variant)
            assert str(gb_in["missing_treatment"]["table_or_hdfs"]) \
                == str(st_out["missing_treatment"]["table_or_hdfs"])
            assert str(ean_in["group_by_txn"]["table_or_hdfs"]) \
                == str(gb_out["group_by_txn"]["table_or_hdfs"])
            assert str(ean_in["group_by_id"]["table_or_hdfs"]) \
                == str(gb_out["group_by_id"]["table_or_hdfs"])
            assert str(gf_in["edges"]["table_or_hdfs"]) \
                == str(ean_out["edges"]["table_or_hdfs"])
            assert str(gf_in["nodes"]["table_or_hdfs"]) \
                == str(ean_out["nodes"]["table_or_hdfs"])
            assert str(tp_in["group_by_id"]["table_or_hdfs"]) \
                == str(gb_out["group_by_id"]["table_or_hdfs"])
            assert str(tp_in["edges"]["table_or_hdfs"]) \
                == str(ean_out["edges"]["table_or_hdfs"])
            assert str(cl_in["components"]["table_or_hdfs"]) \
                == str(gf_out["components"]["table_or_hdfs"])
            assert str(cl_in["nodes_join_target_lovelace"]["table_or_hdfs"]) \
                == str(tp_out["nodes_join_target_lovelace"]["table_or_hdfs"])
            assert str(va_in["nodes"]["table_or_hdfs"]) \
                == str(ean_out["nodes"]["table_or_hdfs"])

    def test_nodes_join_target_cta_no_pisa_rfc(self):
        import config.features.ceps.target_propagation_features as cfcf
        _, out_rfc = cfcf.build("rfc")
        _, out_cta = cfcf.build("cta")
        assert str(out_rfc["nodes_join_target_lovelace"]["table_or_hdfs"]) \
            != str(out_cta["nodes_join_target_lovelace"]["table_or_hdfs"])

    def test_feature_sources_namespaced(self):
        import config.features.ceps.cluster_features as cclf
        import config.features.ceps.vector_assembler as cvas
        for path in cclf.build_graph_feature_sources("cta").values():
            assert "/cta/" in str(path)
        for path in cclf.build_graph_feature_sources("rfc").values():
            assert "/cta/" not in str(path)
        for source in cvas.build_feature_sources("cta").values():
            if "path" in source:
                assert "/cta/" in str(source["path"])

    def test_semillas_lovelace_compartidas(self):
        """target_cta/target_numcliente son externos al grafo: misma ruta."""
        import config.features.ceps.target_propagation_features as cfcf
        in_rfc, _ = cfcf.build("rfc")
        in_cta, _ = cfcf.build("cta")
        for key in ("target_cta", "target_numcliente"):
            assert str(in_rfc[key]["table_or_hdfs"]) \
                == str(in_cta[key]["table_or_hdfs"])


class TestColumnSpec:
    def test_rfc_es_el_historico(self):
        import config.graph_making.ceps.group_by as ccgb
        spec = ccgb.column_spec("rfc")
        assert spec["SRC_COLUMNS"] == ccgb.SRC_COLUMNS
        assert spec["DST_COLUMNS"] == ccgb.DST_COLUMNS
        assert spec["COMMON_COLUMNS"] == ccgb.COMMON_COLUMNS
        assert "numcliente" in spec["COMMON_COLUMNS"]

    def test_cta_numcliente_por_lado(self):
        import config.graph_making.ceps.group_by as ccgb
        spec = ccgb.column_spec("cta")
        assert "numcliente_src" in spec["SRC_COLUMNS"]
        assert "numcliente_dst" in spec["DST_COLUMNS"]
        # sin el numcliente común: colisionaría con el imputado tras el rename
        assert "numcliente" not in spec["COMMON_COLUMNS"]


class TestStepVariantWiring:
    def test_variant_en_step_y_step_name(self, spark, tmp_path):
        import config.job as cj
        import config.graph_making.ceps.special_treatment as ccspt
        import pipelines.graph_making.ceps.special_treatment as p_gm_st
        input_cta, output_cta = ccspt.build("cta")
        step = p_gm_st.CepsSpecialTreatment(
            date_treatment=cj.date_treatment,
            input_hive=input_cta,
            output_hive=output_cta,
            is_dynamic=True,
            cohort=cj.COHORT,
            sqlContext=spark,
            variant="cta",
        )
        assert step.variant == "cta"
        assert step.step_name.endswith("_cta")

    def test_variant_default_es_rfc(self, spark):
        import config.job as cj
        import config.graph_making.ceps.special_treatment as ccspt
        import pipelines.graph_making.ceps.special_treatment as p_gm_st
        step = p_gm_st.CepsSpecialTreatment(
            date_treatment=cj.date_treatment,
            input_hive=ccspt.input,
            output_hive=ccspt.output,
            is_dynamic=True,
            cohort=cj.COHORT,
            sqlContext=spark,
        )
        assert step.variant == "rfc"
        assert "_cta" not in step.step_name
