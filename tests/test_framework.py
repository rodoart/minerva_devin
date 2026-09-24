"""Tests para libs.framework (Step, decoradores, helpers de substep).

Importar `libs.framework` requiere pyspark instalado, así que todo el módulo
salta si no está disponible. Los tests de IO usan la fixture `spark`.
"""
from datetime import date

import pytest

pytest.importorskip("pyspark")

import libs.framework as ppf
from libs.framework import Step, cached_property


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

class DummyStep(Step):
    """Step mínimo con step_action contable."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.action_calls = 0
        self.is_dynamic = False
    def step_action(self):
        self.action_calls += 1
        return {"done": True}


def make_step(**kwargs) -> DummyStep:
    kwargs.setdefault("input_hive", {})
    kwargs.setdefault("output_hive", {})
    kwargs.setdefault("input_parameters", {
        "vintage": "202307",
        "vintage_date": date(2023, 7, 31),
        "process_date": date(2023, 8, 5),
        "process_date_str": "2023-08-05",
        "current_month_date_str": "2023-07-31",
    })
    return DummyStep(**kwargs)


def date_treatment() -> dict:
    return {
        "vintage": "202307",
        "vintage_date": date(2023, 7, 31),
        "current_month_date_str": "2023-07-31",
        "process_date_str": "2023-08-05",
        "process_date": date(2023, 8, 5),
    }


# ----------------------------------------------------------------------------
# cached_property
# ----------------------------------------------------------------------------

class TestCachedProperty:
    def test_computes_once(self):
        class C:
            calls = 0
            @cached_property
            def value(self):
                self.calls += 1
                return 42
        c = C()
        assert c.value == 42
        assert c.value == 42
        assert c.calls == 1

    def test_per_instance_cache(self):
        class C:
            def __init__(self, v):
                self._v = v
            @cached_property
            def value(self):
                return self._v
        assert C(1).value == 1
        assert C(2).value == 2

    def test_stores_result_in_instance_dict(self):
        class C:
            @cached_property
            def value(self):
                return 7
        c = C()
        assert "_value_cache" not in c.__dict__
        _ = c.value
        assert c.__dict__["_value_cache"] == 7


# ----------------------------------------------------------------------------
# Step
# ----------------------------------------------------------------------------

class TestStepInit:
    def test_defaults(self):
        s = make_step()
        assert s.previous_step is None
        assert s.input_linux == {}
        assert s.output_linux == {}
        assert s.sqlContext is None
        assert s.tmp_paths == []
        assert s._decorated_cache == {}

    def test_output_parameters_keyed_by_step_name(self):
        s = make_step()
        assert list(s.output_parameters.keys()) == [s.step_name]
        assert s.output_parameters[s.step_name] == {}

    def test_auto_step_name_underscored_and_numbered(self):
        s1 = make_step()
        s2 = make_step()
        assert s1.step_name.startswith("dummy_step_")
        assert s2.step_name.startswith("dummy_step_")
        assert s1.step_name != s2.step_name   # contador incremental

    def test_explicit_step_name(self):
        s = make_step(step_name="mi_step")
        assert s.step_name == "mi_step"

    def test_repr_and_str(self):
        s = make_step(step_name="mi_step")
        assert str(s) == "Step(mi_step)"
        assert repr(s) == "Step(mi_step)"


class TestStepExecute:
    def test_execute_calls_step_action(self):
        s = make_step()
        result = s.execute()
        assert s.action_calls == 1
        assert result == {"done": True}

    def test_execute_runs_previous_step_first(self):
        parent = make_step()
        child = make_step(previous_step=[parent])
        child.execute()
        assert parent.action_calls == 1
        assert child.action_calls == 1

    def test_execute_merges_previous_output_parameters(self):
        parent = make_step()
        parent.output_parameters[parent.step_name]["some_key"] = "value"
        child = make_step(previous_step=[parent])
        child.execute()
        assert child.input_parameters[parent.step_name]["some_key"] == "value"

    def test_execute_runs_single_previous_step_not_list(self):
        parent = make_step()
        child = make_step(previous_step=parent)
        child.execute()
        assert parent.action_calls == 1

    def test_base_step_action_raises_not_implemented(self):
        s = Step()
        with pytest.raises(NotImplementedError):
            s.execute()


class TestBuildStepInitKwargs:
    def test_input_parameters_mapping(self):
        kwargs = ppf.build_step_init_kwargs(
            date_treatment=date_treatment(),
            input_hive={"a": 1},
            output_hive={"b": 2},
            step_name_prefix="Prefix_",
        )
        params = kwargs["input_parameters"]
        assert params["vintage"] == "202307"
        assert params["vintage_date"] == date(2023, 7, 31)
        assert params["current_month_date"] == "2023-07-31"
        assert params["process_date_str"] == "2023-08-05"
        assert params["vintage_mmyy"] == "202307"
        assert params["process_date"] == date(2023, 8, 5)

    def test_step_name_includes_vintage(self):
        kwargs = ppf.build_step_init_kwargs(
            date_treatment=date_treatment(),
            input_hive={}, output_hive={}, step_name_prefix="Prefix_",
        )
        assert kwargs["step_name"] == "Prefix_202307"

    def test_extra_kwargs_merged(self):
        kwargs = ppf.build_step_init_kwargs(
            date_treatment=date_treatment(),
            input_hive={}, output_hive={}, step_name_prefix="P_",
            extra_kwargs={"sqlContext": "CTX", "previous_step": ["x"]},
        )
        assert kwargs["sqlContext"] == "CTX"
        assert kwargs["previous_step"] == ["x"]

    def test_no_extra_kwargs(self):
        kwargs = ppf.build_step_init_kwargs(
            date_treatment=date_treatment(),
            input_hive={}, output_hive={}, step_name_prefix="P_",
        )
        assert "sqlContext" not in kwargs


class TestSubstepHelpers:
    def test_collect_step_output(self):
        s = make_step(step_name="s1")
        result = ppf.collect_step_output(s, {"df": "x"}, "my_output")
        assert result["s1"]["my_output"] == {"df": "x"}
        assert s.output_parameters["s1"]["my_output"] == {"df": "x"}

    def test_run_substep_and_collect(self):
        parent = make_step(step_name="parent")
        substep = make_step(step_name="sub")
        substep.output_parameters["sub"]["inner"] = "data"
        result = ppf.run_substep_and_collect(parent, substep, "sub_output")
        assert substep.action_calls == 1                     # ejecutó el substep
        assert result["parent"]["sub_output"] == {"inner": "data"}

    def test_inherit_parent_step_attributes(self):
        parent = make_step()
        parent.cohort = "SBX"
        parent.is_dynamic = True
        parent.sqlContext = "CTX"
        parent.standard_load_parquet_or_table = lambda *a, **k: "load"
        sub = make_step()
        ppf.inherit_parent_step_attributes(sub, parent)
        assert sub.cohort == "SBX"
        assert sub.is_dynamic is True
        assert sub.sqlContext == "CTX"
        assert sub.parent is parent
        assert sub.input_hive is parent.input_hive
        assert sub.output_hive is parent.output_hive
        assert sub.input_parameters is parent.input_parameters
        assert sub.vintage == "202307"
        assert sub.vintage_date == date(2023, 7, 31)
        assert sub.standard_load_parquet_or_table() == "load"


class TestTmpPathCleanup:
    def test_collect_own_tmp_paths(self):
        s = make_step()
        s.tmp_paths = ["/tmp/a", "/tmp/b"]
        assert s.collect_tmp_paths() == ["/tmp/a", "/tmp/b"]

    def test_collects_marked_output_paths(self):
        s = make_step(output_hive={
            "tmp": {"table_or_hdfs": "/tmp/out", "keep_or_delete": "delete"},
            "keep": {"table_or_hdfs": "/keep/out", "keep_or_delete": "keep"},
            "part": {"table_or_hdfs": "/p/out", "keep_or_delete": "delete",
                "information_date_column": "mis_date"},   # particionada: NO se borra
        })
        assert s.collect_tmp_paths() == ["/tmp/out"]

    def test_collects_from_previous_chain(self):
        parent = make_step()
        parent.tmp_paths = ["/tmp/parent"]
        child = make_step(previous_step=[parent])
        child.tmp_paths = ["/tmp/child"]
        assert sorted(child.collect_tmp_paths()) == ["/tmp/child", "/tmp/parent"]

    def test_collects_from_substeps_in_dict(self):
        substep = make_step()
        substep.tmp_paths = ["/tmp/sub"]
        parent = make_step()
        parent.__dict__["cached_substep"] = substep   # simula cached_property
        assert "/tmp/sub" in parent.collect_tmp_paths()

    def test_no_infinite_loop_on_parent_backref(self):
        parent = make_step()
        substep = make_step()
        substep.parent = parent              # back-ref como inherit_parent_step_attributes
        parent.__dict__["sub"] = substep
        substep.__dict__["par"] = parent
        parent.tmp_paths = ["/tmp/p"]
        substep.tmp_paths = ["/tmp/s"]
        assert sorted(parent.collect_tmp_paths()) == ["/tmp/p", "/tmp/s"]

    def test_dedupes_paths(self):
        parent = make_step()
        parent.tmp_paths = ["/tmp/x"]
        child = make_step(previous_step=[parent])
        child.tmp_paths = ["/tmp/x"]
        assert child.collect_tmp_paths() == ["/tmp/x"]

    def test_delete_calls_rmdir(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "libs.framework.HivePath.rmdir",
            lambda self, **kw: calls.append(str(self)))
        s = make_step()
        s.tmp_paths = ["/tmp/a"]
        deleted = s.delete_tmp_paths()
        assert calls == ["/tmp/a"]
        assert deleted == ["/tmp/a"]

    def test_delete_skips_missing(self, monkeypatch):
        def _fail(self, **kw):
            raise FileNotFoundError("no existe")
        monkeypatch.setattr("libs.framework.HivePath.rmdir", _fail)
        s = make_step()
        s.tmp_paths = ["/tmp/a", "/tmp/b"]
        assert s.delete_tmp_paths(skip_missing=True) == []

    def test_delete_raises_when_not_skip_missing(self, monkeypatch):
        def _fail(self, **kw):
            raise FileNotFoundError("no existe")
        monkeypatch.setattr("libs.framework.HivePath.rmdir", _fail)
        s = make_step()
        s.tmp_paths = ["/tmp/a"]
        with pytest.raises(FileNotFoundError):
            s.delete_tmp_paths(skip_missing=False)


class TestPartitionLoadKwargs:
    def test_required_keys(self):
        config = {
            "table_or_hdfs": "/p",
            "information_date_column": "mis_date",
            "process_date_column": "process_date",
        }
        kwargs = ppf._partition_load_kwargs(config, {"vintage_date": date(2023, 7, 31)})
        assert kwargs["table_or_hdfs"] == "/p"
        assert kwargs["information_date_column"] == "mis_date"
        assert kwargs["current_date"] == date(2023, 7, 31)
        assert kwargs["process_date_column"] == "process_date"

    def test_defaults(self):
        config = {
            "table_or_hdfs": "/p",
            "information_date_column": "mis_date",
        }
        kwargs = ppf._partition_load_kwargs(config, {"vintage_date": date(2023, 7, 31)})
        assert kwargs["lag"] == 0
        assert kwargs["history"] == 0
        assert kwargs["process_date_column"] is None
        assert kwargs["process_date_mode"] == "last"
        assert kwargs["information_date_mode"] == "each"
        assert kwargs["missing_months_allowed"] is True

    def test_overrides_respected(self):
        config = {
            "table_or_hdfs": "/p",
            "information_date_column": "mis_date",
            "lag": 2, "history": 6,
            "process_date_mode": "each",
            "information_date_mode": "all",
            "missing_months_allowed": False,
        }
        kwargs = ppf._partition_load_kwargs(config, {"vintage_date": date(2023, 7, 31)})
        assert kwargs["lag"] == 2
        assert kwargs["history"] == 6
        assert kwargs["process_date_mode"] == "each"
        assert kwargs["information_date_mode"] == "all"
        assert kwargs["missing_months_allowed"] is False


class TestGetCachedDecoratedProperty:
    def test_default_property_name_without_path(self):
        s = make_step()
        result = s.get_cached_decorated_table_or_parquet_property(
            method=lambda *a, **k: "RESULT")
        assert result == "RESULT"

    def test_caches_by_property_name(self):
        s = make_step()
        calls = []
        def method(*a, **k):
            calls.append(1)
            return "RESULT"
        s.get_cached_decorated_table_or_parquet_property(
            method=method, property_name="my_prop")
        s.get_cached_decorated_table_or_parquet_property(
            method=method, property_name="my_prop")
        assert calls == [1]   # solo una llamada: la segunda usa caché

    def test_property_name_from_path(self):
        s = make_step()
        result = s.get_cached_decorated_table_or_parquet_property(
            path="/root/output/weight_type=composed",
            input_or_output="input",
            method=lambda *a, **k: "RESULT",
        )
        assert result == "RESULT"
        names = list(s._decorated_cache.keys())
        # '=' se elimina del path antes de sanitizar -> weight_typecomposed
        assert names == ["output_weight_typecomposed"]

    def test_config_dict_key_missing_key_raises(self):
        s = make_step()
        with pytest.raises(KeyError):
            s.get_cached_decorated_table_or_parquet_property(
                config_dict_key="edges",
                method=lambda *a, **k: "RESULT",
            )

    def test_method_kwarg_required(self):
        s = make_step()
        with pytest.raises(AssertionError):
            s.get_cached_decorated_table_or_parquet_property(path="/a/b")


# ----------------------------------------------------------------------------
# cast_to_schema
# ----------------------------------------------------------------------------

class TestCastToSchema:
    def test_casts_matching_columns(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        df = spark.createDataFrame([(1, "x")], ["num", "txt"])
        schema = StructType([StructField("num", StringType())])
        result = ppf.cast_to_schema(schema, df)
        assert result.schema["num"].dataType == StringType()
        assert result.schema["txt"].dataType == StringType()    # intacta (sin cast)

    def test_missing_column_ignored(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType
        df = spark.createDataFrame([("x",)], ["txt"])
        schema = StructType([StructField("other", StringType())])
        result = ppf.cast_to_schema(schema, df)
        assert result.columns == ["txt"]


# ----------------------------------------------------------------------------
# Decoradores de parquet (IO real con spark local)
# ----------------------------------------------------------------------------

class TestDynamicUnpartitionedParquet:
    def test_computes_writes_and_reloads(self, spark, tmp_hdfs):
        path = str(tmp_hdfs / "out")

        class S(DummyStep):
            sqlContext = spark
            @ppf.dynamic_unpartitioned_parquet_with_path(path)
            def make(self):
                return spark.createDataFrame([(1, "a")], ["n", "v"])

        s = S()
        s.is_dynamic = True
        df = s.make()
        assert df.count() == 1
        assert (tmp_hdfs / "out").exists()

        # segunda llamada: recarga del parquet (no re-computa)
        class S2(DummyStep):
            sqlContext = spark
            @ppf.dynamic_unpartitioned_parquet_with_path(path)
            def make(self):
                self.calls += 1
                return spark.createDataFrame([(9, "z")], ["n", "v"])
        s2 = S2()
        s2.is_dynamic = True
        s2.calls = 0
        df2 = s2.make()
        assert s2.calls == 0                      # no recomputó
        assert df2.collect()[0]["n"] == 1         # datos originales

    def test_not_dynamic_always_computes(self, spark, tmp_hdfs):
        path = str(tmp_hdfs / "out2")

        class S(DummyStep):
            sqlContext = spark
            @ppf.dynamic_unpartitioned_parquet_with_path(path)
            def make(self):
                self.calls += 1
                return spark.createDataFrame([(1,)], ["n"])

        s = S()
        s.is_dynamic = False
        s.calls = 0
        s.make()
        s.make()
        assert s.calls == 2

    def test_keep_or_delete_appends_tmp_path(self, spark, tmp_hdfs):
        path = str(tmp_hdfs / "tmp_out")

        class S(DummyStep):
            sqlContext = spark
            @ppf.dynamic_unpartitioned_parquet("tmp_out")
            def make(self):
                return spark.createDataFrame([(1,)], ["n"])

        s = S(output_hive={
            "tmp_out": {"table_or_hdfs": path, "keep_or_delete": "delete"}})
        s.is_dynamic = True
        s.make()
        assert len(s.tmp_paths) == 1
        assert str(s.tmp_paths[0]) == path

    def test_config_dict_key_simple_dict_uses_unpartitioned(self, spark, tmp_hdfs):
        """Un dict de config de 2 claves (table_or_hdfs + keep_or_delete) debe
        elegir el decorador unpartitioned; uno particionado (>=7 claves) elige
        el decorador particionado."""
        path = str(tmp_hdfs / "edges_out")
        s = make_step(output_hive={
            "edges": {"table_or_hdfs": path, "keep_or_delete": "keep"}})
        s.is_dynamic = True
        s.sqlContext = spark
        df = s.get_cached_decorated_table_or_parquet_property(
            config_dict_key="edges",
            input_or_output="output",
            method=lambda *a, **k: spark.createDataFrame([(1,)], ["n"]),
        )
        assert df.count() == 1
        assert (tmp_hdfs / "edges_out").exists()
