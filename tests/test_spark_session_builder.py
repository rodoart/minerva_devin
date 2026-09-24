"""Tests para SparkSessionBuilder de libs/data_engineering_toolbox/context.

Solo se prueban las partes que no levantan sesión (`hdfs_uri`, `linux_uri`,
inicialización): `build()` requiere el cluster YARN (o una sesión local
funcional en modo MINERVA_TEST_MODE=local, que no usa este builder).
"""
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from libs.data_engineering_toolbox.context import SparkSessionBuilder


class TestInit:
    def test_defaults(self):
        builder = SparkSessionBuilder()
        assert builder.app_name == "minerva"
        assert builder.extra_conf == {}

    def test_custom_values(self):
        builder = SparkSessionBuilder(app_name="my_app", extra_conf={"k": "v"})
        assert builder.app_name == "my_app"
        assert builder.extra_conf == {"k": "v"}

    def test_extra_conf_none_becomes_empty_dict(self):
        assert SparkSessionBuilder(extra_conf=None).extra_conf == {}


class TestHdfsUri:
    def test_adds_scheme_to_plain_path(self):
        assert SparkSessionBuilder.hdfs_uri("/data/x") == "hdfs:///data/x"

    def test_adds_scheme_to_relative_path(self):
        assert SparkSessionBuilder.hdfs_uri("data/x") == "hdfs://data/x"

    def test_keeps_existing_scheme(self):
        uri = "hdfs://namenode:8020/data/x"
        assert SparkSessionBuilder.hdfs_uri(uri) == uri

    def test_accepts_path_objects(self):
        assert SparkSessionBuilder.hdfs_uri(Path("/a/b")) == "hdfs:///a/b"


class TestLinuxUri:
    def test_adds_leading_slash(self):
        assert SparkSessionBuilder().linux_uri("a/b") == "/a/b"

    def test_keeps_existing_slash(self):
        assert SparkSessionBuilder().linux_uri("/a/b") == "/a/b"

    def test_accepts_path_objects(self):
        assert SparkSessionBuilder().linux_uri(Path("/a/b")) == "/a/b"
