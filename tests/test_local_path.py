"""Tests para `LocalPath`, `UniversalPath` e `is_hdfs_path`.

`LocalPath` opera sobre el filesystem real (`tmp_path`); `UniversalPath`
despacha entre `LocalPath` y `HivePath` según esquema/env.
"""
import pytest

import libs.data_engineering_toolbox.path as pm
from libs.data_engineering_toolbox.path import (
    HivePath, LocalPath, UniversalPath, is_hdfs_path, os_mode,
)


class TestOsMode:
    def test_env_override(self, monkeypatch):
        for mode in ("unix", "wsl", "windows"):
            monkeypatch.setenv("APP_OS_MODE", mode)
            assert os_mode() == mode
        monkeypatch.delenv("APP_OS_MODE")
        monkeypatch.setenv("APP_MODE", "wsl")
        assert os_mode() == "wsl"

    def test_default_is_unix_or_detected(self, monkeypatch):
        monkeypatch.delenv("APP_OS_MODE", raising=False)
        monkeypatch.delenv("APP_MODE", raising=False)
        assert os_mode() in ("unix", "wsl", "windows")


class TestIsHdfsPath:
    def test_hdfs_scheme(self):
        assert is_hdfs_path("hdfs:///data/root") is True

    def test_file_scheme_is_local(self):
        assert is_hdfs_path("file:///tmp/x") is False

    def test_windows_drive_is_local(self):
        assert is_hdfs_path("C:\\data\\x") is False
        assert is_hdfs_path("D:/data/x") is False

    @pytest.mark.parametrize("mode", ["windows", "unix", "wsl", "local"])
    def test_local_modes_env(self, monkeypatch, mode):
        monkeypatch.setenv("APP_FS_MODE", mode)
        assert is_hdfs_path("/data/root") is False

    @pytest.mark.parametrize("mode", ["hdfs", "cluster"])
    def test_hdfs_modes_env(self, monkeypatch, mode):
        monkeypatch.setenv("APP_FS_MODE", mode)
        assert is_hdfs_path("/data/root") is True

    def test_app_test_mode_unix(self, monkeypatch):
        monkeypatch.delenv("APP_FS_MODE", raising=False)
        monkeypatch.delenv("APP_MODE", raising=False)
        monkeypatch.setenv("APP_TEST_MODE", "unix")
        assert is_hdfs_path("/data/root") is False


class TestUniversalPath:
    def test_local_dispatch(self, monkeypatch):
        monkeypatch.setenv("APP_FS_MODE", "unix")
        p = UniversalPath("/data/root")
        assert isinstance(p, LocalPath)

    def test_hdfs_dispatch(self):
        p = UniversalPath("hdfs:///data/root")
        assert isinstance(p, HivePath)


class TestLocalPathOps:
    def test_mkdir_and_exists(self, tmp_path):
        leaf = LocalPath(str(tmp_path), "a", "b")
        leaf.mkdir(parents=True)
        assert leaf.is_dir() is True
        assert leaf.exists() is True

    def test_touch_and_rm(self, tmp_path):
        f = LocalPath(str(tmp_path), "f.txt")
        f.touch()
        assert f.is_file() is True
        f.rm()
        assert f.exists() is False

    def test_iterdir_and_listdirs(self, tmp_path):
        LocalPath(str(tmp_path), "d1").mkdir(parents=True)
        LocalPath(str(tmp_path), "f.txt").touch()
        entries = sorted(e.name for e in LocalPath(str(tmp_path)).iterdir())
        assert entries == ["d1", "f.txt"]
        assert [d.name for d in LocalPath(str(tmp_path)).listdirs()] == ["d1"]
        assert [f.name for f in
                LocalPath(str(tmp_path)).listfiles()] == ["f.txt"]

    def test_listfiles_recursive(self, tmp_path):
        leaf = LocalPath(str(tmp_path), "x", "y")
        leaf.mkdir(parents=True)
        LocalPath(str(leaf), "deep.txt").touch()
        names = [f.name for f in
                 LocalPath(str(tmp_path)).listfiles(recursive=True)]
        assert names == ["deep.txt"]

    def test_glob(self, tmp_path):
        LocalPath(str(tmp_path), "a.parquet").touch()
        LocalPath(str(tmp_path), "b.txt").touch()
        matches = [f.name for f in
                   LocalPath(str(tmp_path)).glob("*.parquet")]
        assert matches == ["a.parquet"]

    def test_is_successful(self, tmp_path):
        parquet = LocalPath(str(tmp_path), "out")
        parquet.mkdir(parents=True)
        assert parquet.is_successful() is False
        LocalPath(str(parquet), "_SUCCESS").touch()
        assert parquet.is_successful() is True
        assert parquet.is_parquet() is True

    def test_listparquets(self, tmp_path):
        leaf = LocalPath(str(tmp_path), "p", "year=2023")
        leaf.mkdir(parents=True)
        LocalPath(str(leaf), "_SUCCESS").touch()
        found = list(LocalPath(str(tmp_path)).listparquets())
        assert [str(p) for p in found] == [str(leaf)]

    def test_mv_and_rename(self, tmp_path):
        src = LocalPath(str(tmp_path), "a.txt")
        src.touch()
        dst = LocalPath(str(tmp_path), "dest")
        src.mv(dst)
        assert LocalPath(str(dst), "a.txt").is_file() is True
        renamed = LocalPath(str(dst), "a.txt").rename("b.txt")
        assert renamed.is_file() is True

    def test_rmdir_recursive(self, tmp_path):
        leaf = LocalPath(str(tmp_path), "x", "y")
        leaf.mkdir(parents=True)
        LocalPath(str(tmp_path), "x").rmdir(recursive=True)
        assert LocalPath(str(tmp_path), "x").exists() is False

    def test_resolve(self, tmp_path):
        rel = LocalPath(str(tmp_path), "sub")
        assert str(rel.resolve()).startswith(str(LocalPath(str(tmp_path))))


class TestHivePathLocalBackend:
    """`HivePath` con `APP_FS_MODE` local ejercita el backend local."""

    def test_hivepath_ops_on_local(self, monkeypatch, tmp_path):
        monkeypatch.setenv("APP_FS_MODE", os_mode())
        leaf = HivePath(str(tmp_path), "d")
        leaf.mkdir(parents=True)
        HivePath(str(leaf), "_SUCCESS").touch()
        assert leaf.is_successful() is True
        assert [f.name for f in leaf.iterdir()] == ["_SUCCESS"]

    def test_module_dispatchers(self, monkeypatch, tmp_path):
        monkeypatch.setenv("APP_FS_MODE", os_mode())
        pm.mkdir(str(tmp_path / "m"), "-p")
        assert pm.is_dir(str(tmp_path / "m")) is True
        assert pm.exists(str(tmp_path / "m")) is True
        assert pm.is_file(str(tmp_path / "m")) is False
        pm.touch(str(tmp_path / "m" / "f"))
        assert pm.is_successful(str(tmp_path / "m")) is False
        pm.touch(str(tmp_path / "m" / "_SUCCESS"))
        assert pm.is_successful(str(tmp_path / "m")) is True
        assert sorted(pm.list_files(str(tmp_path / "m"))) == [
            "_SUCCESS", "f"]
        pm.rmdir(str(tmp_path / "m"), "-r")
        assert pm.exists(str(tmp_path / "m")) is False
