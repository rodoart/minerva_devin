"""Tests para libs.data_engineering_toolbox.path.HivePath.

Las operaciones puras de path se testean directamente; las que consultan HDFS
(`_ls`, `exists`, `is_dir`, ...) se testean con monkeypatch sobre las funciones
del módulo `libs.data_engineering_toolbox.path`.
"""
import pytest

from libs.data_engineering_toolbox.path import HivePath
import libs.data_engineering_toolbox.path as path_module


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def make_ls_entry(path, file_type="-", size="0", date_and_time="2023-07-01 10:00"):
    return {
        "file_type": file_type,
        "permissions": "drwxrwxrwx" if file_type == "d" else "-rw-rw-r--",
        "copies": "3",
        "user": "user",
        "group": "group",
        "size": size,
        "date_and_time": date_and_time,
        "path": path,
    }


@pytest.fixture()
def fake_ls(monkeypatch):
    """Permite definir qué devuelve `_ls` por directorio."""
    responses = {}
    def _fake_ls(path, *args):
        return responses.get(str(path), [])
    monkeypatch.setattr(path_module, "_ls", _fake_ls)
    return responses


# ----------------------------------------------------------------------------
# Operaciones puras de path
# ----------------------------------------------------------------------------

class TestHivePathPureOps:
    def test_str_and_repr(self):
        p = HivePath("/data/root/file")
        assert str(p) == "/data/root/file"
        assert "data" in repr(p)

    def test_joinpath_returns_hivepath(self):
        p = HivePath("/a/b").joinpath("c")
        assert isinstance(p, HivePath)
        assert str(p) == "/a/b/c"

    def test_joinpath_multiple(self):
        p = HivePath("/a").joinpath("b", "c", "d")
        assert str(p) == "/a/b/c/d"

    def test_parent_and_name(self):
        p = HivePath("/a/b/c.parquet")
        assert p.name == "c.parquet"
        assert str(p.parent) == "/a/b"

    def test_relative_to(self):
        p = HivePath("/root/x/y")
        assert str(p.relative_to(HivePath("/root"))) == "x/y"

    def test_with_name(self):
        p = HivePath("/a/b/old")
        assert str(p.with_name("new")) == "/a/b/new"

    def test_dict_names_kwargs_populated(self):
        p = HivePath("/x", file_type="d", permissions="drwxr-xr-x", size="10")
        assert p.file_type == "d"
        assert p.permissions == "drwxr-xr-x"
        assert p.size == "10"
        assert p.user is None          # no proporcionado -> None
        assert "path" not in p.__dict__  # 'path' se elimina del dict

    def test_dict_names_default_none(self):
        p = HivePath("/x")
        for key in HivePath.DICT_NAMES:
            if key == "path":
                continue
            assert getattr(p, key) is None


# ----------------------------------------------------------------------------
# Iteración (mock de _ls)
# ----------------------------------------------------------------------------

class TestHivePathListing:
    def test_iterdir_yields_hivepaths_with_metadata(self, fake_ls):
        root = "/data/root"
        fake_ls[root] = [
            make_ls_entry("/data/root/a.txt", "-", "5"),
            make_ls_entry("/data/root/sub", "d"),
        ]
        entries = list(HivePath(root).iterdir())
        assert [e.name for e in entries] == ["a.txt", "sub"]
        assert all(isinstance(e, HivePath) for e in entries)
        assert entries[0].size == "5"
        assert entries[0].file_type == "-"

    def test_listdirs_only_dirs(self, fake_ls):
        root = "/data/root"
        fake_ls[root] = [
            make_ls_entry("/data/root/a.txt", "-"),
            make_ls_entry("/data/root/sub1", "d"),
            make_ls_entry("/data/root/sub2", "d"),
        ]
        dirs = list(HivePath(root).listdirs())
        assert [d.name for d in dirs] == ["sub1", "sub2"]

    def test_listfiles_only_files(self, fake_ls):
        root = "/data/root"
        fake_ls[root] = [
            make_ls_entry("/data/root/a.txt", "-"),
            make_ls_entry("/data/root/_SUCCESS", "-"),
            make_ls_entry("/data/root/sub", "d"),
        ]
        files = list(HivePath(root).listfiles())
        assert {f.name for f in files} == {"a.txt", "_SUCCESS"}

    def test_listfiles_recursive_flag(self, fake_ls, monkeypatch):
        calls = []
        def _fake_ls(path, *args):
            calls.append(args)
            return []
        monkeypatch.setattr(path_module, "_ls", _fake_ls)
        list(HivePath("/root").listfiles(recursive=True))
        assert calls == [("-R",)]

    def test_listparquets_finds_success_dirs(self, fake_ls):
        root = "/data/root"
        fake_ls[root] = [
            make_ls_entry("/data/root/table/part-000", "-"),
            make_ls_entry("/data/root/table/_SUCCESS", "-"),
            make_ls_entry("/data/root/other/x", "-"),
        ]
        parquets = list(HivePath(root).listparquets())
        assert [str(p) for p in parquets] == ["/data/root/table"]

    def test_is_successful_true(self, fake_ls, monkeypatch):
        monkeypatch.setattr(path_module, "is_dir", lambda p: True)
        fake_ls["/data/root"] = [make_ls_entry("/data/root/_SUCCESS", "-")]
        assert HivePath("/data/root").is_successful() is True

    def test_is_successful_false_no_success_file(self, fake_ls, monkeypatch):
        monkeypatch.setattr(path_module, "is_dir", lambda p: True)
        fake_ls["/data/root"] = [make_ls_entry("/data/root/part-000", "-")]
        assert HivePath("/data/root").is_successful() is False

    def test_is_successful_false_not_dir(self, monkeypatch):
        monkeypatch.setattr(path_module, "is_dir", lambda p: False)
        assert HivePath("/data/root").is_successful() is False

    def test_is_parquet_delegates_to_is_successful(self, fake_ls, monkeypatch):
        monkeypatch.setattr(path_module, "is_dir", lambda p: True)
        fake_ls["/data/root"] = [make_ls_entry("/data/root/_SUCCESS", "-")]
        assert HivePath("/data/root").is_parquet() is True

    def test_glob_matches_pattern(self, fake_ls):
        root = "/data/root"
        fake_ls[root] = [
            make_ls_entry("/data/root/a.parquet", "-"),
            make_ls_entry("/data/root/b.txt", "-"),
        ]
        matches = list(HivePath(root).glob("*.parquet"))
        assert [m.name for m in matches] == ["a.parquet"]

    def test_glob_empty_pattern_raises(self):
        with pytest.raises(ValueError):
            list(HivePath("/root").glob(""))

    def test_sorted_by_time_orders(self, fake_ls):
        root = "/data/root"
        fake_ls[root] = [
            make_ls_entry("/data/root/b", "d", date_and_time="2023-07-02 10:00"),
            make_ls_entry("/data/root/a", "d", date_and_time="2023-07-01 10:00"),
        ]
        dirs = list(HivePath(root).listdirs(sorted_by_time=True))
        assert [d.name for d in dirs] == ["a", "b"]

    def test_iter_terminal_dirs(self, fake_ls):
        root = "/data/root"
        fake_ls[root] = [
            make_ls_entry("/data/root/a", "d"),
            make_ls_entry("/data/root/a/b", "d"),
            make_ls_entry("/data/root/c", "d"),
        ]
        terminals = list(HivePath(root)._iter_terminal_dirs())
        assert {str(t) for t in terminals} == {"/data/root/a/b", "/data/root/c"}


# ----------------------------------------------------------------------------
# Métodos que delegan en comandos HDFS
# ----------------------------------------------------------------------------

class TestHivePathHdfsDelegates:
    def test_exists_calls_module_function(self, monkeypatch):
        calls = []
        monkeypatch.setattr(path_module, "exists",
            lambda p: calls.append(p) or True)
        assert HivePath("/data/x").exists() is True
        assert calls == ["/data/x"]

    def test_is_dir_calls_module_function(self, monkeypatch):
        monkeypatch.setattr(path_module, "is_dir", lambda p: p.endswith("dir"))
        assert HivePath("/data/dir").is_dir() is True
        assert HivePath("/data/file").is_dir() is False

    def test_is_file_calls_module_function(self, monkeypatch):
        monkeypatch.setattr(path_module, "is_file", lambda p: p.endswith(".parquet"))
        assert HivePath("/data/x.parquet").is_file() is True

    def test_rmdir_requires_dir(self, monkeypatch):
        monkeypatch.setattr(path_module, "is_dir", lambda p: False)
        with pytest.raises(TypeError):
            HivePath("/data/file").rmdir()

    def test_rmdir_recursive_and_skip_trash_args(self, monkeypatch):
        calls = []
        monkeypatch.setattr(path_module, "is_dir", lambda p: True)
        monkeypatch.setattr(path_module, "rmdir",
            lambda p, *args: calls.append((p, args)))
        HivePath("/data/x").rmdir(recursive=True, skip_trash=True)
        assert calls == [("/data/x", ("-r", "-skipTrash"))]

    def test_rm_requires_file(self, monkeypatch):
        monkeypatch.setattr(path_module, "is_file", lambda p: False)
        with pytest.raises(TypeError):
            HivePath("/data/dir").rm()

    def test_touch_raises_when_exists_and_not_ok(self, monkeypatch):
        monkeypatch.setattr(path_module, "exists", lambda p: True)
        with pytest.raises(FileExistsError):
            HivePath("/data/x").touch(exist_ok=False)

    def test_touch_calls_hdfs_touch(self, monkeypatch):
        calls = []
        monkeypatch.setattr(path_module, "exists", lambda p: False)
        monkeypatch.setattr(path_module, "touch", lambda p: calls.append(p))
        HivePath("/data/x").touch()
        assert calls == ["/data/x"]
