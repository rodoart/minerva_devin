"""Backend de filesystem local para :mod:`libs.data_engineering_toolbox.path`.

Espejo de :mod:`.hdfs`: las mismas firmas (`exists`, `_ls`, `mkdir`,
`touch`, `rmdir`, `mv`, `cp`, ...) implementadas con ``os``/``shutil`` para
que ``HivePath`` y ``LocalPath`` funcionen sin clúster (tests, ``--local``,
rutas del driver como ``LOGS_DIR_LINUX``).

Este módulo es deliberadamente **autocontenido** (solo stdlib, sin imports
del paquete): ``context/logging.py`` y ``supervisor.py`` lo cargan con
``importlib`` cuando el ``__init__`` del paquete (que requiere pyspark)
no está disponible.
"""
from __future__ import annotations

import os
import platform
import shutil
import sys
from datetime import datetime
from fnmatch import fnmatch
from operator import itemgetter
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Generator, Iterable, Iterator, List, Optional, Union

#: Cualquier cosa convertible a ruta.
PathLike = Union[str, Path, "LocalPath"]
#: Fila de un ``_ls`` (mismo formato de dict que el parse de ``hdfs dfs -ls``).
LsEntry = Dict[str, Any]

#: Modos de plataforma reconocidos.
OS_MODES = ("unix", "wsl", "windows")


def os_mode() -> str:
    """Plataforma de ejecución: ``"unix"`` | ``"wsl"`` | ``"windows"``.

    Orden de resolución:

    1. ``APP_OS_MODE`` / ``APP_MODE`` si tienen un valor válido.
    2. Autodetección: ``win32`` -> ``windows``; kernel con
       ``microsoft``/``wsl`` en el release -> ``wsl``.
    3. **Default: ``"unix"``** — cualquier sistema POSIX (Linux, macOS)
       funciona sin configuración.
    """
    mode = (os.environ.get("APP_OS_MODE")
            or os.environ.get("APP_MODE") or "").lower()
    if mode in OS_MODES:
        return mode
    if os.name == "nt" or sys.platform == "win32":
        return "windows"
    try:
        release = (platform.release() + " " + platform.version()).lower()
    except Exception:
        release = ""
    if "microsoft" in release or "wsl" in release:
        return "wsl"
    return "unix"


def is_windows() -> bool:
    """``True`` si la plataforma es Windows nativo."""
    return os_mode() == "windows"


def is_wsl() -> bool:
    """``True`` si se ejecuta dentro de Windows Subsystem for Linux."""
    return os_mode() == "wsl"


def _local(path: PathLike) -> str:
    """Vista local de la ruta (quita el esquema ``file:`` si lo tiene)."""
    return str(path).replace("file:", "")


def test(path: PathLike, *args: str) -> bool:
    """Equivalente local de ``hdfs dfs -test <flag> path``.

    Soporta ``-e`` (existe), ``-d`` (directorio) y ``-f`` (fichero regular);
    cualquier otro flag cae a ``exists``.
    """
    assert args, 'At least one arg is requiered.'
    flag, local = args[0], _local(path)
    if flag == '-d':
        return os.path.isdir(local)
    if flag == '-f':
        return os.path.isfile(local)
    return os.path.exists(local)


def exists(path: PathLike, *args: str) -> bool:
    """``True`` si la ruta existe."""
    return test(path, '-e')


def is_dir(path: PathLike, *args: str) -> bool:
    """``True`` si la ruta es un directorio."""
    return test(path, '-d')


def is_file(path: PathLike, *args: str) -> bool:
    """``True`` si la ruta es un fichero regular."""
    return test(path, '-f')


def _ls(path: PathLike, *args: str) -> List[LsEntry]:
    """Lista ``path`` con el mismo formato de dict que el parse de ``-ls``.

    ``-R`` en ``args`` desciende el árbol completo (``os.walk``). Cada
    entrada trae ``file_type`` (``d``/``-``), ``size``,
    ``date_and_time`` (``datetime``) y ``path`` (posix).
    """
    root = _local(path)
    if '-R' in args:
        entries = [os.path.join(dirpath, name)
                   for dirpath, dirnames, filenames in os.walk(root)
                   for name in dirnames + filenames]
    else:
        entries = ([os.path.join(root, name) for name in os.listdir(root)]
                   if os.path.isdir(root) else [])
    result: List[LsEntry] = []
    for full in entries:
        try:
            st = os.stat(full)
        except OSError:
            continue
        directory = os.path.isdir(full)
        result.append({
            'file_type': 'd' if directory else '-',
            'permissions': 'drwxrwxrwx' if directory else '-rw-r--r--',
            'copies': '1',
            'user': '',
            'group': '',
            'size': str(st.st_size),
            'date_and_time': datetime.fromtimestamp(st.st_mtime),
            'path': Path(full).as_posix(),
        })
    return result


def list_files(path: PathLike) -> List[str]:
    """Nombres (basename) de las entradas dentro de ``path``."""
    return [file_dict['path'].split('/')[-1] for file_dict in _ls(path)]


def is_successful(path: PathLike) -> bool:
    """``path`` es dir y contiene ``_SUCCESS`` (parquet terminado)."""
    return is_dir(path) and '_SUCCESS' in list_files(path)


def obtain_path(path: PathLike, substring: str) -> Optional[str]:
    """Primera entrada dentro de ``path`` cuyo nombre contiene ``substring``."""
    for name in list_files(path):
        if substring in name:
            return os.path.join(_local(path), name)
    return None


def mkdir(path: PathLike, *args: str) -> None:
    """Crea un directorio; ``-p`` crea padres e ignora si ya existe."""
    if '-p' in args:
        os.makedirs(_local(path), exist_ok=True)
    else:
        os.mkdir(_local(path))


def touch(path: PathLike, *args: str) -> None:
    """Crea un fichero vacío (o actualiza su timestamp)."""
    target = Path(_local(path))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.touch(exist_ok=True)


def rmdir(path: PathLike, *args: str) -> None:
    """Borra ``path``; con ``-r`` (o ``-skipTrash``) recursivo."""
    local = _local(path)
    if '-r' in args or '-skipTrash' in args:
        if os.path.isdir(local):
            shutil.rmtree(local)
        elif os.path.exists(local):
            os.remove(local)
    elif os.path.isdir(local):
        os.rmdir(local)
    elif os.path.isfile(local):
        os.remove(local)


def mv(src: PathLike, dst: PathLike, *args: str) -> None:
    """Mueve ``src`` a ``dst``."""
    shutil.move(_local(src), _local(dst))


def cp(src: PathLike, dst_dir: PathLike, *args: str) -> None:
    """Copia ``src`` *dentro* del directorio ``dst_dir`` (semántica ``-cp``)."""
    src_local, dst_local = _local(src), _local(dst_dir)
    target = os.path.join(dst_local, os.path.basename(src_local))
    if os.path.isdir(src_local):
        shutil.copytree(src_local, target, dirs_exist_ok=True)
    else:
        shutil.copy2(src_local, target)


class LocalPath(type(PurePosixPath())):
    """``PurePosixPath`` + operaciones sobre el filesystem **local**.

    Misma API que :class:`~.path.HivePath` pero forzando siempre el
    backend local — sirve para rutas que son locales incluso en el clúster
    (logs del driver, ``sys.path``, scripts) y como mitad local de
    :class:`~.path.UniversalPath`.
    """

    #: Claves de metadatos que un dict de ``_ls`` puede poblar.
    DICT_NAMES = ('file_type', 'permissions', 'copies', 'user', 'group',
                  'size', 'date_and_time', 'path')

    def __new__(cls, *pathsegments: PathLike, **kwargs: Any) -> "LocalPath":
        if os_mode() == "windows":
            # Separador canónico "/" también en Windows (las funciones os lo
            # aceptan) para que str(p) sea coherente con las rutas posix
            # que emiten _ls/iterdir. En unix/wsl un "\" es un carácter de
            # nombre de fichero legítimo y no se toca.
            pathsegments = tuple(
                s.replace("\\", "/") if isinstance(s, str) else s
                for s in pathsegments)
        return super().__new__(cls, *pathsegments)

    def __init__(self, *pathsegments: PathLike, **kwargs: Any) -> None:
        try:
            super().__init__(*pathsegments)
        except TypeError:
            super().__init__()
        empty_dict = {key: None for key in self.DICT_NAMES
                      if key not in list(kwargs.keys())}
        empty_dict.update(kwargs)
        empty_dict.pop('path', None)
        self.__dict__.update(empty_dict)

    def resolve(self) -> "LocalPath":
        """Ruta absoluta local (``os.path.abspath``)."""
        return LocalPath(os.path.abspath(_local(self)))

    def absolute(self) -> "LocalPath":
        """Alias de :meth:`resolve` por paridad con ``pathlib.Path``."""
        return self.resolve()

    def exists(self) -> bool:
        """``True`` si la ruta existe."""
        return exists(str(self))

    def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
        """Crea el directorio; ``parents`` crea los intermedios."""
        if self.exists():
            if not exist_ok:
                raise FileExistsError
            return
        if not parents:
            if not self.parent.exists():
                raise FileNotFoundError('Parent folder does not exist.')
            mkdir(str(self))
        else:
            mkdir(str(self), '-p')

    def _retrieve_own_ls_dict(self) -> Optional[LsEntry]:
        """Popula ``self`` con los metadatos del ``ls`` del directorio padre."""
        for file_dict in _ls(str(self.parent)):
            if self.name == LocalPath(file_dict['path']).name:
                file_dict.pop('path')
                self.__dict__.update(file_dict)
                return file_dict
        return None

    def is_dir(self) -> bool:
        """``True`` si la ruta es un directorio."""
        return is_dir(str(self))

    def is_file(self) -> bool:
        """``True`` si la ruta es un fichero regular."""
        return is_file(str(self))

    def is_parquet(self) -> bool:
        """El directorio existe y contiene salida parquet (``_SUCCESS``)."""
        return self.is_successful()

    def _iterdict(self, recursive: bool = False,
                  sorted_by_time: bool = False
                  ) -> Generator[LsEntry, None, None]:
        """Dicts ``ls`` del contenido de este directorio."""
        ls_dir = _ls(str(self), '-R') if recursive else _ls(str(self))
        if sorted_by_time:
            ls_dir = sorted(ls_dir,
                            key=itemgetter('date_and_time', 'path'))
        for file_dict in ls_dir:
            yield file_dict

    def iterdir(self, recursive: bool = False,
                sorted_by_time: bool = False) -> Iterator["LocalPath"]:
        """Itera sobre ficheros y directorios contenidos en este directorio."""
        for file_dict in self._iterdict(recursive, sorted_by_time):
            yield LocalPath(file_dict['path'], **file_dict)

    def listdirs(self, recursive: bool = False,
                 sorted_by_time: bool = False) -> Iterator["LocalPath"]:
        """Itera solo sobre los directorios contenidos."""
        for file_dict in self._iterdict(recursive, sorted_by_time):
            if file_dict['file_type'] == 'd':
                yield LocalPath(file_dict['path'], **file_dict)

    def _iter_terminal_dirs(self,
                            sorted_by_time: bool = False
                            ) -> Iterator["LocalPath"]:
        """Directorios hoja (que no son padre de ningún otro listado)."""
        paths = list(self.listdirs(True, sorted_by_time))
        all_parents = {parent for leaf in paths for parent in leaf.parents}
        for path in paths:
            if path not in all_parents:
                yield path

    def listfiles(self, recursive: bool = False,
                  sorted_by_time: bool = False) -> Iterator["LocalPath"]:
        """Itera solo sobre los ficheros contenidos."""
        for file_dict in self._iterdict(recursive, sorted_by_time):
            if file_dict['file_type'] == '-':
                yield LocalPath(file_dict['path'])

    def listparquets(self, recursive: bool = True,
                     sorted_by_time: bool = False) -> Iterator["LocalPath"]:
        """Itera sobre los directorios parquet (con ``_SUCCESS``)."""
        for leaf in self.listfiles(recursive, sorted_by_time):
            if leaf.name == '_SUCCESS':
                yield leaf.parent

    def glob(self, pattern: str) -> Iterator["LocalPath"]:
        """Entradas del subárbol cuyo ``name`` cumple ``pattern`` (fnmatch)."""
        if not pattern:
            raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        for leaf in self.iterdir(recursive=True):
            if fnmatch(leaf.name, pattern):
                yield leaf

    def touch(self, exist_ok: bool = True) -> None:
        """Crea el fichero si no existe."""
        if not exist_ok and self.exists():
            raise FileExistsError
        touch(str(self))

    def rmdir(self, recursive: bool = False, skip_trash: bool = False) -> None:
        """Borra el directorio (``recursive`` equivale a ``-r``)."""
        if not self.is_dir():
            raise TypeError('It has to be a directory.')
        arguments = ['-r'] if recursive else []
        if skip_trash:
            arguments += ['-skipTrash']
        rmdir(str(self), *arguments)

    def rm(self) -> None:
        """Borra el fichero."""
        if not self.is_file():
            raise TypeError('It has to be a file.')
        rmdir(str(self))

    def mv(self, destiny: PathLike, overwrite: bool = False) -> None:
        """Mueve esta ruta dentro de ``destiny`` (directorio)."""
        if isinstance(destiny, str):
            destiny = LocalPath(destiny)
        if overwrite:
            try:
                destiny.joinpath(self.name).rmdir(recursive=True)
            except (TypeError, OSError):
                pass
        destiny.mkdir(exist_ok=True, parents=True)
        mv(str(self), str(destiny.joinpath(self.name)))

    def rename(self, new_name: str) -> "LocalPath":
        """Renombra manteniendo el directorio padre."""
        mv(str(self), str(self.with_name(new_name)))
        return LocalPath(self.with_name(new_name))

    def is_successful(self) -> bool:
        """``True`` si es directorio y contiene ``_SUCCESS``."""
        return is_dir(str(self)) and '_SUCCESS' in [
            leaf.name for leaf in self.iterdir()]
