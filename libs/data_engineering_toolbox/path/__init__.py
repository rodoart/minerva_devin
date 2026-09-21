"""
Project:Objects related to path management
Developer: Rodolfo Arturo Gonzalez
Update Date: 2023-05-22
Version: 0.2

Objects to manage quickly paths in hive and linux.
"""
from pathlib import PurePosixPath
from fnmatch import fnmatch
from typing import List, Optional, Dict, Callable, Union
from operator import itemgetter
from multiprocessing.pool import ThreadPool


from data_engineering_toolbox.path.hdfs import mkdir, exists, _ls, is_dir, is_file, touch, rmdir, mv, hdfs_command




class HivePath(type(PurePosixPath())):
    """
    Based on pathlib, inherits the methods of 'PurePosixPath'. Gives a
    lot of methods to manipulate and evaluate paths.
    """
    DICT_NAMES = ('file_type', 'permissions', 'copies', 'user', 'group', 'size',
                  'date_and_time', 'path')
    #
    def __new__(cls, *pathsegments, **kwargs) -> Self:
        return super().__new__(cls, *pathsegments)
    #
    #
    def __init__(self, *pathsegments, **kwargs) -> None:
        # Load kwargs
        empty_dict = {key:None for key in self.DICT_NAMES if key not in list(kwargs.keys())}
        empty_dict.update(kwargs)
        empty_dict.pop('path')
        self.__dict__.update(empty_dict)
    #
    #
    def exists(self) -> bool:
        """
        Whether this path exists.
        """
        return exists(str(self))
    #
    #
    def mkdir(self, parents=False, exist_ok=False) -> None:
        """
        Create a new directory at this given path.
        """
        if self.exists():
            if not exist_ok:
                raise FileExistsError
            else:
                return
        #
        if not parents:
            if not self.parent.exists():
                raise FileNotFoundError('Parent folder does not exist.')
            else:
                mkdir(str(self))
        else:
            mkdir(str(self), '-p')
    #
    #
    def _retrieve_own_ls_dict(self)->dict:
        ls_list = _ls(str(self.parent))
        #
        for file_dict in ls_list:
            if self.name == HivePath(file_dict['path']).name:
                file_dict.pop('path')
                self.__dict__.update(file_dict)
                return file_dict
    #
    #
    def is_dir(self)->bool:
        """
        Verifies if the path is a directory.
        """
        return is_dir(str(self))
    #
    def is_file(self)->bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        return is_file(str(self))
    #
    #
    def is_parquet(self)->bool:
        """
        Test if folder contains parquet files,
        """
        #
        return self.is_successful()
    #
    #
    def _iterdict(self, recursive=False, sorted_by_time=False) -> Generator[dict[Any, Any], Any, None]:
        """Iterate over the command ls, returning a list of dictionaries
        with values of ls return.
        """
        if recursive:
            ls_dir = _ls(str(self), '-R')
        else:
            ls_dir = _ls(str(self))

        if sorted_by_time:
            ls_dir = sorted(ls_dir, key=itemgetter('date_and_time', 'path'), reverse=False)

        for file_dict in ls_dir:
            yield file_dict
    #
    #
    def iterdir(self, recursive=False, sorted_by_time=False)->list:
        """Iterate over the files and dirs in this directory.  Does not
        yield any result for the special paths '.' and '..'.
        """
        for file_dict in self._iterdict(recursive, sorted_by_time):
            yield HivePath(file_dict['path'], **file_dict)
    #
    #
    def listdirs(self, recursive=False, sorted_by_time=False)->list:
        """Iterate over the dirs in this directory."""
        for file_dict in self._iterdict(recursive, sorted_by_time):
            if file_dict['file_type'] == 'd':
                yield HivePath(file_dict['path'], **file_dict)
    #
    #
    def _iter_terminal_dirs(self, sorted_by_time=False)->list:
        paths = [hive for hive in self.listdirs(True, sorted_by_time)]
        parents = [hive.parents for hive in paths]
        for path in paths:
            if path not in parents:
                yield path
    #
    #
    def listfiles(self, recursive=False, sorted_by_time=False)->list:
        """Iterate over the files in this directory."""
        for file_dict in self._iterdict(recursive, sorted_by_time):
            if file_dict['file_type'] == '-':
                yield HivePath(file_dict['path'])
    #
    #
    def listparquets(self, recursive=True, sorted_by_time=False)->list:
        """Iterate over the parquet dirs in this directory."""
        for hive in self.listfiles(recursive, sorted_by_time):
            if hive.name == '_SUCCESS':
                yield hive.parent
    #
    #
    def glob(self, pattern:str)->list:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given pattern.
        """
        if not pattern:
            raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        #
        for hive in self.iterdir(recursive=True):
            if fnmatch(hive.name, pattern):
                yield hive
    #
    #
    def touch(self, exist_ok=True) -> None:
        """
        Create this file , if it doesn't exist.
        """
        if not exist_ok and self.exists():
            raise FileExistsError
        touch(str(self))
    #
    #
    def rmdir(self, recursive=False, skip_trash=False) -> None:
        if not self.is_dir():
            raise TypeError('It has to be a directory.')
        arguments = []
        if recursive:
            arguments += ['-r']
        if skip_trash:
            arguments += ['-skipTrash']
        #
        if recursive or skip_trash:
            rmdir(str(self), *arguments)
        else:
            rmdir(str(self))
    #
    #
    def rm(self) -> None:
        if not self.is_file():
            raise TypeError('It has to be a file.')
        rmdir(str(self))
    #
    def mv(self, destiny:Union['HivePath', str], overwrite=False) -> None:
        if isinstance(destiny, str):
            destiny = HivePath(destiny)
        if overwrite:
            try:
                destiny.joinpath(self.name).rmdir(recursive=True, skip_trash=False)
            except:
                pass
        #
        destiny.mkdir(exist_ok=True, parents=True)
        mv(str(self), str(destiny))
    #
    #
    def rename(self, new_name:str) -> HivePath:
        mv(str(self), str(self.with_name(new_name)))
        return HivePath(self.with_name(new_name))
    #
    #
    def is_successful(self)->bool:
        """ Evaluate if path exists and contains _SUCCESS file
        Parameters
        ----------
        :path: string
            path to check if exists
        #
        Returns
        -------
        Boolean
            True if path is successful or not
        """
        return is_dir(str(self)) and '_SUCCESS' in [hive.name for hive
                                                    in self.iterdir()]
    #
    #
    def _fast_copy(
        self,
        destiny_path:str,
        iter_method: str,
        overwrite:Optional[bool]=False,
        recursive:Optional[bool]=True,
        threads:Optional[int]=20,
        replace_dict:Optional[Dict]=None,
        filter: Optional[Callable[[list, dict], bool]]=None
        )->None:
        """
        Copy with multithreading in hdfs.
        ----------
        Parameters
        ----------
        :destiny_path: string
            path to copy files.
        :iter_method: str
            name of the class method to iterate, recommended:
            listparquets, listdirs, listfiles (faster).
        :recursive: bool
            If you want to copy all dirs and subdirs.
        :threads: int
            The number nodes processes copying simultaneously.
        :replace_dict: dict
            It contains replace rules to change from the path to the
            destiny, example: {'20237':'20236', '202307_stress':'202306'}
        :filter: function
            If the function is true, then the path won't be copied
            The function must contain dir_list and replace_dict as
            arguments, dir_list is a list cointaining the parts of the
            relatiye origin path of a file inside the current path.
        Example: lambda dir_list, replace_dict: not
        any(item in dir_list for item in list(replace_dict.keys())))
        """
        #
        origin_paths = [parquet for parquet in
                        getattr(self, iter_method)(recursive)]
        destiny_path = HivePath(destiny_path)
        #
        # Task for multitaksing
        def task(origin_hive) -> bool | None:
            # Make relative list of dirs
            dir_list = str(origin_hive.relative_to(self)).split('/')
            #
            # Filter
            if filter is not None:
                if filter(dir_list, replace_dict):
                    return None
            #
            # Replacement
            new_dir_list = dir_list.copy()
            if replace_dict is not None:
                for origin_str, destiny_str in replace_dict.items():
                    new_dir_list = [destiny_str if part == origin_str else
                                    part for part in new_dir_list]
            #
            # Destiny path reformat
            destiny_hive = destiny_path.joinpath(*new_dir_list)
            destiny_hive = destiny_hive.parent
            print(f'Starting {destiny_hive.joinpath(origin_hive.name)}')
            #
            try:
                # Making dir if not exists
                destiny_hive.mkdir(parents=True, exist_ok=True)
                if overwrite:
                    try:
                        hdfs_command('-rm','-r', str(destiny_hive.joinpath(origin_hive.name)))

                    except:
                        pass
                #
                hdfs_command('-cp',str(origin_hive), str(destiny_hive))
                print(f'Finished {destiny_hive.joinpath(origin_hive.name)}')
                return True
            except Exception as e:
                print(e)
                return False
        #
        # Multi processes
        pool = ThreadPool(processes=threads)
        # create a thread pool
        with ThreadPool(20) as pool:
            # call a function on each item in a list and handle results
            for _ in pool.map(task, origin_paths):
                pass
    #
    def copy_parquets(
        self,
        destiny_path:str,
        replace_dict:Optional[Dict]=None,
        overwrite:Optional[bool]=False,
    ) -> None:
        """
        Copy all parquets inside the path with multithreading.
        ----------
        Parameters
        ----------
        :destiny_path: string
            path to copy files.
        :replace_dict: dict
            It contains replace rules to change from the path to the
            destiny, example: {'20237':'20236', '202307_stress':'202306'}

        """
        #
        if replace_dict is not None:
            def keep_if_is_in_dict(dir_list, remplace_dict) -> bool:
                origin_keys = list(remplace_dict.keys())
                test = any(item in dir_list for item in origin_keys)
                return not test
        else:
            keep_if_is_in_dict = None

        self._fast_copy(
            destiny_path=destiny_path,
            iter_method='listparquets',
            overwrite=overwrite,
            recursive=True,
            threads=20,
            replace_dict=replace_dict,
            filter=keep_if_is_in_dict)
    #
    def cp(
        self,
        destiny_path:str,
        overwrite:Optional[bool]=False,
        recursive:Optional[bool]=False,
    ) -> None:
        self._fast_copy(
            destiny_path = str(destiny_path),
            iter_method='_iter_terminal_dirs',
            overwrite=overwrite,
            recursive=recursive,
            threads=20
            )
