"""
Project: HDFS functions
Developer: Rodolfo Arturo Gonzalez
Update Date: 2023-05-22
Version: 0.2

Contains functions useful to manage the HDFS via hive.

Based from functions taken from the Datapull:
ssh://git@cedt-gct-bitbucketcli.nam.nsroot.net:7999/dsat/
data_engineering_coe.git

"""
# !/usr/bin/env python
from subprocess import Popen, PIPE
import regex as re
from typing import List, Union, Iterable
from os.path import join
from datetime import datetime
# import fnmatch


def _arg_expander(*args:Union[str, Iterable])->list:
    """
    Expand a mixed args input made of lists, tuples and/or strings into
    and homogeneous list of strings.
    """
    result = []
    for arg in args:
        if isinstance(arg, str):
            result.append(arg)
        elif isinstance(arg, Iterable):
            result += [i for i in arg]
        else:
            raise TypeError(f'{arg} is type {type(arg)}, not admitted.')
    return result


def hdfs_command(*args:Union[str, Iterable], wait=False) -> tuple[Popen[bytes], str, str]:
    """
    Send a directly via terminal for hdfs
    Parameters
    -------
    """
    input = ['hdfs', 'dfs'] + _arg_expander(*args)
    proc = Popen(input, stdout=PIPE,stderr=PIPE)
    stdout, stderror = proc.communicate()
    if wait:
        proc.wait()
    if proc.returncode != 0:
        raise SystemError(stderror.decode('utf-8'))
    else:
        return proc, stdout.decode('utf-8'), stderror.decode('utf-8')


def list_files(path:str) -> List:
    """
    Obtain a list with the files of certain path in hadoop.
    Parameters
    ----------
    :path: string
        path to obtain the files
    :Returns:
    -------
    List
        List with all the files in the path
    """
    return [file_dict['path'].split('/')[-1] for file_dict in _ls(path)]


def test(path:str, *args ) -> bool:
    assert args, 'At least one arg is requiered.'
    args_list = ['-test', args, path]
    ''
    try:
        proc, _, _ = hdfs_command(*args_list)
        value=proc.returncode
    except:
        value = 1
    return False if value!=0 else True


def exists(path:str, *args) -> bool:
    """h
    Evaluate if certain path exists in hadoop
    Parameters
    --------
    :path: string
        path to check if exists
    Returns
    -------
    Boolean
        True if path exist false if not
    """
    return test(path, '-e')


def is_dir(path:str, *args) -> bool:
    """
    Evaluate if certain path exists and if is a dir in hadoop
    Parameters
    --------
    :path: string
        path to check if exists
    :Returns:
    -------
    Boolean
        True if path exist false if not
    """
    return test(path, '-d')


def is_file(path:str, *args) -> bool:
    """
    Evaluate if certain path exists and if is a dir in hadoop
    Parameters
    --------
    :path: string
        path to check if exists
    :Returns:
    -------
    Boolean
        True if path exist false if not
    """
    return test(path, '-f')


def obtain_path(path:str,substring:str)->str:
    """
    Obtain the path that contain a certain substring
    Parameters
    --------
    :path: string
        Obtain the path that contains a certain substring
    :substring: string
        Substring to obtain path
    :Returns:
    -------
    string
        The path that contains subscript
    """
    files = list_files(path)
    for string in files:
        if substring in string:
            return join(path,string)


def is_successful(path:str)->bool:
    """ Evaluate if path exists and contains _SUCCESS file
    Parameters
    ----------
    :path: string
        path to check if exists
    :Returns:
    -------
    Boolean
        True if path is successful or not
    """
    return exists(path) and '_SUCCESS' in list_files(path)

def mkdir(path:str, *args) -> None:
    if args:
        args_list = ['-mkdir', args, path]
    else:
        args_list = ['-mkdir', path]
    hdfs_command(*args_list)


# CONSTANTS FOR LS
_LEGEND = ('file_type', 'permissions', 'copies', 'user', 'group', 'size', 'date_and_time', 'path')
_LS_PRINT_PATTERN = re.compile(
    '([d\-]{1})' # file_type
    +'([rwx\-\+]{9,10})' # permissions
    +'\s+'
    +'([\-d]{1})' # copies
    +'\s+'
    +'([a-z0-9]+)' # user
    +'\s+'
    +'([a-z]+)' # group
    +'\s+'
    +'([0-9]+)' # size
    +'\s+'
    +'(\d{4}-\d{2}-\d{2} \d{2}:\d{2})' # date_and_time
    +'\s+'
    +'([\w\/\d\.\-\=]+)' #'path
)

_DATETIME_FORMAT = '%Y-%m-%d %H:%M'


def _ls(path:str, *args) -> List[dict]:
    if args:
        args_list = ['-ls', args, path]
    else:
        args_list = ['-ls', path]
    #
    _,stdout,_ = hdfs_command(*args_list, wait=True)
    findings = _LS_PRINT_PATTERN.findall(stdout)
    #
    result = []
    #
    for find_ in findings:
        temp_dict = {}
        {key:value for (key,value) in zip(_LEGEND, find_)}
        for key,value in zip(_LEGEND, find_):
            if key == 'date_and_time':
                temp_dict[key] = (datetime.strptime(value, _DATETIME_FORMAT))
            else:
                temp_dict[key] = (value)
        result.append(temp_dict)
    #
    return result


def touch(path:str, *args) -> None:
    if args:
        args_list = ['-touch', args, path]
    else:
        args_list = ['-touch', path]
    hdfs_command(*args_list)


def rmdir(path:str, *args) -> None:
    if args:
        args_list = ['-rm', args, path]
    else:
        args_list = ['-rm', path]
    hdfs_command(*args_list)


def mv(src:str, dst:str, *args) -> None:
    if args:
        args_list = ['-mv', args, src, dst]
    else:
        args_list = ['-mv', src, dst ]
    hdfs_command(*args_list)
