import functools
import logging
import asyncio
import hashlib
import shutil
import os
import pathlib
from textwrap import wrap
import aiofiles
import shortuuid
from typing import Optional
from typing import Any
__all__ = ["ProcessMap"]

ProcessMap = {}


_locks_: list[asyncio.Lock] = {}


def wrapper(func):
    logging.debug(f"loading {func.__name__}")
    ProcessMap[func.__name__] = func

    @functools.wraps(func)
    def f(*args, **kwargs):
        return func(*args, **kwargs)
    return f


@wrapper
async def delay(context: dict, arg: float | str) -> Optional[dict]:
    """delay some time

    input: None
    arg: time to be delayed, in second
    output: None
    """
    ts = float(arg)
    logging.debug(f"delay {ts}")
    await asyncio.sleep(ts)
    return context


@wrapper
def chown_to_parent(context: dict, arg: None) -> Optional[dict]:
    """chown to the uid/pid of parent

    input: source
    arg: None
    output: None
    """
    path = context["source"]
    os.chown(path, path.parent.stat().st_uid, path.parent.stat().st_gid)
    return context


@wrapper
def mkpath(context: dict, arg: str) -> Optional[dict]:
    """make path

    the owners of all directories are set to the uid/pid of parent
    the permission of all directories are set to 0o774
    input: destination
    arg: None
    output: None
    """
    def iter(path: pathlib.Path):
        if path.exists():
            return True
        if path.parent == path:
            return False
        if not path.parent.exists():
            if not iter(path.parent):
                return False
        os.mkdir(path, 0o774)
        os.chown(path, path.parent.stat().st_uid, path.parent.stat().st_gid)
        return True

    if iter(context["destination"].parent):
        return context
    else:
        logging.error(f"cannot make path for {context['destination']}")
        return None


@wrapper
def move(context: dict, arg: str) -> Optional[dict]:
    """move file to destination

    input: source, and fields in format argument
    arg: string, with format of destination
    output: source, destination
    """
    context["destination"] = pathlib.Path(arg.format(**context)).absolute().resolve()
    context = mkpath(context, arg)
    if context:
        shutil.move(context["source"], context["destination"])
    if context:
        context["source"] = context["destination"]
    context = chown_to_parent(context, arg)
    return context


@wrapper
def debug_info(context: dict, arg: None) -> dict:
    """print debug infomation

    input: None
    arg: None
    output: None
    """
    logging.info(f"{context}")
    return context


@wrapper
def failure(context: Any, arg: None):
    """trigger failure and drop everything

    input: None
    arg: None
    output: None
    """
    return None


@wrapper
def skip_directory(context: dict, arg: None) -> Optional[dict]:
    """trigger failure if is directory

    input: is_dir
    arg: None
    output: None
    """
    if context["is_dir"]:
        return None
    else:
        return context


@wrapper
def parse_filename(context: dict, arg: None) -> Optional[dict]:
    """parse file name and get relative information

    input: source
    arg: None
    output: parent, relative_parent, suffix, stem
    """
    context["parent"] = context["source"].parent
    context["relative_parent"] = context["relative_path"].parent
    context["suffix"] = ''.join(context["source"].suffixes)
    context["stem"] = context["source"].name.removesuffix(context["suffix"])
    return context


@wrapper
async def digest(context: dict, arg: str) -> Optional[dict]:
    """calculate digest with file's content

    input: source
    arg: ["md5", "sha1", "sha256"]
    output: digest, md5 | sha1 | sha256
    """
    hash = None
    match arg.lower():
        case 'md5':
            hash = hashlib.md5()
        case 'sha1':
            hash = hashlib.sha1()
        case 'sha256':
            hash = hashlib.sha256()
        case default:
            raise NotImplementedError(f"unknown algorithm={arg}")
    async with aiofiles.open(context['source'], 'rb') as f:
        while True:
            buffer = await f.read(16 * 1024 * 1024)
            if len(buffer) == 0:
                break
            logging.debug(f"get {buffer}")
            hash.update(buffer)
    context["digest"] = hash.hexdigest()
    context[arg.lower()] = hash.hexdigest()
    return context


@wrapper
def generate_uuid(context: dict, arg: int | str) -> Optional[dict]:
    """generate short uuid

    input: None
    arg: length
    output: uuid
    """
    context["uuid"] = shortuuid.ShortUUID().random(int(arg))
    return context


@wrapper
async def lock_acquire(context: dict, arg: str) -> Optional[dict]:
    """acquire named lock

    input: None
    arg: name of lock
    output: None
    """
    if arg not in _locks_.keys():
        logging.info(f"create new named lock: {arg}")
        _locks_[arg] = asyncio.Lock()
    await _locks_[arg].acquire()
    return context


@wrapper
def lock_release(context: dict, arg: str) -> Optional[dict]:
    """release named lock

    input: None
    arg: name of lock
    output: None
    """
    try:
        _locks_[arg].release()
    except KeyError as e:
        logging.error(f"named lock \"{arg}\" not found")
        raise e
    return context
