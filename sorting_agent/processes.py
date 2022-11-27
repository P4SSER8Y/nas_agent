from copy import deepcopy
from datetime import datetime
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
import stat
from typing import Optional, Set
from typing import Any
__all__ = ["ProcessMap"]

ProcessMap = {}


_mutex_locks_ = asyncio.Lock()
_locks_: dict[str, asyncio.Lock] = {}


def wrapper(func):
    logging.debug(f"loading {func.__name__}")
    ProcessMap[func.__name__] = func

    @functools.wraps(func)
    def f(*args, **kwargs):
        return func(*args, **kwargs)
    return f


@wrapper
async def delay(context: dict, arg: float | str) -> dict:
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
def chown_to_parent(context: dict, arg: None) -> dict:
    """chown to the uid/pid of parent

    input: source
    arg: None
    output: None
    """
    path = context["source"]
    os.chown(path, path.parent.stat().st_uid, path.parent.stat().st_gid)
    return context


@wrapper
def mkpath(context: dict, arg: str | pathlib.Path) -> dict:
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
        os.mkdir(path)
        os.chown(path, path.parent.stat().st_uid, path.parent.stat().st_gid)
        os.chmod(path, 0o777)
        return True

    if isinstance(arg, str):
        path = pathlib.Path(arg.format(**context))
    else:
        path = arg
    if not iter(path):
        logging.error(f"cannot make path for {path}")
        context["_ok"] = False
    return context


@wrapper
def move(context: dict, arg: str) -> dict:
    """move file to destination

    input: source, and fields in format argument
    arg: string, with format of destination
    output: source, destination
    """
    context["destination"] = pathlib.Path(arg.format(**context)).absolute().resolve()
    context = mkpath(context, context["destination"].parent)
    if context:
        shutil.move(context["source"], context["destination"])
    if context:
        context["source"] = context["destination"]
    context = chown_to_parent(context, arg)
    context = parse_filename(context, None)
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
    context["_ok"] = False
    return context


@wrapper
def error(context: Any, arg: Any):
    """trigger error

    input: Any
    arg: Any
    output: None
    """
    raise RuntimeError("error")


@wrapper
def skip_directory(context: dict, arg: None) -> dict:
    """trigger failure if is directory

    input: is_dir
    arg: None
    output: None
    """
    context["_ok"] = not context["is_dir"]
    return context


@wrapper
def parse_filename(context: dict, arg: None) -> dict:
    """parse file name and get relative information

    input: source
    arg: None
    output: filename, parent, relative_parent, suffix, stem
    """
    context["filename"] = context["source"].name
    context["parent"] = context["source"].parent
    context["relative_parent"] = context["relative_path"].parent
    context["suffix"] = ''.join(context["source"].suffixes)
    context["stem"] = context["source"].name.removesuffix(context["suffix"])
    return context


@wrapper
async def digest(context: dict, arg: str) -> dict:
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
def generate_uuid(context: dict, arg: int | str) -> dict:
    """generate short uuid

    input: None
    arg: length
    output: uuid
    """
    context["uuid"] = shortuuid.ShortUUID().random(int(arg))
    return context


@wrapper
async def lock_acquire(context: dict, arg: list[str] | str) -> dict:
    """acquire named lock

    input: None
    arg: name of lock, case insensitive
    output: None
    """
    if isinstance(arg, str):
        arg = [arg]
    arg = [x.lower() for x in arg]

    async with _mutex_locks_:
        for item in arg:
            if item not in _locks_.keys():
                logging.info(f"create new named lock: {item}")
                _locks_[item] = asyncio.Lock()

    while True:
        await _mutex_locks_.acquire()
        for item in arg:
            if _locks_[item].locked():
                break
        else:
            break
        _mutex_locks_.release()
        logging.debug(f"cannot acquire locks")
        await asyncio.sleep(0)

    if "locks" not in context.keys():
        context["locks"] = set()
    for item in arg:
        await _locks_[item].acquire()
        logging.info(f"acquired lock '{item}'")
        context["locks"].add(item.lower())
    _mutex_locks_.release()
    return context


@wrapper
async def lock_release(context: dict, arg: str | list[str] | None) -> dict:
    """release named lock

    input: None or lock
    arg: str --- name of lock, case insensitive
         None --- release all locked
    output: None
    """
    async with _mutex_locks_:
        if isinstance(arg, str):
            arg = [arg]
        elif isinstance(arg, list) and len(arg) > 0:
            pass
        else:
            arg: list[str] = context.get("locks", [])
        arg = [x.lower() for x in arg]
        logging.debug(f"try to release {arg}")

        for item in arg:
            try:
                logging.debug(f"unlock {item}")
                _locks_[item].release()
            except KeyError:
                logging.error(f"named lock \"{arg}\" not found")
            except RuntimeError:
                logging.error(f"named lock \"{arg}\" already unlocked")
    return context


@wrapper
async def publish(context: dict, arg: dict[str, str]) -> dict:
    import dove
    server = arg["server"]
    for key in arg.keys():
        if isinstance(arg[key], str):
            arg[key] = arg[key].format(**context)
    logging.info(arg)
    await dove.publish(server, arg, arg.get("names", None))
    return context


@wrapper
async def execute(context: dict, arg: list[str]) -> dict:
    """exec extern commands

    input: dict
    arg: arugments, first of which is the command
    output: None
    """
    if (arg is None) or (len(arg) == 0):
        logging.error(f"subprocess_exec's argument is empty")
        context["_ok"] = False
        return context

    arg = [x.format(**context) for x in arg]
    logging.debug(f"args: {arg}")
    ts = datetime.now()
    p = await asyncio.subprocess.create_subprocess_exec(*arg, stdout=asyncio.subprocess.PIPE)
    ts = datetime.now() - ts
    context["_ok"] = (await p.wait() == 0)
    stdout = await p.stdout.readline()
    logging.info(f"running {arg[0]} consumed {ts.total_seconds():0.6} second(s)")
    if not context["_ok"]:
        logging.error(f"running {arg[0]} failed with return code={p.returncode}, args={' '.join(arg)}")
    logging.debug(f"stdout:\n{stdout.decode()}")
    return context


@wrapper
def get_datetime(context: None, arg: str) -> dict:
    """get date time and format it

    input: None
    arg: datetime format, cf. https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
    output: datetime
    """
    t = datetime.fromtimestamp(context["timestamp"] / 1e9)
    context["datetime"] = t.strftime(arg)
    return context


@wrapper
def copy_field(context: None, arg: list[str]) -> dict:
    """copy field of context

    input: arg[0]
    arg: [source, destination]
    output: arg[1]
    """
    if (not isinstance(arg, list)) or (len(arg) != 2):
        context["_ok"] = False
        return context
    context[arg[1]] = deepcopy(context[arg[0]])
    return context
