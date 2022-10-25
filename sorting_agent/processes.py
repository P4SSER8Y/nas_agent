import functools
import logging
import asyncio
import hashlib
import shutil
import os
import sys
import pathlib
__all__ = ["ProcessMap"]

ProcessMap = {}


def wrapper(func):
    logging.debug(f"loading {func.__name__}")
    ProcessMap[func.__name__] = func

    @functools.wraps(func)
    def f(*args, **kwargs):
        return func(*args, **kwargs)
    return f


@wrapper
async def delay(context, arg):
    ts = float(arg)
    logging.debug(f"delay {ts}")
    await asyncio.sleep(ts)
    return context


@wrapper
def chown_to_parent(context, arg: str):
    path = context["source"]
    os.chown(path, path.parent.stat().st_uid, path.parent.stat().st_gid)
    return context


@wrapper
def mkpath(context, arg: str):
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
def move(context, arg: str):
    context["destination"] = pathlib.Path(arg.format(**context)).absolute().resolve()
    context = mkpath(context, arg)
    if context:
        shutil.move(context["source"], context["destination"])
    if context:
        context["source"] = context["destination"]
    context = chown_to_parent(context, arg)
    return context


@wrapper
def debug_info(context, arg):
    logging.info(f"{context}")
    return context


@wrapper
def failure(context, arg):
    return None


@wrapper
def skip_directory(context, arg):
    if context["is_dir"]:
        return None
    else:
        return context


@wrapper
def parse_filename(context, arg):
    context["parent"] = context["source"].parent
    context["relative_parent"] = context["relative_path"].parent
    context["suffix"] = ''.join(context["source"].suffixes)
    context["stem"] = context["source"].name.removesuffix(context["suffix"])
    return context


@wrapper
async def get_md5(context, arg):
    pass
