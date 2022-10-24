import functools
import logging
import asyncio

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
async def move(context, arg):
    raise NotImplementedError()


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
