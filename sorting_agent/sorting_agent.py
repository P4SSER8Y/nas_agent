from copy import deepcopy
import os
from datetime import datetime
import threading
import pathlib
import asyncio
import logging
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer
from time import sleep
import yaml
from .processes import ProcessMap
import re
import traceback
from fnmatch import fnmatch

__all__ = ["SortingAgent"]


class Handler(FileSystemEventHandler):
    _agent_ = None
    _observer_ = None

    def __init__(self, observer, agent):
        self._observer_ = observer
        self._agent_ = agent

    def _format_and_push_event_(self, event: FileSystemEvent):
        context = {}
        context["source"] = pathlib.Path(event.src_path).absolute().resolve()
        context["event"] = event.event_type
        context["is_dir"] = event.is_directory
        if self._agent_:
            self._agent_.push(context)

    def on_modified(self, event: FileSystemEvent):
        logging.debug(
            "{} modified dir={} is_synthetic={}".format(
                event.src_path,
                event.is_directory,
                event.is_synthetic))
        self._format_and_push_event_(event)

    def on_moved(self, event: FileSystemEvent):
        logging.debug("{} moved dir={} is_synthetic={}".format(event.src_path, event.is_directory, event.is_synthetic))
        self._format_and_push_event_(event)


class SortingAgent(threading.Thread):
    def __init__(self, group=None, name="SortingAgent", args=(), kwargs={}, daemon=None):
        super().__init__(group=group, name=name, args=args, kwargs=kwargs, daemon=daemon)
        self._loop_ = asyncio.new_event_loop()
        self._event_quit_ = asyncio.Event()
        self._event_quit_.clear()
        self._observer_ = Observer()
        self._mutex_ = threading.Lock()
        self._pipelines_ = []
        self._current_tasks_ = {}
        self._cnt_ = 0
        self._init_scan_ = []

    def load_config(self, path):
        raw = None
        logging.info("loading {}".format(pathlib.Path(path).absolute().resolve()))
        with open(path, "r") as f:
            raw = yaml.load(f, Loader=yaml.SafeLoader)
        try:
            for item in raw["pipelines"]:
                temp = {}
                temp["name"] = item["name"]
                if ("glob" in item.keys()) and ("re" in item.keys()):
                    raise Exception(f"use either glob or re in {item['name']}")
                if "glob" in item.keys():
                    temp["glob"] = item["glob"]
                else:
                    temp["re"] = item["re"]
                temp["process"] = []
                temp["input"] = pathlib.Path(item["input"]).absolute().resolve()
                temp["context"] = item.get("context", {})
                temp["blacklist"] = item.get("blacklist", [])
                temp["process"] = item["process"]
                for i in temp["process"]:
                    if i["type"] not in ProcessMap.keys():
                        raise KeyError(f"invalid process '{i['type']}'")
                temp["failure"] = item.get("failure", [])
                for i in temp["failure"]:
                    if i["type"] not in ProcessMap.keys():
                        raise KeyError(f"invalid process '{i['type']}'")
                self._pipelines_.append(temp)
                self._init_scan_.append(temp["input"])
                logging.debug(temp)
        except KeyError as e:
            logging.critical(f"parse config failed: Key {e} not found")
            raise e
        logging.debug(self._pipelines_)

    def run(self):
        for item in self._pipelines_:
            logging.info(f"{item['name']}: monitor {item['input']}")
            self._observer_.schedule(Handler(self._observer_, self), item["input"], True)

        self._observer_.start()
        asyncio.set_event_loop(self._loop_)
        logging.info("start initial scanning")
        for item in self._init_scan_:
            for root, _, files in os.walk(item):
                for file in files:
                    self.push({
                        "source": pathlib.Path(os.path.join(root, file)).absolute().resolve(),
                        "event": "initialize",
                        "is_dir": False,
                    })
                self.push({
                    "source": pathlib.Path(root).absolute().resolve(),
                    "event": "initialize",
                    "is_dir": True
                })
        logging.info("agent started")
        self._loop_.run_until_complete(self._event_quit_.wait())
        logging.info("agent stopped")
        self._observer_.stop()
        self._observer_.join()

    async def _async_quit(self):
        self._event_quit_.set()

    def require_quit(self):
        asyncio.run_coroutine_threadsafe(self._async_quit(), self._loop_)

    def _blacklisted(self, path: pathlib.Path, blacklist):
        for item in blacklist:
            for part in path.parts:
                if fnmatch(part, item):
                    logging.debug(f"\'{path}\' is blacklisted by \'{item}\'")
                    return True
        return False

    async def _async_handle(self, context: dict):
        try:
            await asyncio.sleep(1)
            cnt = self._cnt_
            self._cnt_ += 1
            success = False
            for pipeline in self._pipelines_:
                t = deepcopy(context)
                t["name"] = pipeline["name"]
                t["_ok"] = True
                if t["source"] == pipeline["input"]:
                    continue
                if not t["source"].is_relative_to(pipeline["input"]):
                    continue
                t["relative_path"] = t["source"].relative_to(pipeline["input"])
                if "re" in pipeline.keys():
                    if not re.match(pipeline["re"], str(t["relative_path"])):
                        logging.debug(f"[{cnt}] \"{t['relative_path']}\" unmatched to REGEX\"{pipeline['re']}\"")
                        continue
                elif "glob" in pipeline.keys():
                    if not t["relative_path"].match(pipeline["glob"]):
                        logging.debug(f"[{cnt}] \"{t['relative_path']}\" unmatched to GLOB\"{pipeline['glob']}\"")
                        continue
                else:
                    continue
                if self._blacklisted(t["relative_path"], pipeline["blacklist"]):
                    continue
                logging.info(f"[{cnt}] matched {pipeline['name']} for {t['source']}")
                t.update(pipeline["context"])
                for h in pipeline["process"]:
                    f = ProcessMap[h["type"]]
                    arg = deepcopy(h.get("arg", None))
                    logging.debug(f"[{cnt}] enter {f.__name__}({arg})")
                    try:
                        if asyncio.iscoroutinefunction(f):
                            t = await f(t, arg)
                        else:
                            t = f(t, arg)
                    except Exception as e:
                        logging.critical(f"[{cnt}] handle {f.__name__} error")
                        logging.critical(traceback.format_exc())
                        t["_ok"] = False
                    if not t["_ok"]:
                        break
                    logging.debug(f'[{cnt}] {t}')

                if t["_ok"]:
                    success = True
                    break
                else:
                    logging.warning(f"[{cnt}] failed, start failure cleanup")
                    for h in pipeline["failure"]:
                        f = ProcessMap[h["type"]]
                        arg = deepcopy(h.get("arg", None))
                        logging.debug(f"[{cnt}] enter {f.__name__}({arg})")
                        try:
                            if asyncio.iscoroutinefunction(f):
                                t = await f(t, arg)
                            else:
                                t = f(t, arg)
                        except Exception as e:
                            logging.critical(f"[{cnt}] handle {f.__name__} error, skipped")

            if success:
                logging.info(f"[{cnt}] success to process {context['source']}")
            else:
                logging.warning(f"[{cnt}] unmatched any patterns for {context['source']}")
        except Exception as e:
            logging.critical(traceback.format_exc())

    def push(self, context):
        self._mutex_.acquire()
        if context["source"] in self._current_tasks_.keys():
            logging.debug(f"debounce {context['source']}")
        else:
            logging.debug(f"push {context}")
            context["timestamp"] = int(datetime.now().timestamp() * 1e9)
            context["original"] = context["source"]
            task = asyncio.run_coroutine_threadsafe(self._async_handle(context), self._loop_)
            self._current_tasks_[context["source"]] = task
            task.add_done_callback(lambda task: self._current_tasks_.pop(context["source"]))
        self._mutex_.release()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    agent = SortingAgent()
    agent.start()
    sleep(1.0)
    agent.require_quit()
    agent.join()
