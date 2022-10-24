from copy import deepcopy
import threading
import pathlib
import asyncio
import logging
from watchdog.events import LoggingEventHandler, FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer
from time import sleep
import yaml
from .processes import ProcessMap
import re

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
    _event_quit_ = None
    _loop_ = None
    _pipelines_ = None
    _observer_ = None
    _cnt_ = None
    _timer_debounce_ = None
    _mutex_ = None
    _current_tasks_ = None

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

    def load_config(self, path):
        raw = None
        logging.info("loading {}".format(path))
        with open(path, "r") as f:
            raw = yaml.load(f, Loader=yaml.FullLoader)
        for item in raw["pipelines"]:
            temp = {}
            temp["name"] = item["name"]
            temp["pattern"] = item["pattern"]
            temp["process"] = []
            temp["input"] = pathlib.Path(item["input"]).absolute().resolve()
            if "context" in item.keys():
                temp["context"] = item["context"]
            else:
                temp["context"] = {}
            for process in item["process"]:
                p = {}
                p["function"] = ProcessMap[process["type"]]
                if "arg" in process.keys():
                    p["arg"] = process["arg"]
                else:
                    p["arg"] = ""
                temp["process"].append(p)
            self._pipelines_.append(temp)
        logging.debug(self._pipelines_)

    def run(self):
        for item in self._pipelines_:
            logging.info(f"{item['name']}: monitor {item['input']}")
            self._observer_.schedule(Handler(self._observer_, self), item["input"], False)

        self._observer_.start()
        asyncio.set_event_loop(self._loop_)
        logging.info("agent started")
        self._loop_.run_until_complete(self._event_quit_.wait())
        logging.info("agent stopped")
        self._observer_.stop()
        self._observer_.join()

    async def _async_quit(self):
        self._event_quit_.set()

    def require_quit(self):
        asyncio.run_coroutine_threadsafe(self._async_quit(), self._loop_)

    async def _async_handle(self, context: dict):
        await asyncio.sleep(1)
        cnt = self._cnt_
        self._cnt_ += 1
        success = False
        for pipeline in self._pipelines_:
            t = deepcopy(context)
            if t["source"] == pipeline["input"]:
                continue
            if not t["source"].is_relative_to(pipeline["input"]):
                continue
            t["relative_path"] = t["source"].relative_to(pipeline["input"])
            if not re.match(pipeline["pattern"], str(t["relative_path"])):
                continue
            logging.info(f"[{cnt}] matched {pipeline['name']} for {t['source']}")
            t.update(pipeline["context"])
            for h in pipeline["process"]:
                f, arg = h["function"], h["arg"]
                # logging.debug(f"[{cnt}] enter {f.__name__}")
                try:
                    if asyncio.iscoroutinefunction(f):
                        t = await f(t, arg)
                    else:
                        t = f(t, arg)
                except Exception as e:
                    logging.critical(f"[{cnt}] handle {f.__name__} error: {e}")
                    t = None
                if not t:
                    break
                # logging.debug(f'[{cnt}] {t}')
            if t:
                success = True
                break
        if success:
            logging.info(f"[{cnt}] success to process {context['source']}")
        else:
            logging.warning(f"[{cnt}] unmatched any patterns for {context['source']}")

    def push(self, context):
        self._mutex_.acquire()
        if context["source"] in self._current_tasks_.keys():
            logging.debug(f"debounce {context['source']}")
        else:
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
