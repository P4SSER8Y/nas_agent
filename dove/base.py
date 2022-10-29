import logging
from typing import Optional

__all__ = ["DoveBase"]

class DoveBase():
    def __init__(self, name: str) -> None:
        self._name_ = name

    async def publish(self, msg: dict) -> None:
        logging.info(f"publish msg={msg}")
        raise NotImplementedError
