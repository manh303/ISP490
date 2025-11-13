# -*- coding: utf-8 -*-
# crawlers/base_crawler.py
import os, sys, time, random, logging
from pathlib import Path
from datetime import datetime

class BaseCrawler:
    name = "BaseCrawler"

    def __init__(self, min_delay=1.2, max_delay=2.8):
        self.min_delay = float(min_delay)
        self.max_delay = float(max_delay)
        self.logger = self._setup_logger()

    # ---------- LOGGING: ghi vào volume /app/data/logs hoặc /tmp ----------
    def _setup_logger(self) -> logging.Logger:
        log_dir = (
            os.getenv("FORCE_CRAWLER_LOG_DIR")
            or os.getenv("CRAWLER_LOG_DIR")
            or "/app/data/logs"
        )
        try:
            Path(log_dir).mkdir(parents=True, exist_ok=True)
            t = Path(log_dir) / ".w"; t.write_text("ok", encoding="utf-8"); t.unlink()
        except Exception:
            log_dir = "/tmp/crawler_logs"
            Path(log_dir).mkdir(parents=True, exist_ok=True)

        name = getattr(self, "name", self.__class__.__name__)
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        for h in list(logger.handlers):
            logger.removeHandler(h)

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        logfile = Path(log_dir) / f"{name}_{ts}.log"

        fh = logging.FileHandler(logfile, encoding="utf-8")
        sh = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        fh.setFormatter(fmt); sh.setFormatter(fmt)
        logger.addHandler(fh); logger.addHandler(sh)
        logger.info(f"Using log_dir={log_dir}")
        return logger

    # ---------- politeness ----------
    def random_delay(self):
        d = random.uniform(self.min_delay, self.max_delay)
        time.sleep(d)

    # ---------- helpers ----------
    @staticmethod
    def now_utc_iso():
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
