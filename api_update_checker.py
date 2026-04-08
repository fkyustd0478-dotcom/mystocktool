"""
api_update_checker.py — 測試各 API 每日何時開始提供當天資料
=============================================================
用途：定時輪詢 TWSE / TPEx 的股價、三大法人、處置 API，
      記錄首次取得當日有效資料的時間點。

執行方式：
  python api_update_checker.py

Log 輸出：
  logs/price.log        — 股價 API
  logs/institution.log  — 三大法人 API
  logs/disposal.log     — 處置 API
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timedelta
from threading import Thread, Event

# ── 時區設定（台灣 UTC+8）─────────────────────────────────────────────
TZ_OFFSET = timedelta(hours=8)


def now_tw() -> datetime:
    """取得台灣當前時間（不依賴 pytz）"""
    return datetime.utcnow() + TZ_OFFSET


def today_roc() -> str:
    """回傳民國年日期字串，如 '1150408'"""
    tw = now_tw()
    roc_year = tw.year - 1911
    return f"{roc_year}{tw.month:02d}{tw.day:02d}"


def today_western() -> str:
    """回傳西元日期字串，如 '20260408'"""
    return now_tw().strftime("%Y%m%d")


def today_iso() -> str:
    """回傳 ISO 日期，如 '2026-04-08'"""
    return now_tw().strftime("%Y-%m-%d")


# ── Log 設定 ──────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")


def setup_logger(name: str, filename: str) -> logging.Logger:
    """為每個任務建立獨立的 logger + 檔案 handler"""
    os.makedirs(LOG_DIR, exist_ok=True)
    filepath = os.path.join(LOG_DIR, filename)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # 避免重複加 handler
    if not logger.handlers:
        file_handler = logging.FileHandler(filepath, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


# ── HTTP 請求封裝 ─────────────────────────────────────────────────────
REQUEST_TIMEOUT = 30  # 秒
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) StockAPIChecker/1.0",
    "Accept": "application/json",
}


def fetch_json(url: str, logger: logging.Logger) -> dict | list | None:
    """
    發送 GET 請求並回傳 JSON，失敗回傳 None。
    所有錯誤都會寫入 log。
    """
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        logger.warning("請求逾時 | url=%s", url)
    except requests.exceptions.HTTPError as exc:
        logger.warning("HTTP 錯誤 %s | url=%s", exc.response.status_code, url)
    except requests.exceptions.RequestException as exc:
        logger.warning("請求失敗 | url=%s | %s", url, exc)
    except ValueError:
        logger.warning("JSON 解析失敗 | url=%s", url)
    return None


# ── 資料驗證函式 ──────────────────────────────────────────────────────

def _has_today_data_tpex(data: list | None, today_roc_str: str) -> bool:
    """TPEx JSON Array：檢查第一筆的 Date 是否為今日（民國年）"""
    if not data or not isinstance(data, list) or len(data) == 0:
        return False
    first = data[0]
    return first.get("Date", "").strip() == today_roc_str


def _has_today_data_twse_array(data: list | None, today_roc_str: str) -> bool:
    """TWSE JSON Array（STOCK_DAY_ALL / punish）：檢查 Date 欄位"""
    if not data or not isinstance(data, list) or len(data) == 0:
        return False
    first = data[0]
    return first.get("Date", "").strip() == today_roc_str


def _has_today_data_twse_t86(data: dict | None, today_roc_str: str) -> bool:
    """TWSE T86（三大法人）：頂層有 stat/data，需檢查 data 是否非空"""
    if not data or not isinstance(data, dict):
        return False
    stat = data.get("stat", "")
    rows = data.get("data")
    if not rows or not isinstance(rows, list) or len(rows) == 0:
        return False
    # T86 的 date 在 URL 參數帶入，有 data 且 stat 含 "OK" 即表示有資料
    return "OK" in stat.upper() if stat else len(rows) > 0


def _summarize(data, max_len: int = 200) -> str:
    """產生回應摘要（截斷）"""
    if data is None:
        return "(無回應)"
    if isinstance(data, list):
        count = len(data)
        preview = str(data[0])[:max_len] if count > 0 else "[]"
        return f"array[{count}] first={preview}"
    if isinstance(data, dict):
        preview = str(data)[:max_len]
        return preview
    return str(data)[:max_len]


# ── API 定義 ──────────────────────────────────────────────────────────

def build_api_tasks() -> list[dict]:
    """
    回傳所有 API 任務定義。
    每個任務為一個 dict，包含：
      - name        : 任務名稱
      - category    : 類別（price / institution / disposal）
      - log_file    : log 檔名
      - start_time  : 開始輪詢時間 "HH:MM"
      - stop_time   : 強制停止時間 "HH:MM"
      - interval    : 輪詢間隔（秒）
      - url_fn      : 回傳 URL 的函式（無參數）
      - validate_fn : 驗證函式 (data, today_roc_str) -> bool
    """
    today_roc_str = today_roc()
    today_western_str = today_western()

    return [
        # ── 股價 ──
        {
            "name": "TPEx 股價",
            "category": "price",
            "log_file": "price.log",
            "start_time": "13:45",
            "stop_time": "19:00",
            "interval": 900,
            "url_fn": lambda: "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
            "validate_fn": lambda data, roc=today_roc_str: _has_today_data_tpex(data, roc),
        },
        {
            "name": "TWSE 股價",
            "category": "price",
            "log_file": "price.log",
            "start_time": "13:45",
            "stop_time": "19:00",
            "interval": 900,
            "url_fn": lambda: "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
            "validate_fn": lambda data, roc=today_roc_str: _has_today_data_twse_array(data, roc),
        },
        # ── 三大法人 ──
        {
            "name": "TPEx 三大法人",
            "category": "institution",
            "log_file": "institution.log",
            "start_time": "14:00",
            "stop_time": "19:00",
            "interval": 900,
            "url_fn": lambda: "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading",
            "validate_fn": lambda data, roc=today_roc_str: _has_today_data_tpex(data, roc),
        },
        {
            "name": "TWSE 三大法人",
            "category": "institution",
            "log_file": "institution.log",
            "start_time": "14:00",
            "stop_time": "19:00",
            "interval": 900,
            "url_fn": lambda: (
                f"https://www.twse.com.tw/rwd/zh/fund/T86"
                f"?date={today_western_str}&selectType=ALL&response=json"
            ),
            "validate_fn": lambda data, roc=today_roc_str: _has_today_data_twse_t86(data, roc),
        },
        # ── 處置 ──
        {
            "name": "TPEx 處置",
            "category": "disposal",
            "log_file": "disposal.log",
            "start_time": "18:30",
            "stop_time": "19:00",
            "interval": 900,
            "url_fn": lambda: "https://www.tpex.org.tw/openapi/v1/tpex_disposal_information",
            "validate_fn": lambda data, roc=today_roc_str: _has_today_data_tpex(data, roc),
        },
        {
            "name": "TWSE 處置",
            "category": "disposal",
            "log_file": "disposal.log",
            "start_time": "18:30",
            "stop_time": "19:00",
            "interval": 900,
            "url_fn": lambda: "https://openapi.twse.com.tw/v1/announcement/punish",
            "validate_fn": lambda data, roc=today_roc_str: _has_today_data_twse_array(data, roc),
        },
    ]


# ── 輪詢邏輯 ─────────────────────────────────────────────────────────

def parse_time_today(time_str: str) -> datetime:
    """將 'HH:MM' 轉為今天台灣時間的 datetime"""
    tw = now_tw()
    h, m = map(int, time_str.split(":"))
    return tw.replace(hour=h, minute=m, second=0, microsecond=0)


def poll_single_api(task: dict, logger: logging.Logger, stop_event: Event):
    """
    輪詢單一 API 直到取得當日資料或超過停止時間。
    """
    name = task["name"]
    start_dt = parse_time_today(task["start_time"])
    stop_dt = parse_time_today(task["stop_time"])
    interval = task["interval"]
    url_fn = task["url_fn"]
    validate_fn = task["validate_fn"]

    # 等到開始時間
    wait_sec = (start_dt - now_tw()).total_seconds()
    if wait_sec > 0:
        logger.info("[%s] 等待至 %s 開始輪詢（%.0f 秒後）", name, task["start_time"], wait_sec)
        # 以 1 秒為單位等待，以便 stop_event 能中斷
        while wait_sec > 0 and not stop_event.is_set():
            time.sleep(min(wait_sec, 1))
            wait_sec = (start_dt - now_tw()).total_seconds()

    if stop_event.is_set():
        logger.info("[%s] 收到停止信號，結束", name)
        return

    logger.info("[%s] 開始輪詢 | 間隔=%d秒 | 停止時間=%s", name, interval, task["stop_time"])

    while not stop_event.is_set():
        current = now_tw()

        # 超過停止時間
        if current >= stop_dt:
            logger.warning("[%s] 已達停止時間 %s，強制結束（未取得當日資料）", name, task["stop_time"])
            break

        url = url_fn()
        logger.info("[%s] 發送請求 | url=%s", name, url)

        data = fetch_json(url, logger)
        has_data = validate_fn(data)
        summary = _summarize(data)

        logger.info(
            "[%s] 結果 | 當日資料=%s | 摘要=%s",
            name,
            has_data,
            summary,
        )

        if has_data:
            logger.info("[%s] 成功取得當日資料！時間=%s", name, current.strftime("%H:%M:%S"))
            break

        # 等待下次輪詢
        next_poll = current + timedelta(seconds=interval)
        if next_poll >= stop_dt:
            remaining = (stop_dt - current).total_seconds()
            if remaining > 0:
                logger.info("[%s] 下次輪詢將超過停止時間，等待 %.0f 秒後做最後一次", name, remaining)
                while remaining > 0 and not stop_event.is_set():
                    time.sleep(min(remaining, 1))
                    remaining = (stop_dt - now_tw()).total_seconds()
            continue

        sleep_sec = interval
        logger.debug("[%s] 等待 %d 秒後重試", name, sleep_sec)
        while sleep_sec > 0 and not stop_event.is_set():
            time.sleep(min(sleep_sec, 1))
            sleep_sec -= 1


def run_category(category: str, tasks: list[dict], stop_event: Event):
    """
    執行同一類別（共用同一個 log）的所有 API 輪詢。
    每個 API 各自在獨立 thread 中輪詢。
    """
    if not tasks:
        return

    log_file = tasks[0]["log_file"]
    logger = setup_logger(f"checker.{category}", log_file)

    logger.info("=" * 60)
    logger.info("類別 [%s] 啟動 | 日期=%s | 民國=%s", category, today_iso(), today_roc())
    logger.info("包含 API：%s", ", ".join(t["name"] for t in tasks))
    logger.info("=" * 60)

    threads = []
    for task in tasks:
        thread = Thread(
            target=poll_single_api,
            args=(task, logger, stop_event),
            name=f"poll-{task['name']}",
            daemon=True,
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    logger.info("類別 [%s] 全部完成", category)


# ── 主程式 ────────────────────────────────────────────────────────────

def main():
    print(f"=== API 更新時間測試工具 ===")
    print(f"日期：{today_iso()}（民國 {today_roc()}）")
    print(f"台灣時間：{now_tw().strftime('%H:%M:%S')}")
    print(f"Log 目錄：{LOG_DIR}")
    print()

    all_tasks = build_api_tasks()
    stop_event = Event()

    # 依類別分組
    categories: dict[str, list[dict]] = {}
    for task in all_tasks:
        categories.setdefault(task["category"], []).append(task)

    # 每個類別一個 thread
    category_threads = []
    for category, tasks in categories.items():
        thread = Thread(
            target=run_category,
            args=(category, tasks, stop_event),
            name=f"category-{category}",
            daemon=True,
        )
        category_threads.append(thread)
        thread.start()

    try:
        for thread in category_threads:
            thread.join()
    except KeyboardInterrupt:
        print("\n收到中斷信號，正在停止所有輪詢…")
        stop_event.set()
        for thread in category_threads:
            thread.join(timeout=5)

    print("\n=== 全部完成 ===")


if __name__ == "__main__":
    main()
