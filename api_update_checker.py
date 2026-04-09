#!/usr/bin/env python3
"""
api_update_checker.py — 每日資料抓取與存檔腳本
================================================
用法：
  python api_update_checker.py price        # 14:30 抓股價
  python api_update_checker.py institution  # 18:00 抓三大法人（合併至股價檔）
  python api_update_checker.py disposal     # 19:00 抓處置（每日覆寫）
  python api_update_checker.py conference   # 21:00 抓法說會（5日滾動）

存檔目錄：daily_data/
  {yyyy}/{mm}/{yyyy-mm-dd}.json  — 股價 + 三大法人（5日滾動）
  disposal.json                  — 處置（每日覆寫）
  conference.json                — 法說會（5日滾動）

Log 目錄：logs/
  price.log / institution.log / disposal.log / conference.log
"""

import os
import sys
import json
import logging
import argparse
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════
#  常數
# ══════════════════════════════════════════════════════════════════════

TW_TZ = timezone(timedelta(hours=8))
BASE_DIR = Path(__file__).resolve().parent
DAILY_DIR = BASE_DIR / "daily_data"
LOG_DIR = BASE_DIR / "logs"
ROLLING_DAYS = 5

# schema_config.py 定義的欄位順序（short name）
FIELDS = ["id", "n", "m", "o", "c", "h", "l", "s", "p", "v", "val", "fi", "si", "de"]
# 欄位索引
IDX_ID, IDX_FI, IDX_SI, IDX_DE = 0, 11, 12, 13

REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) StockDataFetcher/2.0",
    "Accept": "application/json",
}

# ── API URLs ─────────────────────────────────────────────────────────

TWSE_PRICE_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_PRICE_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"

# TWSE T86 需帶 date 參數（西元 YYYYMMDD）
TWSE_INST_URL_TPL = (
    "https://www.twse.com.tw/rwd/zh/fund/T86"
    "?date={date}&selectType=ALL&response=json"
)
TPEX_INST_URL = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"

TWSE_DISPOSAL_URL = "https://openapi.twse.com.tw/v1/announcement/punish"
TPEX_DISPOSAL_URL = "https://www.tpex.org.tw/openapi/v1/tpex_disposal_information"

TWSE_CONFERENCE_URL = "https://openapi.twse.com.tw/opendata/t187ap46_L_7"
TPEX_CONFERENCE_URL = "https://www.tpex.org.tw/openapi/v1/t187ap46_O_7"


# ══════════════════════════════════════════════════════════════════════
#  工具函式
# ══════════════════════════════════════════════════════════════════════

def now_tw() -> datetime:
    return datetime.now(TW_TZ)


def today_western() -> str:
    """'20260410'"""
    return now_tw().strftime("%Y%m%d")


def today_iso() -> str:
    """'2026-04-10'"""
    return now_tw().strftime("%Y-%m-%d")


def today_roc() -> str:
    """'1150410'"""
    tw = now_tw()
    return f"{tw.year - 1911}{tw.month:02d}{tw.day:02d}"


def date_path(date_iso: str) -> Path:
    """'2026-04-10' → daily_data/2026/04/2026-04-10.json"""
    y, m, _ = date_iso.split("-")
    return DAILY_DIR / y / m / f"{date_iso}.json"


# ── 安全型別轉換 ─────────────────────────────────────────────────────

def safe_float(val, default=0.0) -> float:
    if val is None:
        return default
    s = str(val).strip().replace(",", "").replace("+", "")
    if not s or s in ("--", "-", "N/A", "除息", "除權", "除權息"):
        return default
    try:
        return float(s)
    except ValueError:
        return default


def safe_int(val, default=0) -> int:
    return int(safe_float(val, float(default)))


# ── Logging ──────────────────────────────────────────────────────────

def setup_logger(name: str, filename: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        fmt = logging.Formatter(
            "[%(asctime)s] %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh = logging.FileHandler(LOG_DIR / filename, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


# ── HTTP ─────────────────────────────────────────────────────────────

def fetch_json(url: str, logger: logging.Logger):
    """GET JSON，失敗回傳 None"""
    logger.info("請求 %s", url)
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        logger.info("回應成功 | 類型=%s | 長度=%s",
                     type(data).__name__,
                     len(data) if isinstance(data, list) else "dict")
        return data
    except requests.exceptions.RequestException as exc:
        logger.error("請求失敗 | %s", exc)
    except ValueError:
        logger.error("JSON 解析失敗 | url=%s", url)
    return None


# ── 檔案 I/O ─────────────────────────────────────────────────────────

def save_json(path: Path, obj, logger: logging.Logger):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    logger.info("已存檔 %s（%.1f KB）", path, path.stat().st_size / 1024)


def load_json(path: Path, logger: logging.Logger):
    if not path.exists():
        logger.warning("檔案不存在：%s", path)
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── 滾動清理 ─────────────────────────────────────────────────────────

def rolling_cleanup(logger: logging.Logger):
    """刪除 daily_data/ 中超過 ROLLING_DAYS 天的 {date}.json"""
    cutoff = (now_tw() - timedelta(days=ROLLING_DAYS)).strftime("%Y-%m-%d")
    removed = 0
    for json_file in DAILY_DIR.rglob("????-??-??.json"):
        date_str = json_file.stem  # e.g. "2026-04-05"
        if date_str < cutoff:
            json_file.unlink()
            removed += 1
            logger.info("已刪除過期檔案：%s", json_file)
    # 清空空目錄
    for dirpath in sorted(DAILY_DIR.rglob("*"), reverse=True):
        if dirpath.is_dir() and not any(dirpath.iterdir()):
            dirpath.rmdir()
    if removed:
        logger.info("滾動清理完成，刪除 %d 個檔案", removed)


# ══════════════════════════════════════════════════════════════════════
#  Command: price（股價）
# ══════════════════════════════════════════════════════════════════════

def _build_row(stock_id, name, market, open_p, close_p, high, low, spread, vol_shares, val):
    """
    依 schema_config.py FIELDS 順序建立一列。
    計算欄位：
      pct  = spread / (close - spread) * 100
      vol  = vol_shares / 1000 （股→張）
      fi/si/de = 0（稍後由 institution 合併）
    """
    prev = close_p - spread
    pct = round(spread / prev * 100, 2) if prev != 0 else 0.0
    vol = round(vol_shares / 1000)
    return [
        stock_id,       # id
        name,           # n
        market,         # m
        open_p,         # o
        close_p,        # c
        high,           # h
        low,            # l
        spread,         # s
        pct,            # p（計算）
        vol,            # v（計算，張）
        val,            # val
        0,              # fi（待合併）
        0,              # si（待合併）
        0,              # de（待合併）
    ]


def _parse_twse_price(data: list, today_roc_str: str, logger: logging.Logger) -> list:
    """解析 TWSE STOCK_DAY_ALL → list of rows"""
    if not data or not isinstance(data, list):
        logger.warning("TWSE 股價：無資料")
        return []

    # 驗證日期
    first_date = data[0].get("Date", "").strip() if data else ""
    if first_date != today_roc_str:
        logger.warning("TWSE 股價日期不符：API=%s, 預期=%s", first_date, today_roc_str)
        return []

    rows = []
    for rec in data:
        stock_id = rec.get("Code", "").strip()
        if not stock_id:
            continue
        rows.append(_build_row(
            stock_id=stock_id,
            name=rec.get("Name", "").strip(),
            market="上市",
            open_p=safe_float(rec.get("OpeningPrice")),
            close_p=safe_float(rec.get("ClosingPrice")),
            high=safe_float(rec.get("HighestPrice")),
            low=safe_float(rec.get("LowestPrice")),
            spread=safe_float(rec.get("Change")),
            vol_shares=safe_int(rec.get("TradeVolume")),
            val=safe_int(rec.get("TradeValue")),
        ))
    logger.info("TWSE 股價：解析 %d 筆", len(rows))
    return rows


def _parse_tpex_price(data: list, today_roc_str: str, logger: logging.Logger) -> list:
    """解析 TPEx mainboard_quotes → list of rows"""
    if not data or not isinstance(data, list):
        logger.warning("TPEx 股價：無資料")
        return []

    first_date = data[0].get("Date", "").strip() if data else ""
    if first_date != today_roc_str:
        logger.warning("TPEx 股價日期不符：API=%s, 預期=%s", first_date, today_roc_str)
        return []

    rows = []
    for rec in data:
        stock_id = rec.get("SecuritiesCompanyCode", "").strip()
        if not stock_id:
            continue
        rows.append(_build_row(
            stock_id=stock_id,
            name=rec.get("CompanyName", "").strip(),
            market="上櫃",
            open_p=safe_float(rec.get("Open")),
            close_p=safe_float(rec.get("Close")),
            high=safe_float(rec.get("High")),
            low=safe_float(rec.get("Low")),
            spread=safe_float(rec.get("Change")),
            vol_shares=safe_int(rec.get("TradingShares")),
            val=safe_int(rec.get("TransactionAmount")),
        ))
    logger.info("TPEx 股價：解析 %d 筆", len(rows))
    return rows


def cmd_price():
    logger = setup_logger("price", "price.log")
    logger.info("=" * 50)
    logger.info("開始抓取股價 | %s（民國 %s）", today_iso(), today_roc())

    roc = today_roc()
    twse_data = fetch_json(TWSE_PRICE_URL, logger)
    tpex_data = fetch_json(TPEX_PRICE_URL, logger)

    twse_rows = _parse_twse_price(twse_data, roc, logger)
    tpex_rows = _parse_tpex_price(tpex_data, roc, logger)

    all_rows = twse_rows + tpex_rows
    if not all_rows:
        logger.error("未取得任何股價資料，不存檔")
        return False

    output = {"f": FIELDS, "d": all_rows}
    out_path = date_path(today_iso())
    save_json(out_path, output, logger)
    rolling_cleanup(logger)
    logger.info("股價完成：共 %d 筆（上市 %d / 上櫃 %d）",
                len(all_rows), len(twse_rows), len(tpex_rows))
    return True


# ══════════════════════════════════════════════════════════════════════
#  Command: institution（三大法人）→ 追加寫入股價檔
# ══════════════════════════════════════════════════════════════════════

def _find_key(record: dict, *keywords, exclude=None) -> str | None:
    """在 record 的 key 中，找含有所有 keywords 且不含 exclude 的 key"""
    for key in record:
        key_lower = key.lower()
        if all(kw.lower() in key_lower for kw in keywords):
            if exclude and any(ex.lower() in key_lower for ex in exclude):
                continue
            return key
    return None


def _parse_tpex_institution(data: list, today_roc_str: str, logger: logging.Logger) -> dict:
    """
    解析 TPEx 三大法人 → {stock_id: {"fi": 張, "si": 張, "de": 張}}
    API 欄位為英文，名稱不一致，用關鍵字匹配。
    """
    if not data or not isinstance(data, list):
        logger.warning("TPEx 三大法人：無資料")
        return {}

    first_date = data[0].get("Date", "").strip() if data else ""
    if first_date != today_roc_str:
        logger.warning("TPEx 三大法人日期不符：API=%s, 預期=%s", first_date, today_roc_str)
        return {}

    # 從第一筆找出欄位名稱
    sample = data[0]
    all_keys = list(sample.keys())
    logger.debug("TPEx 三大法人欄位：%s", all_keys)

    # 外資（不含外資自營商）差額
    fi_main_key = _find_key(sample, "foreign", "investor", "difference", exclude=["dealer"])
    # 外資自營商差額
    fi_dealer_key = _find_key(sample, "foreign", "dealer", "difference")
    # 投信差額
    si_key = _find_key(sample, "investment trust", "difference")
    if not si_key:
        si_key = _find_key(sample, "trust", "difference")
    # 自營商差額（排除 foreign dealer）
    de_key = _find_key(sample, "dealer", "difference", exclude=["foreign"])
    if not de_key:
        de_key = _find_key(sample, "securities dealer", "difference")

    logger.info("TPEx 三大法人 key 匹配：fi_main=%s, fi_dealer=%s, si=%s, de=%s",
                fi_main_key, fi_dealer_key, si_key, de_key)

    lookup = {}
    for rec in data:
        stock_id = rec.get("SecuritiesCompanyCode", "").strip()
        if not stock_id:
            continue
        # 外資合計 = 外陸資(不含外資自營商) + 外資自營商（單位：股）
        fi_main = safe_int(rec.get(fi_main_key)) if fi_main_key else 0
        fi_dealer = safe_int(rec.get(fi_dealer_key)) if fi_dealer_key else 0
        fi_total = fi_main + fi_dealer
        si_val = safe_int(rec.get(si_key)) if si_key else 0
        de_val = safe_int(rec.get(de_key)) if de_key else 0
        # 股 → 張
        lookup[stock_id] = {
            "fi": round(fi_total / 1000),
            "si": round(si_val / 1000),
            "de": round(de_val / 1000),
        }

    logger.info("TPEx 三大法人：解析 %d 筆", len(lookup))
    return lookup


def _parse_twse_institution(data, today_western_str: str, logger: logging.Logger) -> dict:
    """
    解析 TWSE T86 → {stock_id: {"fi": 張, "si": 張, "de": 張}}
    T86 結構：{"stat":"OK", "fields":[...], "data":[[...], ...]}
    """
    if not data or not isinstance(data, dict):
        logger.warning("TWSE 三大法人：無資料")
        return {}

    stat = data.get("stat", "")
    rows = data.get("data")
    fields = data.get("fields", [])

    if not rows or not isinstance(rows, list) or "OK" not in str(stat).upper():
        logger.warning("TWSE 三大法人：stat=%s, 無有效資料", stat)
        return {}

    # 依關鍵字找欄位索引
    def find_idx(*keywords, exclude=None):
        for i, f in enumerate(fields):
            if all(kw in f for kw in keywords):
                if exclude and any(ex in f for ex in exclude):
                    continue
                return i
        return None

    code_idx = find_idx("證券代號")
    # 外陸資買賣超（不含外資自營商）
    fi_main_idx = find_idx("外陸資", "買賣超", exclude=["自營商"])
    if fi_main_idx is None:
        fi_main_idx = find_idx("外資", "買賣超", exclude=["自營商"])
    # 外資自營商買賣超
    fi_dealer_idx = find_idx("外資自營商", "買賣超")
    # 投信買賣超
    si_idx = find_idx("投信", "買賣超")
    # 自營商買賣超（合計）
    de_idx = find_idx("自營商", "買賣超", exclude=["外資", "自行", "避險"])
    if de_idx is None:
        # 嘗試「自營商(合計)」或最後一個含「自營商」和「買賣超」的欄位
        candidates = [i for i, f in enumerate(fields)
                      if "自營商" in f and "買賣超" in f and "外資" not in f]
        de_idx = candidates[-1] if candidates else None

    logger.info("TWSE T86 欄位索引：code=%s, fi_main=%s, fi_dealer=%s, si=%s, de=%s",
                code_idx, fi_main_idx, fi_dealer_idx, si_idx, de_idx)
    logger.debug("TWSE T86 fields：%s", fields)

    if code_idx is None:
        logger.error("TWSE T86：找不到證券代號欄位")
        return {}

    lookup = {}
    for row in rows:
        stock_id = str(row[code_idx]).strip() if code_idx < len(row) else ""
        if not stock_id:
            continue
        fi_main = safe_int(row[fi_main_idx]) if fi_main_idx is not None and fi_main_idx < len(row) else 0
        fi_dealer = safe_int(row[fi_dealer_idx]) if fi_dealer_idx is not None and fi_dealer_idx < len(row) else 0
        fi_total = fi_main + fi_dealer
        si_val = safe_int(row[si_idx]) if si_idx is not None and si_idx < len(row) else 0
        de_val = safe_int(row[de_idx]) if de_idx is not None and de_idx < len(row) else 0
        # 股 → 張
        lookup[stock_id] = {
            "fi": round(fi_total / 1000),
            "si": round(si_val / 1000),
            "de": round(de_val / 1000),
        }

    logger.info("TWSE 三大法人：解析 %d 筆", len(lookup))
    return lookup


def cmd_institution():
    logger = setup_logger("institution", "institution.log")
    logger.info("=" * 50)
    logger.info("開始抓取三大法人 | %s", today_iso())

    # 讀取今天的股價檔
    price_path = date_path(today_iso())
    price_json = load_json(price_path, logger)
    if not price_json:
        logger.error("找不到今日股價檔 %s，無法合併", price_path)
        return False

    roc = today_roc()
    western = today_western()

    tpex_data = fetch_json(TPEX_INST_URL, logger)
    twse_data = fetch_json(TWSE_INST_URL_TPL.format(date=western), logger)

    tpex_lookup = _parse_tpex_institution(tpex_data, roc, logger)
    twse_lookup = _parse_twse_institution(twse_data, western, logger)

    # 合併：以 stock_id 匹配
    merged_count = 0
    for row in price_json["d"]:
        stock_id = row[IDX_ID]
        inst = twse_lookup.get(stock_id) or tpex_lookup.get(stock_id)
        if inst:
            row[IDX_FI] = inst["fi"]
            row[IDX_SI] = inst["si"]
            row[IDX_DE] = inst["de"]
            merged_count += 1

    save_json(price_path, price_json, logger)
    logger.info("三大法人合併完成：%d / %d 筆匹配",
                merged_count, len(price_json["d"]))
    return True


# ══════════════════════════════════════════════════════════════════════
#  Command: disposal（處置）→ 每日覆寫
# ══════════════════════════════════════════════════════════════════════

DISPOSAL_FIELDS = ["code", "name", "market", "period", "reason", "measures"]


def _parse_twse_disposal(data: list, logger: logging.Logger) -> list:
    if not data or not isinstance(data, list):
        logger.warning("TWSE 處置：無資料")
        return []
    rows = []
    for rec in data:
        rows.append([
            rec.get("Code", "").strip(),
            rec.get("Name", "").strip(),
            "上市",
            rec.get("DispositionPeriod", "").strip(),
            rec.get("ReasonsOfDisposition", "").strip(),
            rec.get("DispositionMeasures", "").strip(),
        ])
    logger.info("TWSE 處置：%d 筆", len(rows))
    return rows


def _parse_tpex_disposal(data: list, today_roc_str: str, logger: logging.Logger) -> list:
    if not data or not isinstance(data, list):
        logger.warning("TPEx 處置：無資料")
        return []
    rows = []
    for rec in data:
        rows.append([
            rec.get("SecuritiesCompanyCode", "").strip(),
            rec.get("CompanyName", "").strip(),
            "上櫃",
            rec.get("DispositionPeriod", "").strip(),
            rec.get("DispositionReasons", "").strip(),
            rec.get("DisposalCondition", "").strip(),
        ])
    logger.info("TPEx 處置：%d 筆", len(rows))
    return rows


def cmd_disposal():
    logger = setup_logger("disposal", "disposal.log")
    logger.info("=" * 50)
    logger.info("開始抓取處置 | %s", today_iso())

    twse_data = fetch_json(TWSE_DISPOSAL_URL, logger)
    tpex_data = fetch_json(TPEX_DISPOSAL_URL, logger)

    roc = today_roc()
    twse_rows = _parse_twse_disposal(twse_data, logger)
    tpex_rows = _parse_tpex_disposal(tpex_data, roc, logger)

    all_rows = twse_rows + tpex_rows
    output = {
        "updated": today_iso(),
        "f": DISPOSAL_FIELDS,
        "d": all_rows,
    }

    out_path = DAILY_DIR / "disposal.json"
    save_json(out_path, output, logger)
    logger.info("處置完成：共 %d 筆", len(all_rows))
    return True


# ══════════════════════════════════════════════════════════════════════
#  Command: conference（法說會）→ 5日滾動
# ══════════════════════════════════════════════════════════════════════

def cmd_conference():
    logger = setup_logger("conference", "conference.log")
    logger.info("=" * 50)
    logger.info("開始抓取法說會 | %s", today_iso())

    twse_data = fetch_json(TWSE_CONFERENCE_URL, logger)
    tpex_data = fetch_json(TPEX_CONFERENCE_URL, logger)

    today_entry = {
        "date": today_iso(),
        "twse": twse_data if isinstance(twse_data, list) else [],
        "tpex": tpex_data if isinstance(tpex_data, list) else [],
    }

    # 載入既有檔案，合併滾動
    conf_path = DAILY_DIR / "conference.json"
    existing = load_json(conf_path, logger)
    if not existing or not isinstance(existing, list):
        existing = []

    # 移除同日重複，加入今天
    existing = [e for e in existing if e.get("date") != today_iso()]
    existing.insert(0, today_entry)

    # 只保留最近 ROLLING_DAYS 天
    cutoff = (now_tw() - timedelta(days=ROLLING_DAYS)).strftime("%Y-%m-%d")
    existing = [e for e in existing if e.get("date", "") >= cutoff]

    save_json(conf_path, existing, logger)
    logger.info("法說會完成：TWSE %d 筆 / TPEx %d 筆 / 保留 %d 天",
                len(today_entry["twse"]), len(today_entry["tpex"]), len(existing))
    return True


# ══════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════

COMMANDS = {
    "price": cmd_price,
    "institution": cmd_institution,
    "disposal": cmd_disposal,
    "conference": cmd_conference,
}


def main():
    parser = argparse.ArgumentParser(description="每日資料抓取腳本")
    parser.add_argument(
        "command",
        choices=COMMANDS.keys(),
        help="要執行的任務：price / institution / disposal / conference",
    )
    args = parser.parse_args()

    print(f"=== 資料抓取：{args.command} ===")
    print(f"日期：{today_iso()}（民國 {today_roc()}）")
    print(f"台灣時間：{now_tw().strftime('%H:%M:%S')}")
    print(f"存檔目錄：{DAILY_DIR}")
    print()

    success = COMMANDS[args.command]()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
