# MyStockTool

台灣股市工具集，提供股票篩選、損益計算、事件追蹤等功能，以純前端靜態頁面實作，可直接部署於 GitHub Pages。

## 功能頁面

| 頁面 | 說明 |
|------|------|
| `index.html` | Dashboard — 工具總覽入口 |
| `stocks.html` | 股票篩選 — 依漲跌幅、三大法人買賣超等條件篩選台股 |
| `calculator.html` | 損益計算機 — 計算買賣損益、手續費、交易稅，支援做多／做空與當沖 |
| `events.html` | 事件 — 追蹤法說會、處置股票等市場事件 |
| `fetcher.html` | 資料下載 — 透過 Cloudflare Worker 手動抓取最新股票資料 |

## 資料來源

- **上市股價 / 三大法人**：TWSE（台灣證券交易所）公開 API
- **上櫃股價 / 三大法人**：TPEx（櫃買中心）公開 API
- **法說會、處置股票**：TWSE 公開 API
- 資料格式：`public_data/YYYY/MM/YYYY-MM-DD.json`（壓縮 JSON，以短鍵減少體積）

## 自動化（GitHub Actions）

### `fetch_daily.yml` — 每日盤後自動更新
- 觸發時間：週一至週五，台灣時間 17:45（UTC 09:45）
- 執行 `scripts/fetch.py`，將當日股價與三大法人資料寫入 `public_data/`
- 自動 commit & push

### `api-update-checker.yml` — API 更新時間監測
- 觸發時間：週一至週五，台灣時間 13:40（UTC 05:40）
- 執行 `api_update_checker.py`，輪詢 TWSE / TPEx API，記錄當日資料首次可取得的時間點
- Logs 以 artifact 形式保存 30 天

## 股票篩選功能

- **排行模式**：漲幅排行、跌幅排行、外資買超／賣超排行、投信買超／賣超排行
- **篩選條件**：漲跌幅（%）、成交量（張）、股價（元）範圍，以及股名／股號關鍵字
- **欄位**：股號、名稱、市場（上市／上櫃）、股價、漲跌、漲跌幅、成交量、成交值、外資、投信、自營商
- **歷史查詢**：透過日期選擇器載入指定日期的資料

## 介面特色

- 深色 / 淺色主題切換，偏好設定儲存於 `localStorage`
- 響應式設計，支援桌機與行動裝置
- 字型：JetBrains Mono（數字）、Noto Sans TC（中文）
- 無外部框架依賴，純原生 HTML / CSS / JavaScript
