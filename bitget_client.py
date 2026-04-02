"""
Bitget USDT-M perpetual public REST client (Mix API v2).

Design goals:
- Normalize symbols once so scanner / strategy / logs all use the same format.
- Pace requests well below the public burst limit.
- Retry only on transient failures (429 / 5xx / transport errors).
- Drop the newest still-forming candle so signals use closed bars only.
- Support a small authenticated read-only account check path.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import time
from urllib.parse import urlencode
from typing import Any, Dict, List, Optional

import requests


class BitgetAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        response_msg: str | None = None,
        status_code: int | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.response_msg = response_msg
        self.status_code = status_code


class BitgetClient:
    BASE_URL = "https://api.bitget.com"

    def __init__(
        self,
        product_type: str = "USDT-FUTURES",
        request_delay_sec: float = 0.08,
        timeout: float = 20.0,
        max_retries: int = 3,
        retry_backoff_sec: float = 0.75,
        contracts_cache_sec: float = 3600.0,
        api_key: str | None = None,
        api_secret: str | None = None,
        api_passphrase: str | None = None,
    ):
        self.product_type = product_type
        self.request_delay_sec = request_delay_sec
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff_sec = retry_backoff_sec
        self.contracts_cache_sec = contracts_cache_sec
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.api_passphrase = (api_passphrase or "").strip()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "crypto-scanner-bot/1.0"})
        self._next_request_at = 0.0
        self._contracts_cache: List[Dict[str, Any]] | None = None
        self._contracts_cached_at = 0.0
        self.requests_made = 0
        self.retries_used = 0

    def reset_counters(self) -> None:
        self.requests_made = 0
        self.retries_used = 0

    def normalize_symbol(self, symbol: str) -> str:
        """
        Return the canonical Bitget v2 contract symbol like `BTCUSDT`.
        Accepts plain v2 symbols and legacy `_UMCBL`-style variants.
        """
        raw = re.sub(r"[^A-Z0-9_]", "", str(symbol).upper().strip())
        if raw.endswith("_UMCBL"):
            raw = raw[: -len("_UMCBL")]
        return raw

    def _pace(self) -> None:
        now = time.monotonic()
        if self._next_request_at > now:
            time.sleep(self._next_request_at - now)
        self._next_request_at = max(self._next_request_at, time.monotonic()) + self.request_delay_sec

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params, private=False)

    def _private_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params, private=True)

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        private: bool = False,
    ) -> Any:
        url = f"{self.BASE_URL}{path}"
        transient_error: str | None = None
        method = method.upper()
        for attempt in range(self.max_retries + 1):
            self._pace()
            self.requests_made += 1
            try:
                headers = {}
                request_kwargs: Dict[str, Any] = {"timeout": self.timeout}
                query_string = _encode_query_string(params)
                if private:
                    if not self.has_private_credentials():
                        raise BitgetAPIError("Missing Bitget API credentials in environment.")
                    timestamp = str(int(time.time() * 1000))
                    body_text = json.dumps(body, separators=(",", ":")) if body else ""
                    sign_payload = f"{timestamp}{method}{path}"
                    if query_string:
                        sign_payload += f"?{query_string}"
                    sign_payload += body_text
                    signature = _sign_hmac_base64(sign_payload, self.api_secret)
                    headers = {
                        "ACCESS-KEY": self.api_key,
                        "ACCESS-SIGN": signature,
                        "ACCESS-TIMESTAMP": timestamp,
                        "ACCESS-PASSPHRASE": self.api_passphrase,
                        "locale": "en-US",
                    }
                    if body is not None:
                        headers["Content-Type"] = "application/json"
                if method == "GET":
                    request_kwargs["params"] = params or {}
                    response = self._session.get(url, headers=headers, **request_kwargs)
                else:
                    request_kwargs["params"] = params or {}
                    request_kwargs["json"] = body
                    response = self._session.request(method, url, headers=headers, **request_kwargs)
                if response.status_code == 429 or response.status_code >= 500:
                    transient_error = f"http {response.status_code}"
                    raise requests.HTTPError(transient_error, response=response)
                response.raise_for_status()
                body = response.json()
                code = body.get("code")
                if code != "00000":
                    raise BitgetAPIError(
                        f"Bitget error code={code} msg={body.get('msg')} path={path}",
                        code=str(code),
                        response_msg=str(body.get("msg")),
                        status_code=response.status_code,
                    )
                return body.get("data")
            except BitgetAPIError:
                raise
            except (requests.RequestException, ValueError) as exc:
                transient = True
                if isinstance(exc, requests.HTTPError) and exc.response is not None:
                    status = exc.response.status_code
                    transient = status == 429 or status >= 500
                if not transient or attempt >= self.max_retries:
                    raise BitgetAPIError(
                        f"request failed path={path} params={params} err={exc}",
                        status_code=getattr(getattr(exc, "response", None), "status_code", None),
                    ) from exc
                self.retries_used += 1
                time.sleep(self.retry_backoff_sec * (attempt + 1))
        raise BitgetAPIError(
            f"request failed path={path} params={params} err={transient_error or 'unknown'}"
        )

    def fetch_usdt_perpetual_symbols(self) -> List[Dict[str, Any]]:
        """All normal USDT perpetual contracts, cached within the process."""
        now = time.time()
        if self._contracts_cache and (now - self._contracts_cached_at) < self.contracts_cache_sec:
            return list(self._contracts_cache)
        data = self._get("/api/v2/mix/market/contracts", {"productType": self.product_type})
        if not isinstance(data, list):
            return []
        out: List[Dict[str, Any]] = []
        for row in data:
            if row.get("quoteCoin") != "USDT":
                continue
            if row.get("symbolType") != "perpetual":
                continue
            if row.get("symbolStatus") != "normal":
                continue
            sym = row.get("symbol")
            if not sym:
                continue
            clean = self.normalize_symbol(str(sym))
            if not clean.endswith("USDT"):
                continue
            new_row = dict(row)
            new_row["symbol"] = clean
            out.append(new_row)
        self._contracts_cache = list(out)
        self._contracts_cached_at = now
        return out

    def fetch_tickers(self) -> Dict[str, Dict[str, Any]]:
        """Map canonical symbol -> normalized ticker row used by the scanner and strategy."""
        data = self._get("/api/v2/mix/market/tickers", {"productType": self.product_type})
        if not isinstance(data, list):
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        for row in data:
            sym = row.get("symbol")
            if not sym:
                continue
            clean = self.normalize_symbol(str(sym))
            out[clean] = _normalize_ticker_v2(row, clean)
        return out

    def fetch_candles(self, symbol: str, granularity: str, limit: int) -> List[List[str]]:
        """
        Fetch recent candles oldest->newest and drop the newest unfinished bar.
        This avoids scoring on a 5m/1H/4H candle that can still repaint until close.
        """
        clean_symbol = self.normalize_symbol(symbol)
        clean_granularity = _normalize_granularity(granularity)
        lim = min(max(int(limit), 10), 1000)
        params = {
            "symbol": clean_symbol,
            "productType": self.product_type,
            "granularity": clean_granularity,
            "limit": str(lim),
        }
        data = self._get("/api/v2/mix/market/candles", params)
        if not isinstance(data, list):
            return []
        rows = _dedupe_and_sort_candles(data)
        gran_ms = granularity_ms(clean_granularity)
        now_ms = int(time.time() * 1000)
        while rows and int(rows[-1][0]) + gran_ms > now_ms - 2000:
            rows.pop()
        return rows

    def fetch_historical_candles(
        self,
        symbol: str,
        granularity: str,
        start_ms: int,
        end_ms: int,
        limit: int = 200,
    ) -> List[List[str]]:
        """
        Fetch closed historical candles in ascending order for a date range.

        Bitget history endpoints are windowed and capped, so this method paginates
        forward by time without future leakage.
        """
        clean_symbol = self.normalize_symbol(symbol)
        clean_granularity = _normalize_granularity(granularity)
        gran_ms = granularity_ms(clean_granularity)
        page_limit = min(max(int(limit), 10), 200)
        cursor = int(start_ms)
        end_ms = int(end_ms)
        all_rows: List[List[str]] = []
        while cursor <= end_ms:
            window_end = min(end_ms, cursor + gran_ms * (page_limit - 1))
            params = {
                "symbol": clean_symbol,
                "productType": self.product_type,
                "granularity": clean_granularity,
                "startTime": str(cursor),
                "endTime": str(window_end),
                "limit": str(page_limit),
            }
            data = self._get("/api/v2/mix/market/history-candles", params)
            if not isinstance(data, list) or not data:
                cursor = window_end + gran_ms
                continue
            rows = _dedupe_and_sort_candles(data)
            all_rows.extend(rows)
            last_ts = int(rows[-1][0])
            next_cursor = last_ts + gran_ms
            if next_cursor <= cursor:
                next_cursor = window_end + gran_ms
            cursor = next_cursor
        rows = _dedupe_and_sort_candles(all_rows)
        return [row for row in rows if start_ms <= int(row[0]) <= end_ms]

    def has_private_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret and self.api_passphrase)

    def fetch_private_account_overview(self, margin_coin: str = "USDT") -> Dict[str, Any]:
        """
        Read-only futures account connectivity check.

        Returns a compact summary derived from a few private endpoints:
        - account balances / equity
        - open positions count
        - pending orders count
        """
        accounts_data = self._private_get(
            "/api/v2/mix/account/accounts",
            {"productType": self.product_type},
        )
        accounts = accounts_data if isinstance(accounts_data, list) else []
        selected_accounts = [
            row for row in accounts if str(row.get("marginCoin", "")).upper() == margin_coin.upper()
        ] or accounts
        account_summary = _summarize_accounts(selected_accounts)

        positions_count = self._safe_count_positions(margin_coin)
        open_orders_count = self._safe_count_pending_orders(margin_coin)
        return {
            "product_type": self.product_type,
            "margin_coin": margin_coin.upper(),
            "accounts_found": len(accounts),
            "accounts_considered": len(selected_accounts),
            "account_summary": account_summary,
            "open_positions_count": positions_count,
            "open_orders_count": open_orders_count,
        }

    def _safe_count_positions(self, margin_coin: str) -> int | None:
        candidates = [
            {"productType": self.product_type, "marginCoin": margin_coin.upper()},
            {"productType": self.product_type},
        ]
        for params in candidates:
            try:
                data = self._private_get("/api/v2/mix/position/all-position", params)
                if isinstance(data, list):
                    active = [row for row in data if _position_is_open(row)]
                    return len(active)
            except BitgetAPIError:
                continue
        return None

    def _safe_count_pending_orders(self, margin_coin: str) -> int | None:
        candidates = [
            {"productType": self.product_type, "marginCoin": margin_coin.upper(), "limit": "100"},
            {"productType": self.product_type, "limit": "100"},
            {"productType": self.product_type},
        ]
        for params in candidates:
            try:
                data = self._private_get("/api/v2/mix/order/orders-pending", params)
                if isinstance(data, dict):
                    order_list = data.get("entrustedList") or data.get("dataList") or data.get("orderList") or []
                    if isinstance(order_list, list):
                        return len(order_list)
                if isinstance(data, list):
                    return len(data)
            except BitgetAPIError:
                continue
        return None


def _normalize_ticker_v2(r: Dict[str, Any], clean_symbol: str) -> Dict[str, Any]:
    """Expose stable keys used by the rest of the project."""
    row = dict(r)
    last_price = _safe_float(r.get("lastPr"))
    best_bid = _safe_float(r.get("bidPr"))
    best_ask = _safe_float(r.get("askPr"))
    quote_volume = _safe_float(r.get("usdtVolume") or r.get("quoteVolume"))
    open_interest_base = _safe_float(r.get("holdingAmount"))
    open_interest_usdt = open_interest_base * last_price if last_price > 0 else 0.0
    spread_pct = None
    if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        mid = (best_bid + best_ask) / 2.0
        if mid > 0:
            spread_pct = (best_ask - best_bid) / mid
    row["symbol"] = clean_symbol
    row["bestBid"] = r.get("bidPr")
    row["bestAsk"] = r.get("askPr")
    row["last"] = r.get("lastPr")
    row["fundingRate"] = r.get("fundingRate")
    row["quoteVolumeUSDT"] = quote_volume
    row["openInterestBase"] = open_interest_base
    row["openInterestUSDT"] = open_interest_usdt
    row["spreadPct"] = spread_pct
    row["hasLiveDerivativesContext"] = True
    return row


def _sign_hmac_base64(payload: str, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def _encode_query_string(params: Optional[Dict[str, Any]]) -> str:
    if not params:
        return ""
    items = [(str(k), str(v)) for k, v in params.items() if v is not None]
    return urlencode(items)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _summarize_accounts(accounts: List[Dict[str, Any]]) -> Dict[str, float | str | None]:
    equity = 0.0
    available = 0.0
    locked = 0.0
    unrealized = 0.0
    for row in accounts:
        equity += _safe_float(
            row.get("accountEquity")
            or row.get("equity")
            or row.get("usdtEquity")
            or row.get("accountEquityUSDT")
        )
        available += _safe_float(
            row.get("available")
            or row.get("availableBalance")
            or row.get("availableEquity")
            or row.get("maxOpenPosAvailable")
        )
        locked += _safe_float(
            row.get("locked")
            or row.get("lockedBalance")
            or row.get("crossedLocked")
            or row.get("isolatedLocked")
        )
        unrealized += _safe_float(
            row.get("unrealizedPL")
            or row.get("unrealizedProfit")
            or row.get("crossedUnrealizedPL")
            or row.get("isolatedUnrealizedPL")
        )
    margin_mode = None
    if accounts:
        margin_mode = str(accounts[0].get("marginMode") or accounts[0].get("marginType") or "")
    return {
        "equity_usdt": round(equity, 6),
        "available_usdt": round(available, 6),
        "locked_usdt": round(locked, 6),
        "unrealized_pnl_usdt": round(unrealized, 6),
        "margin_mode": margin_mode or None,
    }


def _position_is_open(row: Dict[str, Any]) -> bool:
    qty_fields = (
        row.get("total"),
        row.get("openTotalPos"),
        row.get("available"),
        row.get("holdVolume"),
        row.get("positionSize"),
    )
    return any(abs(_safe_float(value)) > 0 for value in qty_fields)


def _normalize_granularity(granularity: str) -> str:
    g = str(granularity).strip()
    if g.endswith("m") and g[:-1].isdigit():
        return f"{int(g[:-1])}m"
    if g.endswith("H") and g[:-1].isdigit():
        return f"{int(g[:-1])}H"
    raise ValueError(f"Unsupported granularity: {granularity}")


def granularity_ms(granularity: str) -> int:
    if granularity.endswith("m") and granularity[:-1].isdigit():
        return int(granularity[:-1]) * 60 * 1000
    if granularity.endswith("H") and granularity[:-1].isdigit():
        return int(granularity[:-1]) * 3600 * 1000
    raise ValueError(f"Unsupported granularity: {granularity}")


def _dedupe_and_sort_candles(rows: List[List[str]]) -> List[List[str]]:
    by_ts: Dict[int, List[str]] = {}
    for row in rows:
        if len(row) < 5:
            continue
        by_ts[int(row[0])] = row
    ordered = [by_ts[k] for k in sorted(by_ts)]
    return ordered
