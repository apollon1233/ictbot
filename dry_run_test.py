import os
import sys
import time
import json
import random
import types
from urllib.parse import urlparse, parse_qs
from unittest.mock import patch


def now_ms():
    return int(time.time() * 1000)


def gen_klines(interval: str, limit: int):
    # Simple random-walk generator near 60000 ms per minute
    step_ms = {
        "1m": 60_000,
        "5m": 300_000,
        "1h": 3_600_000,
        "4h": 14_400_000,
        "1d": 86_400_000,
        "1w": 7 * 86_400_000,
        "1M": 30 * 86_400_000,
    }.get(interval, 60_000)
    end = now_ms() - 30_000
    start = end - (limit - 1) * step_ms
    px = 30000.0
    out = []
    t = start
    for i in range(limit):
        # controlled drift
        drift = (random.random() - 0.5) * 20.0
        o = px
        h = o + abs(drift) * 0.8 + 5
        l = o - abs(drift) * 0.8 - 5
        c = o + drift
        v = 10.0 + abs(drift)
        out.append([int(t), float(o), float(h), float(l), float(c), float(v)])
        px = c
        t += step_ms
    return out


class FakeResp:
    def __init__(self, status=200, payload=None, text=""):
        self.status_code = status
        self._payload = payload
        self.text = text or (json.dumps(payload) if payload is not None else "")

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise RuntimeError(f"HTTP {self.status_code}")


def make_handler(phase_fn):
    orders_by_cid = {}
    order_counter = 1000

    def handler(method, url, params=None, data=None, **kwargs):
        nonlocal order_counter
        u = urlparse(url)
        path = u.path
        q = parse_qs(u.query)
        # Normalize params from args too
        qp = {}
        if params:
            qp.update(params)
        for k, v in q.items():
            qp[k] = v[-1] if isinstance(v, list) else v

        # Exchange Info
        if path == "/fapi/v1/exchangeInfo" and method == "GET":
            payload = {
                "symbols": [
                    {
                        "symbol": os.getenv("BOT_SYMBOL", "BTCUSDT"),
                        "quoteAsset": "USDT",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                            {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"},
                        ],
                    }
                ]
            }
            return FakeResp(200, payload)

        # Server time
        if path == "/fapi/v1/time" and method == "GET":
            return FakeResp(200, {"serverTime": now_ms()})

        # Position side mode
        if path == "/fapi/v1/positionSide/dual" and method == "GET":
            return FakeResp(200, {"dualSidePosition": False})

        # Leverage
        if path == "/fapi/v1/leverage" and method == "POST":
            return FakeResp(200, {"leverage": int((data or {}).get("leverage", 5))})

        # Balances
        if path == "/fapi/v2/balance" and method == "GET":
            return FakeResp(200, [{"asset": "USDT", "balance": "1000.0"}])

        # Positions
        if path == "/fapi/v2/positionRisk" and method == "GET":
            return FakeResp(200, [])

        # Premium index (funding and markPrice)
        if path == "/fapi/v1/premiumIndex" and method == "GET":
            phase = phase_fn()
            # Defaults
            rate = 0.0001   # 1 bps
            nft = now_ms() + 3600_000
            if phase == "funding_near":
                nft = now_ms() + 2 * 60_000  # 2 minutes to funding -> near window
                rate = 0.0001
            elif phase == "funding_high":
                nft = now_ms() + 3 * 3600_000  # far away, not near
                rate = 0.005   # 50 bps -> high
            if "symbol" in qp:
                return FakeResp(200, {
                    "symbol": qp["symbol"],
                    "markPrice": "30000.0",
                    "lastFundingRate": f"{rate}",
                    "nextFundingTime": nft,
                })
            else:
                # all symbols list
                sym = os.getenv("BOT_SYMBOL", "BTCUSDT").upper()
                return FakeResp(200, [{"symbol": sym, "markPrice": "30000.0"}])

        # Ticker price
        if path == "/fapi/v1/ticker/price" and method == "GET":
            sym = os.getenv("BOT_SYMBOL", "BTCUSDT").upper()
            return FakeResp(200, {"symbol": sym, "price": "30000.0"})

        # Klines
        if path == "/fapi/v1/klines" and method == "GET":
            interval = qp.get("interval", "1m")
            limit = int(qp.get("limit", 150))
            data = gen_klines(interval, limit)
            # Binance klines format is arrays; ictbot expects list of lists convertible
            # We already return that shape
            return FakeResp(200, data)

        # Open orders
        if path == "/fapi/v1/openOrders" and method == "GET":
            return FakeResp(200, [])

        # Income – build cumulative day realized PnL rows
        if path == "/fapi/v1/income" and method == "GET":
            phase = phase_fn()
            rows = []
            if phase == "pnl_losses":
                # Sum to -40 to trip 3% daily loss on 1000 equity
                base_ts = now_ms() - 1_000_000
                vals = [-10.0, -12.0, -9.0, -9.0]
                for i, v in enumerate(vals):
                    rows.append({
                        "asset": "USDT",
                        "income": str(v),
                        "time": base_ts + i * 1000,
                        "symbol": os.getenv("BOT_SYMBOL", "BTCUSDT"),
                        "incomeType": "REALIZED_PNL",
                    })
            return FakeResp(200, rows)

        # Orders (submit/cancel/get)
        if path == "/fapi/v1/order":
            if method == "POST":
                # capture clientOrderId
                cid = (data or {}).get("newClientOrderId")
                order_counter += 1
                oid = order_counter
                if cid:
                    orders_by_cid[cid] = {
                        "orderId": oid,
                        "clientOrderId": cid,
                        "status": "FILLED",
                        "avgPrice": "30000.0",
                        "executedQty": str((data or {}).get("quantity", 0.001)),
                        "cumQuote": "30.0",
                        "symbol": (data or {}).get("symbol", os.getenv("BOT_SYMBOL", "BTCUSDT")),
                    }
                return FakeResp(200, {"orderId": oid})
            if method == "GET":
                sym = qp.get("symbol", os.getenv("BOT_SYMBOL", "BTCUSDT"))
                ocid = qp.get("origClientOrderId")
                if ocid and ocid in orders_by_cid:
                    od = dict(orders_by_cid[ocid])
                    od["symbol"] = sym
                    return FakeResp(200, od)
                return FakeResp(404, {"msg": "Unknown order"})
            if method == "DELETE":
                return FakeResp(200, {"status": "CANCELED"})

        # listenKey create/keepalive
        if path == "/fapi/v1/listenKey":
            if method == "POST":
                return FakeResp(200, {"listenKey": "dummy_listen_key"})
            if method == "PUT":
                return FakeResp(200, {"result": "OK"})

        # default
        return FakeResp(200, {})

    return handler


def main():
    # Minimal env for dry run
    os.environ.setdefault("BINANCE_API_KEY", "test")
    os.environ.setdefault("BINANCE_API_SECRET", "test")
    os.environ.setdefault("BOT_SYMBOL", "BTCUSDT")
    os.environ.setdefault("BOT_REST_ONLY", "true")
    os.environ.setdefault("BOT_WS_MARKET", "false")
    os.environ.setdefault("BOT_SAFE_REQUIRE_LISTENKEY", "false")
    os.environ.setdefault("BOT_HTTP_RETRY_ENABLE", "false")
    os.environ.setdefault("BOT_HB_EVERY", "1")
    os.environ.setdefault("BOT_LOOP_SLEEP", "0.1")
    # Make it easier to trigger entries in test
    os.environ.setdefault("BOT_SCORE_THRESHOLD", "0.65")
    # Disable signal cooldown to allow multiple entries per minute
    os.environ.setdefault("BOT_SIGNAL_COOLDOWN_MIN", "0")

    # Scenarios per iteration
    phases = ["normal", "funding_near", "funding_high", "pnl_losses"]
    current = {"value": phases[0]}

    def phase_fn():
        return current["value"]

    handler = make_handler(phase_fn)

    def _wrap(method_name):
        def _f(self, url, *args, **kwargs):
            return handler(method_name.upper(), url, *args, **kwargs)
        return _f

    with patch("requests.Session.get", new=_wrap("get")), \
         patch("requests.Session.post", new=_wrap("post")), \
         patch("requests.Session.delete", new=_wrap("delete")), \
         patch("requests.Session.put", new=_wrap("put")):
        # Import after patching so sessions inside ictbot use the patched class
        import ictbot as bot

        cfg = bot.Config()
        ex = bot.BinanceUM(cfg)
        st = bot.RunState()

        bot.init_instrument(ex, st, cfg)
        # Skip websockets entirely in this dry run; don't call WSManager.start()
        # Simulate startup housekeeping
        bot.sanity_checks(ex, cfg)
        bot.reconcile_startup_orders(ex, st, cfg)
        bot.update_drift_policy(ex, st, cfg, force=True)

        last_time_poll = 0.0
        for ph in phases:
            current["value"] = ph
            print(f"\n--- PHASE: {ph} ---")
            # Force immediate REST income poll in this phase if needed
            st.last_pnl_poll_ts = -1e9
            st.loop_idx += 1
            last_time_poll = bot.poll_maintenance(ex, st, cfg, ws=None, last_time_poll=last_time_poll)
            fetched = bot.fetch_data_and_guard(ex, st, cfg)
            if fetched is None:
                time.sleep(0.05)
                continue
            exec_kl, exec_df, ws_fresh = fetched
            bot.process_trade_opportunities(ex, st, cfg, exec_kl, exec_df, ws_fresh)
            time.sleep(0.05)

        print("\n--- DRY RUN COMPLETE ---")


if __name__ == "__main__":
    main()
