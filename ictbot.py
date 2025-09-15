#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lean ICT-style FVG/Sweep bot for Binance UM Futures TESTNET — hardened build (patched)

This file includes fixes 1–37 from the audit, plus the following new hardening/features:
- Startup reconciliation of exchange state (positions & open orders)
- Account sanity checks (one-way vs hedge, cross vs isolated)
- Idempotent entries with client IDs and retry-by-lookup
- Funding-time awareness (block/penalize near funding; throttle risk on high funding)
- Order sizing guards (use available balance; per-trade notional cap; gross exposure cap)
- Exit hygiene on fills (cleanup stops/TPs)
- Websocket backoff with jitter (market & UDS) and reconnect metrics
- Safe-mode gates (WS freshness / listenKey health / sustained clock drift)
- Clock-drift hardening (median-of-3) with auto recovery
- Lightweight CSV metrics snapshotting
"""

import os
import sys
import math
import hmac
import hashlib
import time
import json
import random
import threading
import traceback
import datetime as dt
from decimal import Decimal, localcontext, Context
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Dict, Any, Set
from collections import deque
from urllib.parse import urlencode
import csv

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# Optional ZoneInfo import to avoid hard dependency in older environments
_ZoneInfo: Any = None
try:
    from zoneinfo import ZoneInfo as _ZoneInfo  # type: ignore[assignment]
except Exception:  # pragma: no cover
    _ZoneInfo = None

# ---- Python version guard
if sys.version_info < (3, 10):
    raise RuntimeError("Python 3.10+ required to run this bot (dataclasses + typing usage).")

# Optional websocket support
try:
    import websocket  # websocket-client
    WEBSOCKET_AVAILABLE = True
except Exception:
    WEBSOCKET_AVAILABLE = False


# ============================== Config ==============================

def _env_bool(key: str, default: bool) -> bool:
    val = os.getenv(key)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    val = os.getenv(key)
    if val is None or val == "":
        return int(default)
    try:
        return int(str(val).strip())
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    val = os.getenv(key)
    if val is None or val == "":
        return float(default)
    try:
        return float(str(val).strip())
    except Exception:
        return float(default)


@dataclass
class Config:
    # Exchange / symbol
    api_key: str = field(default_factory=lambda: os.getenv("BINANCE_API_KEY", ""))
    api_secret: str = field(default_factory=lambda: os.getenv("BINANCE_API_SECRET", ""))
    symbol: str = field(default_factory=lambda: os.getenv("BOT_SYMBOL", "BTCUSDT"))

    # Endpoints
    base_url: str = field(default_factory=lambda: os.getenv("BOT_BASE_URL", "https://testnet.binancefuture.com"))
    ws_base: str = field(default_factory=lambda: os.getenv("BOT_WS_BASE", ""))

    # Timings
    timeframe_exec: str = field(default_factory=lambda: os.getenv("BOT_TF_EXEC", "1m"))
    stale_data_minutes: int = field(default_factory=lambda: _env_int("BOT_STALE_MIN", 10))
    loop_sleep: float = field(default_factory=lambda: _env_float("BOT_LOOP_SLEEP", 2.0))

    # Risk & limits
    leverage: int = field(default_factory=lambda: _env_int("BOT_LEVERAGE", 5))
    risk_pct: float = field(default_factory=lambda: _env_float("BOT_RISK_PCT", 0.005))  # 0.5%
    rr_target: float = field(default_factory=lambda: _env_float("BOT_RR_TARGET", 2.0))
    max_rr_cap: float = field(default_factory=lambda: _env_float("BOT_MAX_RR_CAP", 4.0))
    min_rr_to_pool: float = field(default_factory=lambda: _env_float("BOT_MIN_RR_TO_POOL", 1.2))
    max_trades_per_day: int = field(default_factory=lambda: _env_int("BOT_MAX_TRADES_DAY", 6))
    max_daily_loss_pct: float = field(default_factory=lambda: _env_float("BOT_MAX_DAILY_LOSS_PCT", 0.03))
    cooldown_minutes_after_loss: int = field(default_factory=lambda: _env_int("BOT_COOLDOWN_MIN", 20))
    fee_bps: float = field(default_factory=lambda: _env_float("BOT_FEE_BPS", 5.0))  # realistic taker-ish both sides budget
    min_stop_ticks: int = field(default_factory=lambda: _env_int("BOT_MIN_STOP_TICKS", 2))
    partial_tp_frac: float = field(default_factory=lambda: _env_float("BOT_PARTIAL_TP_FRAC", 0.35))

    # Throttle ladder
    throttle_drawdown1: float = field(default_factory=lambda: _env_float("BOT_THR_DD1", 0.02))
    throttle_drawdown2: float = field(default_factory=lambda: _env_float("BOT_THR_DD2", 0.05))
    throttle_risk1: float = field(default_factory=lambda: _env_float("BOT_THR_RISK1", 0.5))
    throttle_risk2: float = field(default_factory=lambda: _env_float("BOT_THR_RISK2", 0.25))

    # Sweep strictness (baseline; ATR-adapted)
    sweep_min_penetration_bps: float = field(default_factory=lambda: _env_float("BOT_SWEEP_PEN_BPS", 2.0))
    sweep_closeback_bps: float = field(default_factory=lambda: _env_float("BOT_SWEEP_CB_BPS", 2.0))
    sweep_t_reject_max_bars: int = field(default_factory=lambda: _env_int("BOT_SWEEP_TMAX", 5))

    # FVG scan
    fvg_min_ticks: int = field(default_factory=lambda: _env_int("BOT_FVG_MIN_TICKS", 2))
    # Optional gap threshold in BPS instead of ticks
    fvg_gap_use_bps: bool = field(default_factory=lambda: _env_bool("BOT_FVG_USE_BPS", False))
    fvg_gap_min_bps: float = field(default_factory=lambda: _env_float("BOT_FVG_MIN_BPS", 8.0))
    # Allow slight overlap tolerance in gap detection (bps). 0 = strict.
    fvg_gap_relax_bps: float = field(default_factory=lambda: _env_float("BOT_FVG_RELAX_BPS", 0.0))

    # FVG filled logic (improved)
    fvg_fill_rule: str = field(default_factory=lambda: os.getenv("BOT_FVG_RULE", "touch"))  # touch|mid|full
    fvg_fill_require_body: bool = field(default_factory=lambda: _env_bool("BOT_FVG_REQUIRE_BODY", False))
    fvg_fill_min_penetration_atr: float = field(default_factory=lambda: _env_float("BOT_FVG_MIN_ATR", 0.0))
    fvg_fill_min_consecutive: int = field(default_factory=lambda: _env_int("BOT_FVG_MIN_CONSEC", 1))

    # Liquidity pools toggles
    enable_pools_eqh_eql: bool = field(default_factory=lambda: _env_bool("BOT_ENABLE_POOLS_EQH_EQL", True))
    enable_pools_prev_extremes: bool = field(default_factory=lambda: _env_bool("BOT_ENABLE_POOLS_PREV_EXT", True))
    enable_pools_sessions: bool = field(default_factory=lambda: _env_bool("BOT_ENABLE_POOLS_SESSIONS", True))
    use_session_london: bool = field(default_factory=lambda: _env_bool("BOT_USE_LONDON", True))
    use_session_newyork: bool = field(default_factory=lambda: _env_bool("BOT_USE_NEWYORK", True))
    use_opening_range: bool = field(default_factory=lambda: _env_bool("BOT_USE_ORB", True))
    opening_range_minutes: int = field(default_factory=lambda: _env_int("BOT_ORB_MIN", 60))
    # ORB anchoring: midnight or session
    opening_range_anchor: str = field(default_factory=lambda: os.getenv("BOT_ORB_ANCHOR", "midnight").lower())
    # Session to anchor ORB when opening_range_anchor == "session": asia|london|newyork
    opening_range_session: str = field(default_factory=lambda: os.getenv("BOT_ORB_SESSION", "newyork").lower())
    w_orb: float = field(default_factory=lambda: _env_float("BOT_W_ORB", 0.45))
    use_rolling_range: bool = field(default_factory=lambda: _env_bool("BOT_USE_ROLLING_RANGE", True))
    rolling_range_window: int = field(default_factory=lambda: _env_int("BOT_RNG_WIN", 60))
    rolling_range_min_touches: int = field(default_factory=lambda: _env_int("BOT_RNG_MIN_TOUCH", 3))
    rolling_range_atr_frac_max: float = field(default_factory=lambda: _env_float("BOT_RNG_ATR_FRAC_MAX", 0.60))
    w_rng: float = field(default_factory=lambda: _env_float("BOT_W_RNG", 0.45))
    use_fvg_edges_as_pools: bool = field(default_factory=lambda: _env_bool("BOT_USE_FVG_EDGES", True))
    fvg_pool_age_bars: int = field(default_factory=lambda: _env_int("BOT_FVG_POOL_AGE", 40))
    use_big_figures: bool = field(default_factory=lambda: _env_bool("BOT_USE_FIGS", False))
    figure_increment: float = field(default_factory=lambda: _env_float("BOT_FIG_INC", 100.0))
    include_quarter_figs: bool = field(default_factory=lambda: _env_bool("BOT_FIG_QUARTERS", False))
    use_session_vwap_bands: bool = field(default_factory=lambda: _env_bool("BOT_USE_VWAP", False))
    session_vwap_std_k: float = field(default_factory=lambda: _env_float("BOT_VWAP_K", 1.0))
    w_vwap: float = field(default_factory=lambda: _env_float("BOT_W_VWAP", 0.25))

    # Pool clustering & weights
    pool_cluster_bps: float = field(default_factory=lambda: _env_float("BOT_POOL_CLUSTER_BPS", 8.0))
    pool_topk_per_side: int = field(default_factory=lambda: _env_int("BOT_POOL_TOPK", 4))
    eq_spread_ticks: int = field(default_factory=lambda: _env_int("BOT_EQ_SPREAD_TICKS", 2))
    eq_min_hits: int = field(default_factory=lambda: _env_int("BOT_EQ_MIN_HITS", 2))
    eq_lookback: int = field(default_factory=lambda: _env_int("BOT_EQ_LOOKBACK", 120))
    w_eq: float = field(default_factory=lambda: _env_float("BOT_W_EQ", 0.55))
    w_sess: float = field(default_factory=lambda: _env_float("BOT_W_SESS", 0.60))
    w_fvg: float = field(default_factory=lambda: _env_float("BOT_W_FVG", 0.35))
    w_fig: float = field(default_factory=lambda: _env_float("BOT_W_FIG", 0.20))
    w_htf_mult: float = field(default_factory=lambda: _env_float("BOT_W_HTF_MULT", 1.25))
    # HTF tag set used in pool_scoring_v2 proximity weighting
    htf_tag_string: str = field(default_factory=lambda: os.getenv(
        "BOT_HTF_TAGS",
        "PMH,PML,PWH,PWL,PDH,PDL,NY_H,NY_L,LON_H,LON_L,ASIA_H,ASIA_L"
    ))

    # Scoring multipliers (soft)
    enable_pool_scoring_bonus: bool = field(default_factory=lambda: _env_bool("BOT_POOL_SCORE_ENABLE", True))
    pool_score_gain: float = field(default_factory=lambda: _env_float("BOT_POOL_SCORE_GAIN", 0.20))
    session_active_bonus: float = field(default_factory=lambda: _env_float("BOT_SESS_ACTIVE_BONUS", 0.06))
    cross_session_bonus: float = field(default_factory=lambda: _env_float("BOT_SESS_CROSS_BONUS", 0.08))
    bos_bonus_w: float = field(default_factory=lambda: _env_float("BOT_W_BOS", 0.05))
    choch_bonus_w: float = field(default_factory=lambda: _env_float("BOT_W_CHOCH", 0.05))
    htf_consensus_w: float = field(default_factory=lambda: _env_float("BOT_W_HTF", 0.10))
    containment_15m_w: float = field(default_factory=lambda: _env_float("BOT_W_CONTAIN", 0.05))
    displacement_w: float = field(default_factory=lambda: _env_float("BOT_W_DISP", 0.05))
    # Scoring constants (configurable)
    # Weight applied to each nearby pool proximity unit contribution
    score_pool_near_unit_w: float = field(default_factory=lambda: _env_float("BOT_SCORE_POOL_NEAR_W", 0.15))
    # Weight applied to pool_scoring_v2 aggregate contribution
    score_pool_sv2_w: float = field(default_factory=lambda: _env_float("BOT_SCORE_POOL_SV2_W", 0.15))
    # Base offset and FVG quality weight for candidate seed score
    score_cand_base_offset: float = field(default_factory=lambda: _env_float("BOT_SCORE_CAND_BASE", 0.5))
    score_cand_fvgq_w: float = field(default_factory=lambda: _env_float("BOT_SCORE_CAND_FVGQ_W", 0.35))

    # FVG quality balance (kept optional, defaults mirror existing behavior)
    fvgq_gap_w: float = field(default_factory=lambda: _env_float("BOT_FVGQ_GAP_W", 0.60))
    fvgq_vol_w: float = field(default_factory=lambda: _env_float("BOT_FVGQ_VOL_W", 0.40))
    fvgq_gap_norm: float = field(default_factory=lambda: _env_float("BOT_FVGQ_GAP_NORM", 2.0))
    fvgq_vol_shift: float = field(default_factory=lambda: _env_float("BOT_FVGQ_VOL_SHIFT", 2.0))
    fvgq_vol_scale: float = field(default_factory=lambda: _env_float("BOT_FVGQ_VOL_SCALE", 4.0))
    proximity_window_bps: float = field(default_factory=lambda: _env_float("BOT_PROX_WIN_BPS", 30.0))
    # Entry score threshold
    score_threshold: float = field(default_factory=lambda: _env_float("BOT_SCORE_THRESHOLD", 0.75))

    # Sessions (UTC)
    asia_session_start: str = field(default_factory=lambda: os.getenv("BOT_ASIA_START", "00:00"))
    asia_session_end: str = field(default_factory=lambda: os.getenv("BOT_ASIA_END", "08:00"))
    session_london_utc: Tuple[str, str] = field(
        default_factory=lambda: (os.getenv("BOT_LON_START", "07:00"), os.getenv("BOT_LON_END", "16:00"))
    )
    session_newyork_utc: Tuple[str, str] = field(
        default_factory=lambda: (os.getenv("BOT_NY_START", "12:00"), os.getenv("BOT_NY_END", "21:00"))
    )
    # Timezone for session windows and ORB (IANA name, e.g., "UTC", "America/New_York")
    session_timezone: str = field(default_factory=lambda: os.getenv("BOT_SESSION_TZ", "UTC"))

    # WS toggles
    ws_market_enable: bool = field(default_factory=lambda: _env_bool("BOT_WS_MARKET", True))
    ws_book_ticker_enable: bool = field(default_factory=lambda: _env_bool("BOT_WS_BOOK", False))
    # REST-only mode: do not block on WS freshness; still allows UDS if available
    rest_only_mode: bool = field(default_factory=lambda: _env_bool("BOT_REST_ONLY", False))

    # Resilience / long-run knobs
    uds_auto_refresh: bool = field(default_factory=lambda: _env_bool("BOT_UDS_AUTO_REFRESH", True))
    ws_watchdog_secs: int = field(default_factory=lambda: _env_int("BOT_WS_WATCHDOG", 90))
    exchangeinfo_refresh_secs: int = field(default_factory=lambda: _env_int("BOT_INFO_REFRESH", 6 * 3600))
    server_time_sessions: bool = field(default_factory=lambda: _env_bool("BOT_SERVER_TIME_SESSIONS", True))  # use server time

    # Dynamic recvWindow
    recv_window_ms_base: int = field(default_factory=lambda: _env_int("BOT_RECV_BASE", 5000))
    recv_window_ms_max: int = field(default_factory=lambda: _env_int("BOT_RECV_MAX", 12000))
    drift_threshold_ms: int = field(default_factory=lambda: _env_int("BOT_DRIFT_THRESH", 2000))
    drift_bump_ms: int = field(default_factory=lambda: _env_int("BOT_DRIFT_BUMP", 5000))

    # Persist state
    persist_state_path: str = field(default_factory=lambda: os.getenv("BOT_STATE_PATH", "state/state.json"))

    # Heartbeat throttle
    heartbeat_every_n_loops: int = field(default_factory=lambda: _env_int("BOT_HB_EVERY", 3))

    # Circuit breaker
    circuit_block_threshold: int = field(default_factory=lambda: _env_int("BOT_CIRCUIT_N", 5))
    circuit_sleep: float = field(default_factory=lambda: _env_float("BOT_CIRCUIT_SLEEP", 7.0))
    order_error_block_threshold: int = field(default_factory=lambda: _env_int("BOT_ORDER_ERR_N", 4))
    order_error_block_sleep: float = field(default_factory=lambda: _env_float("BOT_ORDER_ERR_SLEEP", 15.0))

    # Trailing (ATR fraction & activation)
    trail_activation_R: float = field(default_factory=lambda: _env_float("BOT_TRAIL_ACT_R", 1.5))
    trail_atr_k: float = field(default_factory=lambda: _env_float("BOT_TRAIL_ATR_K", 0.75))  # trail distance = k * ATR behind price
    trail_min_tick_improve: int = field(default_factory=lambda: _env_int("BOT_TRAIL_MIN_TICKS", 1))
    position_check_before_trail: bool = field(default_factory=lambda: _env_bool("BOT_POSCHK_TRAIL", True))

    # Gentle retries (non-order endpoints)
    kline_rest_retry: int = field(default_factory=lambda: _env_int("BOT_KLINE_RETRY", 3))
    signed_safe_retry: int = field(default_factory=lambda: _env_int("BOT_SIGNED_SAFE_RETRY", 2))
    # REST kline call min interval to avoid rate-limit thrash when WS is stale (seconds)
    kline_rest_min_interval_secs: float = field(default_factory=lambda: _env_float("BOT_KLINE_REST_MIN_SECS", 3.0))

    # Debug
    signal_cooldown_min: int = field(default_factory=lambda: _env_int("BOT_SIGNAL_COOLDOWN_MIN", 45))
    debug: bool = field(default_factory=lambda: _env_bool("BOT_DEBUG", False))
    debug_jsonl_path: str = field(default_factory=lambda: os.getenv("BOT_DEBUG_JSONL", "bot_debug.jsonl"))

    # HTF bias refresh + hysteresis
    htf_refresh_secs: int = field(default_factory=lambda: _env_int("BOT_HTF_REFRESH", 600))
    htf_bias_hysteresis_bps: float = field(default_factory=lambda: _env_float("BOT_HTF_HYST_BPS", 8.0))

    # Order trigger behavior
    working_type: str = field(default_factory=lambda: os.getenv("BOT_WORKING_TYPE", "MARK_PRICE"))  # or CONTRACT_PRICE

    # Equity cache
    balance_cache_secs: int = field(default_factory=lambda: _env_int("BOT_BALANCE_TTL", 30))

    # HTF level weight
    w_htf_levels: float = field(default_factory=lambda: _env_float("BOT_W_HTF_LVLS", 0.80))

    # --- Account expectations (sanity checks)
    margin_mode: str = field(default_factory=lambda: os.getenv("BOT_MARGIN_MODE", "CROSS").upper())  # CROSS|ISOLATED
    require_one_way: bool = field(default_factory=lambda: _env_bool("BOT_REQUIRE_ONEWAY", True))

    # --- Funding awareness
    funding_aware_enable: bool = field(default_factory=lambda: _env_bool("BOT_FUNDING_AWARE", True))
    funding_window_min: int = field(default_factory=lambda: _env_int("BOT_FUNDING_WIN_MIN", 7))   # block/penalize within ±N minutes
    funding_window_symmetric: bool = field(default_factory=lambda: _env_bool("BOT_FUNDING_WIN_SYM", True))
    funding_block_near: bool = field(default_factory=lambda: _env_bool("BOT_FUNDING_BLOCK_NEAR", True))   # if False, apply score/risk penalties instead
    funding_penalty_score: float = field(default_factory=lambda: _env_float("BOT_FUNDING_SCORE_PENALTY", 0.15))
    funding_high_abs_bps: float = field(default_factory=lambda: _env_float("BOT_FUNDING_HIGH_ABS_BPS", 10.0))  # 10 bps = 0.10%
    funding_risk_mult_high: float = field(default_factory=lambda: _env_float("BOT_FUNDING_RISK_MULT_HIGH", 0.6))

    # --- Order sizing guards
    use_available_balance: bool = field(default_factory=lambda: _env_bool("BOT_USE_AVAILABLE_BAL", True))
    max_notional_per_trade: float = field(default_factory=lambda: _env_float("BOT_MAX_NOTIONAL_PER_TRADE", 0.0))  # 0=disabled
    max_gross_usdt_exposure: float = field(default_factory=lambda: _env_float("BOT_MAX_GROSS_EXPOSURE", 0.0))     # 0=disabled

    # --- Websocket backoff
    ws_backoff_min: float = field(default_factory=lambda: _env_float("BOT_WS_BACKOFF_MIN", 0.6))
    ws_backoff_max: float = field(default_factory=lambda: _env_float("BOT_WS_BACKOFF_MAX", 30.0))
    ws_backoff_mult: float = field(default_factory=lambda: _env_float("BOT_WS_BACKOFF_MULT", 1.7))
    # WS freshness hard upper bound (ms) when scaling by TF
    ws_fresh_max_ms: int = field(default_factory=lambda: _env_int("BOT_WS_FRESH_MAX_MS", 120_000))
    # Ticker price cache TTL (seconds)
    ticker_cache_secs: float = field(default_factory=lambda: _env_float("BOT_TICKER_CACHE_SECS", 5.0))

    # --- Safe mode gating
    safe_mode_enable: bool = field(default_factory=lambda: _env_bool("BOT_SAFE_MODE_ENABLE", True))
    safe_require_listenkey: bool = field(default_factory=lambda: _env_bool("BOT_SAFE_REQUIRE_LISTENKEY", True))
    safe_stale_hard_minutes: int = field(default_factory=lambda: _env_int("BOT_SAFE_STALE_MIN", 30))
    safe_auto_recover: bool = field(default_factory=lambda: _env_bool("BOT_SAFE_AUTO_RECOVER", True))

    # --- Clock drift hardening
    drift_hard_ms: int = field(default_factory=lambda: _env_int("BOT_DRIFT_HARD_MS", 5000))
    drift_sustain_n: int = field(default_factory=lambda: _env_int("BOT_DRIFT_SUSTAIN_N", 3))

    # --- Lightweight health metrics
    metrics_enable: bool = field(default_factory=lambda: _env_bool("BOT_METRICS_ENABLE", True))
    metrics_csv_path: str = field(default_factory=lambda: os.getenv("BOT_METRICS_CSV", "state/metrics.csv"))

    # --- HTTP timeout (seconds)
    http_timeout: float = field(default_factory=lambda: _env_float("BOT_HTTP_TIMEOUT", 12.0))
    # --- HTTP session retries for GETs
    http_retry_enable: bool = field(default_factory=lambda: _env_bool("BOT_HTTP_RETRY_ENABLE", True))
    http_retry_total: int = field(default_factory=lambda: _env_int("BOT_HTTP_RETRY_TOTAL", 1))
    http_retry_connect: int = field(default_factory=lambda: _env_int("BOT_HTTP_RETRY_CONNECT", 1))
    http_retry_read: int = field(default_factory=lambda: _env_int("BOT_HTTP_RETRY_READ", 1))
    http_retry_backoff: float = field(default_factory=lambda: _env_float("BOT_HTTP_RETRY_BACKOFF", 0.2))
    # Disable adapter retries specifically for /klines GET calls (Option A)
    http_retry_disable_klines: bool = field(default_factory=lambda: _env_bool("BOT_HTTP_RETRY_DISABLE_KLINES", True))
    
    # --- Exposure approx price source
    # last = /ticker/price; mark = /premiumIndex markPrice
    exposure_price_source: str = field(default_factory=lambda: os.getenv("BOT_EXPOSURE_PX", "last"))
    exposure_fallback_to_last: bool = field(default_factory=lambda: _env_bool("BOT_EXPOSURE_PX_FALLBACK_LAST", True))
    markprice_cache_secs: float = field(default_factory=lambda: _env_float("BOT_MARKPRICE_TTL", 5.0))

    # --- Dedupe stability
    dedupe_bin_ticks: int = field(default_factory=lambda: _env_int("BOT_DEDUPE_BIN_TICKS", 10))

    def __post_init__(self):
        # Derive sensible default WS base if none provided.
        # - Testnet REST → testnet WS: wss://stream.binancefuture.com
        # - Mainnet REST → mainnet WS: wss://fstream.binance.com
        try:
            if not self.ws_base:
                if "testnet.binancefuture.com" in (self.base_url or ""):
                    self.ws_base = "wss://stream.binancefuture.com"
                else:
                    self.ws_base = "wss://fstream.binance.com"
        except Exception:
            # Fallback to testnet default if anything goes wrong
            if not self.ws_base:
                self.ws_base = "wss://stream.binancefuture.com"
        # Normalize HTF tag string to uppercase trimmed tokens (for robustness)
        try:
            items = [t.strip().upper() for t in str(self.htf_tag_string or "").split(",") if t.strip()]
            if items:
                self.htf_tag_string = ",".join(items)
        except Exception:
            pass


# ============================== Helpers ==============================

EPS = 1e-9


def now_ms() -> int:
    return int(time.time() * 1000)


def print_json(d: Dict[str, Any]) -> None:
    try:
        print(json.dumps(d, separators=(",", ":"), ensure_ascii=False), flush=True)
    except Exception as e:
        try:
            print(f"[json_fallback]{str(d)}", flush=True)
        except Exception:
            print(f"[json_fallback_err]{e}", flush=True)

# Global file I/O locks to prevent concurrent writes from threads
DEBUG_LOG_LOCK = threading.Lock()
METRICS_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()


def within_bps(a: float, b: float, tol_bps: float) -> bool:
    # Symmetric BPS tolerance
    if not (math.isfinite(a) and math.isfinite(b)):
        return False
    denom = max(EPS, (abs(a) + abs(b)) / 2.0)
    return abs(a - b) / denom * 10000.0 <= tol_bps


def bps_distance(a: float, b: float) -> float:
    # Symmetric BPS distance
    denom = max(EPS, (abs(a) + abs(b)) / 2.0)
    return abs(a - b) / denom * 10000.0


def _get_zone(tz_name: Optional[str]) -> Optional[Any]:
    """Return ZoneInfo for tz_name; fallback to UTC or None if unavailable."""
    try:
        if tz_name and _ZoneInfo is not None:
            return _ZoneInfo(tz_name)
    except Exception:
        pass
    try:
        return _ZoneInfo("UTC") if _ZoneInfo is not None else None
    except Exception:
        return None


def active_sessions_utc(mins: int, cfg: Config) -> Set[str]:
    """Return active sessions for a given minute-of-day.

    mins: integer minutes since local midnight in the configured session timezone.
    Overlap-aware: returns all sessions that include this minute.
    """
    out: Set[str] = set()

    def in_span(hm_start: str, hm_end: str) -> bool:
        sh, sm = map(int, hm_start.split(":"))
        eh, em = map(int, hm_end.split(":"))
        smin, emin = sh * 60 + sm, eh * 60 + em
        if smin <= emin:
            return smin <= mins < emin  # end-exclusive
        else:  # wraps midnight
            return (mins >= smin) or (mins < emin)

    if in_span(cfg.asia_session_start, cfg.asia_session_end):
        out.add("asia")
    if in_span(cfg.session_london_utc[0], cfg.session_london_utc[1]):
        out.add("london")
    if in_span(cfg.session_newyork_utc[0], cfg.session_newyork_utc[1]):
        out.add("newyork")
    if not out:
        out.add("off")
    return out


def df_from_klines(kl: List[List[float]]) -> pd.DataFrame:
    if not kl:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(kl, columns=["time", "open", "high", "low", "close", "volume"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def atr(df: pd.DataFrame, period: int = 14) -> float:
    if df is None or len(df) < period + 1:
        return 0.0
    highs, lows, closes = df["high"].values, df["low"].values, df["close"].values
    trs: List[float] = []
    for i in range(1, len(df)):
        h, lo, pc = highs[i], lows[i], closes[i - 1]
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    if len(trs) < period:
        return 0.0
    val = float(pd.Series(trs).rolling(period).mean().iloc[-1])
    return float(val) if math.isfinite(val) else 0.0


def vol_zscore(df: pd.DataFrame, window: int = 20) -> float:
    if df is None or len(df) < window:
        return 0.0
    v = df["volume"].tail(window)
    mu = float(v.mean())
    sd = float(v.std())
    if not math.isfinite(mu) or not math.isfinite(sd):
        return 0.0
    return float((float(v.iloc[-1]) - mu) / (sd + EPS))


def side_round(price: float, tick: float, side: str, favor: str = "entry") -> float:
    """
    Side-aware rounding helper.
    side: "buy"/"sell" or "long"/"short"
    favor="entry" -> tighter entry; favor="protection" -> safer for stops
    """
    if tick <= 0:
        return price
    s = side.lower()
    is_buy = s in ("buy", "long")
    if is_buy:
        v = math.floor(price / tick) * tick if favor == "entry" else math.ceil(price / tick) * tick
    else:
        v = math.ceil(price / tick) * tick if favor == "entry" else math.floor(price / tick) * tick
    try:
        # Normalize to tick decimals to avoid binary float artifacts
        dec = decimals_for_tick(max(EPS, tick))
        return round(v, dec)
    except Exception:
        return v


def round_qty_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    return float(math.floor(qty / step) * step)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def decimals_for_tick(tick: float) -> int:
    try:
        # Use Decimal for stable precision inference with a local context
        with localcontext(Context(prec=28)):
            d = Decimal(str(tick)).normalize()
        # Exponent is negative number of decimals for finite decimals
        exp = int(d.as_tuple().exponent)
        return int(-exp) if exp < 0 else 0
    except Exception:
        s = f"{tick:.18f}".rstrip("0")
        if "." in s:
            return max(0, len(s.split(".")[-1]))
        return 0


# ============================== Binance REST/WS ==============================

class BinanceUM:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.sess = requests.Session()
        self.sess.headers.update({"X-MBX-APIKEY": cfg.api_key})
        # A separate no-retry session for /klines GET calls
        self.sess_noretry = requests.Session()
        self.sess_noretry.headers.update({"X-MBX-APIKEY": cfg.api_key})
        # Configure urllib3 retries for idempotent GETs to smooth transient 5xx/network issues
        try:
            # Always ensure sess_noretry has zero adapter retries (independent of global retry toggle)
            adapter0 = HTTPAdapter(max_retries=0)
            self.sess_noretry.mount("https://", adapter0)
            self.sess_noretry.mount("http://", adapter0)
            if cfg.http_retry_enable:
                retries = Retry(
                    total=max(0, int(cfg.http_retry_total)),
                    connect=max(0, int(cfg.http_retry_connect)),
                    read=max(0, int(cfg.http_retry_read)),
                    backoff_factor=max(0.0, float(cfg.http_retry_backoff)),
                    status_forcelist=(500, 502, 503, 504),
                    allowed_methods=frozenset(["GET"]),
                    raise_on_status=False,
                    respect_retry_after_header=True,
                )
                adapter = HTTPAdapter(max_retries=retries)
                self.sess.mount("https://", adapter)
                self.sess.mount("http://", adapter)
        except requests.exceptions.RequestException as e:
            print(f"[http_retry] setup failed: {e}")
        self._dynamic_recv_window_ms = cfg.recv_window_ms_base
        self._server_offset_ms: int = 0
        self._lock = threading.Lock()
        self._send_lock = threading.Lock()  # serialize sends to be safe across threads
        # quoteAsset cache
        self._quote_map: Dict[str, str] = {}
        self._quote_map_ts: float = 0.0

    def set_drift_offset(self, offset_ms: Optional[int]):
        if offset_ms is None:
            self._dynamic_recv_window_ms = self.cfg.recv_window_ms_base
            self._server_offset_ms = 0
            return
        self._server_offset_ms = int(offset_ms)
        if abs(offset_ms) >= self.cfg.drift_threshold_ms:
            self._dynamic_recv_window_ms = min(
                self.cfg.recv_window_ms_base + self.cfg.drift_bump_ms, self.cfg.recv_window_ms_max
            )
        else:
            self._dynamic_recv_window_ms = self.cfg.recv_window_ms_base

    @staticmethod
    def _normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in (params or {}).items():
            if isinstance(v, bool):
                out[k] = "true" if v else "false"
            else:
                # normalize to string for deterministic signing across numeric/object types
                out[k] = str(v)
        return out

    def _signed_qs(self, params: Dict[str, Any]) -> str:
        items = sorted(self._normalize_params(params).items(), key=lambda kv: kv[0])
        qs = urlencode(items, doseq=True, safe="")
        sig = hmac.new(self.cfg.api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        return qs + f"&signature={sig}"

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
        retry_hint: str = "default",
    ) -> Any:
        base_url = self.cfg.base_url + path
        params = dict(params or {})
        max_attempts = 1
        if path.endswith("/klines"):
            max_attempts = max(1, self.cfg.kline_rest_retry)
        elif retry_hint in ("safe_signed", "listenKey", "leverage", "balance"):
            max_attempts = max(1, self.cfg.signed_safe_retry)

        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            r = None
            try:
                # Build URL and body under lock only for signing
                signed_url = None
                signed_params: Optional[Dict[str, Any]] = None
                if signed:
                    with self._lock:
                        params["timestamp"] = now_ms() + self._server_offset_ms
                        params["recvWindow"] = self._dynamic_recv_window_ms
                        qs_full = self._signed_qs(params)
                        signed_url = base_url + "?" + qs_full
                        sig = qs_full.rsplit("signature=", maxsplit=1)[-1]
                        signed_params = dict(params)
                        signed_params["signature"] = sig
                # Perform the request without holding the lock
                if signed:
                    with self._send_lock:
                        if method == "GET":
                            assert signed_url is not None
                            r = self.sess.get(signed_url, timeout=self.cfg.http_timeout)
                        elif method == "POST":
                            r = self.sess.post(base_url, data=signed_params, timeout=self.cfg.http_timeout)
                        elif method == "DELETE":
                            # Binance DELETE usually expects signed params in query
                            assert signed_url is not None
                            r = self.sess.delete(signed_url, timeout=self.cfg.http_timeout)
                        elif method == "PUT":
                            r = self.sess.put(base_url, data=signed_params, timeout=self.cfg.http_timeout)
                        else:
                            raise RuntimeError("Unsupported method")
                else:
                    with self._send_lock:
                        if method == "GET":
                            # Option A: route /klines through no-retry session to avoid double retries
                            if self.cfg.http_retry_disable_klines and path.endswith("/klines"):
                                r = self.sess_noretry.get(base_url, params=params, timeout=self.cfg.http_timeout)
                            else:
                                r = self.sess.get(base_url, params=params, timeout=self.cfg.http_timeout)
                        elif method == "POST":
                            r = self.sess.post(base_url, data=params, timeout=self.cfg.http_timeout)
                        elif method == "DELETE":
                            r = self.sess.delete(base_url, params=params, timeout=self.cfg.http_timeout)
                        elif method == "PUT":
                            r = self.sess.put(base_url, data=params, timeout=self.cfg.http_timeout)
                        else:
                            raise RuntimeError("Unsupported method")

                if r.status_code in (418, 429) and attempt < max_attempts:
                    time.sleep(0.15 + 0.2 * random.random())
                    continue
                r.raise_for_status()
                try:
                    return r.json()
                except Exception:
                    return {"text": r.text, "status": r.status_code}
            except requests.HTTPError as e:
                last_exc = e
                if r is not None and r.status_code in (418, 429) and attempt < max_attempts:
                    time.sleep(0.15 + 0.2 * random.random())
                    continue
                raise
            except requests.exceptions.RequestException as e:
                last_exc = e
                if attempt < max_attempts:
                    time.sleep(0.1 + 0.2 * random.random())
                    continue
                # Re-raise the current exception for consistent tracebacks
                raise

    # --- endpoints
    def exchange_info(self) -> Any:
        return self._request("GET", "/fapi/v1/exchangeInfo")

    def server_time(self) -> int:
        j = self._request("GET", "/fapi/v1/time")
        return int(j["serverTime"])

    def set_leverage(self, symbol: str, leverage: int) -> Any:
        try:
            return self._request(
                "POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage}, signed=True, retry_hint="leverage"
            )
        except requests.exceptions.RequestException as e:
            print(f"[WARN] set_leverage: {e}")
            return None

    def fetch_klines(self, symbol: str, interval: str, limit: int = 150) -> List[List[float]]:
        j = self._request("GET", "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        return [[int(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in j]

    def balance(self) -> float:
        j = self._request("GET", "/fapi/v2/balance", signed=True, retry_hint="balance")
        try:
            for b in j:
                if b.get("asset") == "USDT":
                    return float(b.get("balance", 0.0))
        except Exception:
            pass
        return 0.0

    def positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"symbol": symbol} if symbol else {}
        j = self._request("GET", "/fapi/v2/positionRisk", params, signed=True, retry_hint="safe_signed")
        return j if isinstance(j, list) else []

    def position_net_and_gross(self, symbol: str) -> Tuple[float, float]:
        rows = self.positions(symbol)
        net = 0.0
        gross = 0.0
        for row in rows:
            try:
                amt = float(row.get("positionAmt", 0.0))
                net += amt
                gross += abs(amt)
            except Exception:
                continue
        return net, gross

    def ticker_prices(self) -> Dict[str, float]:
        """Return a symbol->lastPrice map."""
        try:
            j = self._request("GET", "/fapi/v1/ticker/price")
            if isinstance(j, list):
                out: Dict[str, float] = {}
                for r in j:
                    try:
                        s = str(r.get("symbol", ""))
                        p = float(r.get("price", 0.0))
                        if s:
                            out[s] = p
                    except Exception:
                        continue
                return out
            elif isinstance(j, dict):
                s = str(j.get("symbol", ""))
                p = float(j.get("price", 0.0))
                return {s: p} if s else {}
        except Exception:
            pass
        return {}

    def ticker_prices_cached(self, st: 'RunState', cfg: 'Config') -> Dict[str, float]:
        nowt = time.time()
        with st.buf_lock:
            if (nowt - st.prices_cache_ts) <= float(cfg.ticker_cache_secs) and st.prices_cache:
                return dict(st.prices_cache)
        prices = self.ticker_prices()
        with st.buf_lock:
            st.prices_cache = dict(prices)
            st.prices_cache_ts = nowt
        return prices

    def _quote_asset(self, symbol: str) -> str:
        # Use cached exchangeInfo mapping; fallback to suffix heuristic
        try:
            nowt = time.time()
            if not self._quote_map or (nowt - self._quote_map_ts) > 3600:
                info = self.exchange_info()
                qmap: Dict[str, str] = {}
                for s in info.get("symbols", []):
                    try:
                        sym = str(s.get("symbol", ""))
                        qa = str(s.get("quoteAsset", ""))
                        if sym and qa:
                            qmap[sym] = qa
                    except Exception:
                        continue
                if qmap:
                    self._quote_map = qmap
                    self._quote_map_ts = nowt
            if symbol in self._quote_map:
                return self._quote_map[symbol]
        except Exception:
            pass
        for q in ("USDT", "BUSD", "USDC", "TUSD", "FDUSD", "USDP"):
            if symbol.endswith(q):
                return q
        return "USDT"

    def gross_stable_exposure_by_asset(self, st: 'RunState', cfg: 'Config') -> Dict[str, float]:
        """Approximate total notional per stable-coin quote across ALL positions.
        Price source configurable via cfg.exposure_price_source: 'last' or 'mark'.
        """
        out: Dict[str, float] = {}
        try:
            rows = self.positions()
            use_mark = str(cfg.exposure_price_source or "last").lower().startswith("mark")
            price_map: Dict[str, float] = {}
            if use_mark:
                price_map = self.mark_prices_cached(st, cfg)
                if (not price_map) and cfg.exposure_fallback_to_last:
                    price_map = self.ticker_prices_cached(st, cfg)
            else:
                price_map = self.ticker_prices_cached(st, cfg)
            for row in rows:
                try:
                    s = str(row.get("symbol", ""))
                    amt = float(row.get("positionAmt", 0.0))
                    if not s or amt == 0.0:
                        continue
                    px = float(price_map.get(s, 0.0))
                    qa = self._quote_asset(s)
                    out[qa] = out.get(qa, 0.0) + abs(amt) * px
                except Exception:
                    continue
        except Exception:
            pass
        return out

    def new_order(self, **kwargs) -> Any:
        return self._request("POST", "/fapi/v1/order", kwargs, signed=True)

    def cancel_order(self, **kwargs) -> Any:
        return self._request("DELETE", "/fapi/v1/order", kwargs, signed=True)

    # --- added helpers
    def open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"symbol": symbol} if symbol else {}
        j = self._request("GET", "/fapi/v1/openOrders", params, signed=True, retry_hint="safe_signed")
        return j if isinstance(j, list) else []

    def get_order(self, symbol: str, order_id: Optional[int] = None, orig_client_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = int(order_id)
        if orig_client_id is not None:
            params["origClientOrderId"] = orig_client_id
        try:
            j = self._request("GET", "/fapi/v1/order", params, signed=True, retry_hint="safe_signed")
            return j if isinstance(j, dict) and j.get("symbol") == symbol else None
        except Exception:
            return None

    def income(self, **params) -> Any:
        """GET /fapi/v1/income signed."""
        return self._request("GET", "/fapi/v1/income", params, signed=True, retry_hint="safe_signed")

    def position_side_dual(self) -> Optional[bool]:
        """Returns True if hedge (dual-side) mode is enabled, False if one-way, None on error."""
        try:
            j = self._request("GET", "/fapi/v1/positionSide/dual", signed=True, retry_hint="safe_signed")
            val = j.get("dualSidePosition")
            # Robustly coerce to boolean
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() == "true"
            if isinstance(val, (int, float)):
                return int(val) != 0
            return None
        except Exception:
            return None

    def account(self) -> Dict[str, Any]:
        j = self._request("GET", "/fapi/v2/account", signed=True, retry_hint="safe_signed")
        return j if isinstance(j, dict) else {}

    def available_balance(self, asset: str = "USDT") -> float:
        try:
            acc = self.account()
            for a in acc.get("assets", []):
                if a.get("asset") == asset:
                    return float(a.get("availableBalance", 0.0))
        except Exception:
            pass
        return 0.0

    def premium_index(self, symbol: str) -> Dict[str, Any]:
        """Contains current funding rate and nextFundingTime (ms)."""
        j = self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        return j if isinstance(j, dict) else {}

    def mark_prices(self) -> Dict[str, float]:
        """Return a symbol->markPrice map using /fapi/v1/premiumIndex (all symbols)."""
        out: Dict[str, float] = {}
        try:
            j = self._request("GET", "/fapi/v1/premiumIndex")
            if isinstance(j, list):
                for r in j:
                    try:
                        s = str(r.get("symbol", ""))
                        p = float(r.get("markPrice", 0.0))
                        if s:
                            out[s] = p
                    except Exception:
                        continue
        except Exception:
            pass
        return out

    def mark_prices_cached(self, st: 'RunState', cfg: 'Config') -> Dict[str, float]:
        nowt = time.time()
        with st.buf_lock:
            if (nowt - st.mark_prices_cache_ts) <= float(cfg.markprice_cache_secs) and st.mark_prices_cache:
                return dict(st.mark_prices_cache)
        prices = self.mark_prices()
        with st.buf_lock:
            st.mark_prices_cache = dict(prices)
            st.mark_prices_cache_ts = nowt
        return prices


# ============================== State ==============================

@dataclass
class RunState:
    # WS buffer lock
    buf_lock: threading.Lock = field(default_factory=threading.Lock)

    # market ws buffers (time,open,high,low,close,volume)
    kl_1m: deque = field(default_factory=lambda: deque(maxlen=500))
    kl_5m: deque = field(default_factory=lambda: deque(maxlen=500))
    last_market_ws_ts: int = 0
    last_bid: Optional[float] = None
    last_ask: Optional[float] = None

    # uds pnl / day state
    realized_today: float = 0.0
    trades_today: int = 0
    day_start_equity: float = 0.0
    day_start_date: str = ""  # from server time
    last_loss_server_ms: Optional[int] = None
    # cumulative realized cache per symbol (for ACCOUNT_UPDATE)
    account_cr: Dict[str, float] = field(default_factory=dict)

    # instrument meta
    tick: float = 0.1
    step: float = 0.001
    min_notional: float = 5.0

    # trailing state
    last_trailing_sl: Optional[float] = None
    last_trailing_side: Optional[str] = None
    last_trailing_client_id: Optional[str] = None  # used for cancel/replace
    last_trailing_order_id: Optional[int] = None   # fallback cancel by orderId

    # health
    last_server_offset_ms: Optional[int] = None
    last_server_ms: Optional[int] = None

    # ops
    loop_idx: int = 0
    blocked_count: int = 0
    stale_count: int = 0

    # info refresh
    last_info_refresh_ts: float = 0.0

    # dedupe store (signal_id -> last_trigger_epoch_min)
    dedupe_book: Dict[str, int] = field(default_factory=dict)

    # caches
    prior_extremes_cache: Dict[str, float] = field(default_factory=dict)
    last_extremes_refresh: float = 0.0
    htf_bias_cache: Dict[str, float] = field(default_factory=dict)  # {'bias': +1/-1}
    last_htf_refresh: float = 0.0

    # order error circuit
    consecutive_order_errors: int = 0

    # equity cache
    last_balance: float = 0.0
    last_balance_ts: float = 0.0

    # cold backfill once at boot
    did_cold_backfill: bool = False

    # health/safe-mode
    safe_mode: bool = False
    safe_reason: str = ""
    uds_listenkey_ok: bool = False
    ws_reconnects_market: int = 0
    ws_reconnects_uds: int = 0

    # drift tracking
    drift_samples_ms: deque = field(default_factory=lambda: deque(maxlen=9))
    drift_bad_streak: int = 0

    # metrics
    last_metrics_flush_ts: float = 0.0
    # ws freshness grace (e.g., right after cold backfill)
    ws_fresh_grace_loops: int = 0
    # watchdog state to avoid log thrash
    watchdog_stale: bool = False
    # prices cache for exposure checks
    prices_cache: Dict[str, float] = field(default_factory=dict)
    prices_cache_ts: float = 0.0
    # markPrices cache for exposure checks
    mark_prices_cache: Dict[str, float] = field(default_factory=dict)
    mark_prices_cache_ts: float = 0.0
    # REST PnL polling
    last_pnl_poll_ts: float = 0.0
    # blocked reasons aggregation (per-minute)
    blocked_reason_counts: Dict[str, int] = field(default_factory=dict)
    blocked_counts_minute: int = -1
    # exec klines REST fallback cache
    exec_kl_cache: List[List[float]] = field(default_factory=list)
    exec_kl_cache_ts: float = 0.0
    last_rest_klines_ts: float = 0.0


# ============================== WebSockets ==============================

class WSManager:
    def __init__(self, cfg: Config, st: RunState, ex: BinanceUM):
        self.cfg, self.st, self.ex = cfg, st, ex
        self.ws_mkt: Optional[Any] = None
        self.ws_uds: Optional[Any] = None
        self.listen_key: Optional[str] = None
        self._keepalive_thread: Optional[threading.Thread] = None
        self._stop = False
        self._bk_mkt = cfg.ws_backoff_min
        self._bk_uds = cfg.ws_backoff_min

    def start(self):
        # Auto-disable market WS in REST-only mode
        if self.cfg.rest_only_mode:
            print("[WS] REST-only mode: skipping market WS")
        elif WEBSOCKET_AVAILABLE and self.cfg.ws_market_enable:
            threading.Thread(target=self._run_market_ws, daemon=True).start()
        self._init_uds()

    def stop(self):
        self._stop = True
        try:
            if self.ws_mkt:
                self.ws_mkt.close()
        except Exception as e:
            print(f"[WS] stop market close err: {e}")
        try:
            if self.ws_uds:
                self.ws_uds.close()
        except Exception as e:
            print(f"[UDS] stop close err: {e}")

    def _run_market_ws(self):
        def make_url():
            streams = [f"{self.cfg.symbol.lower()}@kline_1m", f"{self.cfg.symbol.lower()}@kline_5m"]
            if self.cfg.ws_book_ticker_enable:
                streams.append(f"{self.cfg.symbol.lower()}@bookTicker")
            return f"{self.cfg.ws_base}/stream?streams={'/'.join(streams)}"

        def on_open(ws):
            print("[WS] Market opened")
            self._bk_mkt = self.cfg.ws_backoff_min

        def on_message(ws, msg):
            try:
                j = json.loads(msg)
                data = j.get("data")
                if not data:
                    return
                et = data.get("e")
                if et == "kline":
                    kl = data.get("k", {})
                    # only append CLOSED bars; de-dupe by open time
                    if kl.get("x") is True and data.get("s", "").lower() == self.cfg.symbol.lower():
                        k = [
                            int(kl["t"]),
                            float(kl["o"]),
                            float(kl["h"]),
                            float(kl["l"]),
                            float(kl["c"]),
                            float(kl["v"]),
                        ]
                        stype = j.get("stream", "")
                        with self.st.buf_lock:
                            if stype.endswith("kline_1m"):
                                if (not self.st.kl_1m) or (self.st.kl_1m[-1][0] != k[0]):
                                    self.st.kl_1m.append(k)
                            elif stype.endswith("kline_5m"):
                                if (not self.st.kl_5m) or (self.st.kl_5m[-1][0] != k[0]):
                                    self.st.kl_5m.append(k)
                            self.st.last_market_ws_ts = now_ms()
                elif et == "bookTicker":
                    if data.get("s", "").lower() == self.cfg.symbol.lower():
                        b_raw = data.get("b")
                        a_raw = data.get("a")
                        try:
                            b_val = float(b_raw)
                            a_val = float(a_raw)
                            if math.isfinite(b_val) and math.isfinite(a_val):
                                with self.st.buf_lock:
                                    self.st.last_bid = b_val
                                    self.st.last_ask = a_val
                                    self.st.last_market_ws_ts = now_ms()
                        except Exception as e:
                            # keep prior bid/ask on parse issues
                            print(f"[WS] bookTicker parse: {e}")
            except Exception as e:
                print(f"[WS] on_message err: {e}")

        def on_error(ws, err):
            print(f"[WS] Market error: {err}")

        def on_close(ws, *args):
            print("[WS] Market closed")

        while not self._stop:
            try:
                self.ws_mkt = websocket.WebSocketApp(
                    make_url(), on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close
                )
                self.ws_mkt.run_forever(ping_interval=20, ping_timeout=10)
                # closed or returned; backoff and retry
                self.st.ws_reconnects_market += 1
                metrics_emit(self.cfg, self.st, {
                    "t": "ws_market_reconnect",
                    "ts": now_ms(),
                    "count": self.st.ws_reconnects_market
                })
            except Exception as e:
                print(f"[WS] Market exception: {e}")
                self.st.ws_reconnects_market += 1
            # backoff with jitter
            if self._stop:
                break
            delay = min(self._bk_mkt, self.cfg.ws_backoff_max)
            time.sleep(delay + random.uniform(0, delay * 0.2))
            self._bk_mkt = min(self.cfg.ws_backoff_max, self._bk_mkt * self.cfg.ws_backoff_mult)

    # ---- UDS
    def _init_uds(self):
        # Create listenKey
        try:
            j = self.ex._request("POST", "/fapi/v1/listenKey", retry_hint="listenKey")
            self.listen_key = j["listenKey"]
            print("[UDS] listenKey acquired")
            self.st.uds_listenkey_ok = True
        except requests.exceptions.RequestException as e:
            print(f"[UDS] listenKey failed: {e}")
            self.st.uds_listenkey_ok = False
            return

        # Keepalive thread with auto-refresh
        def keepalive():
            while not self._stop:
                try:
                    self.ex._request(
                        "PUT", "/fapi/v1/listenKey", {"listenKey": self.listen_key}, retry_hint="listenKey"
                    )
                    self.st.uds_listenkey_ok = True
                except requests.exceptions.RequestException as e:
                    self.st.uds_listenkey_ok = False
                    if not self.cfg.uds_auto_refresh:
                        print(f"[UDS] keepalive failed: {e}")
                    else:
                        print(f"[UDS] keepalive failed; refreshing listenKey: {e}")
                        # bounded retries to obtain a fresh listenKey
                        ok = False
                        for i in range(3):
                            try:
                                j = self.ex._request("POST", "/fapi/v1/listenKey", retry_hint="listenKey")
                                self.listen_key = j["listenKey"]
                                self.st.uds_listenkey_ok = True
                                ok = True
                                try:
                                    if self.ws_uds:
                                        self.ws_uds.close()
                                except Exception:
                                    pass
                                break
                            except Exception as e2:
                                print(f"[UDS] refresh failed (attempt {i+1}/3): {e2}")
                                time.sleep(0.5 * (i + 1))
                        if not ok:
                            self.st.uds_listenkey_ok = False
                for _ in range(25 * 60):
                    if self._stop:
                        break
                    time.sleep(1)

        self._keepalive_thread = threading.Thread(target=keepalive, daemon=True)
        self._keepalive_thread.start()

        if WEBSOCKET_AVAILABLE:
            def on_open(ws):
                print("[UDS] opened")
                self._bk_uds = self.cfg.ws_backoff_min

            def on_message(ws, msg):
                try:
                    j = json.loads(msg)
                    evt = j.get("e")
                    if evt == "ORDER_TRADE_UPDATE":
                        o = j.get("o", {})
                        # realized PnL is tracked via ACCOUNT_UPDATE cr-deltas to avoid double counting

                        # --- Exit hygiene on fills ---
                        try:
                            status = str(o.get("X", "")).upper()          # order status
                            # order type from 'o' (type); keep uppercase fallback if ever present
                            otype  = str(o.get("o", o.get("OT", ""))).upper()
                            if status == "FILLED":
                                if otype == "TAKE_PROFIT_MARKET":
                                    # TP filled -> only cancel protective stop if flat
                                    try:
                                        _net, gross = self.ex.position_net_and_gross(self.cfg.symbol)
                                        if gross <= EPS:
                                            cancel_all_protective_stops(self.ex, self.cfg)
                                            # clear trailing cache (guard with lock)
                                            with self.st.buf_lock:
                                                self.st.last_trailing_client_id = None
                                                self.st.last_trailing_sl = None
                                                self.st.last_trailing_side = None
                                            print_json({"evt": "exit_hygiene", "action": "tp_filled_cleanup"})
                                    except Exception as e2:
                                        print(f"[UDS] tp_filled poschk: {e2}")
                                elif otype == "STOP_MARKET" and bool(o.get("cp", o.get("closePosition", False))):
                                    # Protective stop filled -> cancel remaining TPs
                                    cancel_all_reduce_only_tps(self.ex, self.cfg)
                                    print_json({"evt": "exit_hygiene", "action": "stop_filled_cleanup"})
                        except Exception as e:
                            print(f"[UDS] exit hygiene parse: {e}")

                    elif evt == "ACCOUNT_UPDATE":
                        # Track cumulative realized "cr" deltas per symbol
                        a = j.get("a", {})
                        poss = a.get("P", [])
                        for p in poss:
                            try:
                                s = str(p.get("s", "")).upper()
                                cr = float(p.get("cr", 0.0))
                                # If we don't have a baseline for this symbol, set it and skip delta to avoid
                                # a first-update double count after restarts.
                                if s not in self.st.account_cr:
                                    self.st.account_cr[s] = cr
                                    continue
                                prev = self.st.account_cr.get(s, 0.0)
                                delta = cr - prev
                                if math.isfinite(delta) and abs(delta) > 0.0:
                                    self.st.realized_today += delta
                                    if delta < 0:
                                        ms = self.st.last_server_ms if self.st.last_server_ms is not None else now_ms()
                                        self.st.last_loss_server_ms = int(ms)
                                    self.st.account_cr[s] = cr
                                    self.st.last_balance_ts = 0.0  # refresh balance cache
                                    print_json({"evt": "uds_pnl_acc", "sym": s, "delta": delta, "cum_today": self.st.realized_today})
                            except Exception as e:
                                print(f"[UDS] ACCOUNT_UPDATE parse: {e}")
                except Exception as e:
                    print(f"[UDS] on_message err: {e}")

            def on_error(ws, err):
                print(f"[UDS] error: {err}")

            def on_close(ws, *args):
                print("[UDS] closed (will reconnect)")

            def runner():
                while not self._stop:
                    try:
                        url = f"{self.cfg.ws_base}/ws/{self.listen_key}"
                        self.ws_uds = websocket.WebSocketApp(
                            url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close
                        )
                        self.ws_uds.run_forever(ping_interval=20, ping_timeout=10)
                        self.st.ws_reconnects_uds += 1
                        metrics_emit(self.cfg, self.st, {
                            "t": "ws_uds_reconnect",
                            "ts": now_ms(),
                            "count": self.st.ws_reconnects_uds
                        })
                    except Exception as e:
                        print(f"[UDS] exception: {e}")
                        self.st.ws_reconnects_uds += 1
                    if self._stop:
                        break
                    if self.cfg.uds_auto_refresh:
                        try:
                            j = self.ex._request("POST", "/fapi/v1/listenKey", retry_hint="listenKey")
                            self.listen_key = j["listenKey"]
                            self.st.uds_listenkey_ok = True
                            print("[UDS] refreshed listenKey post-close")
                        except Exception as e2:
                            self.st.uds_listenkey_ok = False
                            print(f"[UDS] post-close refresh failed: {e2}")
                    delay = min(self._bk_uds, self.cfg.ws_backoff_max)
                    time.sleep(delay + random.uniform(0, delay * 0.2))
                    self._bk_uds = min(self.cfg.ws_backoff_max, self._bk_uds * self.cfg.ws_backoff_mult)

            threading.Thread(target=runner, daemon=True).start()
        else:
            print("[UDS] websocket-client not installed; PnL updates only via REST when applicable.")


# ============================== Liquidity Pools & Detection ==============================

def _utc_minutes(ts_ms: int) -> int:
    d = dt.datetime.utcfromtimestamp(ts_ms / 1000.0)
    return d.hour * 60 + d.minute


def detect_eq_levels(
    highs: List[float], lows: List[float], tick: float, lookback: int, max_spread_ticks: int, min_hits: int
) -> Tuple[List[float], List[float]]:
    """
    Strict pivot detection excludes the current bar from the comparison window.
    Clustering tolerance uses symmetric BPS equivalent of the tick-spread near each pivot.
    """
    eh: List[float] = []
    el: List[float] = []
    start = max(0, len(highs) - lookback)

    # strict pivot highs: higher than neighbors (exclude self)
    piv: List[float] = []
    for i in range(start + 2, len(highs) - 2):
        left = highs[i - 2:i]
        right = highs[i + 1:i + 3]
        if left and right and highs[i] > max(left + right):
            piv.append(highs[i])

    # cluster highs using BPS-equivalent tolerance
    piv.sort()
    if piv:
        cluster = [piv[0]]
        for p in piv[1:]:
            # convert tick-spread to local BPS at price p
            tol_bps = (max_spread_ticks * max(EPS, tick)) / max(EPS, p) * 10000.0
            if within_bps(p, cluster[-1], tol_bps):
                cluster.append(p)
            else:
                if len(cluster) >= min_hits:
                    eh.append(sum(cluster) / len(cluster))
                cluster = [p]
        if len(cluster) >= min_hits:
            eh.append(sum(cluster) / len(cluster))

    # strict pivot lows: lower than neighbors (exclude self)
    piv = []
    for i in range(start + 2, len(lows) - 2):
        left = lows[i - 2:i]
        right = lows[i + 1:i + 3]
        if left and right and lows[i] < min(left + right):
            piv.append(lows[i])

    piv.sort()
    if piv:
        cluster = [piv[0]]
        for p in piv[1:]:
            tol_bps = (max_spread_ticks * max(EPS, tick)) / max(EPS, p) * 10000.0
            if within_bps(p, cluster[-1], tol_bps):
                cluster.append(p)
            else:
                if len(cluster) >= min_hits:
                    el.append(sum(cluster) / len(cluster))
                cluster = [p]
        if len(cluster) >= min_hits:
            el.append(sum(cluster) / len(cluster))

    return eh, el


def _cluster_levels_bps(levels: List[float], tol_bps: float) -> List[float]:
    lv = sorted([x for x in levels if x is not None])
    if not lv:
        return []
    out: List[List[float]] = [[lv[0]]]
    for p in lv[1:]:
        prev = out[-1][-1]
        if within_bps(p, prev, tol_bps):
            out[-1].append(p)
        else:
            out.append([p])
    reps: List[float] = []
    for g in out:
        m = sum(g) / len(g)
        reps.append(min(g, key=lambda v: abs(v - m)))
    return reps


def session_extremes_from_exec(
    exec_kl: List[List[float]], start_hm: str, end_hm: str, tz_name: str = "UTC"
) -> Optional[Dict[str, float]]:
    """
    Compute highs/lows over the last 24h restricted to the time-of-day window (handles midnight wrap).
    """
    if not exec_kl:
        return None
    end_ms = int(exec_kl[-1][0])
    cutoff = end_ms - 24 * 60 * 60 * 1000
    cand = [b for b in exec_kl if int(b[0]) >= cutoff]
    if not cand:
        return None

    def to_min(hm: str) -> int:
        h, m = map(int, hm.split(":"))
        return h * 60 + m

    smin, emin = to_min(start_hm), to_min(end_hm)

    def in_span(ms: int) -> bool:
        z = _get_zone(tz_name)
        if z is not None:
            d = dt.datetime.fromtimestamp(ms / 1000.0, tz=z)
        else:
            d = dt.datetime.utcfromtimestamp(ms / 1000.0)
        mins = d.hour * 60 + d.minute
        if smin <= emin:
            return smin <= mins < emin
        else:
            return (mins >= smin) or (mins < emin)

    s = [b for b in cand if in_span(int(b[0]))]
    if not s:
        return None
    return {"high": max(x[2] for x in s), "low": min(x[3] for x in s)}


def opening_range_today(exec_kl: List[List[float]], minutes: int = 60, tz_name: str = "UTC") -> Optional[Dict[str, float]]:
    # Compute ORB window from midnight (session tz) to midnight+minutes.
    if not exec_kl:
        return None
    last_ts = int(exec_kl[-1][0])
    z = _get_zone(tz_name)
    if z is not None:
        d_local = dt.datetime.fromtimestamp(last_ts / 1000.0, tz=z)
        d0_local = dt.datetime(d_local.year, d_local.month, d_local.day, 0, 0, 0, tzinfo=z)
        start_ms = int(d0_local.timestamp() * 1000)
    else:
        d = dt.datetime.utcfromtimestamp(last_ts / 1000.0)
        d0 = dt.datetime(d.year, d.month, d.day, 0, 0, 0)
        start_ms = int(d0.timestamp() * 1000)
    cutoff = start_ms + int(minutes) * 60 * 1000
    window = [b for b in exec_kl if start_ms <= int(b[0]) < cutoff]
    if not window:
        return None
    return {"high": max(x[2] for x in window), "low": min(x[3] for x in window)}


def opening_range_session_anchor(
    exec_kl: List[List[float]], start_hm: str, minutes: int = 60, tz_name: str = "UTC"
) -> Optional[Dict[str, float]]:
    """Compute opening range anchored at the most recent session start time.

    Chooses the most recent occurrence of start_hm at or before the last bar timestamp
    (in session timezone), and returns high/low over [start, start+minutes).
    """
    if not exec_kl:
        return None
    last_ts = int(exec_kl[-1][0])
    z = _get_zone(tz_name)
    # produce localized datetime for last bar
    if z is not None:
        d_local = dt.datetime.fromtimestamp(last_ts / 1000.0, tz=z)
    else:
        d_local = dt.datetime.utcfromtimestamp(last_ts / 1000.0)
    try:
        sh, sm = map(int, str(start_hm).split(":"))
    except Exception:
        sh, sm = 0, 0
    # anchor candidate for the same local day
    anchor = dt.datetime(d_local.year, d_local.month, d_local.day, sh, sm, 0, tzinfo=d_local.tzinfo)
    # if last bar time is before session start, use previous day anchor
    if d_local < anchor:
        anchor = anchor - dt.timedelta(days=1)
    start_ms = int(anchor.timestamp() * 1000)
    end_ms = start_ms + int(minutes) * 60 * 1000
    window = [b for b in exec_kl if start_ms <= int(b[0]) < end_ms]
    if not window:
        return None
    return {"high": max(x[2] for x in window), "low": min(x[3] for x in window)}


def rolling_range_edges(
    exec_kl: List[List[float]], window: int, min_touches: int, atr_frac_max: float
) -> Optional[Dict[str, float]]:
    if len(exec_kl) < max(window, 25):
        return None
    seg = exec_kl[-window:]
    hi, lo = max(x[2] for x in seg), min(x[3] for x in seg)
    df = df_from_klines(exec_kl)
    a = atr(df, 20) or EPS
    if (hi - lo) <= atr_frac_max * 2.0 * a:
        closes = [x[4] for x in seg]
        near_hi = sum(1 for c in closes if abs(c - hi) <= 0.15 * a)
        near_lo = sum(1 for c in closes if abs(c - lo) <= 0.15 * a)
        if near_hi >= min_touches or near_lo >= min_touches:
            return {"high": hi, "low": lo}
    return None


def big_figure_levels(last_price: float, inc: float = 100.0, include_quarters: bool = False) -> List[float]:
    if last_price <= 0 or inc <= 0:
        return []
    base = math.floor(last_price / inc) * inc
    levels = [base, base + inc]
    if include_quarters:
        for q in (0.25, 0.5, 0.75):
            levels += [base + inc * q, base + inc * (1 + q)]
    return sorted(set(levels))


def session_vwap_bands(
    exec_df: pd.DataFrame, start_hm: str, end_hm: str, k: float = 1.0, tz_name: str = "UTC"
) -> Optional[Dict[str, float]]:
    """
    Vectorized VWAP bands for a session window using typical price over the last 24h.
    """
    if exec_df is None or exec_df.empty:
        return None
    t_last = int(pd.to_datetime(exec_df["time"], unit="ms", utc=True).iloc[-1].value // 10**6)
    cutoff = t_last - 24 * 60 * 60 * 1000
    sdf = exec_df[exec_df["time"] >= cutoff]
    if sdf.empty:
        return None

    def to_min(hm: str) -> int:
        h, m = map(int, hm.split(":"))
        return h * 60 + m

    smin, emin = to_min(start_hm), to_min(end_hm)
    tms_utc = pd.to_datetime(sdf["time"], unit="ms", utc=True)
    z = _get_zone(tz_name)
    if z is not None:
        tms = tms_utc.dt.tz_convert(z)
    else:
        tms = tms_utc
    mins = (tms.dt.hour * 60) + tms.dt.minute
    if smin <= emin:
        mask = (mins >= smin) & (mins < emin)
    else:
        mask = (mins >= smin) | (mins < emin)

    s = sdf[mask]
    if s.empty:
        return None
    tp = (s["high"] + s["low"] + s["close"]) / 3.0
    v = s["volume"].astype(float).clip(lower=0.0)
    wsum = (tp * v).sum()
    vsum = v.sum() + EPS
    vwap = float(wsum / vsum)
    dev = (((tp - vwap) ** 2) * v).sum() / vsum
    std = math.sqrt(max(0.0, float(dev)))
    return {"upper": vwap + k * std, "lower": vwap - k * std, "mid": vwap}


# ============================== FVGs & Sweeps ==============================

def fvg_quality(df: pd.DataFrame, gap_low: float, gap_high: float, cfg: Optional[Config] = None) -> float:
    if df is None or df.empty:
        return 0.0
    a = atr(df, 14) or EPS
    gap = max(0.0, gap_high - gap_low) / a
    vz = vol_zscore(df, 20)
    # If cfg is provided, use configured balance and normalization; otherwise default to previous constants
    if cfg is not None:
        gap_w = float(cfg.fvgq_gap_w)
        vol_w = float(cfg.fvgq_vol_w)
        gap_norm = float(cfg.fvgq_gap_norm)
        vol_shift = float(cfg.fvgq_vol_shift)
        vol_scale = float(cfg.fvgq_vol_scale)
    else:
        gap_w, vol_w, gap_norm, vol_shift, vol_scale = 0.60, 0.40, 2.0, 2.0, 4.0
    score = gap_w * clamp(gap / max(EPS, gap_norm), 0.0, 1.0) + vol_w * clamp((vz + vol_shift) / max(EPS, vol_scale), 0.0, 1.0)
    return clamp(score, 0.0, 1.0)


def fvg_filled(
    df: pd.DataFrame,
    low: float,
    high: float,
    rule: str = "touch",
    require_body_close: bool = False,
    min_penetration_atr: float = 0.0,
    min_consecutive: int = 1,
) -> bool:
    if df is None or df.empty or not (math.isfinite(low) and math.isfinite(high)):
        return False
    if low > high:
        low, high = high, low
    mid = (low + high) / 2.0
    a = atr(df, 14) or EPS
    need_depth = max(0.0, float(min_penetration_atr)) * a

    consec = 0
    for _, row in df.iterrows():
        opn = float(row["open"])
        C = float(row["close"])
        H = float(row["high"])
        L = float(row["low"])
        body_low, body_high = (min(opn, C), max(opn, C))
        test_low, test_high = (body_low, body_high) if require_body_close else (L, H)

        overlaps = (test_high >= low) and (test_low <= high)
        depth = 0.0
        if overlaps:
            depth = min(test_high, high) - max(test_low, low)

        if rule == "touch":
            ok = overlaps and (depth >= need_depth)
        elif rule == "mid":
            ok = (test_low <= mid <= test_high) and (depth >= need_depth)
        elif rule == "full":
            ok = (test_low <= low) and (test_high >= high)
        else:
            ok = overlaps and (depth >= need_depth)

        if ok:
            consec += 1
            if consec >= max(1, int(min_consecutive)):
                return True
        else:
            consec = 0
    return False


def detect_fvgs(df: pd.DataFrame, cfg: Config, tick: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if df is None or len(df) < 5:
        return out
    H, L = df["high"].values, df["low"].values
    for i in range(2, len(df)):
        # bullish: L[i] >= H[i-2] + threshold
        if cfg.fvg_gap_use_bps:
            thr_bull = float(H[i - 2]) * (float(cfg.fvg_gap_min_bps) / 10000.0)
            # Symmetric anchor for bearish case: use L[i-2]
            thr_bear = float(L[i - 2]) * (float(cfg.fvg_gap_min_bps) / 10000.0)
        else:
            thr_bull = float(cfg.fvg_min_ticks) * max(tick, EPS)
            thr_bear = float(cfg.fvg_min_ticks) * max(tick, EPS)
        # Relaxation in BPS (converted to absolute)
        relax_bps = max(0.0, float(getattr(cfg, "fvg_gap_relax_bps", 0.0)))
        bull_relax_abs = float(H[i - 2]) * (relax_bps / 10000.0)
        # Use symmetric anchor for bearish relax as well
        bear_relax_abs = float(L[i - 2]) * (relax_bps / 10000.0)
        thr_bull_eff = max(0.0, thr_bull - bull_relax_abs)
        thr_bear_eff = max(0.0, thr_bear - bear_relax_abs)
        if L[i] >= H[i - 2] + thr_bull_eff:
            out.append({"dir": "bullish", "gap_low": H[i - 2], "gap_high": L[i], "bar_index": i})
        # bearish: L[i-2] >= H[i] + threshold
        if L[i - 2] >= H[i] + thr_bear_eff:
            out.append({"dir": "bearish", "gap_low": H[i], "gap_high": L[i - 2], "bar_index": i})
    return out


def recent_fvg_edges_as_pools(exec_df: pd.DataFrame, max_age_bars: int, tick: float, cfg: Config) -> List[float]:
    out: List[float] = []
    fvgs = detect_fvgs(exec_df, cfg, tick)
    if not fvgs:
        return out
    n_bars = len(exec_df) - 1
    for x in fvgs:
        age = n_bars - int(x["bar_index"])
        if age > max_age_bars:
            continue
        tail_df = exec_df.iloc[int(x["bar_index"]) + 1 :]
        if not fvg_filled(
            tail_df,
            float(x["gap_low"]),
            float(x["gap_high"]),
            rule=cfg.fvg_fill_rule,
            require_body_close=cfg.fvg_fill_require_body,
            min_penetration_atr=cfg.fvg_fill_min_penetration_atr,
            min_consecutive=cfg.fvg_fill_min_consecutive,
        ):
            out += [float(x["gap_low"]), float(x["gap_high"])]
    return out


# ---- Structure helpers: containment, BOS, CHOCH, HTF consensus (lean) ----

def containment_15m_score(
    exec_df: pd.DataFrame, side: str, fvg_gap: Tuple[float, float], tf: str
) -> float:
    """
    Use true last 15 minutes via timestamps.
    If not enough bars land in the window, fallback uses a tf-aware tail size.
    """
    if exec_df is None or exec_df.empty:
        return 0.0
    end_ms = int(exec_df["time"].iloc[-1])
    start_ms = end_ms - 15 * 60 * 1000
    w = exec_df[exec_df["time"] >= start_ms]
    if w.empty:
        # tf-aware fallback: use ~half a TF worth of bars, min 5
        fallback_n = 5
        try:
            if isinstance(tf, str) and tf.endswith("m"):
                mins = int(tf[:-1])
                fallback_n = max(5, mins // 2 or 5)
        except Exception:
            pass
        w = exec_df.tail(fallback_n)
    rng_hi = float(w["high"].max())
    rng_lo = float(w["low"].min())
    low, high = min(fvg_gap), max(fvg_gap)
    inside = (low >= rng_lo) and (high <= rng_hi)
    return 1.0 if inside else 0.0


def last_pivot_high_low(df: pd.DataFrame, window: int = 5) -> Tuple[Optional[float], Optional[float]]:
    if df is None or len(df) < 2 * window + 1:
        return None, None
    H, L = df["high"].values, df["low"].values
    ph = None
    pl = None
    # Walk newest → oldest to get the most recent true pivots (exclude self in comparisons)
    start = len(df) - window - 1
    for j in range(start, window - 1, -1):
        if H[j] > max(H[j - window:j]) and H[j] >= max(H[j + 1:j + window + 1]):
            ph = float(H[j])
            break
    for j in range(start, window - 1, -1):
        if L[j] < min(L[j - window:j]) and L[j] <= min(L[j + 1:j + window + 1]):
            pl = float(L[j])
            break
    return ph, pl


def bos_choch_scores(exec_df: pd.DataFrame, side: str) -> Tuple[float, float]:
    ph, pl = last_pivot_high_low(exec_df, window=4)
    if ph is None or pl is None:
        return 0.0, 0.0
    C = float(exec_df["close"].iloc[-1])
    a = atr(exec_df, 14) or 0.0
    # crude fallback for tick estimation from close diffs with explicit typing
    _min_step_obj = exec_df["close"].diff().abs().min()
    try:
        _min_step_val = float(_min_step_obj)  # type: ignore[arg-type]
    except Exception:
        _min_step_val = 0.0
    if not (math.isfinite(_min_step_val) and _min_step_val > 0.0):
        _min_step_val = 0.0
    tick = float(max(EPS, _min_step_val))
    thr = max(tick, 0.05 * a)
    bos = 0.0
    choch = 0.0
    if side == "short":
        # Bearish BOS: close breaks prior low by threshold
        if C < (pl - thr):
            bos = 1.0
        # CHOCH: recent highs making a lower high by threshold
        highs = exec_df["high"].tail(6).values
        if len(highs) >= 3 and (highs[-3] - highs[-1]) >= thr:
            choch = 1.0
    else:
        # Bullish BOS: close breaks prior high by threshold
        if C > (ph + thr):
            bos = 1.0
        # CHOCH: recent lows making a higher low by threshold
        lows = exec_df["low"].tail(6).values
        if len(lows) >= 3 and (lows[-1] - lows[-3]) >= thr:
            choch = 1.0
    return bos, choch


def cached_htf_bias(ex: BinanceUM, st: RunState, cfg: Config, symbol: str) -> float:
    """
    HTF bias with hysteresis around EMA(20) to reduce flip-flops.
    """
    now_ = time.time()
    # Hard floor on refresh cadence to avoid thrash from misconfiguration
    if (now_ - st.last_htf_refresh) < max(300, cfg.htf_refresh_secs) and "bias" in st.htf_bias_cache:
        return float(st.htf_bias_cache["bias"])
    try:
        h1 = df_from_klines(ex.fetch_klines(symbol, "1h", limit=60))
        h4 = df_from_klines(ex.fetch_klines(symbol, "4h", limit=60))

        def ema(series, n):
            return series.ewm(span=n, adjust=False).mean()

        def sign_with_hysteresis(df):
            if df.empty:
                return 0.0
            e20 = ema(df["close"], 20)
            px = float(df["close"].iloc[-1])
            ema20 = float(e20.iloc[-1])
            # require |px-ema| > hysteresis to set sign
            if within_bps(px, ema20, cfg.htf_bias_hysteresis_bps):
                return 0.0
            return 1.0 if px > ema20 else -1.0

        b1 = sign_with_hysteresis(h1)
        b4 = sign_with_hysteresis(h4)
        bias = 1.0 if (b1 + b4) > 0 else (-1.0 if (b1 + b4) < 0 else 0.0)
        st.htf_bias_cache = {"bias": bias}
        st.last_htf_refresh = now_
        return bias
    except requests.exceptions.RequestException as e:
        print(f"[htf_bias] {e}")
        return 0.0


# ============================== Pool builder & scoring ==============================

def build_pools(
    cfg: Config, ex_kl: List[List[float]], ex_df: pd.DataFrame, tick: float, last_px: float
) -> Tuple[List[Tuple[float, float, str]], List[Tuple[float, float, str]]]:
    highs = [x[2] for x in ex_kl]
    lows = [x[3] for x in ex_kl]
    up_levels: List[Tuple[float, float, str]] = []
    dn_levels: List[Tuple[float, float, str]] = []

    # EQH/EQL
    if cfg.enable_pools_eqh_eql and highs and lows:
        eh, el = detect_eq_levels(highs, lows, tick, cfg.eq_lookback, cfg.eq_spread_ticks, cfg.eq_min_hits)
        up_levels += [(p, cfg.w_eq, "EQH") for p in eh]
        dn_levels += [(p, cfg.w_eq, "EQL") for p in el]

    # Sessions (Asia/London/NY)
    if cfg.enable_pools_sessions:
        asia = session_extremes_from_exec(ex_kl, cfg.asia_session_start, cfg.asia_session_end, cfg.session_timezone)
        if asia:
            up_levels.append((asia["high"], cfg.w_sess, "ASIA_H"))
            dn_levels.append((asia["low"], cfg.w_sess, "ASIA_L"))
        if cfg.use_session_london:
            lon = session_extremes_from_exec(ex_kl, cfg.session_london_utc[0], cfg.session_london_utc[1], cfg.session_timezone)
            if lon:
                up_levels.append((lon["high"], cfg.w_sess, "LON_H"))
                dn_levels.append((lon["low"], cfg.w_sess, "LON_L"))
        if cfg.use_session_newyork:
            ny = session_extremes_from_exec(ex_kl, cfg.session_newyork_utc[0], cfg.session_newyork_utc[1], cfg.session_timezone)
            if ny:
                up_levels.append((ny["high"], cfg.w_sess, "NY_H"))
                dn_levels.append((ny["low"], cfg.w_sess, "NY_L"))

    # Opening range (midnight or session anchor)
    if cfg.use_opening_range:
        orb = None
        try:
            anchor = (cfg.opening_range_anchor or "midnight").lower()
        except Exception:
            anchor = "midnight"
        if anchor == "session":
            sess = (cfg.opening_range_session or "newyork").lower()
            if sess == "asia":
                start_hm = cfg.asia_session_start
            elif sess == "london":
                start_hm = cfg.session_london_utc[0]
            else:
                start_hm = cfg.session_newyork_utc[0]
            orb = opening_range_session_anchor(ex_kl, start_hm, cfg.opening_range_minutes, cfg.session_timezone)
        else:
            orb = opening_range_today(ex_kl, cfg.opening_range_minutes, cfg.session_timezone)
        if orb:
            up_levels.append((orb["high"], cfg.w_orb, "ORB_H"))
            dn_levels.append((orb["low"], cfg.w_orb, "ORB_L"))

    # Rolling range
    if cfg.use_rolling_range:
        rr = rolling_range_edges(ex_kl, cfg.rolling_range_window, cfg.rolling_range_min_touches, cfg.rolling_range_atr_frac_max)
        if rr:
            up_levels.append((rr["high"], cfg.w_rng, "RNG_H"))
            dn_levels.append((rr["low"], cfg.w_rng, "RNG_L"))

    # FVG edges as light pools (strict side split)
    if cfg.use_fvg_edges_as_pools and ex_df is not None and not ex_df.empty:
        edges = recent_fvg_edges_as_pools(ex_df, cfg.fvg_pool_age_bars, tick, cfg)
        for e in edges:
            if e > last_px:
                up_levels.append((e, cfg.w_fvg, "FVG_EDGE"))
            elif e < last_px:
                dn_levels.append((e, cfg.w_fvg, "FVG_EDGE"))

    # Big figures (strict side split)
    if cfg.use_big_figures and math.isfinite(last_px):
        figs = big_figure_levels(last_px, cfg.figure_increment, cfg.include_quarter_figs)
        for f in figs:
            if f > last_px:
                up_levels.append((f, cfg.w_fig, "FIG"))
            elif f < last_px:
                dn_levels.append((f, cfg.w_fig, "FIG"))

    # Cluster & top-K per side using symmetric BPS tolerance
    up_prices = _cluster_levels_bps([x[0] for x in up_levels], cfg.pool_cluster_bps)[: cfg.pool_topk_per_side]
    dn_prices = _cluster_levels_bps([x[0] for x in dn_levels], cfg.pool_cluster_bps)[: cfg.pool_topk_per_side]
    up_out: List[Tuple[float, float, str]] = []
    dn_out: List[Tuple[float, float, str]] = []
    for p in up_prices:
        cand = [(bps_distance(t[0], p), -t[1], t) for t in up_levels if within_bps(t[0], p, cfg.pool_cluster_bps)]
        if cand:
            up_out.append(min(cand, key=lambda z: (z[0], z[1]))[2])
    for p in dn_prices:
        cand = [(bps_distance(t[0], p), -t[1], t) for t in dn_levels if within_bps(t[0], p, cfg.pool_cluster_bps)]
        if cand:
            dn_out.append(min(cand, key=lambda z: (z[0], z[1]))[2])
    return up_out, dn_out


def pool_scoring_v2(
    pools: List[Tuple[float, float, str]],
    ref_price: float,
    htf_mult: float = 1.25,
    prox_win_bps: float = 30.0,
    htf_tags: Optional[Set[str]] = None,
) -> float:
    if not pools or not math.isfinite(ref_price):
        return 0.0
    s = 0.0
    # Normalize tags to uppercase for robust matching
    tags_set: Set[str] = {str(t).strip().upper() for t in (htf_tags or [])}
    for price, w, tag in pools:
        tupan = str(tag).strip().upper()
        prox = clamp(1.0 - bps_distance(price, ref_price) / max(1e-6, prox_win_bps), 0.0, 1.0)
        htf = htf_mult if (tupan in tags_set) else 1.0
        s += w * prox * htf
    return s


def htf_tags_normalized(cfg: Config) -> Set[str]:
    """Return normalized uppercase HTF tags set from config string."""
    try:
        items = str(cfg.htf_tag_string or "").split(",")
        return {str(t).strip().upper() for t in items if str(t).strip()}
    except Exception:
        return set()


# ============================== Risk & Execution ==============================

def regime_adapt(df: pd.DataFrame, cfg: Config) -> Dict[str, Any]:
    a = atr(df, 14) or EPS
    rng = (df["high"].tail(20).max() - df["low"].tail(20).min()) / (a + EPS)
    # Guard against division by zero/near-zero regimes
    rng_eff = max(EPS, rng)
    pen = cfg.sweep_min_penetration_bps * clamp(rng_eff / 6.0, 0.75, 1.25)
    cb = cfg.sweep_closeback_bps * clamp(rng_eff / 6.0, 0.75, 1.25)
    rr = cfg.rr_target * clamp(6.0 / rng_eff, 0.8, 1.2)
    return {"pen_bps": pen, "cb_bps": cb, "rr": rr}


def throttle_risk(equity: float, st: RunState, cfg: Config) -> float:
    cur_equity = st.day_start_equity + st.realized_today
    dd = max(0.0, (st.day_start_equity - cur_equity) / max(EPS, st.day_start_equity))
    rp = cfg.risk_pct
    if dd >= cfg.throttle_drawdown2:
        rp *= cfg.throttle_risk2
    elif dd >= cfg.throttle_drawdown1:
        rp *= cfg.throttle_risk1
    return rp


def cooldown_active(st: RunState, cfg: Config) -> bool:
    if st.last_loss_server_ms is None or st.last_server_ms is None:
        return False
    return ((st.last_server_ms - st.last_loss_server_ms) / 60000.0) < cfg.cooldown_minutes_after_loss


def daily_guardrails_blocked(st: RunState, cfg: Config) -> bool:
    """
    Guardrails use REALIZED PnL only.
    """
    if st.trades_today >= cfg.max_trades_per_day:
        return True
    realized_day_loss = max(0.0, -st.realized_today)
    if (realized_day_loss / max(EPS, st.day_start_equity)) >= cfg.max_daily_loss_pct:
        return True
    return False


def session_soft_bonus(sessions: Set[str], cfg: Config) -> float:
    if "off" in sessions:
        return 0.0
    b = cfg.session_active_bonus
    if len(sessions) > 1:
        b += cfg.cross_session_bonus
    return b


def balance_cached(ex: BinanceUM, st: RunState, cfg: Config) -> float:
    nowt = time.time()
    if (nowt - st.last_balance_ts) > cfg.balance_cache_secs or st.last_balance <= 0.0:
        try:
            st.last_balance = float(ex.balance())
            st.last_balance_ts = nowt
        except requests.exceptions.RequestException as e:
            print(f"[balance] {e}")
    return st.last_balance


# ---- Tuned partial TP: pool if RR>=min, else 1R fallback
def choose_tp_aligned_tuned(
    entry: float,
    stop: float,
    side: str,
    rr_target: float,
    pools_up: List[Tuple[float, float, str]],
    pools_dn: List[Tuple[float, float, str]],
    cfg: Config,
    tick: float,
) -> Tuple[float, Optional[float]]:
    stop_dist = abs(entry - stop)
    # fee/slippage budget (double-sided)
    fee_mult = max(0.0, 1.0 - (cfg.fee_bps / 10000.0) * 2.0)
    adj_rr = rr_target * fee_mult
    base_tp = entry + adj_rr * stop_dist if side == "long" else entry - adj_rr * stop_dist
    cap_tp = entry + clamp(cfg.max_rr_cap, 0.5, 10.0) * stop_dist if side == "long" else entry - clamp(cfg.max_rr_cap, 0.5, 10.0) * stop_dist

    dir_pools = [p for p, _, _ in (pools_up if side == "long" else pools_dn) if (p > entry if side == "long" else p < entry)]
    nearest = min(dir_pools, key=lambda x: abs(x - entry)) if dir_pools else None

    # Final TP only snaps to nearest if it preserves minimum RR
    if nearest is not None:
        rr_to_nearest = abs(nearest - entry) / max(EPS, stop_dist)
        if rr_to_nearest >= cfg.min_rr_to_pool:
            if side == "long":
                final_tp = min(base_tp, nearest)
            else:
                final_tp = max(base_tp, nearest)
        else:
            final_tp = base_tp
    else:
        final_tp = base_tp

    # Respect cap
    if side == "long":
        final_tp = min(final_tp, cap_tp)
    else:
        final_tp = max(final_tp, cap_tp)

    # Optional partial at pool or 1R fallback
    partial_tp: Optional[float] = None
    if nearest is not None:
        rr_to_nearest = abs(nearest - entry) / max(EPS, stop_dist)
        if rr_to_nearest >= cfg.min_rr_to_pool:
            partial_tp = nearest
    if partial_tp is None:
        oneR = entry + stop_dist if side == "long" else entry - stop_dist
        if (side == "long" and oneR < final_tp) or (side == "short" and oneR > final_tp):
            partial_tp = oneR

    # side-aware rounding
    s = "buy" if side == "long" else "sell"
    # Round TPs. Favor toward entry for both sides for easier trigger
    favor_final = "entry"
    final_tp = side_round(final_tp, tick, s, favor=favor_final)
    if partial_tp is not None:
        favor_part = "entry"
        partial_tp = side_round(partial_tp, tick, s, favor=favor_part)
    # de-duplicate if rounded partial equals final within ~half tick
    try:
        if partial_tp is not None and abs(partial_tp - final_tp) < max(EPS, tick) / 2.0:
            partial_tp = None
    except Exception:
        pass
    return final_tp, partial_tp


def _fmt_price(p: float, tick: float) -> str:
    """Format price to string with tick-implied decimals using Decimal to avoid artifacts."""
    dec = max(0, int(decimals_for_tick(max(EPS, tick))))
    try:
        q = Decimal('1').scaleb(-dec)
        d = Decimal(str(p)).quantize(q)
        return format(d, f'.{dec}f')
    except Exception:
        return f"{round(float(p), dec):.{dec}f}"


def _fmt_qty(q: float, step: float) -> str:
    """Format quantity to string with step-implied decimals using Decimal to avoid artifacts."""
    dec = max(0, int(decimals_for_tick(max(EPS, step))))
    try:
        qq = Decimal('1').scaleb(-dec)
        d = Decimal(str(q)).quantize(qq)
        return format(d, f'.{dec}f')
    except Exception:
        return f"{round(float(q), dec):.{dec}f}"


def _unique_cid(prefix: str) -> str:
    return f"{prefix}_{int(time.time()*1000)}_{random.randint(100,999)}"


def _compute_trail_target(
    side: str,
    entry: float,
    stop: float,
    current_price: float,
    r_multiple: float,
    atr_val: float,
    cfg: Config,
) -> Tuple[Optional[float], Optional[str]]:
    """Compute desired trailing stop level.

    Returns (new_sl, skip_reason). If no update is warranted, new_sl is None.
    """
    new_sl: Optional[float] = None
    # Break-even activation when >= 1R
    if r_multiple >= 1.0:
        new_sl = max(stop, entry) if side == "long" else min(stop, entry)

    # ATR trail after activation threshold
    if r_multiple >= cfg.trail_activation_R:
        if atr_val > 0.0:
            trail_dist = cfg.trail_atr_k * atr_val
            if side == "long":
                trail = current_price - trail_dist
                new_sl = max(new_sl or stop, trail)
            else:
                trail = current_price + trail_dist
                new_sl = min(new_sl or stop, trail)
        else:
            return None, "atr_zero"

    return new_sl, None


def _trailing_improvement_ok(st: 'RunState', side: str, new_sl: float, min_tick_improve: int) -> bool:
    """Check if `new_sl` improves over last trailing SL, with tick threshold."""
    min_improve = max(1, int(min_tick_improve)) * max(EPS, st.tick)
    with st.buf_lock:
        last_sl = st.last_trailing_sl
        last_side = st.last_trailing_side
    if last_sl is None or last_side != side:
        return True
    if side == "long" and new_sl > last_sl + min_improve:
        return True
    if side == "short" and new_sl < last_sl - min_improve:
        return True
    return False


def _cancel_prev_trailing_stop(ex: 'BinanceUM', st: 'RunState', cfg: 'Config') -> None:
    """Best-effort cancel of the previously tracked trailing protective stop."""
    try:
        if st.last_trailing_client_id:
            ex.cancel_order(symbol=cfg.symbol, origClientOrderId=st.last_trailing_client_id)
        elif st.last_trailing_order_id is not None:
            ex.cancel_order(symbol=cfg.symbol, orderId=int(st.last_trailing_order_id))
    except requests.exceptions.RequestException:
        pass


def _place_stop_closeposition(
    ex: 'BinanceUM', cfg: 'Config', st: 'RunState', side: str, stop_price: float, cid: str
) -> bool:
    """Place a STOP_MARKET closePosition on exit side; return True on success."""
    exit_side = "SELL" if side == "long" else "BUY"
    try:
        ex.new_order(
            symbol=cfg.symbol,
            side=exit_side,
            type="STOP_MARKET",
            stopPrice=_fmt_price(
                side_round(stop_price, st.tick, "buy" if side == "long" else "sell", favor="protection"),
                st.tick,
            ),
            closePosition=True,
            workingType=cfg.working_type,
            newClientOrderId=cid,
        )
        return True
    except (requests.exceptions.RequestException, AssertionError, ValueError, TypeError):
        return False


def _cleanup_extra_closeposition_stops(ex: 'BinanceUM', cfg: 'Config', keep_cid: str) -> None:
    """Ensure only one closePosition STOP_MARKET remains (identified by `keep_cid`)."""
    try:
        oo = ex.open_orders(cfg.symbol)
        for o in oo:
            try:
                if str(o.get("type", "")).upper() == "STOP_MARKET" and bool(o.get("closePosition", False)):
                    if o.get("clientOrderId") != keep_cid:
                        oid = o.get("orderId")
                        if oid is not None:
                            ex.cancel_order(symbol=cfg.symbol, orderId=int(oid))
            except (requests.exceptions.RequestException, ValueError, TypeError):
                continue
    except requests.exceptions.RequestException:
        pass


@dataclass
class TrailParams:
    side: str
    entry: float
    stop: float
    current_price: float
    exec_df: pd.DataFrame


def update_trailing_sl(
    st: RunState,
    ex: BinanceUM,
    cfg: Config,
    params: TrailParams,
) -> None:
    # optional hedge-mode safe: look at gross exposure
    if cfg.position_check_before_trail:
        try:
            _net, gross = ex.position_net_and_gross(cfg.symbol)
            if gross <= EPS:
                return
        except requests.exceptions.RequestException as e:
            print(f"[trail] pos check: {e}")
            return

    r = abs(params.current_price - params.entry) / max(EPS, abs(params.entry - params.stop))
    a = atr(params.exec_df, 14) or 0.0

    new_sl, skip_reason = _compute_trail_target(params.side, params.entry, params.stop, params.current_price, r, a, cfg)
    if skip_reason:
        print_json({"evt": "trail_skip", "why": skip_reason})

    if new_sl is None:
        return

    # de-dupe by ticks (guard reads with lock)
    if not _trailing_improvement_ok(st, params.side, new_sl, cfg.trail_min_tick_improve):
        return

    # Safer replace: try place first; if rejected due to existing closePosition stop, cancel-then-place
    new_cid = _unique_cid("trail")
    placed = False
    try:
        ex.new_order(
            symbol=cfg.symbol,
            side="SELL" if params.side == "long" else "BUY",
            type="STOP_MARKET",
            stopPrice=_fmt_price(
                side_round(new_sl, st.tick, "buy" if params.side == "long" else "sell", favor="protection"),
                st.tick,
            ),
            closePosition=True,
            workingType=cfg.working_type,
            newClientOrderId=new_cid,
        )
        placed = True
        # success → cancel old (best effort)
        try:
            if st.last_trailing_client_id:
                ex.cancel_order(symbol=cfg.symbol, origClientOrderId=st.last_trailing_client_id)
            elif st.last_trailing_order_id is not None:
                ex.cancel_order(symbol=cfg.symbol, orderId=int(st.last_trailing_order_id))
        except Exception:
            pass
    except Exception as e_first:
        # Likely existing closePosition protective stop - cancel then place once
        try:
            if st.last_trailing_client_id:
                ex.cancel_order(symbol=cfg.symbol, origClientOrderId=st.last_trailing_client_id)
            elif st.last_trailing_order_id is not None:
                ex.cancel_order(symbol=cfg.symbol, orderId=int(st.last_trailing_order_id))
            ex.new_order(
                symbol=cfg.symbol,
                side="SELL" if params.side == "long" else "BUY",
                type="STOP_MARKET",
                stopPrice=_fmt_price(
                    side_round(new_sl, st.tick, "buy" if params.side == "long" else "sell", favor="protection"),
                    st.tick,
                ),
                closePosition=True,
                workingType=cfg.working_type,
                newClientOrderId=new_cid,
            )
            placed = True
        except Exception as e_second:
            print(f"[trail] replace failed (kept previous protective stop): {e_first}; {e_second}")
            return

    if placed:
        with st.buf_lock:
            st.last_trailing_sl = new_sl
            st.last_trailing_side = params.side
            st.last_trailing_client_id = new_cid
            st.last_trailing_order_id = None
        print_json({"evt": "trail_update", "side": params.side, "new_sl": round(new_sl, 8), "atr": round(a, 6)})
        # Best-effort cleanup: ensure only one closePosition STOP exists
        try:
            oo = ex.open_orders(cfg.symbol)
            for o in oo:
                try:
                    if str(o.get("type", "")).upper() == "STOP_MARKET" and bool(o.get("closePosition", False)):
                        if o.get("clientOrderId") != new_cid:
                            oid = o.get("orderId")
                            if oid is not None:
                                ex.cancel_order(symbol=cfg.symbol, orderId=int(oid))
                except Exception:
                    continue
        except Exception:
            pass

def cancel_all_reduce_only_tps(ex: BinanceUM, cfg: Config, keep_side: Optional[str] = None) -> None:
    """Cancel all reduce-only TAKE_PROFIT_MARKET orders for symbol; optionally only those with side."""
    try:
        oo = ex.open_orders(cfg.symbol)
        for o in oo:
            if str(o.get("type","")).upper() == "TAKE_PROFIT_MARKET" and bool(o.get("reduceOnly", False)):
                if keep_side and str(o.get("side","")).upper() != keep_side.upper():
                    continue
                try:
                    oid = o.get("orderId")
                    if oid is not None:
                        ex.cancel_order(symbol=cfg.symbol, orderId=int(oid))
                except requests.exceptions.RequestException as e:
                    print(f"[exit_hygiene] cancel TP {o.get('orderId')}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"[exit_hygiene] list TPs: {e}")


def cancel_all_protective_stops(ex: BinanceUM, cfg: Config, keep_close_position: bool = False) -> None:
    """Cancel STOP_MARKET closePosition protective stops for symbol.
    If keep_close_position is True, do not cancel them (noop).
    """
    try:
        oo = ex.open_orders(cfg.symbol)
        for o in oo:
            if str(o.get("type","")).upper() == "STOP_MARKET" and bool(o.get("closePosition", False)):
                if keep_close_position:
                    continue
                try:
                    oid = o.get("orderId")
                    if oid is not None:
                        ex.cancel_order(symbol=cfg.symbol, orderId=int(oid))
                except requests.exceptions.RequestException as e:
                    print(f"[exit_hygiene] cancel STOP {o.get('orderId')}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"[exit_hygiene] list STOPs: {e}")


# ============================== Persistence & Metrics ==============================

def _trim_dedupe_book(d: Dict[str, int], keep_latest: int = 300) -> Dict[str, int]:
    if len(d) <= keep_latest:
        return d
    items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:keep_latest]
    return dict(items)


def save_state(st: RunState, path: str):
    """Persist selected fields from `st` as JSON to `path` atomically."""
    try:
        # ensure directory exists
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        with STATE_LOCK:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "day_start_equity": st.day_start_equity,
                        "trades_today": st.trades_today,
                        "realized_today": st.realized_today,
                        "day_start_date": st.day_start_date,
                        "dedupe_book": _trim_dedupe_book(st.dedupe_book, keep_latest=400),
                        # Persist UDS cumulative realized baselines to avoid first-update double count
                        "account_cr": st.account_cr,
                    },
                    f,
                )
            os.replace(tmp, path)
    except (OSError, TypeError) as e:
        print(f"[state] save failed: {e}")


def load_state(path: str) -> Optional[Dict[str, Any]]:
    """Load previously saved JSON state from `path`, or None if missing/error."""
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"[state] load failed: {e}")
        return None


def debug_log(cfg: Config, payload: Dict[str, Any]) -> None:
    """Append `payload` as a JSON line to the debug log if `cfg.debug` is enabled."""
    if not cfg.debug:
        return
    try:
        os.makedirs(os.path.dirname(cfg.debug_jsonl_path) or ".", exist_ok=True)  # mkdir
        with DEBUG_LOG_LOCK:
            with open(cfg.debug_jsonl_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"[debug] {e}")


def metrics_emit(cfg: Config, st: RunState, row: Dict[str, Any]) -> None:
    """Append a CSV metrics `row` to `cfg.metrics_csv_path`, creating header if needed."""
    # Mark parameter as used for linters; intentionally unused at call site
    _ = st
    if not cfg.metrics_enable:
        return
    try:
        os.makedirs(os.path.dirname(cfg.metrics_csv_path) or ".", exist_ok=True)
        # Fixed schema to keep columns stable; unknown keys are ignored
        fieldnames = ["t", "ts", "count", "safe", "reco_mkt", "reco_uds", "drift"]
        hdr_needed = not os.path.exists(cfg.metrics_csv_path)
        with METRICS_LOCK:
            with open(cfg.metrics_csv_path, "a", encoding="utf-8", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
                if hdr_needed:
                    w.writeheader()
                w.writerow(row)
    except (OSError, csv.Error) as e:
        print(f"[metrics] {e}")


def metrics_block_inc(st: RunState, reason: str) -> None:
    """Increment in-memory blocked reason counter keyed by a normalized `reason`."""
    try:
        key = str(reason or "unknown").strip().lower().replace(" ", "_")
        st.blocked_reason_counts[key] = st.blocked_reason_counts.get(key, 0) + 1
    except Exception:
        pass


def flush_blocked_metrics_if_due(cfg: Config, st: RunState) -> None:
    """Emit one metrics row per blocked reason at most once per minute."""
    if not cfg.metrics_enable:
        return
    try:
        cur_min = now_epoch_minutes(st.last_server_ms)
        if st.blocked_counts_minute < 0:
            st.blocked_counts_minute = cur_min
            return
        if cur_min != st.blocked_counts_minute and st.blocked_reason_counts:
            # emit one row per reason, then reset
            for reason, count in list(st.blocked_reason_counts.items()):
                metrics_emit(cfg, st, {
                    "t": f"blocked_reason.{reason}",
                    "ts": now_ms(),
                    "count": int(count),
                    "safe": int(st.safe_mode),
                    "reco_mkt": st.ws_reconnects_market,
                    "reco_uds": st.ws_reconnects_uds,
                    "drift": st.last_server_offset_ms if st.last_server_offset_ms is not None else 0,
                })
            st.blocked_reason_counts.clear()
            st.blocked_counts_minute = cur_min
    except Exception as e:
        print(f"[metrics] blocked_flush {e}")


# ============================== Signal de-duplication ==============================

@dataclass
class SignalIdParams:
    side: str
    fvg: Dict[str, Any]
    pool_ref: float
    bar_index: int
    server_ms: Optional[int]
    tick: float
    pool_count: Optional[int] = None
    bin_ticks: int = 10
    tz_name: str = "UTC"
    pool_tag: Optional[str] = None


def _quantize_price(p: float, tick: float, bucket_ticks: int = 5) -> float:
    """Quantize price `p` to the nearest bucket of size `bucket_ticks * tick`."""
    q = max(EPS, tick) * max(1, int(bucket_ticks))
    return round(math.floor(p / q) * q, 12)


def make_signal_id(params: SignalIdParams) -> str:
    """Build a deterministic signal ID string for de-duplication and logging."""
    # Day key aligned to configured session timezone
    tms = params.server_ms or now_ms()
    z = _get_zone(params.tz_name)
    if z is not None:
        daykey = dt.datetime.fromtimestamp(tms / 1000.0, tz=z).strftime("%Y%m%d")
    else:
        daykey = dt.datetime.utcfromtimestamp(tms / 1000.0).strftime("%Y%m%d")
    dec = decimals_for_tick(params.tick)
    gl = round(float(params.fvg["gap_low"]), dec)
    gh = round(float(params.fvg["gap_high"]), dec)
    # Fine quantization of reference and coarse bin to stabilize cluster drift
    pr_q = round(_quantize_price(float(params.pool_ref), params.tick, 5), dec)
    pr_bin = round(_quantize_price(float(params.pool_ref), params.tick, max(1, int(params.bin_ticks))), dec)
    pc = int(params.pool_count) if (params.pool_count is not None and int(params.pool_count) >= 0) else 0
    tag = (params.pool_tag or "").strip().upper()
    tag_short = tag[:6] if tag else "NA"
    return f"{daykey}|{params.side}|{gl}-{gh}|{pr_q}|b{int(params.bar_index)}|n{pc}|q{pr_bin}|t{tag_short}"


def now_epoch_minutes(server_ms: Optional[int]) -> int:
    """Return integer epoch minutes from `server_ms` or current time if None."""
    t_ms = server_ms if server_ms is not None else now_ms()
    return int(t_ms // 60000)


# ============================== Detection & Scoring ==============================

@dataclass
class SweepFvgParams:
    exec_df: pd.DataFrame
    pools_up: List[float]
    pools_dn: List[float]
    tick: float
    cfg: 'Config'
    pen_bps: float
    cb_bps: float

def detect_sweep_and_fvg(params: SweepFvgParams) -> Optional[Dict[str, Any]]:
    """Detect sweep plus qualifying FVG and return a candidate entry dict, or None."""
    exec_df = params.exec_df
    if exec_df is None or len(exec_df) < 30:
        return None
    a = atr(exec_df, 14) or EPS
    tmax = params.cfg.sweep_t_reject_max_bars

    H, L, C, Open = exec_df["high"].values, exec_df["low"].values, exec_df["close"].values, exec_df["open"].values
    n = len(exec_df)
    tags: List[str] = []

    # Sweep above bearish FVG
    for lvl in sorted(params.pools_up):
        for i in range(n - 3, 3, -1):
            if H[i] >= lvl * (1 + params.pen_bps / 10000.0):
                ok = False
                jhit: Optional[int] = None
                # Inclusive window up to j <= i + tmax and j <= n - 1
                for j in range(i, min(n, i + tmax + 1)):
                    if C[j] <= lvl * (1 - params.cb_bps / 10000.0):
                        ok = True
                        jhit = j
                        break
                if not ok:
                    continue
                if jhit is None:
                    continue
                fvgs = detect_fvgs(exec_df.iloc[: jhit + 1], params.cfg, params.tick)
                fvgs = [x for x in fvgs if x["dir"] == "bearish"]
                if not fvgs:
                    continue
                x = fvgs[-1]
                q = fvg_quality(exec_df.iloc[: jhit + 1], float(x["gap_low"]), float(x["gap_high"]), cfg=params.cfg)
                body = abs(float(C[i]) - float(Open[i])) / (a + EPS)
                disp = clamp(body / 1.5, 0.0, 1.0)
                score = float(params.cfg.score_cand_base_offset) + float(params.cfg.score_cand_fvgq_w) * q + params.cfg.displacement_w * disp
                # Bearish entry anchored at upper gap bound; round toward entry for shorts
                entry = side_round(float(x["gap_high"]), params.tick, "sell", favor="entry")
                raw_stop = max(float(H[i]), float(x["gap_high"])) + 1 * max(params.tick, EPS)
                stop = side_round(raw_stop, params.tick, "sell", favor="protection")
                tags.append("sweep_above")
                return {
                    "side": "short",
                    "entry": entry,
                    "stop": stop,
                    "score_base": score,
                    "tags": tags,
                    "fvg": x,
                }

    # Sweep below bullish FVG
    for lvl in sorted(params.pools_dn, reverse=True):
        for i in range(n - 3, 3, -1):
            if L[i] <= lvl * (1 - params.pen_bps / 10000.0):
                ok = False
                jhit = None
                for j in range(i, min(n, i + tmax + 1)):
                    if C[j] >= lvl * (1 + params.cb_bps / 10000.0):
                        ok = True
                        jhit = j
                        break
                if not ok:
                    continue
                if jhit is None:
                    continue
                fvgs = detect_fvgs(exec_df.iloc[: jhit + 1], params.cfg, params.tick)
                fvgs = [x for x in fvgs if x["dir"] == "bullish"]
                if not fvgs:
                    continue
                x = fvgs[-1]
                q = fvg_quality(exec_df.iloc[: jhit + 1], float(x["gap_low"]), float(x["gap_high"]), cfg=params.cfg)
                body = abs(float(C[i]) - float(Open[i])) / (a + EPS)
                disp = clamp(body / 1.5, 0.0, 1.0)
                score = float(params.cfg.score_cand_base_offset) + float(params.cfg.score_cand_fvgq_w) * q + params.cfg.displacement_w * disp
                # Bullish entry anchored at lower gap bound; round toward entry for longs
                entry = side_round(float(x["gap_low"]), params.tick, "buy", favor="entry")
                raw_stop = min(float(L[i]), float(x["gap_low"])) - 1 * max(params.tick, EPS)
                stop = side_round(raw_stop, params.tick, "buy", favor="protection")
                tags.append("sweep_below")
                return {
                    "side": "long",
                    "entry": entry,
                    "stop": stop,
                    "score_base": score,
                    "tags": tags,
                    "fvg": x,
                }

    return None


# ============================== Startup sanity & reconciliation ==============================

def sanity_checks(ex: BinanceUM, cfg: Config) -> None:
    """Validate account mode before trading: one-way vs hedge; margin mode cross/isolated."""
    dual = ex.position_side_dual()
    if dual is None:
        print("[SANITY] Could not determine position side mode (hedge/one-way). Proceeding with caution.")
    else:
        if cfg.require_one_way and dual:
            raise RuntimeError("Account is in HEDGE (dual-side) mode, but BOT_REQUIRE_ONEWAY=true. "
                               "Switch to one-way or set BOT_REQUIRE_ONEWAY=false.")
        if not cfg.require_one_way and dual is False:
            print("[SANITY] Running in one-way mode (OK).")

    # Margin mode check from positionRisk (field 'isolated': 'true'/'false')
    try:
        rows = ex.positions(cfg.symbol)
        iso_vals: Set[bool] = set()
        for r in rows:
            if r.get("symbol") == cfg.symbol:
                iso_vals.add(str(r.get("isolated", "false")).lower() == "true")
        if iso_vals:
            isolated_active = True in iso_vals
            want_isolated = (cfg.margin_mode == "ISOLATED")
            if isolated_active != want_isolated:
                raise RuntimeError(f"Margin mode mismatch for {cfg.symbol}: exchange has "
                                   f"{'ISOLATED' if isolated_active else 'CROSS'}, but BOT_MARGIN_MODE={cfg.margin_mode}.")
    except requests.exceptions.RequestException as e:
        print(f"[SANITY] margin/mode check: {e}")


def reconcile_startup_orders(ex: BinanceUM, st: RunState, cfg: Config) -> None:
    """
    On boot: ensure orders match live position.
    - If flat: cancel all protective stops (closePosition) and TPs (reduceOnly).
    - If in position: allow exactly one protective STOP_MARKET closePosition on the correct side.
      Keep at most 2 TPs (distinct stopPrice) that reduce-only on the correct side.
    - Rehydrate trailing state from the surviving protective stop.
    """
    try:
        net, gross = ex.position_net_and_gross(cfg.symbol)
        side_live: Optional[str] = "long" if net > 0 else ("short" if net < 0 else None)

        openos = ex.open_orders(cfg.symbol)

        # Partition
        def _partition_open_orders(open_orders: List[Dict[str, Any]]):
            prot_: List[Dict[str, Any]] = []
            tps_: List[Dict[str, Any]] = []
            others_: List[Dict[str, Any]] = []
            for o in open_orders:
                typ = str(o.get("type", "")).upper()
                reduce_only = bool(o.get("reduceOnly", False))
                close_pos = bool(o.get("closePosition", False))
                if typ == "STOP_MARKET" and close_pos:
                    prot_.append(o)
                elif typ == "TAKE_PROFIT_MARKET" and reduce_only:
                    tps_.append(o)
                else:
                    others_.append(o)
            return prot_, tps_, others_

        prot, tps, others = _partition_open_orders(openos)

        def _cancel(o: Dict[str, Any]):
            try:
                oid = o.get("orderId")
                if oid is not None:
                    ex.cancel_order(symbol=cfg.symbol, orderId=int(oid))
            except requests.exceptions.RequestException as e:
                print(f"[reconcile] cancel {o.get('orderId')} failed: {e}")

        def _clear_trailing_cache(new_side: Optional[str]):
            with st.buf_lock:
                st.last_trailing_client_id = None
                st.last_trailing_sl = None
                st.last_trailing_side = new_side
                st.last_trailing_order_id = None

        # If flat: cancel all protective & TP orders; clear trailing cache
        if side_live is None or abs(gross) <= EPS:
            for o in prot + tps:
                _cancel(o)
            _clear_trailing_cache(None)
            return

        # In position: ensure one protective closePosition on correct exit side
        want_exit_side = "SELL" if side_live == "long" else "BUY"
        candidates = [o for o in prot if str(o.get("side", "")).upper() == want_exit_side]
        keep_prot: Optional[Dict[str, Any]] = None
        if candidates:
            keep_prot = max(candidates, key=lambda x: int(x.get("updateTime", x.get("time", 0)) or 0))
            for o in candidates:
                if o is not keep_prot:
                    _cancel(o)
        for o in prot:
            if keep_prot is None or o is not keep_prot:
                if str(o.get("side", "")).upper() != want_exit_side:
                    _cancel(o)

        # Rehydrate trailing cache from kept protective stop
        if keep_prot:
            with st.buf_lock:
                st.last_trailing_client_id = keep_prot.get("clientOrderId")
                sp_val = keep_prot.get("stopPrice")
                st.last_trailing_sl = float(sp_val) if sp_val is not None else None
                st.last_trailing_side = side_live
                oid_val = keep_prot.get("orderId")
                st.last_trailing_order_id = int(oid_val) if oid_val is not None else None
        else:
            _clear_trailing_cache(side_live)

        # TPs: keep at most two, correct direction, unique prices
        tp_side = "SELL" if side_live == "long" else "BUY"
        good_tps = [o for o in tps if str(o.get("side", "")).upper() == tp_side]
        uniq: Dict[float, Dict[str, Any]] = {}
        for o in good_tps:
            try:
                sp_raw = o.get("stopPrice")
                if sp_raw is None:
                    continue
                sp = float(sp_raw)
                if sp not in uniq:
                    uniq[sp] = o
            except (ValueError, TypeError):
                continue
        kept = 0
        for sp, o in sorted(uniq.items(), key=lambda kv: kv[0], reverse=(side_live == "long")):
            if kept >= 2:
                _cancel(o)
            else:
                kept += 1
        for o in tps:
            if str(o.get("side", "")).upper() != tp_side:
                _cancel(o)

        if others:
            print_json({"evt": "reconcile_info", "msg": "unclassified_open_orders", "n": len(others)})
    except requests.exceptions.RequestException as e:
        print(f"[reconcile] {e}")


# ============================== Funding awareness & gates ==============================

def current_funding_info(ex: BinanceUM, cfg: Config) -> Tuple[Optional[float], Optional[int]]:
    """Return (funding_rate_decimal, nextFundingTime_ms)."""
    try:
        j = ex.premium_index(cfg.symbol)
        rate = float(j.get("lastFundingRate", j.get("fundingRate", 0.0)))
        nft_raw = j.get("nextFundingTime")
        nft = int(nft_raw) if nft_raw is not None else None
        return (rate, nft)
    except (KeyError, TypeError, ValueError) as e:
        print(f"[funding] info: {e}")
        return (None, None)


def funding_gate(ex: BinanceUM, st: RunState, cfg: Config) -> Tuple[bool, float, float, Dict[str, Any]]:
    """
    Returns (block, score_penalty, risk_mult, meta).
    - block: True if entries should be skipped.
    - score_penalty: subtract from score if not blocking.
    - risk_mult: multiply risk_pct if not blocking.
    """
    meta: Dict[str, Any] = {"reason": None}
    if not cfg.funding_aware_enable:
        return False, 0.0, 1.0, meta

    rate, nft = current_funding_info(ex, cfg)
    if nft is None or st.last_server_ms is None:
        # Be conservative when timing is unknown: block until server time and nextFundingTime are available
        meta["reason"] = "time_unknown"
        return True, 0.0, 1.0, meta

    mins_to = (nft - st.last_server_ms) / 60000.0
    window = max(1, int(cfg.funding_window_min))
    # Near window: symmetric around funding if configured, else only pre-funding
    if getattr(cfg, "funding_window_symmetric", False):
        near = abs(mins_to) <= window
    else:
        near = 0 <= mins_to <= window

    # High absolute funding? (bps from decimal)
    abs_bps = abs(float(rate or 0.0)) * 10000.0
    hi = abs_bps >= cfg.funding_high_abs_bps

    if near and cfg.funding_block_near:
        meta["reason"] = "near_funding_block"
        meta["mins_to"] = round(mins_to, 2)
        meta["abs_bps"] = round(abs_bps, 4)
        return True, 0.0, 1.0, meta

    # Penalize score and risk when near or high funding
    penalty = cfg.funding_penalty_score if near else 0.0
    risk_mult = cfg.funding_risk_mult_high if hi else 1.0
    meta["reason"] = "near_penalty" if near else ("high_funding_risk" if hi else "ok")
    meta["mins_to"] = round(mins_to, 2)
    meta["abs_bps"] = round(abs_bps, 4)
    return False, penalty, risk_mult, meta


# ============================== Drift hardening & safe-mode ==============================

def measure_drift_median3(ex: BinanceUM) -> int:
    """Return median server-now drift in ms from 3 samples (serverTime - localNow)."""
    samples = []
    for _ in range(3):
        try:
            t_server = ex.server_time()
            samples.append(int(t_server - now_ms()))
            time.sleep(0.05)
        except Exception:
            pass
    if not samples:
        return 0
    samples.sort()
    return int(samples[len(samples)//2])


def update_drift_policy(ex: BinanceUM, st: RunState, cfg: Config, force=False) -> None:
    """
    Sample drift and update recvWindow policy.
    Enter/exit safe-mode if drift is sustained over hard threshold.
    """
    # sample less often unless forced
    if (not force) and (st.loop_idx % 10) != 0:
        return
    try:
        off = measure_drift_median3(ex)
        st.drift_samples_ms.append(off)
        ex.set_drift_offset(off)
        # sustained hard breach?
        if abs(off) >= cfg.drift_hard_ms:
            st.drift_bad_streak += 1
        else:
            st.drift_bad_streak = 0

        if cfg.safe_mode_enable and st.drift_bad_streak >= cfg.drift_sustain_n:
            st.safe_mode = True
            st.safe_reason = f"clock_drift_{off}ms>=hard_{cfg.drift_hard_ms}ms"
        elif cfg.safe_auto_recover and st.safe_mode and "clock_drift" in st.safe_reason:
            # recover when last 3 within half the hard limit
            recent = list(st.drift_samples_ms)[-3:]
            if recent and max(abs(x) for x in recent) < (cfg.drift_hard_ms // 2):
                st.safe_mode = False
                st.safe_reason = ""
    except requests.exceptions.RequestException as e:
        print(f"[drift] {e}")


def safe_mode_gate(st: RunState, cfg: Config, ws_fresh: bool) -> Tuple[bool, str]:
    """
    Returns (blocked, reason). Blocks trading if:
      - explicit safe_mode flag set (with reason), or
      - WS data is not fresh (stricter than soft-stale handling), or
      - require listenKey and it's not healthy.
    """
    if cfg.safe_mode_enable and st.safe_mode:
        return True, st.safe_reason or "safe_mode"
    # WS freshness hard block (caller decides what "fresh" means)
    # REST-only mode or missing websocket-client bypass freshness requirement
    if (not cfg.rest_only_mode) and cfg.ws_market_enable and WEBSOCKET_AVAILABLE and not ws_fresh:
        # allow a short grace after cold backfill or transient setup
        if st.ws_fresh_grace_loops > 0:
            st.ws_fresh_grace_loops -= 1
        else:
            return True, "ws_not_fresh"
    # listenKey requirement (only enforce if websocket-client is present and we aren't in REST-only mode)
    if cfg.safe_require_listenkey and WEBSOCKET_AVAILABLE and (not cfg.rest_only_mode) and (not st.uds_listenkey_ok):
        return True, "uds_listenkey_unhealthy"
    return False, ""


# ============================== Idempotent entry ==============================

def _entry_client_params(cfg: Config, side: str, qty: float) -> Dict[str, Any]:
    return {
        "symbol": cfg.symbol,
        "side": "BUY" if side == "long" else "SELL",
        "type": "MARKET",
        "quantity": qty,
    }


def _order_status_ok(od: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    try:
        status = str(od.get("status", "")).upper()
        return status in ("NEW", "PARTIALLY_FILLED", "FILLED"), status
    except Exception:
        return False, None


def _detect_order_by_cid(
    ex: BinanceUM, cfg: Config, cid: str, retries: int = 2, delay: float = 0.3
) -> Tuple[bool, Optional[str]]:
    for _ in range(max(0, int(retries))):
        try:
            od = ex.get_order(cfg.symbol, orig_client_id=cid)
            if od:
                ok, status = _order_status_ok(od)
                if ok:
                    return True, status
        except requests.exceptions.RequestException:
            pass
        time.sleep(max(0.0, float(delay)))
    return False, None


def place_market_entry_idempotent(ex: BinanceUM, cfg: Config, side: str, qty: float, step: Optional[float] = None) -> Tuple[bool, Optional[str]]:
    """
    Place a MARKET entry with a unique client ID.
    If an error occurs, query by client ID to detect success and avoid duplicate entries.
    Returns (placed_or_detected, client_id).
    """
    cid = _unique_cid("entry")
    params = {**_entry_client_params(cfg, side, qty), "newClientOrderId": cid}
    # Format quantity to string using step decimals when available
    try:
        if step is not None and float(step) > 0:
            params["quantity"] = _fmt_qty(float(qty), float(step))
        else:
            params["quantity"] = str(qty)
    except Exception:
        params["quantity"] = str(qty)
    try:
        ex.new_order(**params)
        return True, cid
    except requests.exceptions.RequestException as e:
        found, status = _detect_order_by_cid(ex, cfg, cid, retries=2, delay=0.3)
        if found:
            print(f"[entry] network error after submit; detected order via clientId={cid} status={status}")
            return True, cid
        print(f"[entry] failed submit and not found by clientId: {e}")
        return False, cid


# ============================== Main loop ==============================

def init_instrument(ex: BinanceUM, st: RunState, cfg: Config):
    info = ex.exchange_info()
    sym = None
    for s in info.get("symbols", []):
        if s.get("symbol") == cfg.symbol:
            sym = s
            break
    if not sym:
        raise RuntimeError(f"Symbol {cfg.symbol} not found on testnet.")
    found_price = False
    found_lot = False
    market_lot_step: Optional[float] = None
    for f in sym.get("filters", []):
        if f.get("filterType") == "PRICE_FILTER":
            try:
                st.tick = float(f.get("tickSize", st.tick))
                found_price = True
            except Exception:
                pass
        if f.get("filterType") == "LOT_SIZE":
            try:
                st.step = float(f.get("stepSize", st.step))
                found_lot = True
            except Exception:
                pass
        if f.get("filterType") == "MARKET_LOT_SIZE":
            try:
                market_lot_step = float(f.get("stepSize", market_lot_step or st.step))
            except Exception:
                pass
        if f.get("filterType") == "MIN_NOTIONAL":
            try:
                st.min_notional = float(f.get("notional", st.min_notional))
            except Exception:
                pass
    if not found_price:
        print(f"[instrument] WARN: PRICE_FILTER not found for {cfg.symbol}; using default tick={st.tick}")
    if not found_lot:
        print(f"[instrument] WARN: LOT_SIZE not found for {cfg.symbol}; using default step={st.step}")
    if (not math.isfinite(st.tick)) or st.tick <= 0:
        raise RuntimeError(f"Invalid tick size for {cfg.symbol}: {st.tick}")
    # Prefer MARKET_LOT_SIZE step for market orders if present
    if market_lot_step and math.isfinite(market_lot_step) and market_lot_step > 0:
        st.step = float(market_lot_step)
    if (not math.isfinite(st.step)) or st.step <= 0:
        raise RuntimeError(f"Invalid step size for {cfg.symbol}: {st.step}")
    ex.set_leverage(cfg.symbol, cfg.leverage)
    print(f"[instrument] tick={st.tick} step={st.step} minNotional={st.min_notional}")


def _candles_per_day_for_tf(tf: str) -> int:
    try:
        if tf.endswith("m"):
            mins = int(tf[:-1])
            return max(1, 1440 // max(1, mins))
        if tf.endswith("h"):
            hrs = int(tf[:-1])
            return max(1, 24 // max(1, hrs))
        if tf == "1d":
            return 2  # previous + current
    except Exception:
        pass
    return 600


def fetch_exec_klines(ex: BinanceUM, st: RunState, cfg: Config) -> Tuple[List[List[float]], pd.DataFrame, bool]:
    """
    Returns (klines, df, ws_fresh). Performs a one-time cold backfill after boot to ensure
    session/ORB computations have sufficient history even if WS is already flowing.
    """
    # One-time cold backfill
    if not st.did_cold_backfill:
        limit = max(600, _candles_per_day_for_tf(cfg.timeframe_exec))
        kl = ex.fetch_klines(cfg.symbol, cfg.timeframe_exec, limit=limit)
        st.did_cold_backfill = True
        # cache result to avoid immediate re-fetches
        st.exec_kl_cache = list(kl)
        st.exec_kl_cache_ts = time.time()
        st.last_rest_klines_ts = time.time()
        # allow a couple loops of grace where ws_fresh is treated as True
        st.ws_fresh_grace_loops = max(st.ws_fresh_grace_loops, 2)
        df = df_from_klines(kl)
        return kl, df, False

    ws_fresh = False
    tf = cfg.timeframe_exec
    with st.buf_lock:
        recent_ts = st.last_market_ws_ts
        len_1m = len(st.kl_1m)
        len_5m = len(st.kl_5m)
    # compute a TF-aware freshness threshold (min 20s, ~0.6*TF for minute TFs), then cap it
    fresh_ms = 20_000
    try:
        if isinstance(tf, str) and tf.endswith("m") and tf[:-1].isdigit():
            mins = int(tf[:-1])
            fresh_ms = max(20_000, int(mins * 60_000 * 0.6))
    except Exception:
        pass
    # Hard cap for very slow TFs
    try:
        fresh_ms = min(fresh_ms, int(cfg.ws_fresh_max_ms))
    except Exception:
        pass

    # tf already defined above for freshness threshold; reuse it for buffer checks
    ok_len = False
    with st.buf_lock:
        if tf == "1m":
            ok_len = len_1m > 50
        elif tf == "5m":
            ok_len = len_5m > 20
        else:
            ok_len = True
    if cfg.ws_market_enable and recent_ts and (now_ms() - recent_ts) < fresh_ms and ok_len:
        with st.buf_lock:
            if tf == "1m":
                kl = list(st.kl_1m)
            elif tf == "5m":
                kl = list(st.kl_5m) if len(st.kl_5m) > 20 else ex.fetch_klines(cfg.symbol, "5m", limit=600)
                ws_fresh = len(st.kl_5m) > 20
            else:
                kl = ex.fetch_klines(cfg.symbol, cfg.timeframe_exec, limit=600)
                ws_fresh = False
        if tf in ("1m",) and len(kl) > 0:
            ws_fresh = True
    else:
        # Throttle REST fallback calls to avoid 429s; use cached data if within interval
        nowt = time.time()
        min_iv = max(0.5, float(getattr(cfg, "kline_rest_min_interval_secs", 3.0)))
        if (nowt - st.last_rest_klines_ts) < min_iv and st.exec_kl_cache:
            kl = list(st.exec_kl_cache)
            ws_fresh = False
        else:
            # larger limit to better cover ORB/session windows
            limit_default = 1440 if (cfg.timeframe_exec.endswith("m") and cfg.timeframe_exec.split("m")[0].isdigit() and int(cfg.timeframe_exec[:-1]) in (1,3,5,15,30)) else 600
            kl = ex.fetch_klines(cfg.symbol, cfg.timeframe_exec, limit=limit_default)
            st.exec_kl_cache = list(kl)
            st.exec_kl_cache_ts = nowt
            st.last_rest_klines_ts = nowt
            ws_fresh = False
    df = df_from_klines(kl)
    # In REST-only mode we don't require WS freshness
    if cfg.rest_only_mode:
        ws_fresh = True
    return kl, df, ws_fresh


def update_equity_and_roll(ex: BinanceUM, st: RunState, cfg: Config, server_ms: int):
    # Day key aligned with session timezone
    z = _get_zone(getattr(cfg, "session_timezone", "UTC"))
    if z is not None:
        server_day = dt.datetime.fromtimestamp(server_ms / 1000.0, tz=z).strftime("%Y-%m-%d")
    else:
        server_day = dt.datetime.utcfromtimestamp(server_ms / 1000.0).strftime("%Y-%m-%d")
    if not st.day_start_date:
        st.day_start_date = server_day
        eq = balance_cached(ex, st, cfg)
        st.day_start_equity = eq if eq > 0.0 else (st.day_start_equity or 1.0)
    elif st.day_start_date != server_day:
        st.day_start_date = server_day
        eq = balance_cached(ex, st, cfg)
        st.day_start_equity = eq if eq > 0.0 else (st.day_start_equity or 1.0)
        st.realized_today = 0.0
        st.trades_today = 0
        st.dedupe_book = _trim_dedupe_book(st.dedupe_book, keep_latest=400)
        # Reset UDS cumulative realized baselines so the next ACCOUNT_UPDATE re-seeds them
        st.account_cr = {}


def refresh_prior_extremes_if_needed(ex: BinanceUM, st: RunState, cfg: Config):
    if (time.time() - st.last_extremes_refresh) < 120:
        return
    try:
        pe: Dict[str, float] = {}
        d = ex.fetch_klines(cfg.symbol, "1d", limit=3)
        if len(d) >= 2:
            pe["PDH"], pe["PDL"] = d[-2][2], d[-2][3]
        w = ex.fetch_klines(cfg.symbol, "1w", limit=3)
        if len(w) >= 2:
            pe["PWH"], pe["PWL"] = w[-2][2], w[-2][3]
        try:
            m = ex.fetch_klines(cfg.symbol, "1M", limit=3)
            if m and len(m) >= 2:
                pe["PMH"], pe["PML"] = m[-2][2], m[-2][3]
        except Exception:
            pass
        if pe:
            st.prior_extremes_cache = pe
    except requests.exceptions.RequestException as e:
        print(f"[prior extremes] {e}")
    finally:
        st.last_extremes_refresh = time.time()


def _day_start_ms(server_ms: int, tz_name: str = "UTC") -> int:
    """Epoch ms for midnight at session timezone corresponding to server_ms day.
    Returns UTC epoch ms for that local midnight.
    """
    z = _get_zone(tz_name)
    if z is not None:
        d_local = dt.datetime.fromtimestamp(server_ms / 1000.0, tz=z)
        d0_local = dt.datetime(d_local.year, d_local.month, d_local.day, 0, 0, 0, tzinfo=z)
        return int(d0_local.timestamp() * 1000)
    else:
        d = dt.datetime.utcfromtimestamp(server_ms / 1000.0)
        d0 = dt.datetime(d.year, d.month, d.day, 0, 0, 0)
        return int(d0.timestamp() * 1000)


def refresh_realized_today_rest(ex: BinanceUM, st: RunState, cfg: Config):
    """Fallback realized PnL update via REST income endpoint when UDS is unhealthy."""
    try:
        if st.last_server_ms is None:
            return
        start_ms = _day_start_ms(st.last_server_ms, getattr(cfg, "session_timezone", "UTC"))
        rows = ex.income(symbol=cfg.symbol, incomeType="REALIZED_PNL", startTime=start_ms)
        total = 0.0
        if isinstance(rows, list):
            for r in rows:
                try:
                    total += float(r.get("income", 0.0))
                except Exception:
                    continue
        prev = st.realized_today
        st.realized_today = total
        if st.realized_today < prev:
            st.last_loss_server_ms = int(st.last_server_ms)
    except requests.exceptions.RequestException as e:
        print(f"[pnl_rest] {e}")


# ============================== Main loop helpers ==============================

def poll_maintenance(ex: BinanceUM, st: RunState, cfg: Config, ws: 'WSManager', last_time_poll: float) -> float:
    """Perform periodic maintenance tasks and return updated last_time_poll timestamp."""
    if (time.time() - last_time_poll) > max(3.0, cfg.loop_sleep * 5):
        try:
            server = ex.server_time()
            st.last_server_ms = server
            st.last_server_offset_ms = int(server - now_ms())
            update_drift_policy(ex, st, cfg)
        except requests.exceptions.RequestException as e:
            st.last_server_offset_ms = None
            print(f"[time] {e}")
        last_time_poll = time.time()

    with st.buf_lock:
        lmts = st.last_market_ws_ts
    if cfg.ws_market_enable and lmts:
        stale = (now_ms() - lmts) > cfg.ws_watchdog_secs * 1000
        if stale and not st.watchdog_stale:
            try:
                if ws.ws_mkt:
                    ws.ws_mkt.close()
                st.watchdog_stale = True
                print("[WS] Watchdog forced reconnect")
            except Exception as e:
                print(f"[WS] watchdog: {e}")
        elif (not stale) and st.watchdog_stale:
            st.watchdog_stale = False

    if time.time() - st.last_info_refresh_ts > cfg.exchangeinfo_refresh_secs:
        try:
            init_instrument(ex, st, cfg)
        except requests.exceptions.RequestException as e:
            print(f"[instrument refresh] {e}")
        st.last_info_refresh_ts = time.time()

    if st.last_server_ms is not None:
        update_equity_and_roll(ex, st, cfg, st.last_server_ms)
        if (not WEBSOCKET_AVAILABLE) or (not st.uds_listenkey_ok):
            if (time.time() - st.last_pnl_poll_ts) > 30:
                refresh_realized_today_rest(ex, st, cfg)
                st.last_pnl_poll_ts = time.time()

    return last_time_poll


def fetch_data_and_guard(ex: BinanceUM, st: RunState, cfg: Config) -> Optional[Tuple[List[List[float]], pd.DataFrame, bool]]:
    """Fetch latest data and apply stale/safe-mode gating. Returns tuple or None if skipping."""
    exec_kl, exec_df, ws_fresh = fetch_exec_klines(ex, st, cfg)
    if exec_df is None or exec_df.empty:
        st.stale_count += 1
        time.sleep(cfg.loop_sleep)
        return None

    last_ts = int(exec_df.iloc[-1]["time"])
    age_ms = now_ms() - last_ts
    if cfg.safe_mode_enable and cfg.safe_stale_hard_minutes and age_ms > int(cfg.safe_stale_hard_minutes) * 60 * 1000:
        st.safe_mode = True
        st.safe_reason = f"stale_data_hard_{cfg.safe_stale_hard_minutes}m"

    if age_ms > cfg.stale_data_minutes * 60 * 1000:
        st.stale_count += 1
        if st.stale_count >= cfg.circuit_block_threshold:
            time.sleep(cfg.circuit_sleep)
        print_json({"evt": "heartbeat", "msg": "stale_data_skip", "ws_fresh": ws_fresh})
        try:
            metrics_block_inc(st, "stale_data")
        except Exception:
            pass
        try:
            flush_blocked_metrics_if_due(cfg, st)
        except Exception:
            pass
        time.sleep(cfg.loop_sleep)
        return None
    else:
        st.stale_count = 0

    blocked, why_block = safe_mode_gate(st, cfg, ws_fresh)
    if blocked:
        print_json({"evt": "heartbeat", "msg": "safe_mode_block", "reason": why_block,
                    "ws_reco_mkt": st.ws_reconnects_market, "ws_reco_uds": st.ws_reconnects_uds,
                    "drift_ms": st.last_server_offset_ms})
        try:
            metrics_block_inc(st, f"safe.{why_block}")
        except Exception:
            pass
        try:
            flush_blocked_metrics_if_due(cfg, st)
        except Exception:
            pass
        time.sleep(cfg.loop_sleep)
        return None

    return exec_kl, exec_df, ws_fresh



def _build_trade_context(ex: BinanceUM, st: RunState, cfg: Config,
                         exec_kl: List[List[float]], exec_df: pd.DataFrame) -> Dict[str, Any]:
    """Assemble pools, sessions, adapt and last price for a trading cycle."""
    last_px = float(exec_df.iloc[-1]["close"])
    up_pools, dn_pools = build_pools(cfg, exec_kl, exec_df, st.tick, last_px)
    refresh_prior_extremes_if_needed(ex, st, cfg)
    pe = st.prior_extremes_cache
    if cfg.enable_pools_prev_extremes and pe:
        if "PDH" in pe:
            up_pools.append((pe["PDH"], cfg.w_htf_levels, "PDH"))
        if "PDL" in pe:
            dn_pools.append((pe["PDL"], cfg.w_htf_levels, "PDL"))
        if "PWH" in pe:
            up_pools.append((pe["PWH"], cfg.w_htf_levels, "PWH"))
        if "PWL" in pe:
            dn_pools.append((pe["PWL"], cfg.w_htf_levels, "PWL"))
        if "PMH" in pe:
            up_pools.append((pe["PMH"], cfg.w_htf_levels, "PMH"))
        if "PML" in pe:
            dn_pools.append((pe["PML"], cfg.w_htf_levels, "PML"))
    if cfg.server_time_sessions:
        if st.last_server_ms is not None:
            z = _get_zone(cfg.session_timezone)
            if z is not None:
                dtn = dt.datetime.fromtimestamp(st.last_server_ms / 1000.0, tz=z)
            else:
                dtn = dt.datetime.utcfromtimestamp(st.last_server_ms / 1000.0)
            server_mins = dtn.hour * 60 + dtn.minute
        else:
            server_mins = _utc_minutes(int(exec_df.iloc[-1]["time"]))
    else:
        z = _get_zone(cfg.session_timezone)
        nowt = dt.datetime.now(tz=z) if z is not None else dt.datetime.utcnow()
        server_mins = nowt.hour * 60 + nowt.minute
    sessions = active_sessions_utc(server_mins, cfg)
    if cfg.use_session_vwap_bands:
        def add_vwap(start, end):
            bands = session_vwap_bands(exec_df, start, end, cfg.session_vwap_std_k, cfg.session_timezone)
            if bands:
                if bands["upper"] >= last_px:
                    up_pools.append((bands["upper"], cfg.w_vwap, "VWAP_U"))
                if bands["lower"] <= last_px:
                    dn_pools.append((bands["lower"], cfg.w_vwap, "VWAP_L"))
        if "asia" in sessions:
            add_vwap(cfg.asia_session_start, cfg.asia_session_end)
        if "london" in sessions and cfg.use_session_london:
            add_vwap(cfg.session_london_utc[0], cfg.session_london_utc[1])
        if "newyork" in sessions and cfg.use_session_newyork:
            add_vwap(cfg.session_newyork_utc[0], cfg.session_newyork_utc[1])
    adapt = regime_adapt(exec_df, cfg)
    return {"last_px": last_px, "up_pools": up_pools, "dn_pools": dn_pools, "sessions": sessions, "adapt": adapt}


def _score_candidate_and_ahead(ex: BinanceUM, st: RunState, cfg: Config, exec_df: pd.DataFrame,
                               cand: Dict[str, Any], up_pools: List[Tuple[float, float, str]],
                               dn_pools: List[Tuple[float, float, str]], sessions: Set[str]) -> Tuple[float, List[float], float, float, float, float]:
    """Return (score, ahead_prices, cont, bos, choch, htf_ok)."""
    score = float(cand["score_base"]) + session_soft_bonus(sessions, cfg)
    ahead_prices: List[float] = []
    if cfg.enable_pool_scoring_bonus:
        ahead_prices = [p for p, _, _ in (up_pools if cand["side"] == "long" else dn_pools)]
        near_score = 0.0
        ref = float(cand["entry"])
        for p in ahead_prices:
            near_score += clamp(1.0 - bps_distance(p, ref) / max(1e-6, cfg.proximity_window_bps), 0.0, 1.0) * float(cfg.score_pool_near_unit_w)
        score += cfg.pool_score_gain * near_score
    htf_tags = htf_tags_normalized(cfg)
    score += float(cfg.score_pool_sv2_w) * pool_scoring_v2(
        up_pools if cand["side"] == "long" else dn_pools,
        float(cand["entry"]), cfg.w_htf_mult, cfg.proximity_window_bps, htf_tags,
    )
    cont = containment_15m_score(exec_df, cand["side"],
                                 (float(cand["fvg"]["gap_low"]), float(cand["fvg"]["gap_high"])), cfg.timeframe_exec)
    bos, choch = bos_choch_scores(exec_df, cand["side"])
    htf_bias = cached_htf_bias(ex, st, cfg, cfg.symbol)
    htf_ok = 1.0 if ((htf_bias > 0 and cand["side"] == "long") or (htf_bias < 0 and cand["side"] == "short")) else 0.0
    score += cfg.containment_15m_w * cont + cfg.bos_bonus_w * bos + cfg.choch_bonus_w * choch + cfg.htf_consensus_w * htf_ok
    return score, ahead_prices, cont, bos, choch, htf_ok


def _signal_id_and_dedupe(st: RunState, cfg: Config, cand: Dict[str, Any],
                          up_pools: List[Tuple[float, float, str]],
                          dn_pools: List[Tuple[float, float, str]], last_px: float) -> Tuple[str, bool, int, int]:
    dir_pools = (up_pools if cand["side"] == "long" else dn_pools)
    dir_pools_prices = [p for p, _, _ in dir_pools]
    pool_tag_near: Optional[str] = None
    if dir_pools_prices:
        direction_filter = [p for p in dir_pools_prices if (p > cand["entry"] if cand["side"] == "long" else p < cand["entry"]) ]
        pool_ref = (
            min(direction_filter, key=lambda x: abs(x - cand["entry"]))
            if direction_filter else min(dir_pools_prices, key=lambda x: abs(x - cand["entry"]))
        )
        try:
            for pr, _, tg in dir_pools:
                if within_bps(pr, pool_ref, cfg.pool_cluster_bps):
                    pool_tag_near = tg
                    break
        except Exception:
            pool_tag_near = None
        dir_count = len(direction_filter) if direction_filter else len(dir_pools_prices)
    else:
        pool_ref = last_px
        dir_count = 0
        pool_tag_near = None
    sig_id = make_signal_id(SignalIdParams(
        side=cand["side"], fvg=cand["fvg"], pool_ref=pool_ref,
        bar_index=int(cand["fvg"]["bar_index"]), server_ms=st.last_server_ms,
        tick=st.tick, pool_count=dir_count,
        bin_ticks=getattr(cfg, "dedupe_bin_ticks", 10), tz_name=getattr(cfg, "session_timezone", "UTC"),
        pool_tag=pool_tag_near,
    ))
    now_min = now_epoch_minutes(st.last_server_ms)
    last_hit = st.dedupe_book.get(sig_id, -10**9)
    dedupe_ok = (now_min - last_hit) >= cfg.signal_cooldown_min
    return sig_id, dedupe_ok, now_min, last_hit


def _apply_guardrails(st: RunState, cfg: Config, reason: str) -> None:
    print_json({"evt": "blocked", "reason": reason})
    try:
        metrics_block_inc(st, reason)
    except Exception:
        pass
    debug_log(cfg, {"t": "blocked", "why": reason})

def process_trade_opportunities(ex: BinanceUM, st: RunState, cfg: Config, exec_kl: List[List[float]], exec_df: pd.DataFrame, ws_fresh: bool) -> None:
    """Build pools, evaluate candidate, place orders, emit heartbeats, and persist state."""
    ctx = _build_trade_context(ex, st, cfg, exec_kl, exec_df)
    last_px = ctx["last_px"]
    up_pools = ctx["up_pools"]
    dn_pools = ctx["dn_pools"]
    sessions = ctx["sessions"]
    adapt = ctx["adapt"]

    cand = detect_sweep_and_fvg(SweepFvgParams(
        exec_df=exec_df,
        pools_up=[p for p, _, _ in up_pools],
        pools_dn=[p for p, _, _ in dn_pools],
        tick=st.tick,
        cfg=cfg,
        pen_bps=adapt["pen_bps"],
        cb_bps=adapt["cb_bps"],
    ))

    if cand:
        score = float(cand["score_base"]) + session_soft_bonus(sessions, cfg)
        ahead_prices: List[float] = []
        if cfg.enable_pool_scoring_bonus:
            ahead_prices = [p for p, _, _ in (up_pools if cand["side"] == "long" else dn_pools)]
            near_score = 0.0
            ref = float(cand["entry"])
            for p in ahead_prices:
                near_score += clamp(1.0 - bps_distance(p, ref) / max(1e-6, cfg.proximity_window_bps), 0.0, 1.0) * float(cfg.score_pool_near_unit_w)
            score += cfg.pool_score_gain * near_score

        htf_tags = htf_tags_normalized(cfg)
        score += float(cfg.score_pool_sv2_w) * pool_scoring_v2(
            up_pools if cand["side"] == "long" else dn_pools,
            float(cand["entry"]), cfg.w_htf_mult, cfg.proximity_window_bps, htf_tags,
        )

        cont = containment_15m_score(exec_df, cand["side"],
                                     (float(cand["fvg"]["gap_low"]), float(cand["fvg"]["gap_high"])), cfg.timeframe_exec)
        bos, choch = bos_choch_scores(exec_df, cand["side"])
        htf_bias = cached_htf_bias(ex, st, cfg, cfg.symbol)
        htf_ok = 1.0 if ((htf_bias > 0 and cand["side"] == "long") or (htf_bias < 0 and cand["side"] == "short")) else 0.0
        score += cfg.containment_15m_w * cont + cfg.bos_bonus_w * bos + cfg.choch_bonus_w * choch + cfg.htf_consensus_w * htf_ok

        dir_pools = (up_pools if cand["side"] == "long" else dn_pools)
        dir_pools_prices = [p for p, _, _ in dir_pools]
        pool_tag_near: Optional[str] = None
        if dir_pools_prices:
            direction_filter = [p for p in dir_pools_prices if (p > cand["entry"] if cand["side"] == "long" else p < cand["entry"]) ]
            pool_ref = (
                min(direction_filter, key=lambda x: abs(x - cand["entry"])) if direction_filter
                else min(dir_pools_prices, key=lambda x: abs(x - cand["entry"]))
            )
            try:
                for pr, _, tg in dir_pools:
                    if within_bps(pr, pool_ref, cfg.pool_cluster_bps):
                        pool_tag_near = tg
                        break
            except Exception:
                pool_tag_near = None
            dir_count = len(direction_filter) if direction_filter else len(dir_pools_prices)
        else:
            pool_ref = last_px
            dir_count = 0
            pool_tag_near = None
        sig_id = make_signal_id(SignalIdParams(
            side=cand["side"], fvg=cand["fvg"], pool_ref=pool_ref,
            bar_index=int(cand["fvg"]["bar_index"]), server_ms=st.last_server_ms,
            tick=st.tick, pool_count=dir_count,
            bin_ticks=getattr(cfg, "dedupe_bin_ticks", 10), tz_name=getattr(cfg, "session_timezone", "UTC"),
            pool_tag=pool_tag_near,
        ))
        now_min = now_epoch_minutes(st.last_server_ms)
        last_hit = st.dedupe_book.get(sig_id, -10**9)
        dedupe_ok = (now_min - last_hit) >= cfg.signal_cooldown_min
        debug_log(cfg, {"t": "score", "score": round(score, 4), "sig": sig_id, "now_min": now_min,
                        "last_hit": last_hit, "sess": list(sessions), "ahead_ct": len(ahead_prices),
                        "cont": cont, "bos": bos, "choch": choch, "htf": htf_ok, "adapt": adapt})
        if not dedupe_ok:
            print_json({"evt": "skip", "reason": "dedupe", "sig": sig_id, "ago_min": now_min - last_hit})
            try:
                metrics_block_inc(st, "dedupe")
            except Exception:
                pass
            time.sleep(cfg.loop_sleep)
            return

        block_fund, score_penalty, risk_mult_fund, fund_meta = funding_gate(ex, st, cfg)
        if block_fund:
            print_json({"evt":"skip","reason": fund_meta.get("reason","funding"), **fund_meta})
            try:
                metrics_block_inc(st, str(fund_meta.get("reason","funding")))
            except Exception:
                pass
            time.sleep(cfg.loop_sleep)
            return
        score -= score_penalty

        if score >= cfg.score_threshold:
            side = str(cand.get("side", "")).lower()
            if side not in ("long", "short"):
                time.sleep(cfg.loop_sleep)
                return
            if daily_guardrails_blocked(st, cfg):
                st.blocked_count += 1
                if st.blocked_count >= cfg.circuit_block_threshold:
                    time.sleep(cfg.circuit_sleep)
                print_json({"evt": "blocked", "reason": "daily_guardrails"})
                try:
                    metrics_block_inc(st, "daily_guardrails")
                except Exception:
                    pass
                debug_log(cfg, {"t": "blocked", "why": "daily"})
                time.sleep(cfg.loop_sleep)
                return
            else:
                st.blocked_count = 0
            if cooldown_active(st, cfg):
                st.blocked_count += 1
                if st.blocked_count >= cfg.circuit_block_threshold:
                    time.sleep(cfg.circuit_sleep)
                print_json({"evt": "blocked", "reason": "cooldown"})
                try:
                    metrics_block_inc(st, "cooldown")
                except Exception:
                    pass
                debug_log(cfg, {"t": "blocked", "why": "cooldown"})
                time.sleep(cfg.loop_sleep)
                return
            else:
                st.blocked_count = 0

            if cfg.use_available_balance:
                base_eq = ex.available_balance()
                eq = base_eq if base_eq and base_eq > 0.0 else balance_cached(ex, st, cfg)
            else:
                eq = balance_cached(ex, st, cfg)
            rp = throttle_risk(eq, st, cfg) * risk_mult_fund

            min_guard = cfg.min_stop_ticks * st.tick
            stop_price_raw = float(cand["stop"]) if math.isfinite(float(cand["stop"])) else float(cand["entry"])
            if side == "long":
                stop_price_raw = min(stop_price_raw, cand["entry"] - min_guard)
            else:
                stop_price_raw = max(stop_price_raw, cand["entry"] + min_guard)
            stop_price = side_round(stop_price_raw, st.tick, "buy" if side == "long" else "sell", favor="protection")
            if side == "long":
                stop_price = min(stop_price, cand["entry"] - min_guard)
            else:
                stop_price = max(stop_price, cand["entry"] + min_guard)

            eff_stop_dist = max(abs(cand["entry"] - stop_price), min_guard)
            risk_amt = rp * eq
            qty = round_qty_step(risk_amt / max(EPS, eff_stop_dist), st.step)

            try:
                if cfg.max_notional_per_trade > 0:
                    notional = qty * last_px
                    if notional > cfg.max_notional_per_trade * (1.0 + 1e-6):
                        print_json({"evt":"skip","reason":"notional_cap","n": round(notional,6)})
                        time.sleep(cfg.loop_sleep)
                        return
                if cfg.max_gross_usdt_exposure > 0:
                    exposures = ex.gross_stable_exposure_by_asset(st, cfg)
                    qa = ex._quote_asset(cfg.symbol)
                    gross_now = float(exposures.get(qa, 0.0))
                    if (gross_now + qty * last_px) > cfg.max_gross_usdt_exposure * (1.0 + 1e-6):
                        print_json({"evt":"skip","reason":"gross_cap","gross": round(gross_now,6)})
                        time.sleep(cfg.loop_sleep)
                        return
            except Exception:
                pass

            placed, cid = place_market_entry_idempotent(ex, cfg, side, qty, st.step)
            if not placed:
                time.sleep(cfg.loop_sleep)
                return

            try:
                fill_notional: Optional[float] = None
                if cid:
                    od = ex.get_order(cfg.symbol, orig_client_id=cid)
                    if od:
                        try:
                            avgp = float(od.get("avgPrice", 0.0))
                            exq = float(od.get("executedQty", 0.0))
                            cq = float(od.get("cumQuote", 0.0)) if od.get("cumQuote") is not None else 0.0
                            if avgp > 0 and exq > 0:
                                fill_notional = avgp * exq
                            elif cq > 0:
                                fill_notional = cq
                        except Exception:
                            pass
                if fill_notional is not None:
                    warn_cap = []
                    if cfg.max_notional_per_trade > 0 and fill_notional > cfg.max_notional_per_trade * (1.0 + 1e-6):
                        warn_cap.append("per_trade")
                    try:
                        exposures = ex.gross_stable_exposure_by_asset(st, cfg)
                        qa = ex._quote_asset(cfg.symbol)
                        gross_now = float(exposures.get(qa, 0.0))
                        if cfg.max_gross_usdt_exposure > 0 and gross_now > cfg.max_gross_usdt_exposure * (1.0 + 1e-6):
                            warn_cap.append("gross")
                    except Exception:
                        pass
                    if warn_cap:
                        print_json({"evt":"postfill_cap_exceeded","which":"/".join(warn_cap),"fill_notional": round(fill_notional,6)})
            except Exception:
                pass

            final_tp, partial_tp = choose_tp_aligned_tuned(
                cand["entry"], stop_price, side, adapt["rr"], up_pools, dn_pools, cfg, st.tick
            )
            try:
                stop_cid = _unique_cid("stop")
                ex.new_order(
                    symbol=cfg.symbol,
                    side="SELL" if side == "long" else "BUY",
                    type="STOP_MARKET",
                    stopPrice=_fmt_price(stop_price, st.tick),
                    closePosition=True,
                    workingType=cfg.working_type,
                    newClientOrderId=stop_cid,
                )
                with st.buf_lock:
                    st.last_trailing_client_id = stop_cid
                    st.last_trailing_sl = stop_price
                    st.last_trailing_side = side

                qty_p = (
                    round_qty_step(qty * max(0.0, min(1.0, cfg.partial_tp_frac)), st.step)
                    if partial_tp is not None else 0.0
                )
                qty_f = round_qty_step(max(qty - qty_p, 0.0), st.step)
                if qty_f > 0.0:
                    try:
                        resp = ex.new_order(
                            symbol=cfg.symbol,
                            side="SELL" if side == "long" else "BUY",
                            type="TAKE_PROFIT_MARKET",
                            stopPrice=_fmt_price(final_tp, st.tick),
                            reduceOnly=True,
                            quantity=_fmt_qty(qty_f, st.step),
                            workingType=cfg.working_type,
                        )
                        print_json({"evt":"tp_submit","which":"final","ok":True,"orderId":resp.get("orderId") if isinstance(resp, dict) else None})
                    except Exception as e_tp_f:
                        print_json({"evt":"tp_submit","which":"final","ok":False,"err":str(e_tp_f)})
                if partial_tp is not None and qty_p > 0.0:
                    try:
                        resp_p = ex.new_order(
                            symbol=cfg.symbol,
                            side="SELL" if side == "long" else "BUY",
                            type="TAKE_PROFIT_MARKET",
                            stopPrice=_fmt_price(partial_tp, st.tick),
                            reduceOnly=True,
                            quantity=_fmt_qty(qty_p, st.step),
                            workingType=cfg.working_type,
                        )
                        print_json({"evt":"tp_submit","which":"partial","ok":True,"orderId":resp_p.get("orderId") if isinstance(resp_p, dict) else None})
                    except Exception as e_tp_p:
                        print_json({"evt":"tp_submit","which":"partial","ok":False,"err":str(e_tp_p)})
                st.consecutive_order_errors = 0
            except requests.exceptions.RequestException as e:
                st.consecutive_order_errors += 1
                print(f"[protective_orders] {e}")
                debug_log(cfg, {"t": "error", "where": "protective", "err": str(e)})

            try:
                update_trailing_sl(st, ex, cfg, TrailParams(side=side, entry=cand["entry"], stop=stop_price, current_price=last_px, exec_df=exec_df))
            except requests.exceptions.RequestException as e:
                print(f"[trail_init] {e}")
                debug_log(cfg, {"t": "error", "where": "trail_init", "err": str(e)})

            st.dedupe_book[sig_id] = now_min
            evt = {
                "evt": "entry", "side": side, "qty": qty,
                "entry_ref": round(cand["entry"], 6), "stop": round(stop_price, 6),
                "tp_final": round(final_tp, 6), "tp_partial": (round(partial_tp, 6) if partial_tp is not None else None),
                "score": round(score, 3), "sig": sig_id,
            }
            print_json(evt)
            debug_log(cfg, {"t": "entry", **evt, "ws_fresh": ws_fresh})
        else:
            print_json({"evt": "skip", "reason": "score_below", "score": round(score, 3)})
            try:
                metrics_block_inc(st, "score_below")
            except Exception:
                pass
            debug_log(cfg, {"t": "skip", "why": "score", "score": float(round(score, 5))})

    if st.loop_idx % max(1, cfg.heartbeat_every_n_loops) == 0:
        flush_blocked_metrics_if_due(cfg, st)
        hb = {
            "evt": "heartbeat",
            "trades_today": st.trades_today,
            "realized_today": round(st.realized_today, 3),
            "server_time_offset_ms": st.last_server_offset_ms,
            "last_candle": int(exec_df.iloc[-1]["time"]),
            "ws_fresh": ws_fresh,
            "bid": st.last_bid,
            "ask": st.last_ask,
            "safe_mode": st.safe_mode,
            "safe_reason": st.safe_reason,
            "ws_reconnects_market": st.ws_reconnects_market,
            "ws_reconnects_uds": st.ws_reconnects_uds,
        }
        print_json(hb)
        debug_log(cfg, {"t": "hb", **hb})
        if time.time() - st.last_metrics_flush_ts > 60:
            flush_blocked_metrics_if_due(cfg, st)
            metrics_emit(cfg, st, {
                "t":"hb",
                "ts": now_ms(),
                "safe": int(st.safe_mode),
                "reco_mkt": st.ws_reconnects_market,
                "reco_uds": st.ws_reconnects_uds,
                "drift": st.last_server_offset_ms if st.last_server_offset_ms is not None else 0
            })
            st.last_metrics_flush_ts = time.time()

    if st.loop_idx % 5 == 0:
        st.dedupe_book = _trim_dedupe_book(st.dedupe_book, keep_latest=400)
        save_state(st, cfg.persist_state_path)

def main():
    cfg = Config()

    # Optional: namespace default file paths by symbol to avoid collisions in multi-bot runs
    try:
        sym = (cfg.symbol or "").upper()
        if cfg.persist_state_path == "state/state.json":
            cfg.persist_state_path = f"state/state_{sym}.json"
        if cfg.metrics_csv_path == "state/metrics.csv":
            cfg.metrics_csv_path = f"state/metrics_{sym}.csv"
        if cfg.debug_jsonl_path == "bot_debug.jsonl":
            cfg.debug_jsonl_path = f"bot_debug_{sym}.jsonl"
    except Exception:
        pass

    # Early API key/secret validation
    if not cfg.api_key or not cfg.api_secret:
        raise RuntimeError("BINANCE_API_KEY and BINANCE_API_SECRET must be set for signed endpoints (balance, orders).")

    # Warn only for truly mismatched REST/WS envs:
    # - Futures Testnet REST: https://testnet.binancefuture.com
    # - Futures Testnet WS:   wss://stream.binancefuture.com
    # - Futures PROD WS:      wss://fstream.binance.com
    base_is_testnet = "testnet.binancefuture.com" in cfg.base_url
    ws_is_testnet = "stream.binancefuture.com" in cfg.ws_base
    ws_is_prod = "fstream.binance.com" in cfg.ws_base
    if base_is_testnet and ws_is_prod:
        print("[WARN] REST is testnet but WS base looks PROD (wss://fstream.binance.com). Use testnet WS wss://stream.binancefuture.com.")
    if (not base_is_testnet) and ws_is_testnet:
        print("[WARN] REST looks PROD but WS base is TESTNET. Verify endpoints.")

    ex = BinanceUM(cfg)
    st = RunState()
    init_instrument(ex, st, cfg)
    ws = WSManager(cfg, st, ex)
    ws.start()

    # restore persisted day state (incl. dedupe_book)
    persisted = load_state(cfg.persist_state_path)
    if persisted:
        st.day_start_date = persisted.get("day_start_date", st.day_start_date)
        st.day_start_equity = float(persisted.get("day_start_equity", st.day_start_equity))
        st.trades_today = int(persisted.get("trades_today", 0))
        st.realized_today = float(persisted.get("realized_today", 0.0))
        ded = persisted.get("dedupe_book", {})
        if isinstance(ded, dict):
            try:
                st.dedupe_book = {str(k): int(v) for k, v in ded.items()}
            except Exception:
                st.dedupe_book = {}
        # Restore cumulative realized baselines (UDS 'cr') when available
        acr = persisted.get("account_cr")
        if isinstance(acr, dict):
            try:
                st.account_cr = {str(k): float(v) for k, v in acr.items()}
            except Exception:
                st.account_cr = dict(st.account_cr)

    # Sanity checks & reconciliation
    try:
        sanity_checks(ex, cfg)
    except requests.exceptions.RequestException as e:
        # downgrade to safe mode instead of aborting
        st.safe_mode = True
        st.safe_reason = f"sanity:{str(e)}"
    reconcile_startup_orders(ex, st, cfg)

    st.last_info_refresh_ts = time.time()
    last_time_poll = 0.0

    # Initial drift sample before loops; may engage safe-mode if extreme
    update_drift_policy(ex, st, cfg, force=True)

    while True:
        try:
            st.loop_idx += 1
            last_time_poll = poll_maintenance(ex, st, cfg, ws, last_time_poll)
            fetched = fetch_data_and_guard(ex, st, cfg)
            if fetched is None:
                time.sleep(cfg.loop_sleep)
                continue
            exec_kl, exec_df, ws_fresh = fetched
            process_trade_opportunities(ex, st, cfg, exec_kl, exec_df, ws_fresh)
        except Exception as e:
            print(f"[loop] {e}\n{traceback.format_exc()}")
            debug_log(cfg, {"t": "error", "where": "loop", "err": str(e)})
            time.sleep(cfg.loop_sleep)
        time.sleep(cfg.loop_sleep)
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Bye.")




