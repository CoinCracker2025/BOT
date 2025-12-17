# paid_runners_bot.py (V8.0.clean)
# DexScreener paid runners scanner for Solana
# - Candidates: token-boosts (paid boosts), ads (sponsored), CTO (community takeovers)
# - Enrichment: token profiles + paid orders (dex paid)
# - Pair data: uses official /tokens/v1 (batch) and /token-pairs/v1 (fallback) endpoints
#
# Notes:
# - Designed to be called from app.py via run_scan_for_modes(...)
# - Rate limits (per DexScreener docs): 60 rpm for boosts/profiles/ads/cto/orders; 300 rpm for pairs endpoints.
#
# Official DexScreener API docs:
#   https://docs.dexscreener.com/api/reference

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import math
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEX_BASE = "https://api.dexscreener.com"
CHAIN_ID = "solana"

DEFAULT_TIMEOUT_S = 12
DEFAULT_USER_AGENT = "BotTrading/paid_runners_bot (contact: local)"


# =============================================================================
# Helpers
# =============================================================================

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return default
        # handle commas in some locales
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            return int(x)
        s = str(x).strip()
        if not s:
            return default
        # sometimes values like "12.0"
        return int(float(s))
    except Exception:
        return default


def _now_utc_ms() -> int:
    return int(time.time() * 1000)


def _age_minutes_from_pair_created_at(pair_created_at: Any) -> float:
    # DexScreener usually returns pairCreatedAt as ms timestamp
    ts = _safe_int(pair_created_at, 0)
    if ts <= 0:
        return 999999.0
    age_ms = max(0, _now_utc_ms() - ts)
    return age_ms / 60000.0


def _chunks(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.35,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s.headers.update({"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


_SESS = _session()


def _http_get_json(url: str, *, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[Any, Dict[str, Any]]:
    debug: Dict[str, Any] = {"url": url}
    try:
        r = _SESS.get(url, timeout=timeout_s)
        debug["status"] = r.status_code
        debug["elapsed_s"] = getattr(r, "elapsed", None).total_seconds() if getattr(r, "elapsed", None) else None
        if r.status_code >= 400:
            debug["text_snippet"] = (r.text or "")[:200]
            return None, debug
        return r.json(), debug
    except Exception as e:
        debug["error"] = repr(e)
        return None, debug


def _normalize_chain_id(chain_id: Any) -> str:
    c = str(chain_id or "").strip().lower()
    if c in ("sol", "solana-mainnet", "mainnet", "sol-mainnet"):
        return "solana"
    return c


# =============================================================================
# API wrappers (DexScreener)
# =============================================================================

def fetch_token_boosts_latest(*, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = f"{DEX_BASE}/token-boosts/latest/v1"
    data, dbg = _http_get_json(url, timeout_s=timeout_s)
    if isinstance(data, dict):
        # docs show "Response object"; in practice may be { "data": [...] } or { "boosts": [...] }
        for k in ("data", "boosts", "tokens", "results"):
            if isinstance(data.get(k), list):
                return data[k], dbg
    if isinstance(data, list):
        return data, dbg
    return [], dbg


def fetch_token_boosts_top(*, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = f"{DEX_BASE}/token-boosts/top/v1"
    data, dbg = _http_get_json(url, timeout_s=timeout_s)
    if isinstance(data, dict):
        for k in ("data", "boosts", "tokens", "results"):
            if isinstance(data.get(k), list):
                return data[k], dbg
    if isinstance(data, list):
        return data, dbg
    return [], dbg


def fetch_ads_latest(*, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = f"{DEX_BASE}/ads/latest/v1"
    data, dbg = _http_get_json(url, timeout_s=timeout_s)
    if isinstance(data, list):
        return data, dbg
    if isinstance(data, dict):
        for k in ("data", "ads", "results"):
            if isinstance(data.get(k), list):
                return data[k], dbg
    return [], dbg


def fetch_token_profiles_latest(*, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = f"{DEX_BASE}/token-profiles/latest/v1"
    data, dbg = _http_get_json(url, timeout_s=timeout_s)
    if isinstance(data, list):
        return data, dbg
    if isinstance(data, dict):
        # docs show single object, but in practice might be {data:[...]}
        for k in ("data", "profiles", "results"):
            if isinstance(data.get(k), list):
                return data[k], dbg
        # single profile object
        if isinstance(data.get("tokenAddress"), str) and isinstance(data.get("chainId"), str):
            return [data], dbg
    return [], dbg


def fetch_community_takeovers_latest(*, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = f"{DEX_BASE}/community-takeovers/latest/v1"
    data, dbg = _http_get_json(url, timeout_s=timeout_s)
    if isinstance(data, list):
        return data, dbg
    if isinstance(data, dict):
        for k in ("data", "results", "ctos"):
            if isinstance(data.get(k), list):
                return data[k], dbg
    return [], dbg


def fetch_orders_for_token(chain_id: str, token_address: str, *, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    chain_id = _normalize_chain_id(chain_id)
    url = f"{DEX_BASE}/orders/v1/{chain_id}/{token_address}"
    data, dbg = _http_get_json(url, timeout_s=timeout_s)
    if isinstance(data, list):
        return data, dbg
    if isinstance(data, dict):
        for k in ("orders", "data", "results"):
            if isinstance(data.get(k), list):
                return data[k], dbg
    return [], dbg


def fetch_pairs_for_tokens_batch(chain_id: str, token_addresses: List[str], *, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Uses /tokens/v1/{chainId}/{tokenAddresses} (up to 30 token addresses, comma-separated).
    Returns a flat list of pair objects.
    """
    chain_id = _normalize_chain_id(chain_id)
    # Keep unique, preserve order
    seen: set[str] = set()
    addrs = []
    for a in token_addresses:
        aa = (a or "").strip()
        if not aa:
            continue
        if aa.lower() in seen:
            continue
        seen.add(aa.lower())
        addrs.append(aa)

    if not addrs:
        return [], {"note": "empty_token_addresses"}

    url = f"{DEX_BASE}/tokens/v1/{chain_id}/" + ",".join(addrs)
    data, dbg = _http_get_json(url, timeout_s=timeout_s)
    if isinstance(data, list):
        return data, dbg
    if isinstance(data, dict):
        for k in ("pairs", "data", "results"):
            if isinstance(data.get(k), list):
                return data[k], dbg
    return [], dbg


def fetch_token_pairs_fallback(chain_id: str, token_address: str, *, timeout_s: int = DEFAULT_TIMEOUT_S) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Uses /token-pairs/v1/{chainId}/{tokenAddress} for single token.
    """
    chain_id = _normalize_chain_id(chain_id)
    url = f"{DEX_BASE}/token-pairs/v1/{chain_id}/{token_address}"
    data, dbg = _http_get_json(url, timeout_s=timeout_s)
    if isinstance(data, list):
        return data, dbg
    if isinstance(data, dict):
        for k in ("pairs", "data", "results"):
            if isinstance(data.get(k), list):
                return data[k], dbg
    return [], dbg


# =============================================================================
# Scoring / filtering
# =============================================================================

def compute_metrics_from_pair(pair: Dict[str, Any]) -> Dict[str, Any]:
    liq_usd = _safe_float((pair.get("liquidity") or {}).get("usd"))
    fdv = _safe_float(pair.get("fdv"))
    mcap = _safe_float(pair.get("marketCap"))
    vol = pair.get("volume") or {}
    vol5 = _safe_float(vol.get("m5"))
    vol1 = _safe_float(vol.get("h1"))
    vol24 = _safe_float(vol.get("h24"))
    txns = pair.get("txns") or {}
    m5 = txns.get("m5") or {}
    buys5 = _safe_int(m5.get("buys"))
    sells5 = _safe_int(m5.get("sells"))
    net_buy_5m = buys5 - sells5

    pc = pair.get("priceChange") or {}
    m5pct = _safe_float(pc.get("m5"))
    h1pct = _safe_float(pc.get("h1"))
    h6pct = _safe_float(pc.get("h6"))
    h24pct = _safe_float(pc.get("h24"))

    age_min = _age_minutes_from_pair_created_at(pair.get("pairCreatedAt"))
    turnover_1h_over_liq = (vol1 / liq_usd) if liq_usd > 0 else 0.0

    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}

    return {
        "pairAddress": pair.get("pairAddress") or "",
        "dexId": pair.get("dexId") or "",
        "priceUsd": pair.get("priceUsd"),
        "liquidityUsd": liq_usd,
        "fdv": fdv,
        "marketCap": mcap,
        "vol5m": vol5,
        "vol1h": vol1,
        "vol24h": vol24,
        "buys5m": buys5,
        "sells5m": sells5,
        "netBuy5m": net_buy_5m,
        "m5pct": m5pct,
        "h1pct": h1pct,
        "h6pct": h6pct,
        "h24pct": h24pct,
        "ageMin": age_min,
        "turnover_1h_over_liq": turnover_1h_over_liq,
        "baseTokenSymbol": base.get("symbol"),
        "baseTokenName": base.get("name"),
        "baseTokenAddress": base.get("address"),
        "quoteTokenSymbol": quote.get("symbol"),
        "quoteTokenAddress": quote.get("address"),
        "urlDexscreener": f"https://dexscreener.com/solana/{pair.get('pairAddress')}" if pair.get("pairAddress") else "",
    }


def score_pair(row: Dict[str, Any], *, pump_mode: bool) -> Dict[str, Any]:
    """
    Returns {score, spike_score}. Heuristics:
    - spike_score: weighted momentum + flow + turnover
    - score: liquidity/volume sanity + spike_score + (small) boost for m5 positive
    """
    liq = _safe_float(row.get("liquidityUsd"))
    vol5 = _safe_float(row.get("vol5m"))
    vol1 = _safe_float(row.get("vol1h"))
    netbuy = _safe_int(row.get("netBuy5m"))
    m5pct = _safe_float(row.get("m5pct"))
    h1pct = _safe_float(row.get("h1pct"))
    turnover = _safe_float(row.get("turnover_1h_over_liq"))

    # Normalize components to keep score stable
    liq_s = math.log10(1.0 + max(liq, 0.0))
    vol_s = math.log10(1.0 + max(vol1, 0.0))
    flow_s = math.log10(1.0 + max(netbuy, 0))

    # Momentum emphasis: in pump_mode, give more weight to m5
    mom = (m5pct * (1.15 if pump_mode else 0.85)) + (h1pct * 0.35)

    spike_score = (
        0.55 * max(0.0, mom / 10.0) +
        0.30 * min(2.5, turnover * 12.0) +
        0.25 * min(2.0, flow_s / 2.0)
    )

    # Base score: sanity check plus spike
    score = (
        0.30 * liq_s +
        0.25 * vol_s +
        1.15 * spike_score +
        (0.15 if m5pct > 0 else 0.0)
    )

    return {"score": float(score), "spike_score": float(spike_score)}


def _mode_thresholds(mode: str, *, pump_mode: bool) -> Dict[str, float]:
    """
    Thresholds are designed for Solana microcaps.
    Values are intentionally permissive in pump_mode.
    """
    mode = (mode or "").strip().lower()

    if mode == "ultra_early":
        return {
            "min_liq": 2500.0 if pump_mode else 4000.0,
            "min_vol5": 40.0 if pump_mode else 80.0,
            "min_netbuy5": 0.0 if pump_mode else 1.0,
            "max_age_min": 240.0,
            "min_score": 0.15,
        }
    if mode == "early_strict":
        return {
            "min_liq": 6000.0 if pump_mode else 12000.0,
            "min_vol5": 120.0 if pump_mode else 250.0,
            "min_netbuy5": 1.0 if pump_mode else 2.0,
            "max_age_min": 24.0 * 60.0,
            "min_score": 0.45,
        }
    if mode == "early":
        return {
            "min_liq": 3500.0 if pump_mode else 8000.0,
            "min_vol5": 80.0 if pump_mode else 160.0,
            "min_netbuy5": 0.0 if pump_mode else 1.0,
            "max_age_min": 24.0 * 60.0,
            "min_score": 0.25,
        }
    if mode == "strict":
        return {
            "min_liq": 15000.0 if pump_mode else 25000.0,
            "min_vol5": 250.0 if pump_mode else 500.0,
            "min_netbuy5": 2.0 if pump_mode else 3.0,
            "max_age_min": 7.0 * 24.0 * 60.0,
            "min_score": 0.85,
        }
    # default: degen
    return {
        "min_liq": 2500.0 if pump_mode else 5000.0,
        "min_vol5": 60.0 if pump_mode else 120.0,
        "min_netbuy5": 0.0 if pump_mode else 1.0,
        "max_age_min": 7.0 * 24.0 * 60.0,
        "min_score": 0.20,
    }


def _anti_dead_pass(row: Dict[str, Any], opts: "ScanOptions") -> bool:
    """
    Quick prefilter to avoid completely dead pairs.
    Crucially, it respects the user's trending thresholds (and pump_mode).
    """
    liq = _safe_float(row.get("liquidityUsd"))
    vol5 = _safe_float(row.get("vol5m"))
    buys5 = _safe_int(row.get("buys5m"))
    sells5 = _safe_int(row.get("sells5m"))
    age_min = _safe_float(row.get("ageMin"), 999999.0)

    # Keep recent tokens even with low metrics
    if age_min <= (90.0 if opts.pump_mode else 60.0):
        return True

    # Liquidity guard aligned with opts
    liq_floor = max(1200.0, opts.trending_min_liquidity * (0.75 if opts.pump_mode else 0.85))
    if liq < liq_floor:
        return False

    # Activity guard aligned with opts
    txns5 = buys5 + sells5
    vol_floor = max(40.0, opts.trending_min_vol5m * (0.75 if opts.pump_mode else 0.85))
    if txns5 < (1 if opts.pump_mode else 2) and vol5 < vol_floor:
        return False

    # Avoid very old stagnating tokens
    if age_min > (10 * 24 * 60):
        return False

    return True


def _trending_reasons(row: Dict[str, Any], opts: "ScanOptions") -> List[str]:
    """
    Returns reasons of rejection (empty list = passes).
    The idea is to be conservative against "paid but dead" tokens.
    """
    reasons: List[str] = []

    liq = _safe_float(row.get("liquidityUsd"))
    vol1 = _safe_float(row.get("vol1h"))
    vol5 = _safe_float(row.get("vol5m"))
    netbuy5 = _safe_int(row.get("netBuy5m"))
    spike_score = _safe_float(row.get("spike_score"))
    turnover = _safe_float(row.get("turnover_1h_over_liq"))
    m5pct = _safe_float(row.get("m5pct"))
    boost_amt = _safe_float(row.get("boostAmount"))

    if liq < opts.trending_min_liquidity:
        reasons.append(f"low_liq:{liq:.0f}")

    if (vol1 < opts.trending_min_vol1h) and (vol5 < opts.trending_min_vol5m):
        reasons.append(f"low_vol:1h={vol1:.0f},5m={vol5:.0f}")

    if netbuy5 < opts.trending_min_netbuy5m:
        reasons.append(f"low_netbuy:{netbuy5}")

    # Minimum spike requirement (if configured)
    if spike_score < opts.spike_score_min:
        reasons.append(f"low_spike:{spike_score:.3f}")

    # Paid promotion sanity: boosted but no real flow/turnover
    if boost_amt > 0 and netbuy5 <= 0 and turnover < 0.01 and spike_score < max(opts.spike_score_min, 0.40):
        reasons.append("promoted_no_real_buy")

    # If m5 is negative, keep only if spike is strong (avoid catching the top too often)
    if m5pct <= 0.0 and spike_score < max(opts.spike_score_min, 0.55):
        reasons.append("m5_non_positive")

    return reasons


# =============================================================================
# Main scan
# =============================================================================

@dataclass
class ScanOptions:
    selected_modes: List[str]
    top_n: int
    candidates_max: int
    anti_dead: bool
    include_boosts: bool
    include_profiles: bool
    include_cto: bool
    include_ads: bool
    include_orders: bool
    unique_per_token: bool
    trending_filters: bool
    trending_min_liquidity: float
    trending_min_vol1h: float
    trending_min_vol5m: float
    trending_min_netbuy5m: int
    spike_score_min: float
    verbose_debug: bool
    pump_mode: bool
    sort_by_spike: bool
    progress_callback: Optional[Callable[[int, int], None]] = None
    chain_id: str = CHAIN_ID


def _index_by_token(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for it in items:
        ta = (it.get("tokenAddress") or it.get("token_address") or "").strip().lower()
        if not ta:
            continue
        idx[ta] = it
    return idx


def _merge_candidate(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge src into dst, preferring existing strong fields but keeping the maximum boost values.
    """
    if not dst:
        return dict(src)

    out = dict(dst)

    # Merge sources
    s1 = out.get("sources") or []
    if isinstance(s1, str):
        s1 = [s1]
    s2 = src.get("sources") or []
    if isinstance(s2, str):
        s2 = [s2]
    for s in (src.get("source"), *s2):
        if s and s not in s1:
            s1.append(s)
    out["sources"] = s1

    # Priority for primary source
    priority = {"boosts": 3, "ads": 2, "cto": 1, "profiles": 0}
    src_primary = src.get("source")
    dst_primary = out.get("source")
    if priority.get(str(src_primary), 0) > priority.get(str(dst_primary), 0):
        out["source"] = src_primary

    # Boost numbers (max)
    out["boostAmount"] = max(_safe_float(out.get("boostAmount")), _safe_float(src.get("boostAmount")))
    out["boostTotal"] = max(_safe_float(out.get("boostTotal")), _safe_float(src.get("boostTotal")))
    out["boostType"] = out.get("boostType") or src.get("boostType")

    # CTO
    out["isCTO"] = bool(out.get("isCTO")) or bool(src.get("isCTO"))

    # Profile
    for k in ("profileUrl", "profileDescription", "profileLinksCount", "icon", "header"):
        if not out.get(k) and src.get(k):
            out[k] = src[k]

    return out


def _candidate_from_boost_item(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    chain = _normalize_chain_id(it.get("chainId"))
    if chain != CHAIN_ID:
        return None
    ta = (it.get("tokenAddress") or "").strip()
    if not ta:
        return None

    # field names vary, keep robust
    amount = it.get("amount", it.get("boostAmount", it.get("activeBoosts", 0)))
    total = it.get("totalAmount", it.get("boostTotal", it.get("totalBoosts", amount)))
    btype = it.get("type", it.get("boostType"))

    return {
        "chainId": chain,
        "tokenAddress": ta,
        "source": "boosts",
        "sources": ["boosts"],
        "boostAmount": _safe_float(amount),
        "boostTotal": _safe_float(total),
        "boostType": btype,
        "profileUrl": it.get("url") or "",
    }


def _candidate_from_ad_item(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    chain = _normalize_chain_id(it.get("chainId"))
    if chain != CHAIN_ID:
        return None
    ta = (it.get("tokenAddress") or "").strip()
    if not ta:
        return None
    return {
        "chainId": chain,
        "tokenAddress": ta,
        "source": "ads",
        "sources": ["ads"],
        "adType": it.get("type"),
        "adDate": it.get("date"),
        "adDurationHours": it.get("durationHours"),
        "profileUrl": it.get("url") or "",
    }


def _candidate_from_cto_item(it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    chain = _normalize_chain_id(it.get("chainId"))
    if chain != CHAIN_ID:
        return None
    ta = (it.get("tokenAddress") or "").strip()
    if not ta:
        return None
    return {
        "chainId": chain,
        "tokenAddress": ta,
        "source": "cto",
        "sources": ["cto"],
        "isCTO": True,
        "profileUrl": it.get("url") or "",
        "profileDescription": it.get("description") or "",
        "profileLinksCount": len(it.get("links") or []) if isinstance(it.get("links"), list) else 0,
    }


def run_scan_for_modes(
    *,
    selected_modes: List[str],
    top_n: int = 20,
    candidates_max: int = 250,
    anti_dead: bool = True,
    include_boosts: bool = True,
    include_profiles: bool = True,
    include_cto: bool = True,
    include_ads: bool = False,
    include_orders: bool = False,
    unique_per_token: bool = True,
    trending_filters: bool = True,
    trending_min_liquidity: float = 15000.0,
    trending_min_vol1h: float = 1000.0,
    trending_min_vol5m: float = 500.0,
    trending_min_netbuy5m: int = 2,
    spike_score_min: float = 0.25,
    verbose_debug: bool = False,
    pump_mode: bool = False,
    sort_by_spike: bool = True,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Main entry point used by app.py.

    Returns: (runners_rows, debug_dict)
    """

    opts = ScanOptions(
        selected_modes=[(m or "").strip().lower() for m in (selected_modes or [])],
        top_n=int(top_n),
        candidates_max=int(candidates_max),
        anti_dead=bool(anti_dead),
        include_boosts=bool(include_boosts),
        include_profiles=bool(include_profiles),
        include_cto=bool(include_cto),
        include_ads=bool(include_ads),
        include_orders=bool(include_orders),
        unique_per_token=bool(unique_per_token),
        trending_filters=bool(trending_filters),
        trending_min_liquidity=float(trending_min_liquidity),
        trending_min_vol1h=float(trending_min_vol1h),
        trending_min_vol5m=float(trending_min_vol5m),
        trending_min_netbuy5m=int(trending_min_netbuy5m),
        spike_score_min=float(spike_score_min),
        verbose_debug=bool(verbose_debug),
        pump_mode=bool(pump_mode),
        sort_by_spike=bool(sort_by_spike),
        progress_callback=progress_callback,
        chain_id=CHAIN_ID,
    )

    debug: Dict[str, Any] = {
        "ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "opts": {
            "selected_modes": opts.selected_modes,
            "top_n": opts.top_n,
            "candidates_max": opts.candidates_max,
            "anti_dead": opts.anti_dead,
            "include_boosts": opts.include_boosts,
            "include_profiles": opts.include_profiles,
            "include_cto": opts.include_cto,
            "include_ads": opts.include_ads,
            "include_orders": opts.include_orders,
            "unique_per_token": opts.unique_per_token,
            "trending_filters": opts.trending_filters,
            "trending_min_liquidity": opts.trending_min_liquidity,
            "trending_min_vol1h": opts.trending_min_vol1h,
            "trending_min_vol5m": opts.trending_min_vol5m,
            "trending_min_netbuy5m": opts.trending_min_netbuy5m,
            "spike_score_min": opts.spike_score_min,
            "verbose_debug": opts.verbose_debug,
            "pump_mode": opts.pump_mode,
            "sort_by_spike": opts.sort_by_spike,
        },
        "api_debug": {},
    }

    errors: List[str] = []
    why_filtered_list: List[Dict[str, Any]] = []

    # -------------------------------------------------------------------------
    # 1) Collect paid candidates: Boosts, Ads, CTO
    # -------------------------------------------------------------------------
    candidates_by_token: Dict[str, Dict[str, Any]] = {}

    boosts_items: List[Dict[str, Any]] = []
    if opts.include_boosts:
        # Always include latest boosts; optionally merge with "top" for visibility
        latest, dbg_latest = fetch_token_boosts_latest()
        debug["api_debug"]["token_boosts_latest"] = dbg_latest
        boosts_items.extend(latest)

        if opts.pump_mode:
            top, dbg_top = fetch_token_boosts_top()
            debug["api_debug"]["token_boosts_top"] = dbg_top
            boosts_items.extend(top)

    debug["counts"] = {"boost_items_raw": len(boosts_items)}

    for it in boosts_items:
        c = _candidate_from_boost_item(it)
        if not c:
            continue
        key = c["tokenAddress"].lower()
        candidates_by_token[key] = _merge_candidate(candidates_by_token.get(key, {}), c)

    if opts.include_ads:
        ads_items, dbg_ads = fetch_ads_latest()
        debug["api_debug"]["ads_latest"] = dbg_ads
        debug["counts"]["ads_items_raw"] = len(ads_items)
        for it in ads_items:
            c = _candidate_from_ad_item(it)
            if not c:
                continue
            key = c["tokenAddress"].lower()
            candidates_by_token[key] = _merge_candidate(candidates_by_token.get(key, {}), c)
    else:
        debug["counts"]["ads_items_raw"] = 0

    cto_items: List[Dict[str, Any]] = []
    if opts.include_cto:
        cto_items, dbg_cto = fetch_community_takeovers_latest()
        debug["api_debug"]["cto_latest"] = dbg_cto
        debug["counts"]["cto_items_raw"] = len(cto_items)
        for it in cto_items:
            c = _candidate_from_cto_item(it)
            if not c:
                continue
            key = c["tokenAddress"].lower()
            candidates_by_token[key] = _merge_candidate(candidates_by_token.get(key, {}), c)
    else:
        debug["counts"]["cto_items_raw"] = 0

    # If boosts disabled and ads disabled and cto disabled, we will have no candidates
    # This is expected.
    candidates = list(candidates_by_token.values())

    # Cut to candidates_max (prefer boosted > ads > cto)
    def _cand_priority(c: Dict[str, Any]) -> Tuple[int, float]:
        src = str(c.get("source") or "")
        p = {"boosts": 3, "ads": 2, "cto": 1}.get(src, 0)
        return (p, _safe_float(c.get("boostAmount")))

    candidates.sort(key=_cand_priority, reverse=True)
    candidates = candidates[: max(1, opts.candidates_max)]

    debug["counts"]["candidates_unique"] = len(candidates)

    if not candidates:
        debug["why"] = "no_candidates"
        return [], debug

    # -------------------------------------------------------------------------
    # 2) Profiles enrichment
    # -------------------------------------------------------------------------
    profiles_idx: Dict[str, Dict[str, Any]] = {}
    if opts.include_profiles:
        profiles, dbg_profiles = fetch_token_profiles_latest()
        debug["api_debug"]["profiles_latest"] = dbg_profiles
        debug["counts"]["profiles_raw"] = len(profiles)
        for p in profiles:
            chain = _normalize_chain_id(p.get("chainId"))
            if chain != CHAIN_ID:
                continue
            ta = (p.get("tokenAddress") or "").strip().lower()
            if not ta:
                continue
            profiles_idx[ta] = {
                "profileUrl": p.get("url") or "",
                "profileDescription": p.get("description") or "",
                "profileLinksCount": len(p.get("links") or []) if isinstance(p.get("links"), list) else 0,
                "icon": p.get("icon") or "",
                "header": p.get("header") or "",
            }
    else:
        debug["counts"]["profiles_raw"] = 0

    for c in candidates:
        ta = (c.get("tokenAddress") or "").strip().lower()
        if not ta:
            continue
        if ta in profiles_idx:
            c = _merge_candidate(c, profiles_idx[ta])
            candidates_by_token[ta] = c  # keep map in sync

    # -------------------------------------------------------------------------
    # 3) Fetch pairs data in batches (official /tokens/v1, up to 30 addresses)
    # -------------------------------------------------------------------------
    token_addrs = [c.get("tokenAddress") for c in candidates if c.get("tokenAddress")]
    token_addrs = [str(a).strip() for a in token_addrs if str(a).strip()]

    all_pairs: List[Dict[str, Any]] = []
    api_pairs_debug: List[Dict[str, Any]] = []
    for ch in _chunks(token_addrs, 30):
        pairs_chunk, dbg_pairs = fetch_pairs_for_tokens_batch(CHAIN_ID, ch)
        api_pairs_debug.append(dbg_pairs)
        if pairs_chunk:
            all_pairs.extend(pairs_chunk)

    debug["api_debug"]["pairs_batch_calls"] = api_pairs_debug
    debug["counts"]["pairs_returned"] = len(all_pairs)

    # Group pairs by token address (base or quote)
    pairs_by_token: Dict[str, List[Dict[str, Any]]] = {}
    for p in all_pairs:
        base = (p.get("baseToken") or {}).get("address") or ""
        quote = (p.get("quoteToken") or {}).get("address") or ""
        base_l = str(base).strip().lower()
        quote_l = str(quote).strip().lower()
        if base_l:
            pairs_by_token.setdefault(base_l, []).append(p)
        if quote_l:
            pairs_by_token.setdefault(quote_l, []).append(p)

    # Fallback for tokens missing from batch result (rare but happens)
    missing = [a for a in token_addrs if a.lower() not in pairs_by_token]
    debug["counts"]["tokens_missing_from_batch"] = len(missing)

    # Only fallback for a small number to avoid hammering the API
    for a in missing[: min(15, len(missing))]:
        pairs_fb, dbg_fb = fetch_token_pairs_fallback(CHAIN_ID, a)
        if dbg_fb.get("status") and dbg_fb.get("status") >= 400:
            errors.append(f"pairs_fallback {a[:6]}.. status={dbg_fb.get('status')}")
        if pairs_fb:
            pairs_by_token.setdefault(a.lower(), []).extend(pairs_fb)

    # -------------------------------------------------------------------------
    # 4) Build best pair per token, compute metrics and score
    # -------------------------------------------------------------------------
    rows: List[Dict[str, Any]] = []
    done = 0
    total = len(token_addrs)

    for token_addr in token_addrs:
        ta_l = token_addr.lower()
        pairs = pairs_by_token.get(ta_l) or []

        if not pairs:
            if opts.verbose_debug:
                why_filtered_list.append(
                    {"tokenAddress": token_addr, "reason": "no_pairs_returned"}
                )
            done += 1
            if opts.progress_callback:
                opts.progress_callback(done, total)
            continue

        # pick best pair by liquidity then vol24
        best = None
        best_key = (-1.0, -1.0)
        for p in pairs:
            liq = _safe_float((p.get("liquidity") or {}).get("usd"))
            vol24 = _safe_float((p.get("volume") or {}).get("h24"))
            key = (liq, vol24)
            if key > best_key:
                best_key = key
                best = p

        if not best:
            done += 1
            if opts.progress_callback:
                opts.progress_callback(done, total)
            continue

        base = (best.get("baseToken") or {})
        quote = (best.get("quoteToken") or {})
        symbol = base.get("symbol") or ""
        name = base.get("name") or ""

        # If token is quote token, symbol/name might be wrong; fix by checking addresses
        if str(base.get("address") or "").strip().lower() != ta_l and str(quote.get("address") or "").strip().lower() == ta_l:
            symbol = quote.get("symbol") or symbol
            name = quote.get("name") or name

        base_row = {
            "tokenAddress": token_addr,
            "symbol": symbol,
            "name": name,
        }

        base_row.update(compute_metrics_from_pair(best))

        # Merge candidate enrichment fields
        cand = candidates_by_token.get(ta_l, {})
        base_row["source"] = cand.get("source") or ""
        base_row["sources"] = cand.get("sources") or [base_row["source"]] if base_row["source"] else []
        base_row["boostAmount"] = _safe_float(cand.get("boostAmount"))
        base_row["boostTotal"] = _safe_float(cand.get("boostTotal"))
        base_row["boostType"] = cand.get("boostType")
        base_row["isCTO"] = bool(cand.get("isCTO"))
        base_row["profileUrl"] = cand.get("profileUrl") or base_row.get("profileUrl") or ""
        base_row["profileDescription"] = cand.get("profileDescription") or ""
        base_row["profileLinksCount"] = _safe_int(cand.get("profileLinksCount"))
        base_row["urlGMGN"] = f"https://gmgn.ai/sol/token/{token_addr}" if token_addr else ""

        # Score
        sc = score_pair(base_row, pump_mode=opts.pump_mode)
        base_row.update(sc)

        # Anti-dead (prefilter)
        if opts.anti_dead and not _anti_dead_pass(base_row, opts):
            if opts.verbose_debug:
                why_filtered_list.append(
                    {"tokenAddress": token_addr, "symbol": symbol, "reason": "anti_dead"}
                )
            done += 1
            if opts.progress_callback:
                opts.progress_callback(done, total)
            continue

        # Trending filters (optional)
        if opts.trending_filters:
            reasons = _trending_reasons(base_row, opts)
            if reasons:
                if opts.verbose_debug:
                    why_filtered_list.append(
                        {"tokenAddress": token_addr, "symbol": symbol, "reason": ",".join(reasons)}
                    )
                done += 1
                if opts.progress_callback:
                    opts.progress_callback(done, total)
                continue

        # Mode eligibility: a token can qualify for multiple modes
        for mode in opts.selected_modes:
            t = _mode_thresholds(mode, pump_mode=opts.pump_mode)

            if _safe_float(base_row.get("liquidityUsd")) < t["min_liq"]:
                continue
            if _safe_float(base_row.get("vol5m")) < t["min_vol5"]:
                continue
            if _safe_int(base_row.get("netBuy5m")) < int(t["min_netbuy5"]):
                continue
            if _safe_float(base_row.get("ageMin")) > t["max_age_min"]:
                continue
            if _safe_float(base_row.get("score")) < t["min_score"]:
                continue

            row = dict(base_row)
            row["mode"] = mode
            rows.append(row)

        done += 1
        if opts.progress_callback:
            opts.progress_callback(done, total)

    debug["counts"]["rows_after_filters"] = len(rows)

    if opts.verbose_debug:
        debug["why_filtered_list"] = why_filtered_list[:400]

    if not rows:
        if errors:
            debug["errors"] = errors[:50]
        return [], debug

    # -------------------------------------------------------------------------
    # 5) Ranking / dedupe
    # -------------------------------------------------------------------------
    def _rank_key(r: Dict[str, Any]) -> Tuple[float, float, float]:
        # Primary: spike_score (or score), then liquidity
        spike = _safe_float(r.get("spike_score"))
        score = _safe_float(r.get("score"))
        liq = _safe_float(r.get("liquidityUsd"))
        return (spike if opts.sort_by_spike else score, score, liq)

    rows.sort(key=_rank_key, reverse=True)

    if opts.unique_per_token:
        best_per_token: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            ta = (r.get("tokenAddress") or "").lower()
            if not ta:
                continue
            if ta not in best_per_token:
                best_per_token[ta] = r
        rows = list(best_per_token.values())
        rows.sort(key=_rank_key, reverse=True)

    # Trim
    rows = rows[: max(1, opts.top_n)]

    # -------------------------------------------------------------------------
    # 6) Paid orders (dex paid) for finalists only
    # -------------------------------------------------------------------------
    if opts.include_orders:
        orders_dbg: Dict[str, Any] = {}
        for r in rows:
            ta = (r.get("tokenAddress") or "").strip()
            if not ta:
                continue
            orders, dbg_o = fetch_orders_for_token(CHAIN_ID, ta)
            orders_dbg[ta[:6]] = dbg_o
            r["orders_count"] = len(orders)
            r["isDexPaid"] = bool(orders)
        debug["api_debug"]["orders"] = orders_dbg

    if errors:
        debug["errors"] = errors[:50]

    return rows, debug


# =============================================================================
# Backward compatible helpers (older UI versions)
# =============================================================================

def get_candidates_boosts(*, chain_id: str = CHAIN_ID) -> List[Dict[str, Any]]:
    items, _ = fetch_token_boosts_latest()
    out: List[Dict[str, Any]] = []
    for it in items:
        c = _candidate_from_boost_item(it)
        if c:
            out.append(c)
    return out


def get_candidates_ads(*, chain_id: str = CHAIN_ID) -> List[Dict[str, Any]]:
    items, _ = fetch_ads_latest()
    out: List[Dict[str, Any]] = []
    for it in items:
        c = _candidate_from_ad_item(it)
        if c:
            out.append(c)
    return out


def scan_runners(
    *,
    candidates: List[Dict[str, Any]],
    selected_modes: List[str],
    top_n_per_mode: int = 3,
    pump_mode: bool = True,
    sort_by_spike: bool = True,
) -> List[Dict[str, Any]]:
    """
    Legacy wrapper: expects candidates already provided.
    Uses run_scan_for_modes-style evaluation but without boosts/ads/cto collection.
    """
    # Build a minimal "fake" scan by temporarily treating candidates as boosts.
    # We push them through the same pipeline by calling run_scan_for_modes with boosted candidates.
    # This keeps legacy code working without rewriting the old UI.
    if not candidates:
        return []
    # We cannot directly inject candidates into run_scan_for_modes without changing its signature,
    # so we approximate by re-scoring the best pair for each candidate locally.
    # If you still use this path heavily, prefer migrating to run_scan_for_modes in app.py.
    token_addrs = [c.get("tokenAddress") for c in candidates if c.get("tokenAddress")]
    token_addrs = [str(a).strip() for a in token_addrs if str(a).strip()]
    if not token_addrs:
        return []

    # Fetch pairs in batch
    rows: List[Dict[str, Any]] = []
    all_pairs: List[Dict[str, Any]] = []
    for ch in _chunks(token_addrs[:300], 30):
        pairs_chunk, _ = fetch_pairs_for_tokens_batch(CHAIN_ID, ch)
        all_pairs.extend(pairs_chunk)

    pairs_by_token: Dict[str, List[Dict[str, Any]]] = {}
    for p in all_pairs:
        base = (p.get("baseToken") or {}).get("address") or ""
        quote = (p.get("quoteToken") or {}).get("address") or ""
        if base:
            pairs_by_token.setdefault(base.lower(), []).append(p)
        if quote:
            pairs_by_token.setdefault(quote.lower(), []).append(p)

    # Score and filter using the mode thresholds
    for ta in token_addrs:
        pairs = pairs_by_token.get(ta.lower()) or []
        if not pairs:
            continue
        best = max(
            pairs,
            key=lambda p: (_safe_float((p.get("liquidity") or {}).get("usd")), _safe_float((p.get("volume") or {}).get("h24"))),
        )
        base = (best.get("baseToken") or {})
        quote = (best.get("quoteToken") or {})
        symbol = base.get("symbol") or ""
        name = base.get("name") or ""
        if str(base.get("address") or "").strip().lower() != ta.lower() and str(quote.get("address") or "").strip().lower() == ta.lower():
            symbol = quote.get("symbol") or symbol
            name = quote.get("name") or name

        row = {"tokenAddress": ta, "symbol": symbol, "name": name}
        row.update(compute_metrics_from_pair(best))
        row.update(score_pair(row, pump_mode=pump_mode))

        for mode in selected_modes:
            t = _mode_thresholds(mode, pump_mode=pump_mode)
            if _safe_float(row.get("liquidityUsd")) < t["min_liq"]:
                continue
            if _safe_float(row.get("vol5m")) < t["min_vol5"]:
                continue
            if _safe_int(row.get("netBuy5m")) < int(t["min_netbuy5"]):
                continue
            if _safe_float(row.get("ageMin")) > t["max_age_min"]:
                continue
            if _safe_float(row.get("score")) < t["min_score"]:
                continue
            rr = dict(row)
            rr["mode"] = mode
            rows.append(rr)

    # Rank globally; then take top_n_per_mode by mode
    rows.sort(key=lambda r: (_safe_float(r.get("spike_score")) if sort_by_spike else _safe_float(r.get("score"))), reverse=True)
    per_mode: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        per_mode.setdefault(r.get("mode") or "", []).append(r)
    out: List[Dict[str, Any]] = []
    for m in selected_modes:
        out.extend((per_mode.get(m) or [])[: max(1, int(top_n_per_mode))])
    out.sort(key=lambda r: (_safe_float(r.get("spike_score")) if sort_by_spike else _safe_float(r.get("score"))), reverse=True)
    return out
