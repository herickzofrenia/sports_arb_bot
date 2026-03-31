"""
sports_arb_bot.py - Polymarket Player Props Arbitrage Bot
==========================================================
Compara player props de casas de apostas (Betfair/Bet365 via odds-api.io)
com precos do Polymarket pra encontrar +EV e comprar automaticamente.

Estrategia:
  1. Puxa jogos de hoje (NBA, NHL) via odds-api.io
  2. Pra cada jogo, puxa odds incluindo player props
  3. Busca no Polymarket os mesmos props (Over/Under pontos, rebounds, etc)
  4. Compara: se casa diz Over 27.5pts @ 1.90 (52.6%) e Poly ta a 45c -> edge 7.6% -> COMPRA
  5. Executa via CLOB API (FAK + GTC)

Uso:
    python sports_arb_bot.py --dry-run    # scan sem executar
    python sports_arb_bot.py --live       # loop continuo

Requer:
    pip install requests python-dotenv py-clob-client
"""

import os, sys, time, json, logging, threading, requests
from datetime import datetime, timezone, timedelta
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except: pass

env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)
print(f"  [ENV] Loaded from: {env_path} (exists={env_path.exists()})")
print(f"  [ENV] DRY_RUN raw = '{os.getenv('DRY_RUN', 'NOT SET')}'")
print(f"  [ENV] MIN_EDGE raw = '{os.getenv('MIN_EDGE', 'NOT SET')}'")


# ── CONFIG ──
# 4 API keys for rotation (400 req/hora total)
ODDS_API_KEYS = [k.strip() for k in os.getenv("ODDS_API_KEYS", os.getenv("ODDS_API_KEY", "")).split(",") if k.strip()]
_key_idx = 0
_key_fails = {}  # key -> timestamp when it got rate limited

def next_api_key():
    """Rotate to next working API key."""
    global _key_idx
    if not ODDS_API_KEYS: return ""
    now = time.time()
    for _ in range(len(ODDS_API_KEYS)):
        key = ODDS_API_KEYS[_key_idx % len(ODDS_API_KEYS)]
        fail_ts = _key_fails.get(key, 0)
        if now - fail_ts > 60:  # cooldown 60s after rate limit
            return key
        _key_idx = (_key_idx + 1) % len(ODDS_API_KEYS)
    # All keys on cooldown, return first anyway
    return ODDS_API_KEYS[0]

def mark_key_limited(key):
    """Mark a key as rate limited."""
    global _key_idx
    _key_fails[key] = time.time()
    _key_idx = (_key_idx + 1) % len(ODDS_API_KEYS)
    log.warning(f"Key ...{key[-6:]} rate limited, rotating to next")

WALLET_PRIVATE_KEY = os.getenv("WALLET_PRIVATE_KEY", "")
POLY_SAFE_ADDRESS  = os.getenv("POLY_SAFE_ADDRESS", "")
POLYMARKET_API_KEY = os.getenv("POLYMARKET_API_KEY", "")
POLYMARKET_SECRET  = os.getenv("POLYMARKET_SECRET", "")
POLYMARKET_PASSPHRASE = os.getenv("POLYMARKET_PASSPHRASE", "")

BET_SIZE         = float(os.getenv("BET_SIZE", "2.0"))
MIN_EDGE         = float(os.getenv("MIN_EDGE", "0.20"))
MAX_PRICE        = float(os.getenv("MAX_PRICE", "0.75"))
MIN_PRICE        = float(os.getenv("MIN_PRICE", "0.15"))
MAX_OPEN_POS     = int(os.getenv("MAX_OPEN_POS", "5"))
SCAN_INTERVAL    = int(os.getenv("SCAN_INTERVAL", "300"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "-15.0"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

ODDS_API_BASE = "https://api.odds-api.io/v3"
GAMMA_API     = "https://gamma-api.polymarket.com"
CLOB_API      = "https://clob.polymarket.com"
BOOKMAKERS    = "DraftKings,FanDuel"

SPORTS = [
    {"sport": "basketball", "league": "usa-nba", "name": "NBA"},
    {"sport": "ice-hockey", "league": "usa-nhl", "name": "NHL"},
    {"sport": "baseball", "league": "usa-mlb", "name": "MLB"},
    {"sport": "mixed-martial-arts", "league": "ufc", "name": "UFC", "alt_leagues": ["usa-ufc", "ufc-mma"]},
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler("sports_arb_bot.log", encoding='utf-8'),
              logging.StreamHandler(open(sys.stdout.fileno(), mode='w', encoding='utf-8', errors='replace', closefd=False))])
log = logging.getLogger("props")

# ── STATE ──
open_positions = []; trade_history = []; daily_pnl = 0.0
credits_used = 0; trades_executed = 0
last_scan_time = ""; last_opportunities = []; scan_count = 0
_already_bet = {}  # dedup_key (player|type|line|side) -> times bet (persists between scans, max 2x)
BETS_FILE = Path(__file__).parent / "_bets.json"

def _bet_key(opp):
    """Create dedup key from player+type+line+side (not token_id, which changes between scans)."""
    player = opp.get("player", "").lower().strip()
    ptype = opp.get("prop_type", opp.get("type", "")).lower().strip()
    line = str(opp.get("line", ""))
    side = opp.get("side", "over").lower()
    return f"{player}|{ptype}|{line}|{side}"

def _load_bets():
    global _already_bet
    try:
        if BETS_FILE.exists():
            _already_bet = json.loads(BETS_FILE.read_text())
            log.info(f"  Loaded {len(_already_bet)} previous bets from _bets.json")
    except: pass

def _save_bets():
    try: BETS_FILE.write_text(json.dumps(_already_bet))
    except: pass

def _sync_bets_from_polymarket():
    """Sync _already_bet with REAL positions on Polymarket. Prevents duplicates even if _bets.json is deleted."""
    global _already_bet
    if not POLY_SAFE_ADDRESS: return
    try:
        resp = requests.get(
            f"https://data-api.polymarket.com/positions?user={POLY_SAFE_ADDRESS}&sizeThreshold=0.1&limit=100",
            timeout=15
        )
        if not resp.ok: return
        positions = resp.json()
        if not isinstance(positions, list): return
        
        synced = 0
        for p in positions:
            size = float(p.get("size", 0))
            if size < 0.1: continue
            
            token_id = p.get("asset", "")
            title = p.get("title", "")
            
            # Mark token_id as already bet
            if token_id and token_id not in _already_bet:
                _already_bet[token_id] = 1
                synced += 1
            
            # Also create bet_key from title to catch by player+prop
            if ":" in title:
                player = title.split(":")[0].strip().lower()
                outcome = p.get("outcome", "").lower()
                side = "over" if outcome in ("yes", "over") else "under"
                import re as _re
                nums = _re.findall(r'[\d]+\.[\d]+', title)
                line = nums[0] if nums else ""
                
                tl = title.lower()
                ptype = "stats"
                if "points" in tl: ptype = "pts"
                elif "rebound" in tl: ptype = "reb"
                elif "assist" in tl: ptype = "ast"
                elif "3-pointer" in tl: ptype = "3pt"
                
                bk = f"{player}|{ptype}|{line}|{side}"
                if bk not in _already_bet:
                    _already_bet[bk] = 1
                    synced += 1
        
        if synced > 0:
            _save_bets()
            log.info(f"  [SYNC] Synced {synced} existing positions from Polymarket into dedup list")
    except Exception as e:
        log.warning(f"  [SYNC] Failed to sync bets: {e}")
MAX_BET_REPEATS = 1  # max times to bet on same token (1 = no duplicates)

# ── LIMIT ORDER STRATEGY ──
# When mid-price edge >= MIN_EDGE but real ask edge < MIN_EDGE,
# place a GTC limit order between mid and ask to get filled passively.
LIMIT_ORDER_ENABLED = os.getenv("LIMIT_ORDER_ENABLED", "true").lower() == "true"
LIMIT_ORDER_MIN_MID_EDGE = float(os.getenv("LIMIT_ORDER_MIN_MID_EDGE", "0.15"))  # 15% mid edge to trigger
LIMIT_ORDER_MAX_AGE = int(os.getenv("LIMIT_ORDER_MAX_AGE", "600"))  # cancel after 10 min
LIMIT_ORDER_MAX_PENDING = int(os.getenv("LIMIT_ORDER_MAX_PENDING", "20"))  # max pending limit orders

# Track pending limit orders: [{order_id, token_id, outcome, price, size, time, ...}]
_pending_limit_orders = []
LIMIT_ORDERS_FILE = Path(__file__).parent / "_limit_orders.json"

def _load_limit_orders():
    global _pending_limit_orders
    try:
        if LIMIT_ORDERS_FILE.exists():
            _pending_limit_orders = json.loads(LIMIT_ORDERS_FILE.read_text())
            log.info(f"  Loaded {len(_pending_limit_orders)} pending limit orders")
    except: pass

def _save_limit_orders():
    try: LIMIT_ORDERS_FILE.write_text(json.dumps(_pending_limit_orders))
    except: pass

def cancel_stale_orders():
    """Cancel all pending limit orders at start of each scan.
    They'll be reposted with fresh prices if still good."""
    global _pending_limit_orders
    if not _pending_limit_orders:
        return
    
    cancelled = 0
    
    for lo in _pending_limit_orders:
        oid = lo.get("order_id", "")
        age = time.time() - lo.get("placed_ts", 0)
        if oid and not DRY_RUN:
            try:
                from py_clob_client.client import ClobClient
                c = ClobClient(CLOB_API, key=WALLET_PRIVATE_KEY, chain_id=137, signature_type=2, funder=POLY_SAFE_ADDRESS)
                c.set_api_creds(c.create_or_derive_api_creds())
                c.cancel(oid)
                log.info(f"  [LIMIT] Cancelled: {lo['outcome']} @ {lo['price']:.2f} (age {age:.0f}s)")
                cancelled += 1
            except Exception as e:
                log.warning(f"  [LIMIT] Cancel failed {lo['outcome']}: {e}")
                cancelled += 1  # still remove from tracking
        else:
            log.info(f"  [LIMIT] Expired: {lo['outcome']} @ {lo['price']:.2f} (age {age:.0f}s)")
            cancelled += 1
    
    if cancelled:
        log.info(f"  [LIMIT] Cleared {cancelled} orders, will repost fresh ones")
    _pending_limit_orders = []
    _save_limit_orders()

def place_limit_order(opp, mid_price, best_ask, fair_prob):
    """Place a GTC limit order between mid and ask price.
    Strategy: bid at mid + (ask - mid) * 0.25 — close to mid for max edge.
    Stops placing orders 1h before game starts."""
    global _pending_limit_orders, trades_executed
    
    # CHECK: Do we already have a position for this token? (prevents double fills)
    token_id = opp["token_id"]
    for pos in open_positions:
        if pos.get("token_id") == token_id:
            existing_size = pos.get("size", 0)
            if existing_size >= BET_SIZE * 0.5:  # already have at least half a bet
                log.info(f"    [LIMIT] Already have position in {opp['outcome']} (${existing_size:.2f}), skipping")
                return False
    
    # Don't place limit orders within 1h of game start — they won't have time to fill
    ct = opp.get("commence_time", "")
    if ct:
        try:
            game_time = datetime.fromisoformat(ct.replace("Z", "+00:00"))
            time_to_game = (game_time - datetime.now(timezone.utc)).total_seconds()
            if time_to_game < 3600:  # less than 1 hour
                log.info(f"    [LIMIT] Game starts in {time_to_game/60:.0f}min, skipping limit order")
                return False
        except: pass
    
    # FRESH INJURY RE-CHECK for limit orders (bypass cache)
    # Limit orders persist across scans, so player status may have changed
    player_name = opp.get("player", "")
    if player_name:
        global _injury_cache_time
        old_cache_time = _injury_cache_time
        _injury_cache_time = 0  # force fresh fetch
        available, inj_status = is_player_available(player_name)
        if not available:
            log.info(f"    [LIMIT] FRESH INJURY CHECK: {player_name} is now {inj_status}, skipping")
            _injury_cache_time = time.time()  # update cache time since we just fetched
            return False
    
    if len(_pending_limit_orders) >= LIMIT_ORDER_MAX_PENDING:
        log.info(f"    [LIMIT] Max pending ({LIMIT_ORDER_MAX_PENDING}) reached, skipping")
        return False
    
    # Check if we already have a limit order for this token
    token_id = opp["token_id"]
    for lo in _pending_limit_orders:
        if lo.get("token_id") == token_id:
            log.info(f"    [LIMIT] Already have order for {opp['outcome']}")
            return False
    
    spread = best_ask - mid_price
    if spread < 0.02:
        return False  # spread too tight, no room for limit
    
    # Bid at mid + 25% of spread (closer to mid = more edge, less likely to fill)
    limit_price = round(mid_price + spread * 0.25, 2)
    limit_price = max(limit_price, mid_price + 0.01)  # at least 1 cent above mid
    limit_price = min(limit_price, best_ask - 0.01)   # at least 1 cent below ask
    
    # Verify edge at our limit price
    limit_edge = fair_prob - limit_price
    if limit_edge < 0.08:  # need at least 8% edge at limit price
        log.info(f"    [LIMIT] Edge too low at limit price: {limit_edge:.1%}")
        return False
    
    amt = BET_SIZE
    shares = round(amt / limit_price, 2)
    
    # Polymarket requires minimum 5 shares per order
    if shares < 5.0:
        log.info(f"    [LIMIT] Skip {opp['outcome']} — {shares} shares < 5 min (price {limit_price:.2f} too high for ${amt})")
        return False
    
    log.info(f"  [LIMIT] {opp['outcome']} | mid={mid_price:.2f} ask={best_ask:.2f} -> LIMIT@{limit_price:.2f} | fair={fair_prob:.2f} edge={limit_edge:.1%} | {shares} shares")
    
    if DRY_RUN:
        log.info(f"    [DRY] Would place limit order @ {limit_price:.2f}")
        _pending_limit_orders.append({
            "order_id": f"DRY_{int(time.time())}",
            "token_id": token_id,
            "outcome": opp["outcome"],
            "price": limit_price,
            "size": amt,
            "shares": shares,
            "fair_prob": fair_prob,
            "mid_price": mid_price,
            "best_ask": best_ask,
            "edge_at_limit": limit_edge,
            "placed_ts": time.time(),
            "status": "DRY",
            "player": opp.get("player", ""),
            "home": opp.get("home", ""),
            "away": opp.get("away", ""),
        })
        _save_limit_orders()
        return True
    
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
        
        c = ClobClient(CLOB_API, key=WALLET_PRIVATE_KEY, chain_id=137, signature_type=2, funder=POLY_SAFE_ADDRESS)
        c.set_api_creds(c.create_or_derive_api_creds())
        
        order = c.create_order(OrderArgs(
            token_id=token_id,
            price=limit_price,
            size=shares,
            side=BUY
        ))
        result = c.post_order(order, OrderType.GTC)
        
        # Log full response for debugging
        log.info(f"    [LIMIT RESP] {result}")
        
        err_msg = ""
        status = ""
        success = False
        if isinstance(result, dict):
            err_msg = result.get("errorMsg", "")
            status = result.get("status", "")
            success = result.get("success", False)
        
        if success and not err_msg:
            order_id = result.get("orderID", result.get("order_id", f"GTC_{int(time.time())}"))
            log.info(f"    [LIMIT OK] GTC @ {limit_price:.2f} | {shares} shares | id={order_id} | status={status}")
            
            _pending_limit_orders.append({
                "order_id": order_id,
                "token_id": token_id,
                "outcome": opp["outcome"],
                "price": limit_price,
                "size": amt,
                "shares": shares,
                "fair_prob": fair_prob,
                "mid_price": mid_price,
                "best_ask": best_ask,
                "edge_at_limit": limit_edge,
                "placed_ts": time.time(),
                "status": status or "GTC",
                "player": opp.get("player", ""),
                "home": opp.get("home", ""),
                "away": opp.get("away", ""),
            })
            _save_limit_orders()
            trades_executed += 1
            return True
        else:
            log.warning(f"    [LIMIT FAIL] success={success} status={status} err={err_msg}")
            return False
    
    except ImportError:
        log.error("pip install py-clob-client")
        return False
    except Exception as e:
        log.error(f"    [LIMIT ERR] {e}")
        return False

# ── MATH ──
def dec2imp(p): return 1/p if p > 1 else 0
def rm_vig(a, b):
    t = a + b
    return (a/t, b/t) if t > 0 else (0.5, 0.5)

# ── ODDS-API.IO ──
def api_get(endpoint, params):
    global credits_used
    key = next_api_key()
    if not key: log.error("No API keys configured"); return None
    try:
        url = f"{ODDS_API_BASE}/{endpoint}"
        resp = requests.get(url, params={**params, "apiKey": key}, timeout=15)
        credits_used += 1
        if resp.status_code == 429:
            mark_key_limited(key)
            # Retry with next key
            key2 = next_api_key()
            if key2 != key:
                resp = requests.get(url, params={**params, "apiKey": key2}, timeout=15)
                credits_used += 1
                if resp.status_code == 429: mark_key_limited(key2); return None
                if not resp.ok:
                    log.warning(f"  [API] {endpoint} retry failed: HTTP {resp.status_code} — {resp.text[:200]}")
                    return None
                return resp.json()
            return None
        if not resp.ok:
            log.warning(f"  [API] {endpoint} failed: HTTP {resp.status_code} — {resp.text[:200]}")
            return None
        return resp.json()
    except Exception as e: log.error(f"API err: {e}"); return None

def get_events(sport, league):
    data = api_get("events", {"sport": sport, "league": league, "status": "pending", "limit": 15})
    if data is None:
        log.warning(f"  [EVENTS] {sport}/{league}: API returned None")
        return []
    if isinstance(data, dict):
        log.warning(f"  [EVENTS] {sport}/{league}: API returned dict instead of list: {str(data)[:200]}")
        return []
    if not data:
        log.info(f"  [EVENTS] {sport}/{league}: 0 events returned")
    return data if isinstance(data, list) else []

def get_odds_batch(eids):
    if not eids: return []
    data = api_get("odds/multi", {"eventIds": ",".join(str(e) for e in eids[:10]), "bookmakers": BOOKMAKERS})
    if isinstance(data, list): return data
    if isinstance(data, dict): return [data]
    return []

def get_odds_single(eid):
    return api_get("odds", {"eventId": eid, "bookmakers": BOOKMAKERS})

def extract_props(odds_data):
    """Extract player props from odds response. Returns list of prop dicts."""
    props = []
    for book, markets in odds_data.get("bookmakers", {}).items():
        for mkt in markets:
            mn = mkt.get("name", "")
            if "Player Props" not in mn and "player" not in mn.lower(): continue
            ptype = mn.replace("Player Props - ", "").replace("Player Props", "").strip()
            # Normalize common types (NBA + NHL + MLB)
            ptype_map = {"Points": "Pts", "Rebounds": "Reb", "Assists": "Ast", 
                        "Three Pointers": "3PT", "3-Pointers Made": "3PT",
                        "Steals": "Stl", "Blocks": "Blk", "Turnovers": "TO",
                        "Points + Rebounds + Assists": "PRA", "Points + Rebounds": "P+R",
                        "Points + Assists": "P+A", "Rebounds + Assists": "R+A",
                        "Double Doubles": "DD", "Triple Doubles": "TD",
                        # MLB props
                        "Hits": "Hits", "Home Runs": "HR", "RBIs": "RBI",
                        "Runs Batted In": "RBI", "Total Bases": "TB",
                        "Strikeouts": "K", "Pitcher Strikeouts": "K",
                        "Hits Allowed": "HA", "Earned Runs": "ER",
                        "Walks": "BB", "Runs": "R", "Stolen Bases": "SB",
                        "Singles": "1B", "Doubles": "2B", "Triples": "3B",
                        "Outs Recorded": "Outs", "Pitching Outs": "Outs",
                        # NHL props
                        "Goals": "G", "Shots on Goal": "SOG", "Saves": "SV",
                        "Goals Against": "GA",
                        }
            if not ptype: ptype = "Stats"
            ptype_short = ptype_map.get(ptype, ptype)
            for o in mkt.get("odds", []):
                player = o.get("label", "")
                # Clean player name: remove "(Assists)", "(Points)" etc from name
                import re
                player = re.sub(r'\s*\(.*?\)\s*', ' ', player).strip()
                if not player: continue
                try:
                    line = float(o.get("hdp", 0) or 0)
                    ov = float(o.get("over", 0) or 0)
                    un = float(o.get("under", 0) or 0)
                except (ValueError, TypeError):
                    continue
                if not player or line <= 0 or ov <= 1 or un <= 1: continue
                fo, fu = rm_vig(dec2imp(ov), dec2imp(un))
                props.append({"player": player, "type": ptype, "type_short": ptype_short, "line": line,
                    "fair_over": fo, "fair_under": fu, "source": book})
    # Dedup same player+type+line
    seen = {}
    for p in props:
        k = f"{p['player']}|{p['type']}|{p['line']}"
        if k not in seen: seen[k] = p
        else:
            seen[k]["fair_over"] = (seen[k]["fair_over"] + p["fair_over"]) / 2
            seen[k]["fair_under"] = (seen[k]["fair_under"] + p["fair_under"]) / 2
            seen[k]["source"] = "consensus"
    
    # Sort: Points first, then Rebounds, then Assists, then rest
    priority = {"Points": 0, "Rebounds": 1, "Assists": 2, "Three Pointers": 3, "3-Pointers Made": 3}
    result = list(seen.values())
    result.sort(key=lambda x: priority.get(x["type"], 10))
    return result

def extract_ml(odds_data):
    """Extract moneyline fair probs."""
    for book, markets in odds_data.get("bookmakers", {}).items():
        for mkt in markets:
            if mkt.get("name") != "ML": continue
            o = mkt.get("odds", [{}])[0]
            try:
                h = float(o.get("home", 0) or 0); a = float(o.get("away", 0) or 0)
            except (ValueError, TypeError):
                continue
            if h <= 1 or a <= 1: continue
            hi, ai = dec2imp(h), dec2imp(a)
            try:
                d = float(o.get("draw", 0) or 0)
            except (ValueError, TypeError):
                d = 0
            if d > 1:
                di = dec2imp(d); t = hi+di+ai
                return {odds_data["home"]: hi/t, "Draw": di/t, odds_data["away"]: ai/t}, book
            t = hi + ai
            return {odds_data["home"]: hi/t, odds_data["away"]: ai/t}, book
    return None, None

# ── POLYMARKET ──
def poly_search(query, limit=8):
    try:
        r = requests.get(f"{GAMMA_API}/public-search", params={"q": query, "limit": limit}, timeout=8)
        if not r.ok: return []
        d = r.json()
        return d if isinstance(d, list) else d.get("results", d.get("events", []))
    except: return []

def parse_mkt(mkt, title=""):
    q = mkt.get("question", "")
    if not q: return None
    oc = mkt.get("outcomes", []); op = mkt.get("outcomePrices", []); ct = mkt.get("clobTokenIds", [])
    if isinstance(oc, str):
        try: oc = json.loads(oc)
        except: oc = []
    if isinstance(op, str):
        try: op = json.loads(op)
        except: op = []
    if isinstance(ct, str):
        try: ct = json.loads(ct)
        except: ct = []
    if not oc or not ct: return None
    prices = {}; tokens = {}
    for i, n in enumerate(oc):
        if i < len(op):
            try: prices[n] = float(op[i])
            except: prices[n] = 0.5
        if i < len(ct): tokens[n] = ct[i]
    return {"id": mkt.get("id",""), "question": q, "event_title": title,
        "outcomes": oc, "prices": prices, "tokens": tokens,
        "volume": float(mkt.get("volume",0) or 0),
        "liquidity": float(mkt.get("liquidityClob",0) or 0),
        "condition_id": mkt.get("conditionId",""), "neg_risk": mkt.get("negRisk", False)}

# Cache: game -> list of all poly markets (props + ML + spreads)
_poly_game_cache = {}

def get_poly_game_markets(home, away):
    """Find the Polymarket event for a game and return ALL its sub-markets (including props)."""
    hl = home.split()[-1]; al = away.split()[-1]
    cache_key = f"{hl}_{al}"
    if cache_key in _poly_game_cache:
        return _poly_game_cache[cache_key]
    
    all_markets = []
    
    # Search for the game event
    for query in [f"{hl} {al}", f"{home} vs {away}"]:
        events = poly_search(query, 10)
        for evt in events:
            title = (evt.get("title", "") or "").lower()
            # Must contain both teams
            if hl.lower() not in title and al.lower() not in title:
                continue
            
            # Get all markets from this event
            mkts = evt.get("markets", [])
            if not mkts:
                continue
            
            for mkt in mkts:
                mkt["_evt_title"] = evt.get("title", "")
                all_markets.append(mkt)
            
            if all_markets:
                break  # found the right event
        if all_markets:
            break
    
    _poly_game_cache[cache_key] = all_markets
    return all_markets


def find_poly_prop(player, ptype, line, home, away):
    """Find a matching prop market on Polymarket for this player+line."""
    plast = player.split()[-1].lower()
    line_str = str(line)
    line_int = str(int(line)) if line == int(line) else ""
    
    # Get all markets for this game
    all_mkts = get_poly_game_markets(home, away)
    
    for raw in all_mkts:
        pm = parse_mkt(raw, raw.get("_evt_title", ""))
        if not pm:
            continue
        
        ql = pm["question"].lower()
        
        # Must contain player last name
        if plast not in ql:
            continue
        
        # Must contain the line number
        if line_str not in ql and (not line_int or line_int not in ql):
            continue
        
        # FOUND player + line match!
        # This is a matching prop! Now determine Over vs Under
        # Polymarket formats: "Paolo Banchero: Points O/U 24.5" with outcomes ["Over", "Under"]
        #                     "Will X score over 24.5?" with outcomes ["Yes", "No"]
        is_over_under = "o/u" in ql or ("over" in ql and "under" in ql) or "over/under" in ql
        is_over_only = "over" in ql and "under" not in ql and not is_over_under
        is_under_only = "under" in ql and "over" not in ql and not is_over_under
        
        # Return BOTH sides for O/U markets so caller can pick
        results = []
        for oc_name in pm["outcomes"]:
            ol = oc_name.lower()
            price = pm["prices"].get(oc_name, 0)
            token = pm["tokens"].get(oc_name, "")
            if not token or not (MIN_PRICE <= price <= MAX_PRICE):
                continue
            
            # O/U market: outcomes can be "Over"/"Under" OR "Yes"/"No"
            if is_over_under:
                if ol in ("over", "yes"):
                    results.append({"side": "Over", "price": price, "token": token, "market": pm})
                elif ol in ("under", "no"):
                    results.append({"side": "Under", "price": price, "token": token, "market": pm})
            
            # "Will X score over 24.5?" -> Yes = Over, No = Under
            elif is_over_only:
                if ol in ("yes", "over"):
                    results.append({"side": "Over", "price": price, "token": token, "market": pm})
                elif ol == "no":
                    results.append({"side": "Under(No)", "price": price, "token": token, "market": pm})
            
            # "Will X score under 24.5?" -> Yes = Under, No = Over  
            elif is_under_only:
                if ol in ("yes", "under"):
                    results.append({"side": "Under", "price": price, "token": token, "market": pm})
                elif ol == "no":
                    results.append({"side": "Over(No)", "price": price, "token": token, "market": pm})
            
            # Generic: assume first outcome is "over" side
            else:
                if ol in ("yes", "over"):
                    results.append({"side": "Over", "price": price, "token": token, "market": pm})
                elif ol in ("no", "under"):
                    results.append({"side": "Under", "price": price, "token": token, "market": pm})
        
        if results:
            return results  # Return ALL sides
    
    return None

# ── EXECUTION ──
def get_best_ask(token_id):
    """Fetch the best ask price from the CLOB order book for a specific token."""
    try:
        # Use the price endpoint - side=sell gives the ask (price to buy at)
        resp = requests.get(f"{CLOB_API}/price?token_id={token_id}&side=sell", timeout=5)
        if resp.ok:
            data = resp.json()
            price = float(data.get("price", 0))
            if 0.01 < price < 0.99:
                return price
        
        # Fallback: use book endpoint
        resp2 = requests.get(f"{CLOB_API}/book?token_id={token_id}", timeout=5)
        if resp2.ok:
            data2 = resp2.json()
            asks = data2.get("asks", [])
            # Filter out 0.99 asks (these are usually the wrong side)
            valid_asks = [float(a.get("price", 0)) for a in asks if 0.01 < float(a.get("price", 0)) < 0.95]
            if valid_asks:
                return min(valid_asks)  # best (lowest) ask
    except Exception as e:
        log.warning(f"  [BOOK] Failed to get ask: {e}")
    return None

def execute_trade(opp):
    global trades_executed
    token_id = opp["token_id"]; pp = opp["poly_price"]
    times_bet = opp.get("_times_bet", 0)
    is_ladder = opp.get("_is_ladder", False)
    if is_ladder:
        amt = opp.get("_ladder_amt", 1.0)
    else:
        amt = BET_SIZE
    
    # Use pre-fetched ask price from Phase 2 (or fetch fresh)
    best_ask = opp.get("_real_ask") or get_best_ask(token_id)
    fair = opp["fair_prob"]
    
    if best_ask and 0.01 < best_ask < 0.95:
        ask_edge = fair - best_ask
        if ask_edge < MIN_EDGE:
            log.warning(f"  Skip: ask={best_ask:.2f} fair={fair:.2f} real_edge={ask_edge:.1%} < {MIN_EDGE:.0%}")
            return False
        ap = round(min(best_ask + 0.01, 0.92), 2)
    else:
        ap = round(min(fair * 0.85, 0.90), 2)
    
    log.info(f"[TRADE] {opp['outcome']} | ask={best_ask or '?'} -> buy@{ap:.2f} | fair={fair:.2f} | ${amt}")
    
    if DRY_RUN:
        log.info(f"  [DRY RUN] Would buy ${amt} @ {ap:.2f}")
        trade_history.append({"time": datetime.now(timezone.utc).isoformat(),
            "outcome": opp["outcome"], "price": ap, "size": amt,
            "edge": opp["edge"], "fair": opp["fair_prob"], "status": "DRY"})
        open_positions.append({"token_id":token_id,"outcome":opp["outcome"],"entry_price":ap,"size":amt,"edge":opp["edge"],"fair_prob":opp["fair_prob"],"time":datetime.now(timezone.utc).isoformat(),"question":opp.get("question",""),"status":"DRY","home":opp.get("home",""),"away":opp.get("away",""),"player":opp.get("player",""),"sport":opp.get("sport","")})
        trades_executed += 1; return True
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import MarketOrderArgs, OrderType, OrderArgs
        from py_clob_client.order_builder.constants import BUY
        c = ClobClient(CLOB_API, key=WALLET_PRIVATE_KEY, chain_id=137, signature_type=2, funder=POLY_SAFE_ADDRESS)
        c.set_api_creds(c.create_or_derive_api_creds())
        try:
            o = c.create_market_order(MarketOrderArgs(token_id=token_id, amount=amt, side=BUY, price=ap))
            r = c.post_order(o, OrderType.FAK)
            log.info(f"  [FAK RESP] {r}")
            if r and r.get("success") and not r.get("errorMsg"):
                log.info(f"  [OK] FAK @ {ap:.2f} status={r.get('status','')}"); trades_executed += 1
                open_positions.append({"token_id":token_id,"outcome":opp["outcome"],"entry_price":ap,"size":amt,"edge":opp["edge"],"fair_prob":opp["fair_prob"],"time":datetime.now(timezone.utc).isoformat(),"question":opp.get("question",""),"status":"FAK","home":opp.get("home",""),"away":opp.get("away",""),"player":opp.get("player",""),"sport":opp.get("sport","")})
                return True
            elif r:
                log.warning(f"  [FAK] success={r.get('success')} err={r.get('errorMsg','')} status={r.get('status','')}")
        except Exception as e: log.warning(f"  FAK fail: {e}")
        try:
            sh = round(amt/ap, 2)
            if sh < 5.0:
                log.info(f"  [GTC] Skip — {sh} shares < 5 min (price {ap:.2f} too high for ${amt})")
                return False
            o2 = c.create_order(OrderArgs(token_id=token_id, price=ap, size=sh, side=BUY))
            r2 = c.post_order(o2, OrderType.GTC)
            log.info(f"  [GTC RESP] {r2}")
            if r2 and r2.get("success") and not r2.get("errorMsg"):
                log.info(f"  [OK] GTC @ {ap:.2f} ({sh} shares) status={r2.get('status','')}"); trades_executed += 1
                open_positions.append({"token_id":token_id,"outcome":opp["outcome"],"entry_price":ap,"size":amt,"edge":opp["edge"],"fair_prob":opp["fair_prob"],"time":datetime.now(timezone.utc).isoformat(),"question":opp.get("question",""),"status":"GTC","home":opp.get("home",""),"away":opp.get("away",""),"player":opp.get("player",""),"sport":opp.get("sport","")})
                return True
            elif r2:
                log.warning(f"  [GTC] success={r2.get('success')} err={r2.get('errorMsg','')} status={r2.get('status','')}")
        except Exception as e: log.error(f"  GTC fail: {e}")
        return False
    except ImportError: log.error("pip install py-clob-client"); return False
    except Exception as e: log.error(f"  Trade err: {e}"); return False

# ── INJURY CHECK ──
# BUG FIX: old code used last_name as dict key — players with same last name
# (e.g. Jaylen Brown vs Bruce Brown) would overwrite each other.
# New code stores a LIST of injured players per last name, and checks full name match.
_injury_cache = {}  # last_name -> [list of {name, status, full, league, ...}]
_injury_cache_time = 0

def _add_injury(injuries, name, status, league, source="ESPN", reason=""):
    """Add a player to the injuries dict. Stores list per last_name to handle duplicates.
    Source priority: NBA_PDF > ESPN > UNDERDOG > UNDERDOG_LINEUP
    Exception: UNDERDOG "Available"/"Out" can override ESPN "Questionable" (real-time updates).
    """
    import re
    last = name.split()[-1].lower()
    # Handle "Jr." / "III" etc — get real last name
    if last in ("jr.", "jr", "iii", "ii", "iv", "sr.", "sr"):
        parts = name.lower().split()
        last = parts[-2] if len(parts) >= 2 else last
    last = re.sub(r'[.\']', '', last)
    full = name.lower()
    entry = {"name": name, "status": status, "full": full, "league": league, "source": source, "reason": reason}
    if last not in injuries:
        injuries[last] = []
    # Don't duplicate same full name
    SOURCE_RANK = {"NBA_PDF": 3, "ESPN": 2, "UNDERDOG": 1, "UNDERDOG_LINEUP": 0}
    for existing in injuries[last]:
        if existing["full"] == full:
            old_rank = SOURCE_RANK.get(existing["source"], 0)
            new_rank = SOURCE_RANK.get(source, 0)
            # Higher rank source always wins
            if new_rank > old_rank:
                existing["status"] = status
                existing["source"] = source
                existing["reason"] = reason
            # Same rank: update
            elif new_rank == old_rank:
                existing["status"] = status
                existing["source"] = source
                existing["reason"] = reason
            # Lower rank BUT real-time update: UNDERDOG can upgrade ESPN Questionable
            elif source in ("UNDERDOG", "UNDERDOG_LINEUP"):
                old_status = existing["status"].lower()
                new_status = status.lower()
                # Real-time "out" or "available" overrides stale "questionable"
                if old_status in ("questionable", "day-to-day") and new_status in ("out", "available"):
                    existing["status"] = status
                    existing["source"] = source
                    existing["reason"] = reason
            return
    injuries[last].append(entry)

def fetch_injury_report():
    """Fetch injury reports. Primary: NBA official PDF. Fallback: ESPN HTML. Cache 15 min."""
    global _injury_cache, _injury_cache_time
    if time.time() - _injury_cache_time < 900 and _injury_cache:  # 15 min cache
        return _injury_cache
    
    import re
    injuries = {}
    
    # ── PRIMARY: NBA Official Injury Report PDF ──
    now = datetime.now(timezone.utc)
    et_offset = timedelta(hours=-4)  # ET = UTC-4 (EDT)
    et_now = now + et_offset
    
    pdf_urls = []
    # Generate URLs for last 8 hours in 15-min intervals
    for hours_back in range(0, 8):
        t = et_now - timedelta(hours=hours_back)
        for minute in ["00", "15", "30", "45"]:
            hour_12 = t.hour % 12 or 12
            ampm = "PM" if t.hour >= 12 else "AM"
            url = f"https://ak-static.cms.nba.com/referee/injury/Injury-Report_{t.strftime('%Y-%m-%d')}_{hour_12:02d}_{minute}{ampm}.pdf"
            if url not in pdf_urls:
                pdf_urls.append(url)
    
    # Also try common fixed times for today
    for hh, mm, ap in [(7,"15","PM"),(7,"00","PM"),(6,"45","PM"),(6,"30","PM"),(6,"15","PM"),(6,"00","PM"),
                        (5,"45","PM"),(5,"30","PM"),(5,"15","PM"),(5,"00","PM"),(4,"45","PM"),(4,"30","PM"),
                        (4,"15","PM"),(4,"00","PM"),(3,"30","PM"),(3,"00","PM"),(2,"00","PM"),(1,"00","PM"),
                        (12,"00","PM"),(11,"00","AM"),(10,"00","AM"),(9,"00","AM"),(8,"00","AM")]:
        url = f"https://ak-static.cms.nba.com/referee/injury/Injury-Report_{et_now.strftime('%Y-%m-%d')}_{hh:02d}_{mm}{ap}.pdf"
        if url not in pdf_urls:
            pdf_urls.append(url)
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    nba_pdf_ok = False
    for url in pdf_urls[:30]:  # try up to 30 URLs (15-min intervals)
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            text = ""
            try:
                import io
                from pypdf import PdfReader
                reader = PdfReader(io.BytesIO(resp.content))
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
            except ImportError:
                log.warning("  [INJURY] pypdf not installed — run: pip install pypdf")
                try:
                    raw = resp.content.decode('latin-1', errors='replace')
                    matches = re.findall(r'\(([^)]+)\)', raw)
                    text = '\n'.join(matches)
                except:
                    pass
            except Exception as e:
                log.warning(f"  [INJURY] PDF parse error: {e}")
                continue
            
            if not text or len(text) < 200:
                continue
            
            count = 0
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if not line or len(line) < 5:
                    continue
                for status_word in ["Out", "Questionable", "Doubtful", "Probable", "Available"]:
                    if f" {status_word} " in f" {line} ":
                        name_match = re.match(
                            r"([A-Z][A-Za-z\u00C0-\u024F\-\' ]*?(?:\s(?:Jr\.|Sr\.|III|II|IV))?)"
                            r",\s*"
                            r"([A-Z][A-Za-z\u00C0-\u024F\-\' ]+)"
                            r"\s+" + status_word, 
                            line
                        )
                        if name_match:
                            last_name = name_match.group(1).strip()
                            first_name = name_match.group(2).strip()
                            # Skip G-League/Two-Way unless injured
                            if "G League" in line or "Two-Way" in line:
                                if status_word in ("Out", "Questionable"):
                                    if "Injury" not in line and "Illness" not in line:
                                        continue
                            _add_injury(injuries, f"{first_name} {last_name}", status_word, "NBA", "NBA_PDF", line)
                            count += 1
                        break
            
            if count > 20:
                nba_pdf_ok = True
                log.info(f"  [INJURY] NBA official PDF: {count} players from {url.split('/')[-1]}")
                break
            elif count > 0:
                log.info(f"  [INJURY] NBA PDF partial: {count} players (trying next...)")
        except Exception as e:
            continue
    
    # ── ESPN HTML scraping ──
    espn_leagues = []
    if not nba_pdf_ok:
        espn_leagues.append(("NBA", "https://www.espn.com/nba/injuries"))
    espn_leagues.append(("MLB", "https://www.espn.com/mlb/injuries"))
    espn_leagues.append(("NHL", "https://www.espn.com/nhl/injuries"))
    
    for league, url in espn_leagues:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if not resp.ok:
                log.warning(f"  [INJURY] {league} ESPN returned {resp.status_code}")
                continue
            html = resp.text
            count = 0
            rows = re.findall(r'<tr[^>]*class="Table__TR[^"]*"[^>]*>(.*?)</tr>', html, re.DOTALL)
            for row in rows:
                name_match = re.search(r'<a[^>]*href="[^"]*player[^"]*"[^>]*>([^<]+)</a>', row)
                if not name_match:
                    continue
                name = name_match.group(1).strip()
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                status = ""
                for cell in cells:
                    cell_text = re.sub(r'<[^>]+>', '', cell).strip()
                    if cell_text.lower() in ("out", "day-to-day", "doubtful", "questionable", "probable",
                                              "injured reserve", "suspension"):
                        status = cell_text
                        break
                comment = ""
                if len(cells) >= 5:
                    comment = re.sub(r'<[^>]+>', '', cells[-1]).strip().lower()
                if not status and comment:
                    if "questionable" in comment: status = "Day-To-Day"
                    elif "out" in comment: status = "Out"
                if name and status:
                    _add_injury(injuries, name, status, league, "ESPN")
                    count += 1
            if count > 0:
                log.info(f"  [INJURY] {league}: {count} players from ESPN")
        except Exception as e:
            log.warning(f"  [INJURY] {league} ESPN failed: {e}")
    
    # ── LAYER 3: @UnderdogNBA tweets via Nitter RSS ──
    try:
        underdog_count = _fetch_underdog_tweets(injuries)
        if underdog_count > 0:
            log.info(f"  [INJURY] @UnderdogNBA: {underdog_count} status alerts parsed")
    except Exception as e:
        log.warning(f"  [INJURY] @UnderdogNBA failed: {e}")
    
    # Count total unique players
    total = sum(len(v) for v in injuries.values())
    if total > 0:
        _injury_cache = injuries
        _injury_cache_time = time.time()
        log.info(f"  [INJURY] Total: {total} injured/questionable players loaded")
    else:
        log.warning(f"  [INJURY] WARNING: 0 players loaded — trades BLOCKED for safety")
    
    return injuries


def _fetch_underdog_tweets(injuries):
    """Fetch recent @UnderdogNBA tweets via Nitter RSS and parse injury status alerts.
    
    Tweet formats detected:
      - "Status alert: Player (injury) won't return Friday." → OUT
      - "Status alert: Player (injury) has been ruled out" → OUT
      - "Status alert: Player (injury) will not play" → OUT
      - "Status alert: Player (injury) is out" → OUT
      - "Status alert: Player has been assessed Xth technical foul" → SUSPENDED (if mentions suspension)
      - "Status alert: Player (injury) has returned" → AVAILABLE  
      - "Status alert: Player (injury) available to return" → AVAILABLE
      - "Lineup alert: Team will start Player1, Player2..." → lineup (available)
    
    Returns count of status alerts parsed.
    """
    import re
    import xml.etree.ElementTree as ET
    
    NITTER_INSTANCES = [
        "https://nitter.net",
        "https://nitter.privacydev.net",
        "https://nitter.poast.org",
        "https://nitter.cz",
        "https://bird.trom.tf",
        "https://nitter.woodland.cafe",
        "https://n.opnxng.com",
    ]
    
    # Also try direct X/Twitter scraping as last resort
    TWITTER_RSS_ALTERNATIVES = [
        "https://rsshub.app/twitter/user/UnderdogNBA",
        "https://twiiit.com/UnderdogNBA/rss",
    ]
    
    rss_text = None
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    # Try Nitter instances first
    for base in NITTER_INSTANCES:
        try:
            url = f"{base}/UnderdogNBA/rss"
            resp = requests.get(url, headers=headers, timeout=8)
            if resp.ok and "<item>" in resp.text:
                rss_text = resp.text
                break
        except:
            continue
    
    # Try alternative RSS sources
    if not rss_text:
        for url in TWITTER_RSS_ALTERNATIVES:
            try:
                resp = requests.get(url, headers=headers, timeout=8)
                if resp.ok and ("<item>" in resp.text or "<entry>" in resp.text):
                    rss_text = resp.text
                    break
            except:
                continue
    
    if not rss_text:
        return 0
    
    # Parse RSS XML
    count = 0
    try:
        root = ET.fromstring(rss_text)
    except ET.ParseError:
        # Try cleaning up common XML issues
        rss_text = re.sub(r'&(?!amp;|lt;|gt;|apos;|quot;)', '&amp;', rss_text)
        try:
            root = ET.fromstring(rss_text)
        except:
            return 0
    
    # Find all items (RSS 2.0 format)
    items = root.findall('.//item')
    if not items:
        # Try Atom format
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        items = root.findall('.//atom:entry', ns)
    
    now = time.time()
    
    for item in items[:30]:  # check last 30 tweets
        # Get tweet text
        title = item.findtext('title', '') or ''
        desc = item.findtext('description', '') or ''
        content = item.findtext('{http://www.w3.org/2005/Atom}content', '') or ''
        
        tweet = title or desc or content
        tweet = re.sub(r'<[^>]+>', '', tweet).strip()  # strip HTML tags
        
        if not tweet:
            continue
        
        # ── PARSE STATUS ALERTS ──
        # Format: "Status alert: Player Name (injury) won't return Friday."
        status_match = re.match(
            r"Status alert:\s*"
            r"(.+?)"               # player name (greedy but stopped by patterns below)
            r"(?:\s*\([^)]*\))?"   # optional (injury reason)  
            r"\s+"
            r"(.+)",              # rest of the status text
            tweet, re.IGNORECASE
        )
        
        if status_match:
            raw_name = status_match.group(1).strip()
            status_text = status_match.group(2).strip().lower()
            
            # Clean name: remove trailing injury in parens if not caught
            raw_name = re.sub(r'\s*\([^)]*\)\s*$', '', raw_name).strip()
            
            # Determine status from the text
            player_status = None
            
            # OUT patterns
            if any(kw in status_text for kw in [
                "won't return", "will not return", "wont return",
                "ruled out", "has been ruled out", "is out",
                "will not play", "won't play", "wont play",
                "will miss", "has been suspended", "is suspended",
                "will be suspended", "will sit out",
                "not in lineup", "dnp", "inactive",
                "placed on", "transferred to",
            ]):
                player_status = "Out"
            
            # SUSPENDED patterns
            elif "technical foul" in status_text and "suspend" in status_text:
                player_status = "Out"  # treat as out
            
            # QUESTIONABLE / GAME-TIME patterns
            elif any(kw in status_text for kw in [
                "game-time decision", "gametime decision",
                "questionable", "listed as questionable",
                "uncertain", "is doubtful",
            ]):
                player_status = "Questionable"
            
            # AVAILABLE / RETURNED patterns
            elif any(kw in status_text for kw in [
                "has returned", "available to return",
                "will return", "is available", "will play",
                "has been cleared", "upgraded to available",
                "is active", "in tonight's lineup",
                "has been activated",
            ]):
                player_status = "Available"
            
            if player_status and raw_name:
                # Don't override NBA PDF (highest priority)
                _add_injury(injuries, raw_name, player_status, "NBA", "UNDERDOG", tweet[:80])
                count += 1
        
        # ── PARSE LINEUP ALERTS ──
        # Format: "Lineup alert: Team will start Player1, Player2, Player3, Player4, Player5 on Friday."
        lineup_match = re.match(
            r"Lineup alert:\s*\S+.*?\s+will start\s+(.+?)(?:\s+on\s+|\s+tonight|\s+vs\.?|\.\s*$)",
            tweet, re.IGNORECASE
        )
        if lineup_match:
            players_str = lineup_match.group(1).strip()
            # Remove trailing day/date
            players_str = re.sub(r'\s+(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\.?$', '', players_str, flags=re.IGNORECASE)
            player_names = [p.strip() for p in players_str.split(',') if p.strip()]
            for pname in player_names:
                pname = pname.strip().rstrip('.')
                if pname and len(pname) > 3:
                    # Mark starters as available (override questionable)
                    _add_injury(injuries, pname, "Available", "NBA", "UNDERDOG_LINEUP", tweet[:60])
                    count += 1
    
        # ── PARSE INJURY LIST FORMAT ──
        # Format: "Injuries 3/28  SAS: None  MIL: Player - Status Player2 - Status ..."
        if tweet.lower().startswith("injuries") or "injuries " in tweet[:30].lower():
            team_sections = re.split(r'([A-Z]{2,4}):\s*', tweet)
            idx = 1
            while idx < len(team_sections) - 1:
                team_abbr = team_sections[idx].strip()
                players_text = team_sections[idx + 1].strip()
                idx += 2
                
                if not players_text or players_text.lower() in ('none', 'yet to submit', 'tbd'):
                    continue
                
                # Find "Name - Status" patterns with lookahead for next name or end
                entries = re.findall(
                    r"([A-Z][a-z]+(?:[-'\.]?\s*[A-Za-z]+)*(?:\s+(?:Jr\.|Sr\.|III|II|IV))?)\s*-\s*"
                    r"((?:Out|Questionable|Doubtful|Probable|Said\s+he'll\s+play|GTD|Day-to-Day|Likely|Available|Active|Cleared)[A-Za-z\s']*?)"
                    r"(?=\s+[A-Z][a-z]|\s*$)",
                    players_text, re.IGNORECASE
                )
                
                for pname, status_text in entries:
                    pname = pname.strip().rstrip('.')
                    status_text = status_text.strip().lower()
                    
                    if not pname or len(pname) < 4:
                        continue
                    
                    if any(kw in status_text for kw in ["out", "ruled out"]):
                        player_status = "Out"
                    elif any(kw in status_text for kw in ["questionable", "gtd", "game-time"]):
                        player_status = "Questionable"
                    elif any(kw in status_text for kw in ["doubtful"]):
                        player_status = "Doubtful"
                    elif any(kw in status_text for kw in ["probable", "likely"]):
                        player_status = "Probable"
                    elif any(kw in status_text for kw in ["play", "available", "active", "cleared"]):
                        player_status = "Available"
                    else:
                        player_status = "Questionable"
                    
                    _add_injury(injuries, pname, player_status, "NBA", "UNDERDOG", f"{team_abbr}: {pname} - {status_text}")
                    count += 1
    
    return count

def is_player_available(player_name):
    """Check if player is available to play. Returns (available, status).
    Now checks ALL entries for a last name, matching by first name too."""
    import re
    injuries = fetch_injury_report()
    
    # SAFETY: if no injury data, block all trades
    if not injuries:
        return False, "UNKNOWN_NO_DATA"
    
    last = player_name.split()[-1].lower()
    # Handle "Jr." / "III" etc
    if last in ("jr.", "jr", "iii", "ii", "iv", "sr.", "sr"):
        parts = player_name.lower().split()
        last = parts[-2] if len(parts) >= 2 else last
    last = re.sub(r'[.\']', '', last)
    
    full = player_name.lower()
    first_3 = full.split()[0][:3] if full.split() else ""
    
    if last in injuries:
        for entry in injuries[last]:
            inj_full = entry.get("full", "")
            inj_status = entry.get("status", "").lower()
            
            # Match: first 3 chars of first name match OR full names very similar
            inj_first_3 = inj_full.split()[0][:3] if inj_full.split() else ""
            if first_3 == inj_first_3 or full == inj_full:
                if "out" in inj_status or "il" in inj_status or "injured" in inj_status or "suspend" in inj_status or "rest" in inj_status:
                    return False, "OUT"
                elif "doubtful" in inj_status:
                    return False, "DOUBTFUL"
                elif "questionable" in inj_status or "day-to-day" in inj_status or "dtd" in inj_status:
                    return False, "QUESTIONABLE"
                elif "probable" in inj_status:
                    return True, "PROBABLE"
                elif "available" in inj_status:
                    return True, "AVAILABLE"
    
    return True, "AVAILABLE"

def update_position_prices():
    """Fetch current Polymarket prices for open positions."""
    for pos in open_positions:
        tid = pos.get("token_id", "")
        if not tid: continue
        try:
            resp = requests.get(f"https://clob.polymarket.com/price?token_id={tid}&side=buy", timeout=5)
            if resp.ok:
                data = resp.json()
                pos["current_price"] = float(data.get("price", 0))
        except: pass

def fetch_polymarket_positions():
    """Fetch ALL real positions from Polymarket Data API."""
    global open_positions
    if not POLY_SAFE_ADDRESS: return
    try:
        resp = requests.get(
            f"https://data-api.polymarket.com/positions?user={POLY_SAFE_ADDRESS}&sizeThreshold=0.1&limit=50",
            timeout=10
        )
        if not resp.ok: 
            log.warning(f"  [SYNC] Data API returned {resp.status_code}")
            return
        positions = resp.json()
        if not isinstance(positions, list): return
        
        real_positions = []
        for p in positions:
            size = float(p.get("size", 0))
            if size < 0.1: continue
            
            avg_price = float(p.get("avgPrice", 0))
            cur_price = float(p.get("curPrice", 0))
            initial_val = float(p.get("initialValue", 0))
            current_val = float(p.get("currentValue", 0))
            cash_pnl = float(p.get("cashPnl", 0))
            pct_pnl = float(p.get("percentPnl", 0))
            
            title = p.get("title", "")
            outcome = p.get("outcome", "")
            event_slug = p.get("eventSlug", "")
            token_id = p.get("asset", "")
            condition_id = p.get("conditionId", "")
            
            # Detect prop type from title
            tl = title.lower()
            ptype = "Stats"
            if "points" in tl: ptype = "Pts"
            elif "rebound" in tl: ptype = "Reb"
            elif "assist" in tl: ptype = "Ast"
            elif "3-pointer" in tl or "three" in tl: ptype = "3PT"
            elif "steal" in tl: ptype = "Stl"
            elif "block" in tl: ptype = "Blk"
            elif "spread" in tl: ptype = "Spread"
            elif "o/u" in tl and ":" not in tl: ptype = "O/U"
            
            # Extract player and line from title
            # Format: "Player Name: Type O/U 24.5"
            player = ""
            line = ""
            if ":" in title:
                player = title.split(":")[0].strip()
                import re
                nums = re.findall(r'[\d]+\.[\d]+', title)
                if nums: line = nums[0]
            else:
                player = title
            
            # Side from outcome
            side = outcome
            if outcome.lower() in ("yes", "over"): side = "Over"
            elif outcome.lower() in ("no", "under"): side = "Under"
            
            # Match from event_slug
            match_str = event_slug.replace("-", " ").title() if event_slug else ""
            # Try to extract teams
            home = ""; away = ""
            for sep in [" Vs ", " V "]:
                if sep.lower() in event_slug:
                    parts = event_slug.split("-vs-") if "-vs-" in event_slug else event_slug.split("-v-")
                    if len(parts) >= 2:
                        away = parts[0].replace("-", " ").strip().title()
                        home = parts[1].replace("-", " ").strip().title()
                    break
            
            outcome_str = f"{player} {ptype} {side} {line}" if player else title
            
            real_positions.append({
                "token_id": token_id,
                "outcome": outcome_str,
                "entry_price": avg_price,
                "current_price": cur_price,
                "size": round(initial_val, 2),
                "shares": size,
                "value": round(current_val, 2),
                "pnl": round(cash_pnl, 2),
                "pct_pnl": round(pct_pnl * 100, 1),
                "edge": 0,
                "fair_prob": 0,
                "question": title,
                "home": home,
                "away": away,
                "status": "LIVE",
                "event_slug": event_slug,
            })
        
        if real_positions:
            open_positions = real_positions
    except Exception as e:
        log.warning(f"  [SYNC] Failed: {e}")

# ── SCAN ──
def scan_opportunities():
    global credits_used, last_scan_time, last_opportunities, scan_count
    log.info("="*60)
    log.info(f"PROPS ARB SCAN | {datetime.now(timezone.utc).strftime('%H:%M UTC')} | {'DRY' if DRY_RUN else 'LIVE'} | edge>={MIN_EDGE:.0%}")
    log.info("="*60)
    
    all_opps = []; total_props = 0; seen = set()
    _poly_game_cache.clear()  # fresh cache each scan
    
    for sc in SPORTS:
        events = get_events(sc["sport"], sc["league"])
        # Try alt leagues if primary returns nothing (e.g. UFC)
        if not events and sc.get("alt_leagues"):
            for alt in sc["alt_leagues"]:
                events = get_events(sc["sport"], alt)
                if events:
                    log.info(f"  [{sc['name']}] Found events with league={alt}")
                    break
        if not events:
            log.info(f"\n  {sc['name']}: no events from API")
            continue
        pending = [e for e in events if e.get("status") in ("pending", None, "")]
        if not pending and events:
            statuses = [e.get("status","?") for e in events[:5]]
            log.info(f"\n  {sc['name']}: {len(events)} events but 0 pending (statuses: {statuses})")
        
        # UFC: only fights in next 7 days (not months ahead)
        if sc["name"] == "UFC":
            cutoff = datetime.now(timezone.utc) + timedelta(days=7)
            filtered = []
            for e in pending:
                ct = e.get("commence_time", "")
                if ct:
                    try:
                        t = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                        if t <= cutoff:
                            filtered.append(e)
                    except: filtered.append(e)
                else:
                    filtered.append(e)
            pending = filtered[:15]
        else:
            pending = pending[:12]
        log.info(f"\n  {sc['name']}: {len(pending)} games")
        
        eids = [e["id"] for e in pending if e.get("id")]
        omap = {}
        for i in range(0, len(eids), 10):
            for od in get_odds_batch(eids[i:i+10]):
                if od.get("id"): omap[od["id"]] = od
            time.sleep(0.5)
        
        for evt in pending:
            eid = evt.get("id"); home = evt.get("home",""); away = evt.get("away","")
            if not eid or not home or not away: continue
            od = omap.get(eid)
            if not od: od = get_odds_single(eid)
            if not od: continue
            
            # UFC: skip player props (MMA doesn't have them), only use ML below
            if sc["name"] == "UFC":
                continue
            
            props = extract_props(od)
            total_props += len(props)
            if props: log.info(f"    {home} vs {away}: {len(props)} props")
            
            # Debug: show what Polymarket returns for first game with props
            if props and total_props == len(props):  # first game
                poly_mkts = get_poly_game_markets(home, away)
                log.info(f"    [DEBUG] Poly returned {len(poly_mkts)} markets for {home} vs {away}")
                for pm_raw in poly_mkts[:5]:
                    q = pm_raw.get("question", pm_raw.get("groupItemTitle", ""))[:80]
                    log.info(f"    [DEBUG]   -> {q}")
            
            for p in props[:5]:  # debug first 5 props
                matches = find_poly_prop(p["player"], p["type"], p["line"], home, away)
                if matches:
                    for m in matches:
                        side = m["side"]; pp = m["price"]; tok = m["token"]
                        if "Over" in side: fp = p["fair_over"]
                        else: fp = p["fair_under"]
                        edge = fp - pp
                        log.info(f"      [MATCH] {p['player']} {p['type']} {side} {p['line']} | poly={pp:.2f} fair={fp:.2f} edge={edge:.1%}")
                else:
                    plast = p["player"].split()[-1].lower()
                    mkts = get_poly_game_markets(home, away)
                    player_mkts = [m for m in mkts if plast in (m.get("question","") or "").lower()]
                    if player_mkts:
                        qs = [m.get("question","")[:70] for m in player_mkts[:3]]
                        log.info(f"      [NO MATCH] {p['player']} {p['type']} line={p['line']} | Poly has: {qs}")
                    else:
                        log.info(f"      [NO PLAYER] {p['player']} ({plast}) not found in {len(mkts)} Poly markets")
            
            for p in props:
                matches = find_poly_prop(p["player"], p["type"], p["line"], home, away)
                if not matches: continue
                
                for match in matches:
                    side = match["side"]; pp = match["price"]; tok = match["token"]
                    pm = match["market"]
                    
                    if tok in seen: continue
                    
                    if "Over" in side: fp = p["fair_over"]
                    else: fp = p["fair_under"]
                    
                    edge = fp - pp
                    if edge < MIN_EDGE: continue
                    
                    ev = edge/pp*100 if pp > 0 else 0
                    payout = 1/pp if pp > 0 else 1
                    kelly = max(0, ((fp*payout-1)/(payout-1))*0.25) if payout > 1 else 0
                    
                    # Get prop type from Polymarket question (more reliable)
                    q = pm["question"]
                    ql = q.lower()
                    prop_label = p.get('type_short', p['type'])
                    # NBA
                    if "points" in ql and "earned" not in ql: prop_label = "Pts"
                    elif "rebounds" in ql: prop_label = "Reb"
                    elif "assists" in ql: prop_label = "Ast"
                    elif "3-pointers" in ql or "three" in ql or "3pt" in ql: prop_label = "3PT"
                    elif "steals" in ql: prop_label = "Stl"
                    elif "blocks" in ql: prop_label = "Blk"
                    elif "turnovers" in ql: prop_label = "TO"
                    # MLB
                    elif "home run" in ql or "homer" in ql: prop_label = "HR"
                    elif "total bases" in ql: prop_label = "TB"
                    elif "rbi" in ql or "runs batted" in ql: prop_label = "RBI"
                    elif "strikeout" in ql: prop_label = "K"
                    elif "hits allowed" in ql: prop_label = "HA"
                    elif "earned run" in ql: prop_label = "ER"
                    elif "hits" in ql: prop_label = "Hits"
                    elif "walks" in ql or "bases on balls" in ql: prop_label = "BB"
                    elif "stolen base" in ql: prop_label = "SB"
                    elif "runs" in ql and "earned" not in ql: prop_label = "R"
                    elif "outs" in ql and "strike" not in ql: prop_label = "Outs"
                    # NHL
                    elif "goals" in ql and "against" not in ql: prop_label = "G"
                    elif "shots" in ql: prop_label = "SOG"
                    elif "saves" in ql: prop_label = "SV"
                    elif "goals against" in ql: prop_label = "GA"
                    
                    out = f"{p['player']} {prop_label} {side} {p['line']}"
                    seen.add(tok)
                    all_opps.append({
                        "sport": sc["name"], "home": home, "away": away,
                        "player": p["player"], "prop_type": p["type"], "line": p["line"],
                        "side": side, "outcome": out,
                        "poly_price": pp, "fair_prob": fp,
                        "edge": edge, "ev_pct": ev, "kelly": kelly,
                        "source": p["source"], "book_count": 1,
                        "token_id": tok, "market_id": pm["id"],
                        "question": pm["question"], "volume": pm["volume"],
                        "liquidity": pm["liquidity"], "condition_id": pm["condition_id"],
                        "neg_risk": pm.get("neg_risk", False),
                        "commence_time": evt.get("commence_time", ""),
                    })
                    log.info(f"      [EDGE] {out} @ {pp:.2f} | fair={fp:.2f} | edge={edge:.1%}")
            time.sleep(0.2)
        
        # Also ML (main strategy for UFC)
        ml_limit = 15 if sc["name"] == "UFC" else 8
        for evt in pending[:ml_limit]:
            eid = evt.get("id"); home = evt.get("home",""); away = evt.get("away","")
            od = omap.get(eid); 
            if not od: continue
            ml, src = extract_ml(od)
            if not ml: continue
            hl = home.split()[-1]; al = away.split()[-1]
            for ev in poly_search(f"{hl} {al}", 5):
                mkts = ev.get("markets", [ev] if ev.get("id") else [])
                for raw in mkts:
                    pm = parse_mkt(raw, ev.get("title",""))
                    if not pm: continue
                    ql = pm["question"].lower()
                    if any(k in ql for k in ["championship","champion","mvp","win the 20","playoff","season"]): continue
                    if hl.lower() not in ql and al.lower() not in ql: continue
                    if not any(o.lower() in ("yes","no") for o in pm["outcomes"]): continue
                    yt = None
                    for tn in ml:
                        if tn == "Draw": continue
                        tw = [w.lower() for w in tn.split() if len(w)>2]
                        if tw and tw[-1] in ql: yt = tn; break
                    if not yt: continue
                    yp = pm["prices"].get("Yes", pm["prices"].get("yes",0))
                    ytok = pm["tokens"].get("Yes", pm["tokens"].get("yes",""))
                    if ytok and ytok not in seen and MIN_PRICE <= yp <= MAX_PRICE:
                        edge = ml[yt] - yp
                        if edge >= MIN_EDGE:
                            ev_pct = edge/yp*100; pay=1/yp
                            kelly = max(0, ((ml[yt]*pay-1)/(pay-1))*0.25) if pay>1 else 0
                            seen.add(ytok)
                            all_opps.append({"sport":sc["name"],"home":home,"away":away,"player":"","prop_type":"ML","line":0,"side":"Win","outcome":f"ML {yt}","poly_price":yp,"fair_prob":ml[yt],"edge":edge,"ev_pct":ev_pct,"kelly":kelly,"source":src,"book_count":1,"token_id":ytok,"market_id":pm["id"],"question":pm["question"],"volume":pm["volume"],"liquidity":pm["liquidity"],"condition_id":pm["condition_id"],"neg_risk":pm.get("neg_risk",False)})
                            log.info(f"    [ML] {yt} @ {yp:.2f} | fair={ml[yt]:.2f} | edge={edge:.1%}")
                    break
    
    all_opps.sort(key=lambda x: x["edge"], reverse=True)
    
    # ── LADDER: adjacent lines for top opps ──
    # ex: Banchero Over 24.5 ($2) → also bet 25.5 ($1) and 26.5 ($0.50)
    ladder_opps = []
    ladder_seen = set()
    LADDER_STEPS = [(1.0, 1.0), (2.0, 0.50)]  # (offset, bet $)
    
    for opp in all_opps[:10]:
        if opp["edge"] < 0.20: continue
        player = opp.get("player", "")
        if not player: continue
        
        is_over = "Over" in opp.get("side", "")
        base_line = opp.get("line", 0)
        home = opp["home"]; away = opp["away"]
        
        for offset, ladder_amt in LADDER_STEPS:
            adj_line = base_line + offset if is_over else base_line - offset
            if adj_line <= 0: continue
            
            lkey = f"{player}|{adj_line}|{'O' if is_over else 'U'}"
            if lkey in ladder_seen: continue
            
            match = find_poly_prop(player, opp.get("prop_type", ""), adj_line, home, away)
            if not match: continue
            
            for m in match:
                if is_over and "Over" not in m["side"]: continue
                if not is_over and "Under" not in m["side"]: continue
                
                pp = m["price"]; tok = m["token"]
                if tok in seen or tok in ladder_seen: continue
                if not (MIN_PRICE <= pp <= MAX_PRICE): continue
                
                # Conservative fair prob: -3% per point away
                adj_fair = opp["fair_prob"] - (offset * 0.03)
                if adj_fair <= pp: continue
                
                edge = adj_fair - pp
                if edge < 0.10: continue  # 10% min for ladder
                
                ev = edge/pp*100 if pp > 0 else 0
                payout = 1/pp if pp > 0 else 1
                kelly = max(0, ((adj_fair*payout-1)/(payout-1))*0.25) if payout > 1 else 0
                
                out = f"{player} Stats {'Over' if is_over else 'Under'} {adj_line}"
                ladder_seen.add(lkey); ladder_seen.add(tok)
                
                ladder_opps.append({
                    "sport": opp["sport"], "home": home, "away": away,
                    "player": player, "prop_type": opp.get("prop_type", ""),
                    "line": adj_line, "side": m["side"], "outcome": f"[L] {out}",
                    "poly_price": pp, "fair_prob": adj_fair,
                    "edge": edge, "ev_pct": ev, "kelly": kelly,
                    "source": opp["source"], "book_count": 1,
                    "token_id": tok, "market_id": m["market"]["id"],
                    "question": m["market"]["question"],
                    "volume": m["market"]["volume"],
                    "liquidity": m["market"]["liquidity"],
                    "condition_id": m["market"]["condition_id"],
                    "neg_risk": m["market"].get("neg_risk", False),
                    "_is_ladder": True, "_ladder_amt": ladder_amt,
                })
                log.info(f"    [LADDER +{offset:.0f}] {out} @ {pp:.2f} | ~fair={adj_fair:.2f} | edge={edge:.1%} | ${ladder_amt}")
                break
    
    all_opps.extend(ladder_opps)
    all_opps.sort(key=lambda x: x["edge"], reverse=True)
    log.info(f"\n  Summary: {total_props} props | {len(all_opps)} opps ({len(ladder_opps)} ladder) | edge >= {MIN_EDGE:.0%} | {credits_used} req")
    
    executed = 0
    
    # PHASE 1: Validate all opps (dedup, injury, liquidity) without executing
    valid_opps = []
    log.info(f"\n  Validating {len(all_opps)} opportunities...")
    
    for opp in all_opps:
        if daily_pnl <= DAILY_LOSS_LIMIT: break
        
        bk = _bet_key(opp)
        tid = opp["token_id"]
        times_bet = max(_already_bet.get(bk, 0), _already_bet.get(tid, 0))
        
        if times_bet >= MAX_BET_REPEATS:
            continue
        
        if times_bet > 0:
            if opp["fair_prob"] < 0.75:
                continue
        
        if opp["liquidity"] < 1:  # only skip if literally zero liquidity
            continue
        
        # PLAYER PROP FILTER — block game-level bets (ML, spreads, totals, NRFI, etc.)
        ptype = opp.get("prop_type", "").upper()
        opp_player = opp.get("player", "")
        opp_sport = opp.get("sport", "").upper()
        # Allow ML only for UFC (MMA has no player props, ML is the only strategy)
        if ptype == "ML" and opp_sport == "UFC":
            pass  # allowed
        elif ptype in ("ML", "SPREAD", "O/U", "TOTAL", "NRFI") or (not opp_player and ptype not in ("PTS", "REB", "AST", "3PT", "STL", "BLK", "STATS")):
            log.info(f"    Skip: {opp.get('outcome','')} — not a player prop (type={ptype}, sport={opp_sport})")
            continue
        
        # INJURY CHECK
        player = opp.get("player", "")
        if player:
            available, inj_status = is_player_available(player)
            if not available:
                log.info(f"    Skip: {player} is {inj_status}")
                continue
        
        # PRICE SANITY CHECK (4th layer of protection)
        # If Polymarket mid-price is very low, the market likely knows something
        # our injury data doesn't — player may be OUT/GTD with stale sportsbook lines
        poly_mid = opp.get("poly_price", 0)
        fair_prob = opp.get("fair_prob", 0)
        calc_edge = opp.get("edge", 0)
        if poly_mid <= 0.25 and calc_edge >= 0.20:
            # Market pricing at ≤25¢ but our model says 20%+ edge = RED FLAG
            # Polymarket traders almost certainly know this player won't play
            log.info(f"    Skip: {player or opp.get('outcome','')} — PRICE SANITY FAIL (poly={poly_mid:.2f}, edge={calc_edge:.1%}) — market likely knows player is OUT")
            continue
        if poly_mid <= 0.15:
            # Any prop at ≤15¢ is almost certainly a dead market (player OUT/DNP)
            log.info(f"    Skip: {player or opp.get('outcome','')} — poly price {poly_mid:.2f} too low, likely OUT/DNP")
            continue
        
        opp["_times_bet"] = times_bet
        opp["_bet_key"] = bk
        valid_opps.append(opp)
    
    log.info(f"  {len(valid_opps)} passed validation (dedup/injury/liq)")
    
    # PHASE 2: Fetch real ask prices for all valid opps
    log.info(f"  Fetching real ask prices...")
    tradeable = []
    limit_candidates = []  # opps with mid edge but no real edge — for limit orders
    
    for opp in valid_opps:
        best_ask = get_best_ask(opp["token_id"])
        fair = opp["fair_prob"]
        mid_price = opp["poly_price"]  # mid/last trade price
        mid_edge = opp["edge"]         # edge based on mid price
        
        if best_ask and 0.01 < best_ask < 0.95:
            real_edge = fair - best_ask
            opp["_real_ask"] = best_ask
            opp["_real_edge"] = real_edge
            
            # SPREAD SANITY CHECK: if ask is 2x+ the mid price, market is dead/suspicious
            # e.g. mid=0.32, ask=0.64 → spread ratio 2.0x = likely OUT player, stale mid
            if mid_price > 0.01 and best_ask / mid_price >= 1.8 and mid_price <= 0.40:
                log.info(f"    ⚠ {opp['outcome']} | ask={best_ask:.2f} vs mid={mid_price:.2f} (spread {best_ask/mid_price:.1f}x) — suspicious spread, skipping")
                continue
            
            if real_edge >= MIN_EDGE:
                tradeable.append(opp)
                log.info(f"    ✓ {opp['outcome']} | ask={best_ask:.2f} fair={fair:.2f} real_edge={real_edge:.1%}")
            elif LIMIT_ORDER_ENABLED and mid_edge >= LIMIT_ORDER_MIN_MID_EDGE:
                # Mid-price has good edge but ask is too expensive → limit order candidate
                limit_candidates.append(opp)
                log.info(f"    ⏳ {opp['outcome']} | ask={best_ask:.2f} mid={mid_price:.2f} fair={fair:.2f} mid_edge={mid_edge:.1%} real_edge={real_edge:.1%} → LIMIT")
            else:
                log.info(f"    ✗ {opp['outcome']} | ask={best_ask:.2f} fair={fair:.2f} real_edge={real_edge:.1%} < {MIN_EDGE:.0%}")
        else:
            log.info(f"    ? {opp['outcome']} | no valid ask, skipping")
    
    # PHASE 3: Sort by REAL edge (highest first) and execute market orders
    tradeable.sort(key=lambda x: x["_real_edge"], reverse=True)
    
    if tradeable:
        log.info(f"\n  === TOP {len(tradeable)} TRADES BY REAL EDGE ===")
        for i, opp in enumerate(tradeable):
            log.info(f"  #{i+1} {opp['outcome']} | ask={opp['_real_ask']:.2f} fair={opp['fair_prob']:.2f} real_edge={opp['_real_edge']:.1%}")
    else:
        log.info(f"\n  No trades with real edge >= {MIN_EDGE:.0%}")
    
    for opp in tradeable:
        bk = opp["_bet_key"]
        tid = opp["token_id"]
        times_bet = opp["_times_bet"]
        
        log.info(f"\n{'-'*50}\n  {opp['home']} vs {opp['away']}\n  {opp['outcome']}\n  Ask={opp['_real_ask']:.2f} Fair={opp['fair_prob']:.2f} RealEdge={opp['_real_edge']:.1%}\n  Source={opp['source']} Liq=${opp['liquidity']:.0f}")
        
        if execute_trade(opp):
            executed += 1
            _already_bet[bk] = times_bet + 1
            _already_bet[tid] = times_bet + 1
            _save_bets()
    
    # PHASE 4: Place limit orders for candidates with good mid-edge but expensive asks
    limit_placed = 0
    if limit_candidates and LIMIT_ORDER_ENABLED:
        # Sort by mid_edge (highest first)
        limit_candidates.sort(key=lambda x: x["edge"], reverse=True)
        log.info(f"\n  === LIMIT ORDER CANDIDATES: {len(limit_candidates)} ===")
        for opp in limit_candidates:
            # CHECK _already_bet to prevent duplicate fills
            bk = _bet_key(opp)
            tid = opp["token_id"]
            times_bet = max(_already_bet.get(bk, 0), _already_bet.get(tid, 0))
            if times_bet >= MAX_BET_REPEATS:
                continue  # already filled on this prop, skip silently
            
            mid_price = opp["poly_price"]
            best_ask = opp["_real_ask"]
            fair = opp["fair_prob"]
            
            # LIMIT ORDER SANITY: don't place limits on suspiciously cheap props
            if mid_price <= 0.25:
                log.info(f"    ⚠ LIMIT SKIP: {opp['outcome']} | mid={mid_price:.2f} too low — likely OUT/DNP")
                continue
            
            log.info(f"    {opp['outcome']} | mid={mid_price:.2f} ask={best_ask:.2f} fair={fair:.2f} mid_edge={opp['edge']:.1%}")
            
            if place_limit_order(opp, mid_price, best_ask, fair):
                limit_placed += 1
    
    log.info(f"\n{'='*60}\nDONE: {executed} market + {limit_placed} limit | {len(all_opps)} opps | {credits_used} req\n{'='*60}\n")
    last_scan_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    last_opportunities = all_opps[:30]; scan_count += 1
    return all_opps

# ── HTTP API ──
class H(BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def do_OPTIONS(self): self.send_response(200); self._c(); self.end_headers()
    def _c(self):
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type")
    def do_GET(self):
      try:
        p = self.path.split("?")[0]
        if p == "/api/status":
            fetch_polymarket_positions()
            # Fetch live NBA scores + player stats
            live_scores = {}
            player_stats = {}
            try:
                scores_resp = requests.get(
                    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
                    timeout=5
                )
                if scores_resp.ok:
                    sb = scores_resp.json()
                    game_ids_to_fetch = []
                    for ev in sb.get("events", []):
                        comp = ev.get("competitions", [{}])[0]
                        state = comp.get("status", {}).get("type", {}).get("state", "")
                        clock = comp.get("status", {}).get("displayClock", "")
                        period = comp.get("status", {}).get("period", 0)
                        gid = ev.get("id", "")
                        teams_in_game = []
                        score_parts = []
                        for tm in comp.get("competitors", []):
                            tname = tm.get("team", {}).get("displayName", "").lower()
                            short = tm.get("team", {}).get("shortDisplayName", "").lower()
                            abbr = tm.get("team", {}).get("abbreviation", "").upper()
                            sc = tm.get("score", "0")
                            teams_in_game.append({"name": tname, "short": short, "abbr": abbr, "score": sc})
                            score_parts.append(f"{abbr} {sc}")
                        score_str = " - ".join(score_parts)
                        game_info = {"state": state, "clock": clock, "period": period, "score": score_str, "teams": teams_in_game, "id": gid}
                        for t in teams_in_game:
                            live_scores[t["name"]] = game_info
                            live_scores[t["short"]] = game_info
                            live_scores[t["abbr"].lower()] = game_info
                        if state in ("in", "post"):
                            game_ids_to_fetch.append(gid)
                    
                    # Fetch boxscore for live/finished games (max 3 for speed)
                    # Prioritize: 1) live games matching positions, 2) other games matching positions, 3) rest
                    pos_slugs = " ".join([p.get("event_slug","") for p in open_positions]).lower()
                    live_priority = []
                    done_priority = []
                    other_ids = []
                    # Build a map of game_id -> game_info for state checking
                    gid_state = {}
                    for k, v in live_scores.items():
                        if v.get("id"): gid_state[v["id"]] = v.get("state","")
                    for gid in game_ids_to_fetch:
                        ginfo_check = None
                        for k, v in live_scores.items():
                            if v.get("id") == gid:
                                ginfo_check = v
                                break
                        is_live = gid_state.get(gid) == "in"
                        if ginfo_check:
                            teams_match = any(t["name"] in pos_slugs or t["short"] in pos_slugs or t["abbr"].lower() in pos_slugs for t in ginfo_check.get("teams",[]))
                            if teams_match and is_live:
                                live_priority.append(gid)
                            elif teams_match:
                                done_priority.append(gid)
                            else:
                                other_ids.append(gid)
                        else:
                            other_ids.append(gid)
                    fetch_ids = (live_priority + done_priority + other_ids)[:8]
                    for gid in fetch_ids:
                        try:
                            br = requests.get(f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={gid}", timeout=5)
                            if not br.ok: continue
                            bdata = br.json()
                            for team_box in bdata.get("boxscore", {}).get("players", []):
                                for sg in team_box.get("statistics", []):
                                    hdrs = [h.lower() for h in sg.get("labels", [])]
                                    for ath in sg.get("athletes", []):
                                        ai = ath.get("athlete", {})
                                        dn = ai.get("displayName", "")
                                        if not dn: continue
                                        ln = dn.split()[-1].lower()
                                        fn = dn.split()[0].lower() if dn.split() else ""
                                        vals = ath.get("stats", [])
                                        ps = {"name": dn, "first": fn}
                                        for i, v in enumerate(vals):
                                            if i < len(hdrs):
                                                try:
                                                    ps[hdrs[i]] = float(v) if v and '-' not in str(v) else v
                                                except:
                                                    ps[hdrs[i]] = v
                                        if ln not in player_stats: player_stats[ln] = []
                                        player_stats[ln].append(ps)
                        except: continue
            except: pass

            # Enrich positions
            enriched_positions = []
            for pos in open_positions:
                epos = dict(pos)
                cur = epos.get("current_price", 0)
                entry = epos.get("entry_price", 0)
                
                slug = epos.get("event_slug", "").lower()
                home = epos.get("home", "").lower()
                away = epos.get("away", "").lower()
                
                matched_game = None
                for key, ginfo in live_scores.items():
                    if not key or len(key) < 3:
                        continue
                    # Check if this key matches our position's teams/slug
                    match_target = f"{slug} {home} {away}"
                    if key in match_target:
                        # Verify the OTHER team also matches (prevent cross-game matching)
                        other_teams = [t for t in ginfo.get("teams", []) 
                                      if t.get("name","").lower() != key and t.get("short","").lower() != key and t.get("abbr","").lower() != key]
                        if other_teams:
                            other_match = False
                            for ot in other_teams:
                                oname = ot.get("name","").lower()
                                oshort = ot.get("short","").lower()
                                oabbr = ot.get("abbr","").lower()
                                if oname in match_target or oshort in match_target or oabbr in match_target:
                                    other_match = True
                                    break
                            if other_match:
                                matched_game = ginfo
                                break
                        else:
                            # Single team game info, use as-is (shouldn't happen)
                            matched_game = ginfo
                            break
                if matched_game:
                    state = matched_game["state"]
                    if state == "in":
                        epos["game_status"] = "inprogress"
                        epos["game_quarter"] = matched_game["period"]
                        epos["game_clock"] = matched_game["clock"]
                        epos["game_score"] = matched_game["score"]
                    elif state == "post":
                        epos["game_status"] = "closed"
                        epos["game_score"] = matched_game["score"]
                    elif state == "pre":
                        epos["game_status"] = "scheduled"
                
                # DNP check: only mark as closed/DNP if game has actually started or finished
                # If price crashed to ~0 but game hasn't started, keep as scheduled/pending
                game_st = epos.get("game_status", "")
                if cur <= 0.01 and entry > 0.05:
                    if game_st in ("inprogress", "closed", "complete"):
                        # Game started/finished and price is 0 = genuine DNP or loss
                        epos["game_status"] = "closed"
                        epos["game_note"] = "DNP"
                    # If game_status is scheduled/empty, do NOT override - keep as pending
                
                # Match player live stats
                question = epos.get("question", "")
                player_name = question.split(":")[0].strip() if ":" in question else ""
                if player_name and player_stats:
                    # Normalize accented chars: Dončić -> doncic, Šengün -> sengun
                    import unicodedata
                    def normalize_name(n):
                        return ''.join(c for c in unicodedata.normalize('NFD', n.lower()) if unicodedata.category(c) != 'Mn')
                    plast_raw = player_name.split()[-1].lower()
                    plast_norm = normalize_name(player_name.split()[-1])
                    pfirst3 = normalize_name(player_name.split()[0])[:3] if player_name.split() else ""
                    # Try exact last name first, then normalized
                    matched_ps = None
                    for try_key in [plast_raw, plast_norm]:
                        if try_key in player_stats:
                            for ps in player_stats[try_key]:
                                ps_first3 = normalize_name(ps.get("first",""))[:3]
                                if ps_first3 == pfirst3 or len(player_stats[try_key]) == 1:
                                    matched_ps = ps
                                    break
                        if matched_ps: break
                    if matched_ps:
                        ps = matched_ps
                        ql = question.lower()
                        sv = None; sl = ""
                        if "points" in ql: sv = ps.get("pts", ps.get("points")); sl = "PTS"
                        elif "rebound" in ql: sv = ps.get("reb", ps.get("rebounds")); sl = "REB"
                        elif "assist" in ql: sv = ps.get("ast", ps.get("assists")); sl = "AST"
                        elif "steal" in ql: sv = ps.get("stl", ps.get("steals")); sl = "STL"
                        elif "block" in ql: sv = ps.get("blk", ps.get("blocks")); sl = "BLK"
                        elif "3-pointer" in ql or "three" in ql:
                            tpt = ps.get("3pt", "0-0")
                            sv = float(str(tpt).split("-")[0]) if isinstance(tpt, str) and "-" in str(tpt) else tpt
                            sl = "3PT"
                        elif "turnover" in ql: sv = ps.get("to", ps.get("turnovers")); sl = "TO"
                        if sv is not None:
                            try: epos["live_stat_val"] = float(sv)
                            except: epos["live_stat_val"] = 0
                            epos["live_stat_label"] = sl
                            import re as _re
                            lm = _re.findall(r'[\d]+\.[\d]+', question)
                            if lm: epos["prop_line"] = float(lm[0])
                        mins = ps.get("min", ps.get("minutes", ""))
                        if mins: epos["live_minutes"] = str(mins)
                enriched_positions.append(epos)
            self._j({"mode":"dry_run" if DRY_RUN else "live","bot":"props_arb","scan_count":scan_count,"last_scan":last_scan_time,"credits_used":credits_used,"trades_executed":trades_executed,"daily_pnl":round(daily_pnl,2),"open_positions":len(open_positions),"opportunities_count":len(last_opportunities),"config":{"min_edge":MIN_EDGE,"bet_size":BET_SIZE,"max_open":MAX_OPEN_POS,"limit_orders_enabled":LIMIT_ORDER_ENABLED},"opportunities":last_opportunities[:20],"positions":enriched_positions,"trades":trade_history[-20:],"limit_orders":_pending_limit_orders})
        elif p == "/health": self._j({"ok":True})
        else: self.send_response(404); self.end_headers()
      except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
        pass
      except Exception as e:
        try: self.send_response(500); self.end_headers()
        except: pass
    def _j(self,d):
        try:
            b = json.dumps(d).encode(); self.send_response(200); self._c()
            self.send_header("Content-Type","application/json"); self.send_header("Content-Length",len(b))
            self.end_headers(); self.wfile.write(b)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass  # Browser closed connection early — harmless
    def log_message(self, format, *args):
        pass  # Suppress default HTTP request logging

def start_api(port=8766):
    s = ThreadingHTTPServer(("localhost",port),H)
    threading.Thread(target=s.serve_forever,daemon=True).start()
    log.info(f"API em http://localhost:{port}")

def panic_sell_injured():
    """Sell positions where the player has been confirmed OUT after we bought."""
    if DRY_RUN or not POLY_SAFE_ADDRESS or not WALLET_PRIVATE_KEY:
        return
    global open_positions, daily_pnl
    
    # Load fresh injuries
    try:
        load_injuries()
    except:
        return
    
    to_sell = []
    for pos in open_positions:
        player = pos.get("player", "")
        if not player:
            continue
        available, inj_status = is_player_available(player)
        status_upper = inj_status.upper() if inj_status else ""
        # Only panic sell if confirmed OUT or DNP (not QUESTIONABLE — they might play)
        if status_upper in ("OUT", "DNP", "SUSPENDED", "NOT WITH TEAM"):
            to_sell.append((pos, inj_status))
    
    if not to_sell:
        return
    
    log.info(f"\n  === PANIC SELL: {len(to_sell)} positions with injured/OUT players ===")
    
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL
        c = ClobClient(CLOB_API, key=WALLET_PRIVATE_KEY, chain_id=137, signature_type=2, funder=POLY_SAFE_ADDRESS)
        c.set_api_creds(c.create_or_derive_api_creds())
        
        sold = []
        for pos, inj_status in to_sell:
            token_id = pos.get("token_id", "")
            outcome = pos.get("outcome", "")
            entry = pos.get("entry_price", 0)
            size = pos.get("size", 0)
            
            if not token_id:
                continue
            
            # Get current best bid to sell into
            try:
                book = requests.get(f"{CLOB_API}/book?token_id={token_id}", timeout=10)
                if book.ok:
                    bids = book.json().get("bids", [])
                    valid_bids = [float(b.get("price", 0)) for b in bids if float(b.get("price", 0)) > 0.005]
                    if not valid_bids:
                        log.info(f"  [PANIC] {outcome} — no bids, cannot sell (will be total loss)")
                        continue
                    best_bid = max(valid_bids)
                else:
                    continue
            except:
                continue
            
            # Calculate shares from entry (size is dollar amount, shares = size / entry_price)
            shares = round(size / entry, 2) if entry > 0 else 0
            if shares < 1:
                continue
            
            log.info(f"  [PANIC SELL] {outcome} | player {pos.get('player','')} is {inj_status} | entry={entry:.2f} bid={best_bid:.2f} | {shares} shares")
            
            try:
                sell_order = c.create_market_order(MarketOrderArgs(
                    token_id=token_id,
                    amount=round(shares * best_bid, 4),
                    side=SELL,
                    price=best_bid
                ))
                result = c.post_order(sell_order, OrderType.FAK)
                if result and result.get("success"):
                    sell_price = float(result.get("takingAmount", 0)) / shares if shares > 0 else 0
                    loss = float(result.get("takingAmount", 0)) - size
                    daily_pnl += loss
                    log.info(f"  [PANIC OK] Sold {outcome} | got ${result.get('takingAmount', '?')} | loss=${loss:.2f}")
                    sold.append(pos)
                else:
                    log.warning(f"  [PANIC FAIL] {outcome}: {result}")
            except Exception as e:
                log.warning(f"  [PANIC ERR] {outcome}: {e}")
        
        # Remove sold positions
        if sold:
            sold_ids = {id(p) for p in sold}
            open_positions = [p for p in open_positions if id(p) not in sold_ids]
            log.info(f"  [PANIC] Removed {len(sold)} sold positions")
    
    except ImportError:
        log.warning("  [PANIC] py-clob-client not available")
    except Exception as e:
        log.error(f"  [PANIC] Error: {e}")

def auto_claim():
    """Auto-claim resolved positions on Polymarket. Runs after each scan."""
    if DRY_RUN or not POLY_SAFE_ADDRESS or not WALLET_PRIVATE_KEY:
        return
    try:
        # Check for claimable positions via merge endpoint
        # Polymarket auto-resolves most positions, but we can trigger claim via CLOB
        from py_clob_client.client import ClobClient
        c = ClobClient(CLOB_API, key=WALLET_PRIVATE_KEY, chain_id=137, signature_type=2, funder=POLY_SAFE_ADDRESS)
        
        # Fetch current positions
        resp = requests.get(
            f"https://data-api.polymarket.com/positions?user={POLY_SAFE_ADDRESS}&sizeThreshold=0.1&limit=50",
            timeout=10
        )
        if not resp.ok:
            return
        positions = resp.json()
        if not isinstance(positions, list):
            return
        
        claimed = 0
        for p in positions:
            cur_price = float(p.get("curPrice", 0))
            size = float(p.get("size", 0))
            cash_pnl = float(p.get("cashPnl", 0))
            condition_id = p.get("conditionId", "")
            title = p.get("title", "")
            
            # Position is resolved if price is exactly 1.0 or 0.0 (or very close)
            if size > 0.1 and (cur_price >= 0.99 or cur_price <= 0.01):
                # Try to redeem via CLOB API
                try:
                    # Use the merge/redeem endpoint
                    redeem_resp = requests.post(
                        f"https://clob.polymarket.com/redeem",
                        json={"conditionId": condition_id},
                        headers={"Authorization": f"Bearer {POLYMARKET_API_KEY}"},
                        timeout=10
                    )
                    if redeem_resp.ok:
                        claimed += 1
                        log.info(f"  [CLAIM] Redeemed: {title} | P&L: ${cash_pnl:.2f}")
                    else:
                        # Try alternative: sell at market price for winning positions
                        if cur_price >= 0.99:
                            token_id = p.get("asset", "")
                            if token_id:
                                try:
                                    from py_clob_client.clob_types import OrderArgs
                                    from py_clob_client.order_builder.constants import SELL
                                    sell_order = c.create_order(OrderArgs(
                                        token_id=token_id,
                                        price=0.99,
                                        size=round(size, 2),
                                        side=SELL
                                    ))
                                    result = c.post_order(sell_order, OrderType="FOK")
                                    if result and result.get("success"):
                                        claimed += 1
                                        log.info(f"  [CLAIM] Sold winning: {title} | ~${size * 0.99:.2f}")
                                except Exception as se:
                                    pass  # Silent fail on sell attempt
                except Exception as re:
                    pass  # Silent fail on individual redeem
        
        if claimed > 0:
            log.info(f"  [CLAIM] Auto-claimed {claimed} resolved positions")
    except ImportError:
        pass  # py-clob-client not available
    except Exception as e:
        pass  # Don't crash the bot on claim errors

def print_status():
    print(f"\n  -- PROPS ARB [{'DRY' if DRY_RUN else 'LIVE'}] --")
    print(f"  Trades: {trades_executed} | Open: {len(open_positions)} | Limit Orders: {len(_pending_limit_orders)} | P&L: ${daily_pnl:.2f}")
    print(f"  Requests: {credits_used} | Opps: {len(last_opportunities)}\n")

def main():
    global DRY_RUN
    if "--dry-run" in sys.argv: DRY_RUN = True
    if not ODDS_API_KEYS: log.error("No ODDS_API_KEYS set"); sys.exit(1)
    live = "--live" in sys.argv
    log.info(f"Props Arb Bot | {'LIVE LOOP' if live else 'SINGLE'} | DRY={DRY_RUN} | {len(ODDS_API_KEYS)} API keys ({len(ODDS_API_KEYS)*100} req/hr)")
    if LIMIT_ORDER_ENABLED:
        log.info(f"  Limit orders: ENABLED | mid_edge>={LIMIT_ORDER_MIN_MID_EDGE:.0%} | max_age={LIMIT_ORDER_MAX_AGE}s | max_pending={LIMIT_ORDER_MAX_PENDING}")
    _load_bets()
    _sync_bets_from_polymarket()
    _load_limit_orders()
    start_api(8766)
    if live:
        log.info(f"Every {SCAN_INTERVAL}s. Ctrl+C to stop.\n")
        while True:
            try:
                cancel_stale_orders()
                panic_sell_injured()
                scan_opportunities()
                auto_claim()
                print_status()
                log.info(f"Next in {SCAN_INTERVAL}s...")
                time.sleep(SCAN_INTERVAL)
            except KeyboardInterrupt: break
            except Exception as e: log.error(f"Err: {e}"); time.sleep(60)
    else:
        opps = scan_opportunities(); print_status()
        if opps:
            print(f"{'Outcome':<45} {'Poly':>5} {'Fair':>5} {'Edge':>6} {'Source':<12}")
            print("-"*75)
            for o in opps[:20]:
                print(f"{o['outcome'][:44]:<45} {o['poly_price']:>4.2f} {o['fair_prob']:>4.2f} {o['edge']:>5.1%} {o['source']:<12}")

if __name__ == "__main__": main()
