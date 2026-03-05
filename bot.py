#!/usr/bin/env python3
"""
Арбітражний бот для перпетуал-ф'ючерсів OKX + Gate.io через CCXT (REST).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import math
import os
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import ccxt  # type: ignore

# ===== API KEYS START =====
OKX_API_KEY = "PUT_OKX_KEY_HERE"
OKX_SECRET = "PUT_OKX_SECRET_HERE"
OKX_PASSPHRASE = "PUT_OKX_PASSPHRASE_HERE"
GATE_API_KEY = "PUT_GATE_KEY_HERE"
GATE_SECRET = "PUT_GATE_SECRET_HERE"
# ===== API KEYS END =====

# Опційне перевизначення через env
OKX_API_KEY = os.getenv("OKX_API_KEY", OKX_API_KEY)
OKX_SECRET = os.getenv("OKX_SECRET", OKX_SECRET)
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", OKX_PASSPHRASE)
GATE_API_KEY = os.getenv("GATE_API_KEY", GATE_API_KEY)
GATE_SECRET = os.getenv("GATE_SECRET", GATE_SECRET)

# Конфіг
CONFIG: Dict[str, Any] = {
    "sandbox": False,
    "dry_run": False,
    "min_notional": 90.0,
    "max_notional": 100.0,
    "min_spread_pct": 0.5,
    "max_spread_pct": 3.0,
    "take_profit_pct": 1.0,
    "stop_loss_pct": 1.0,
    "leverage": 3,
    "margin_mode": "isolated",
    "poll_interval_sec": 0.8,
    "cooldown_sec": 10,
    "on_start_close_all": True,
    "max_retries": 5,
}

LOG_PATH = os.path.join("logs", "bot.log")


class BotError(Exception):
    """Базова помилка бота."""


class RecoverableError(BotError):
    """Помилка, яку можна повторити."""


@dataclass
class LegPlan:
    exchange_name: str
    symbol: str
    side: str  # buy/sell
    position_side: str  # long/short
    amount: float
    price_ref: float
    notional: float


@dataclass
class LegExecution:
    exchange_name: str
    symbol: str
    side: str
    position_side: str
    amount: float
    order_id: Optional[str] = None
    status: str = "pending"
    error: Optional[str] = None


@dataclass
class ArbitrageState:
    state: str = "IDLE"
    trade_id: Optional[str] = None
    symbol: Optional[str] = None
    scenario: Optional[str] = None
    chosen_notional: float = 0.0
    entry_legs: Dict[str, LegExecution] = field(default_factory=dict)
    cooldown_until: float = 0.0


def setup_logger() -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("arb_bot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)
    return logger


def retryable_call(logger: logging.Logger, fn, *args, retries: int = 5, base_delay: float = 0.5, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.DDoSProtection, ccxt.ExchangeNotAvailable) as e:
            if attempt == retries:
                raise RecoverableError(f"Перевищено retry: {e}") from e
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning("Мережева помилка, retry %s/%s через %.2fс: %s", attempt, retries, delay, e)
            time.sleep(delay)
        except ccxt.ExchangeError:
            raise


def init_exchanges(cfg: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
    okx = ccxt.okx(
        {
            "apiKey": OKX_API_KEY,
            "secret": OKX_SECRET,
            "password": OKX_PASSPHRASE,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
            },
        }
    )
    gate = ccxt.gateio(
        {
            "apiKey": GATE_API_KEY,
            "secret": GATE_SECRET,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
            },
        }
    )

    if cfg["sandbox"]:
        logger.info("Увімкнено sandbox/demo режим")
        okx.set_sandbox_mode(True)
        gate.set_sandbox_mode(True)

    retryable_call(logger, okx.load_markets, True, retries=cfg["max_retries"])
    retryable_call(logger, gate.load_markets, True, retries=cfg["max_retries"])

    return {"okx": okx, "gate": gate}


def is_swap_market(market: Dict[str, Any]) -> bool:
    return bool(market.get("swap") or market.get("type") == "swap" or market.get("contract"))


def get_common_symbols(exchanges: Dict[str, Any], one_symbol: Optional[str], logger: logging.Logger) -> List[str]:
    okx_markets = exchanges["okx"].markets
    gate_markets = exchanges["gate"].markets

    okx_syms = {s for s, m in okx_markets.items() if is_swap_market(m) and m.get("active", True)}
    gate_syms = {s for s, m in gate_markets.items() if is_swap_market(m) and m.get("active", True)}
    common = sorted(okx_syms.intersection(gate_syms))

    if one_symbol:
        if one_symbol in common:
            return [one_symbol]
        logger.warning("Символ %s не знайдено як спільний swap", one_symbol)
        return []

    logger.info("Знайдено спільних swap символів: %s", len(common))
    return common


def set_leverage_and_margin_mode(exchange, symbol: str, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    leverage = cfg["leverage"]
    margin_mode = cfg["margin_mode"]

    params: Dict[str, Any] = {}
    exid = exchange.id

    if exid == "okx":
        params = {"mgnMode": "isolated", "posSide": "long"}
        try:
            retryable_call(logger, exchange.set_position_mode, True, symbol=symbol, retries=cfg["max_retries"])
        except Exception as e:
            logger.warning("Не вдалося виставити hedge mode на OKX: %s", e)
        for side in ("long", "short"):
            p = {"mgnMode": "isolated", "posSide": side}
            try:
                retryable_call(logger, exchange.set_leverage, leverage, symbol, p, retries=cfg["max_retries"])
            except Exception as e:
                logger.warning("Leverage %s %s: %s", symbol, side, e)

    elif exid == "gateio":
        try:
            retryable_call(logger, exchange.set_position_mode, True, symbol=symbol, retries=cfg["max_retries"])
        except Exception as e:
            logger.warning("Не вдалося виставити hedge mode на Gate: %s", e)
        try:
            retryable_call(logger, exchange.set_margin_mode, margin_mode, symbol, {"leverage": leverage}, retries=cfg["max_retries"])
        except Exception:
            pass
        try:
            retryable_call(logger, exchange.set_leverage, leverage, symbol, retries=cfg["max_retries"])
        except Exception as e:
            logger.warning("Leverage Gate %s: %s", symbol, e)


def get_best_prices(exchange, symbol: str, logger: logging.Logger, retries: int) -> Tuple[float, float]:
    ticker = retryable_call(logger, exchange.fetch_ticker, symbol, retries=retries)
    bid = ticker.get("bid")
    ask = ticker.get("ask")
    if not bid or not ask or bid <= 0 or ask <= 0:
        raise RecoverableError(f"Некоректні ціни {exchange.id} {symbol}: bid={bid}, ask={ask}")
    return float(bid), float(ask)


def compute_spread(buy_price: float, sell_price: float) -> float:
    return (sell_price - buy_price) / buy_price * 100.0


def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    dv = Decimal(str(value))
    ds = Decimal(str(step))
    floored = (dv / ds).to_integral_value(rounding=ROUND_DOWN) * ds
    return float(floored)


def choose_notional_in_range(min_notional: float, max_notional: float) -> float:
    return float(max_notional)


def calc_order_amount_from_notional(exchange, symbol: str, notional: float, price: float) -> Tuple[float, float]:
    market = exchange.market(symbol)
    contract_size = float(market.get("contractSize") or 1.0)

    contracts_raw = notional / (price * contract_size)
    amount = float(exchange.amount_to_precision(symbol, contracts_raw))
    amount = max(amount, 0.0)

    min_amount = None
    limits = market.get("limits") or {}
    amt_limits = limits.get("amount") or {}
    if amt_limits.get("min") is not None:
        min_amount = float(amt_limits["min"])
    if min_amount and amount < min_amount:
        amount = min_amount
        amount = float(exchange.amount_to_precision(symbol, amount))

    actual_notional = amount * price * contract_size
    return amount, actual_notional


def prepare_dual_leg_amounts(
    exchanges: Dict[str, Any],
    symbol: str,
    buy_exchange_name: str,
    sell_exchange_name: str,
    buy_price: float,
    sell_price: float,
    cfg: Dict[str, Any],
) -> Optional[Tuple[float, float, float]]:
    min_n = cfg["min_notional"]
    max_n = cfg["max_notional"]

    target = choose_notional_in_range(min_n, max_n)
    while target >= min_n:
        buy_amt, buy_notional = calc_order_amount_from_notional(exchanges[buy_exchange_name], symbol, target, buy_price)
        sell_amt, sell_notional = calc_order_amount_from_notional(exchanges[sell_exchange_name], symbol, target, sell_price)
        if buy_amt <= 0 or sell_amt <= 0:
            target -= 0.25
            continue
        if not (min_n <= buy_notional <= max_n and min_n <= sell_notional <= max_n):
            target -= 0.25
            continue
        if abs(buy_notional - sell_notional) > 0.5:
            target -= 0.25
            continue
        chosen = min(buy_notional, sell_notional)
        return buy_amt, sell_amt, chosen
    return None


def get_free_usdt(exchange, logger: logging.Logger, retries: int) -> float:
    bal = retryable_call(logger, exchange.fetch_balance, retries=retries)
    free = 0.0
    if "USDT" in bal.get("free", {}):
        free = float(bal["free"]["USDT"])
    elif "USDT" in bal:
        free = float((bal["USDT"] or {}).get("free", 0.0) or 0.0)
    return free


def ensure_sufficient_balance(exchanges: Dict[str, Any], chosen_notional: float, cfg: Dict[str, Any], logger: logging.Logger) -> bool:
    required_margin = chosen_notional / float(cfg["leverage"])
    okx_free = get_free_usdt(exchanges["okx"], logger, cfg["max_retries"])
    gate_free = get_free_usdt(exchanges["gate"], logger, cfg["max_retries"])
    ok = okx_free >= required_margin and gate_free >= required_margin
    if not ok:
        logger.warning(
            "Недостатній баланс: OKX %.2f, Gate %.2f, потрібно >= %.2f на кожній",
            okx_free,
            gate_free,
            required_margin,
        )
    return ok


def order_params(exchange_name: str, position_side: str, reduce_only: bool = False) -> Dict[str, Any]:
    p: Dict[str, Any] = {}
    if exchange_name == "okx":
        p["tdMode"] = "isolated"
        p["posSide"] = "long" if position_side == "long" else "short"
        if reduce_only:
            p["reduceOnly"] = True
    elif exchange_name == "gate":
        p["reduceOnly"] = reduce_only
        p["hedged"] = True
    return p


def place_leg(exchanges: Dict[str, Any], leg: LegPlan, cfg: Dict[str, Any], logger: logging.Logger) -> LegExecution:
    exe = LegExecution(
        exchange_name=leg.exchange_name,
        symbol=leg.symbol,
        side=leg.side,
        position_side=leg.position_side,
        amount=leg.amount,
    )
    if cfg["dry_run"]:
        exe.status = "filled"
        exe.order_id = f"dry-{uuid.uuid4().hex[:10]}"
        logger.info("DRY-RUN ордер %s %s %s amt=%.6f", leg.exchange_name, leg.symbol, leg.side, leg.amount)
        return exe

    ex = exchanges[leg.exchange_name]
    params = order_params(leg.exchange_name, leg.position_side, reduce_only=False)
    order = retryable_call(
        logger,
        ex.create_order,
        leg.symbol,
        "market",
        leg.side,
        leg.amount,
        None,
        params,
        retries=cfg["max_retries"],
    )
    exe.order_id = order.get("id")
    exe.status = "filled" if order.get("status") in ("closed", "filled") else "open"
    logger.info("Відкрито ногу %s %s %s amt=%.6f id=%s", leg.exchange_name, leg.symbol, leg.side, leg.amount, exe.order_id)
    return exe


def close_position_market(exchange, exchange_name: str, symbol: str, position_side: str, amount: float, cfg: Dict[str, Any], logger: logging.Logger) -> bool:
    side = "sell" if position_side == "long" else "buy"
    params = order_params(exchange_name, position_side, reduce_only=True)
    try:
        if cfg["dry_run"]:
            logger.info("DRY-RUN закриття %s %s %s amt=%.6f", exchange_name, symbol, side, amount)
            return True
        retryable_call(
            logger,
            exchange.create_order,
            symbol,
            "market",
            side,
            amount,
            None,
            params,
            retries=cfg["max_retries"],
        )
        logger.info("Закрито позицію %s %s %s amt=%.6f", exchange_name, symbol, position_side, amount)
        return True
    except Exception as e:
        logger.error("Помилка закриття %s %s: %s", exchange_name, symbol, e)
        return False


def place_two_legs_parallel(exchanges: Dict[str, Any], leg1: LegPlan, leg2: LegPlan, cfg: Dict[str, Any], logger: logging.Logger) -> Tuple[Optional[LegExecution], Optional[LegExecution]]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(place_leg, exchanges, leg1, cfg, logger)
        f2 = pool.submit(place_leg, exchanges, leg2, cfg, logger)
        res1 = res2 = None
        err1 = err2 = None
        try:
            res1 = f1.result()
        except Exception as e:
            err1 = e
        try:
            res2 = f2.result()
        except Exception as e:
            err2 = e

    if err1:
        logger.error("Перша нога не виконана: %s", err1)
    if err2:
        logger.error("Друга нога не виконана: %s", err2)

    if (res1 and not err1) and (res2 and not err2):
        return res1, res2

    # Аварійне вирівнювання якщо одна нога відкрита
    if res1 and not err1:
        ex = exchanges[res1.exchange_name]
        close_position_market(ex, res1.exchange_name, res1.symbol, res1.position_side, res1.amount, cfg, logger)
    if res2 and not err2:
        ex = exchanges[res2.exchange_name]
        close_position_market(ex, res2.exchange_name, res2.symbol, res2.position_side, res2.amount, cfg, logger)

    return None, None


def fetch_positions(exchange, symbol: str, logger: logging.Logger, retries: int) -> List[Dict[str, Any]]:
    try:
        positions = retryable_call(logger, exchange.fetch_positions, [symbol], retries=retries)
    except Exception:
        positions = retryable_call(logger, exchange.fetch_positions, retries=retries)
    result: List[Dict[str, Any]] = []
    for p in positions:
        if p.get("symbol") != symbol:
            continue
        contracts = float(p.get("contracts") or p.get("positionAmt") or p.get("info", {}).get("size") or 0.0)
        side = (p.get("side") or "").lower()
        if contracts > 0:
            result.append({"side": side if side in ("long", "short") else "long", "contracts": contracts, "raw": p})
    return result


def get_combined_unrealized_pnl(exchanges: Dict[str, Any], symbol: str, cfg: Dict[str, Any], logger: logging.Logger) -> float:
    pnl = 0.0
    for name in ("okx", "gate"):
        ps = fetch_positions(exchanges[name], symbol, logger, cfg["max_retries"])
        for p in ps:
            raw = p["raw"]
            val = raw.get("unrealizedPnl")
            if val is None:
                info = raw.get("info", {})
                val = info.get("upl") or info.get("unrealised_pnl") or info.get("unrealizedPnl") or 0
            pnl += float(val or 0.0)
    return pnl


def close_both_legs(exchanges: Dict[str, Any], state: ArbitrageState, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    logger.info("Починаю закриття обох ніг")
    if not state.symbol:
        return
    for name, leg in list(state.entry_legs.items()):
        ex = exchanges[name]
        close_position_market(ex, name, state.symbol, leg.position_side, leg.amount, cfg, logger)


def reconcile_and_heal_state(exchanges: Dict[str, Any], state: ArbitrageState, cfg: Dict[str, Any], logger: logging.Logger) -> None:
    if state.state not in ("ENTERING", "IN_POSITION", "EXITING") or not state.symbol:
        return

    open_map: Dict[str, List[Dict[str, Any]]] = {}
    for name in ("okx", "gate"):
        try:
            open_map[name] = fetch_positions(exchanges[name], state.symbol, logger, cfg["max_retries"])
        except Exception as e:
            logger.warning("Не вдалось отримати позиції %s: %s", name, e)
            open_map[name] = []

    okx_open = len(open_map["okx"]) > 0
    gate_open = len(open_map["gate"]) > 0

    if okx_open and gate_open:
        return

    if not okx_open and not gate_open:
        logger.warning("Позицій вже немає, повертаюсь в IDLE")
        state.state = "COOLDOWN"
        state.cooldown_until = time.time() + cfg["cooldown_sec"]
        state.entry_legs.clear()
        return

    logger.warning("Десинхрон: відкрита лише одна нога. Запускаю авто-відновлення")
    # Спроба відкрити відсутню ногу по поточному плану, інакше закрити все
    try_open_missing = False
    if try_open_missing:
        pass
    else:
        close_both_legs(exchanges, state, cfg, logger)
        state.state = "COOLDOWN"
        state.cooldown_until = time.time() + cfg["cooldown_sec"]
        state.entry_legs.clear()


def scan_opportunity(exchanges: Dict[str, Any], symbols: List[str], cfg: Dict[str, Any], logger: logging.Logger):
    best = None
    for symbol in symbols:
        try:
            okx_bid, okx_ask = get_best_prices(exchanges["okx"], symbol, logger, cfg["max_retries"])
            gate_bid, gate_ask = get_best_prices(exchanges["gate"], symbol, logger, cfg["max_retries"])
        except Exception as e:
            logger.warning("Пропуск %s через помилку цін: %s", symbol, e)
            continue

        spread_a = compute_spread(okx_ask, gate_bid)  # buy okx, sell gate
        spread_b = compute_spread(gate_ask, okx_bid)  # buy gate, sell okx

        if cfg["min_spread_pct"] <= spread_a <= cfg["max_spread_pct"]:
            cand = (spread_a, symbol, "A", okx_ask, gate_bid)
            if best is None or spread_a > best[0]:
                best = cand

        if cfg["min_spread_pct"] <= spread_b <= cfg["max_spread_pct"]:
            cand = (spread_b, symbol, "B", gate_ask, okx_bid)
            if best is None or spread_b > best[0]:
                best = cand

    return best


def close_all_positions_on_start(exchanges: Dict[str, Any], cfg: Dict[str, Any], logger: logging.Logger) -> None:
    logger.info("on_start_close_all=True: перевіряю відкриті позиції")
    for name in ("okx", "gate"):
        ex = exchanges[name]
        try:
            positions = retryable_call(logger, ex.fetch_positions, retries=cfg["max_retries"])
        except Exception as e:
            logger.warning("Не вдалося завантажити позиції %s: %s", name, e)
            continue
        for p in positions:
            contracts = float(p.get("contracts") or 0.0)
            if contracts <= 0:
                continue
            symbol = p.get("symbol")
            if not symbol:
                continue
            side = (p.get("side") or "long").lower()
            pos_side = "long" if side != "short" else "short"
            amount = contracts
            close_position_market(ex, name, symbol, pos_side, amount, cfg, logger)


def run(cfg: Dict[str, Any], once: bool = False, one_symbol: Optional[str] = None) -> None:
    logger = setup_logger()
    logger.info("Старт бота")

    exchanges = init_exchanges(cfg, logger)
    state = ArbitrageState()
    lock = threading.Lock()

    if cfg["on_start_close_all"]:
        close_all_positions_on_start(exchanges, cfg, logger)

    symbols = get_common_symbols(exchanges, one_symbol, logger)
    if not symbols:
        logger.error("Немає символів для сканування")
        return

    while True:
        try:
            with lock:
                if state.state == "COOLDOWN":
                    if time.time() < state.cooldown_until:
                        time.sleep(0.2)
                        if once:
                            break
                        continue
                    state.state = "IDLE"
                    state.trade_id = None
                    state.symbol = None
                    state.scenario = None
                    state.chosen_notional = 0.0
                    state.entry_legs.clear()
                    logger.info("Cooldown завершено, повернення в IDLE")

                if state.state == "IDLE":
                    opp = scan_opportunity(exchanges, symbols, cfg, logger)
                    if not opp:
                        if once:
                            break
                        time.sleep(cfg["poll_interval_sec"])
                        continue

                    spread, symbol, scenario, buy_price, sell_price = opp
                    logger.info("Сигнал: %s сценарій %s spread=%.4f%%", symbol, scenario, spread)

                    if scenario == "A":
                        sizing = prepare_dual_leg_amounts(exchanges, symbol, "okx", "gate", buy_price, sell_price, cfg)
                    else:
                        sizing = prepare_dual_leg_amounts(exchanges, symbol, "gate", "okx", buy_price, sell_price, cfg)

                    if not sizing:
                        logger.info("Не вдалось підібрати розмір у межах [%.2f, %.2f]", cfg["min_notional"], cfg["max_notional"])
                        if once:
                            break
                        time.sleep(cfg["poll_interval_sec"])
                        continue

                    buy_amt, sell_amt, chosen_notional = sizing
                    if not ensure_sufficient_balance(exchanges, chosen_notional, cfg, logger):
                        if once:
                            break
                        time.sleep(cfg["poll_interval_sec"])
                        continue

                    state.state = "ENTERING"
                    state.trade_id = uuid.uuid4().hex
                    state.symbol = symbol
                    state.scenario = scenario
                    state.chosen_notional = chosen_notional

                    set_leverage_and_margin_mode(exchanges["okx"], symbol, cfg, logger)
                    set_leverage_and_margin_mode(exchanges["gate"], symbol, cfg, logger)

                    if scenario == "A":
                        leg1 = LegPlan("okx", symbol, "buy", "long", buy_amt, buy_price, chosen_notional)
                        leg2 = LegPlan("gate", symbol, "sell", "short", sell_amt, sell_price, chosen_notional)
                    else:
                        leg1 = LegPlan("gate", symbol, "buy", "long", buy_amt, buy_price, chosen_notional)
                        leg2 = LegPlan("okx", symbol, "sell", "short", sell_amt, sell_price, chosen_notional)

                    r1, r2 = place_two_legs_parallel(exchanges, leg1, leg2, cfg, logger)
                    if not r1 or not r2:
                        logger.error("Вхід не вдався, повернення в IDLE")
                        state.state = "IDLE"
                        state.entry_legs.clear()
                        if once:
                            break
                        time.sleep(cfg["poll_interval_sec"])
                        continue

                    state.entry_legs = {r1.exchange_name: r1, r2.exchange_name: r2}
                    state.state = "IN_POSITION"
                    logger.info("Позиція відкрита, моніторинг PnL trade_id=%s", state.trade_id)

                if state.state == "IN_POSITION":
                    reconcile_and_heal_state(exchanges, state, cfg, logger)
                    if state.state != "IN_POSITION":
                        continue

                    pnl = get_combined_unrealized_pnl(exchanges, state.symbol, cfg, logger)
                    pnl_pct = (pnl / state.chosen_notional) * 100.0 if state.chosen_notional else 0.0
                    logger.info("PnL: %.4f USD (%.4f%%)", pnl, pnl_pct)

                    if pnl_pct >= cfg["take_profit_pct"]:
                        logger.info("TP досягнуто (>= %.2f%%), закриваю", cfg["take_profit_pct"])
                        state.state = "EXITING"
                    elif pnl_pct <= -cfg["stop_loss_pct"]:
                        logger.info("SL досягнуто (<= -%.2f%%), закриваю", cfg["stop_loss_pct"])
                        state.state = "EXITING"
                    else:
                        if once:
                            break
                        time.sleep(cfg["poll_interval_sec"])
                        continue

                if state.state == "EXITING":
                    close_both_legs(exchanges, state, cfg, logger)
                    state.state = "COOLDOWN"
                    state.cooldown_until = time.time() + cfg["cooldown_sec"]
                    state.entry_legs.clear()
                    logger.info("Угоду закрито, cooldown %sс", cfg["cooldown_sec"])
                    if once:
                        break

            if once:
                break

        except KeyboardInterrupt:
            logger.info("Зупинка користувачем")
            break
        except Exception as e:
            logger.exception("Неочікувана помилка, продовжую роботу: %s", e)
            try:
                for ex in exchanges.values():
                    retryable_call(logger, ex.load_markets, True, retries=cfg["max_retries"])
            except Exception as e2:
                logger.warning("Не вдалося оновити ринки: %s", e2)
            time.sleep(cfg["poll_interval_sec"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spread arbitrage bot OKX/Gate (swap)")
    parser.add_argument("--sandbox", action="store_true", help="Увімкнути sandbox/demo")
    parser.add_argument("--dry-run", action="store_true", help="Симуляція без реальних ордерів")
    parser.add_argument("--once", action="store_true", help="Один цикл сканування")
    parser.add_argument("--symbol", type=str, default=None, help="Один символ, напр. BTC/USDT:USDT")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = dict(CONFIG)
    if args.sandbox:
        cfg["sandbox"] = True
    if args.dry_run:
        cfg["dry_run"] = True

    run(cfg, once=args.once, one_symbol=args.symbol)


if __name__ == "__main__":
    main()
