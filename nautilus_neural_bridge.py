"""
nautilus_neural_bridge.py
=========================

A NautilusTrader Actor that taps the engine's MessageBus and streams every
market-data / order / position event to the Neural Field visualizer over a
local WebSocket. Works in both backtest and live nodes.

    Nautilus MessageBus  ->  NeuralFieldBridge (Actor)  ->  ws://127.0.0.1:8765  ->  neural-field.html

The bridge is READ-ONLY: it only subscribes and observes. It never submits,
modifies, or cancels orders.

Requires:  pip install websockets   (plus nautilus_trader, which you have)

--------------------------------------------------------------------------
Wire it into a node
--------------------------------------------------------------------------
Live node:

    from nautilus_neural_bridge import NeuralFieldBridge, NeuralFieldBridgeConfig
    from nautilus_trader.model import InstrumentId, BarType

    cfg = NeuralFieldBridgeConfig(
        instrument_ids=[InstrumentId.from_str("BTCUSDT-PERP.BINANCE")],
        bar_types=[BarType.from_str("BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-INTERNAL")],
        ws_host="127.0.0.1",
        ws_port=8765,
    )
    node.trader.add_actor(NeuralFieldBridge(config=cfg))

Backtest engine (great for a first test with historical data):

    engine.add_actor(NeuralFieldBridge(config=cfg))

Then open neural-field.html, make sure the WS box says ws://127.0.0.1:8765,
and click Connect. Run the node. The field lights up with real flow.
--------------------------------------------------------------------------
"""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

try:
    import websockets
except ImportError as e:  # pragma: no cover
    raise SystemExit("NeuralFieldBridge needs the 'websockets' package: pip install websockets") from e

from nautilus_trader.common.actor import Actor
from nautilus_trader.config import ActorConfig
from nautilus_trader.model import BarType, InstrumentId


# --------------------------------------------------------------------------- #
#  Config                                                                      #
# --------------------------------------------------------------------------- #
class NeuralFieldBridgeConfig(ActorConfig, frozen=True):
    """Configuration for the Neural Field WebSocket bridge."""
    instrument_ids: list[InstrumentId] = []
    bar_types: list[BarType] = []
    ws_host: str = "127.0.0.1"
    ws_port: int = 8765
    subscribe_order_events: bool = True
    subscribe_position_events: bool = True
    venue_label: str | None = None


# --------------------------------------------------------------------------- #
#  WebSocket hub  (own asyncio loop in a background thread; thread-safe push)  #
# --------------------------------------------------------------------------- #
class _WSHub:
    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self._clients: set = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._last_meta: str | None = None
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._thread = threading.Thread(target=self._run, name="neural-ws", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        async def handler(conn, *_):  # 2nd positional arg (path) dropped in websockets>=13
            self._clients.add(conn)
            # replay the latest meta so a late-joining client gets its labels
            if self._last_meta is not None:
                try:
                    await conn.send(self._last_meta)
                except Exception:
                    pass
            try:
                async for _msg in conn:  # ignore anything the client sends
                    pass
            except Exception:
                pass
            finally:
                self._clients.discard(conn)

        async def main():
            async with websockets.serve(handler, self.host, self.port, ping_interval=20):
                await asyncio.Future()  # run forever

        try:
            self._loop.run_until_complete(main())
        except Exception:
            pass

    def publish(self, msg: dict[str, Any]) -> None:
        if self._loop is None:
            return
        data = json.dumps(msg, separators=(",", ":"), default=str)
        if msg.get("t") == "meta":
            self._last_meta = data
        if not self._clients:
            return
        try:
            asyncio.run_coroutine_threadsafe(self._broadcast(data), self._loop)
        except Exception:
            pass

    async def _broadcast(self, data: str) -> None:
        dead = []
        for conn in list(self._clients):
            try:
                await conn.send(data)
            except Exception:
                dead.append(conn)
        for conn in dead:
            self._clients.discard(conn)

    def stop(self) -> None:
        if self._loop is not None:
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except Exception:
                pass


# --------------------------------------------------------------------------- #
#  The Actor                                                                    #
# --------------------------------------------------------------------------- #
class NeuralFieldBridge(Actor):
    """Streams MessageBus events to the Neural Field visualizer over WebSocket."""

    def __init__(self, config: NeuralFieldBridgeConfig):
        super().__init__(config)
        self._hub = _WSHub(config.ws_host, config.ws_port)

    # -- lifecycle --------------------------------------------------------- #
    def on_start(self) -> None:
        cfg: NeuralFieldBridgeConfig = self.config
        self._hub.start()
        self.log.info(f"NeuralField WS bridge on ws://{cfg.ws_host}:{cfg.ws_port}")

        # announce instruments so the viz can label its input-layer nodes
        self._hub.publish({
            "t": "meta",
            "venue": cfg.venue_label or (str(cfg.instrument_ids[0].venue) if cfg.instrument_ids else None),
            "instruments": [self._root(str(i)) for i in cfg.instrument_ids],
        })

        # real-time market-data subscriptions (typed Actor handlers)
        for iid in cfg.instrument_ids:
            try:
                self.subscribe_quote_ticks(iid)
                self.subscribe_trade_ticks(iid)
            except Exception as e:
                self.log.warning(f"subscribe quotes/trades failed for {iid}: {e}")
        for bt in cfg.bar_types:
            try:
                self.subscribe_bars(bt)
            except Exception as e:
                self.log.warning(f"subscribe bars failed for {bt}: {e}")

        # firehose the rest of the bus via wildcard topic subscriptions
        if cfg.subscribe_order_events:
            self.msgbus.subscribe("events.order.*", self._on_order_event)
        if cfg.subscribe_position_events:
            self.msgbus.subscribe("events.position.*", self._on_position_event)

    def on_stop(self) -> None:
        self._hub.stop()

    # -- typed market-data handlers --------------------------------------- #
    def on_quote_tick(self, tick) -> None:
        self._hub.publish({
            "t": "quote",
            "root": self._root(str(getattr(tick, "instrument_id", ""))),
            "bid": _f(getattr(tick, "bid_price", None)),
            "ask": _f(getattr(tick, "ask_price", None)),
            "ts": int(getattr(tick, "ts_event", 0)),
        })

    def on_trade_tick(self, tick) -> None:
        side = getattr(tick, "aggressor_side", None)
        self._hub.publish({
            "t": "trade",
            "root": self._root(str(getattr(tick, "instrument_id", ""))),
            "px": _f(getattr(tick, "price", None)),
            "qty": _f(getattr(tick, "size", None)),
            "side": _side(side),
            "ts": int(getattr(tick, "ts_event", 0)),
        })

    def on_bar(self, bar) -> None:
        bt = getattr(bar, "bar_type", None)
        iid = getattr(bt, "instrument_id", "") if bt is not None else ""
        self._hub.publish({
            "t": "bar",
            "root": self._root(str(iid)),
            "close": _f(getattr(bar, "close", None)),
            "volume": _f(getattr(bar, "volume", None)),
            "ts": int(getattr(bar, "ts_event", 0)),
        })

    # -- wildcard event handlers ------------------------------------------ #
    def _on_order_event(self, event) -> None:
        self._hub.publish({
            "t": "order",
            "kind": type(event).__name__.replace("Order", ""),
            "root": self._root(str(getattr(event, "instrument_id", ""))),
            "ts": int(getattr(event, "ts_event", 0) or 0),
        })

    def _on_position_event(self, event) -> None:
        self._hub.publish({
            "t": "position",
            "kind": type(event).__name__.replace("Position", ""),
            "root": self._root(str(getattr(event, "instrument_id", ""))),
            "ts": int(getattr(event, "ts_event", 0) or 0),
        })

    # -- helpers ----------------------------------------------------------- #
    @staticmethod
    def _root(instrument_id: str) -> str:
        # "BTCUSDT-PERP.BINANCE" -> "BTCUSDT-PERP"
        return instrument_id.split(".")[0] if instrument_id else "?"


def _f(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _side(aggressor) -> str:
    """Map AggressorSide -> 'buy' / 'sell' / '' without importing the enum."""
    s = str(aggressor).upper()
    if "BUY" in s:
        return "buy"
    if "SELL" in s:
        return "sell"
    return ""
