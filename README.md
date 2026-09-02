Neural Field → NautilusTrader
Two pieces:

neural-field.html — the visualizer (open in any browser, no build step)
nautilus_neural_bridge.py — a read-only NautilusTrader Actor that streams the engine's MessageBus to the visualizer over a local WebSocket
Nautilus MessageBus ─▶ NeuralFieldBridge (Actor) ─▶ ws://127.0.0.1:8765 ─▶ neural-field.html
The bridge only subscribes — it never submits, modifies, or cancels orders.

Quick start
pip install websockets (Nautilus you already have)

Open neural-field.html. It runs immediately in DEMO mode (synthetic flow) so you can see the look before anything is attached.

Add the actor to a node (backtest is the easiest first test):

from nautilus_neural_bridge import NeuralFieldBridge, NeuralFieldBridgeConfig
from nautilus_trader.model import InstrumentId, BarType

cfg = NeuralFieldBridgeConfig(
    instrument_ids=[InstrumentId.from_str("BTCUSDT-PERP.BINANCE")],
    bar_types=[BarType.from_str("BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-INTERNAL")],
    ws_host="127.0.0.1",
    ws_port=8765,
)

engine.add_actor(NeuralFieldBridge(config=cfg))        # backtest
# node.trader.add_actor(NeuralFieldBridge(config=cfg)) # live
In the page, confirm the WS box reads ws://127.0.0.1:8765 and click Connect (the pill flips from amber DEMO to green LIVE). Run the node.

Tip: append ?ws=ws://127.0.0.1:8765 to the file URL to auto-connect on load.

How events map to the picture
Nautilus event	Visual effect
Quote / Trade / Bar	Pulses the Feed node for that instrument; pulse sweeps L→R
Trade aggressor side	Tints the field green (buy) / red (sell)
Order event	Lights the Execution layer
Position event	Lights the Execution layer
Event rate	Raises wave amplitude (busy tape = choppier field)
Instruments are assigned to input-layer nodes in arrival order; the meta message sent on start labels them and is replayed to any client that connects late.

Notes
Backtest speed: events fire as fast as the engine processes them, so the field can look frantic. Use fewer instruments or throttle in _WSHub.publish if needed.
Wildcard events use self.msgbus.subscribe("events.order.*", …) — the internal bus supports * patterns (Redis external streams do not, but that's a different path).
Other topics: to firehose everything, add self.msgbus.subscribe("data.*", handler) in on_start and route in one handler. The typed on_quote_tick / on_trade_tick / on_bar handlers are used here because they're version-stable and give clean fields.
Skins (Live / Organism / Feed / Trans) all react to the same live feed — only the labels and palette change.
