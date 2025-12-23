import asyncio
import json
from datetime import datetime, timedelta
from collections import deque


from websockets import connect
from chdb import session as chs


BINANCE_WS_URL = "wss://fstream.binance.com/ws/btcusdt@trade"
DB_PATH = "/Users/davidefloriello/genai/chdb_data"

BATCH_SIZE = 1000
MAX_PRICE_JUMP = 0.05          
QTY_WINDOW_SECONDS = 300      

class OFICalculator:
    def __init__(self):
        self.last_price = None

    def compute_ofi(self, tick):
        if self.last_price is None:
            self.last_price = tick["price"]
            return 0.0

        sign = 1 if tick["side"] == "buy" else -1
        ofi = sign * tick["qty"]
        self.last_price = tick["price"]
        return ofi


class TickValidator:
    def __init__(self, max_price_jump, window_seconds):
        self.last_price = None
        self.max_jump = max_price_jump
        self.window_seconds = window_seconds
        self.qty_history = deque()

    def _update_qty_bounds(self, ts):
        cutoff = ts - timedelta(seconds=self.window_seconds)
        while self.qty_history and self.qty_history[0][0] < cutoff:
            self.qty_history.popleft()

        if not self.qty_history:
            return None, None

        qs = [q for _, q in self.qty_history]
        return max(qs), min(qs)

    def adjust(self, tick):
        ts = tick["ts"]
        price = tick["price"]
        qty = tick["qty"]

        if price <= 0:
            if self.last_price is not None:
                tick["price"] = self.last_price
            else:
                return None  
        else:
            if self.last_price and self.last_price > 0:
                delta = (price - self.last_price) / self.last_price
                if abs(delta) > self.max_jump:
                    tick["price"] = self.last_price * (
                        1 + self.max_jump * (1 if delta > 0 else -1)
                    )

        self.last_price = tick["price"]

        self.qty_history.append((ts, qty))
        cutoff = ts - timedelta(seconds=self.window_seconds)
        while self.qty_history and self.qty_history[0][0] < cutoff:
            self.qty_history.popleft()

        if self.qty_history:
            max_qty = max(q for _, q in self.qty_history)
            min_qty = min(q for _, q in self.qty_history)
            if qty > max_qty:
                tick["qty"] = max_qty
            elif qty <= 0:
                tick["qty"] = min_qty

        return tick


class Writer:
    def __init__(self):
        self.sess = chs.Session(DB_PATH)
        self._init_db()

    def _init_db(self):
        self.sess.query("SELECT 1")

        self.sess.query("""
        CREATE TABLE IF NOT EXISTS market_ticks (
            ts DateTime,
            price Float64,
            qty Float64,
            side String,
            ofi Float64
        )
        ENGINE = MergeTree
        ORDER BY ts
        """)

        print("ClickHouse ready")

    def insert_batch(self, rows):
        if not rows:
            return

        values = ",".join(
            f"('{r['ts']}', {r['price']}, {r['qty']}, '{r['side']}', {r['ofi']})"
            for r in rows
        )

        self.sess.query(f"""
        INSERT INTO market_ticks (ts, price, qty, side, ofi)
        VALUES {values}
        """)


async def collector(queue: asyncio.Queue):
    ofi_calc = OFICalculator()
    validator = TickValidator(MAX_PRICE_JUMP, QTY_WINDOW_SECONDS)

    async with connect(BINANCE_WS_URL) as ws:
        print("Connected to Binance")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            tick = {
                "ts": datetime.fromtimestamp(data["T"] / 1000).replace(microsecond=0),
                "price": float(data["p"]),
                "qty": float(data["q"]),
                "side": "buy" if not data["m"] else "sell",
            }

            tick = validator.adjust(tick)
            tick["ofi"] = ofi_calc.compute_ofi(tick)

            await queue.put(tick)



async def writer(queue: asyncio.Queue):
    writer = Writer()
    buffer = []

    while True:
        tick = await queue.get()
        buffer.append(tick)

        if len(buffer) >= BATCH_SIZE:
            writer.insert_batch(buffer)
            buffer.clear()


async def main(run_seconds: int = 300): 
    queue = asyncio.Queue(maxsize=10_000)

    async def run_with_timeout():
        collector_task = asyncio.create_task(collector(queue))
        writer_task = asyncio.create_task(writer(queue))
        tasks = [collector_task, writer_task]

        try:
            await asyncio.sleep(run_seconds)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            print(f"Pipeline stopped after {run_seconds} seconds")

    await run_with_timeout()

if __name__ == "__main__":
    asyncio.run(main())
