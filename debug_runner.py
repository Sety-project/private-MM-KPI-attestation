import asyncio
import random
import time
import os
import pandas as pd
from datetime import datetime, UTC
from monitor import TradeMonitor, DataStore

class MockExchange:
    def __init__(self, symbol):
        self.symbol = symbol
        self.bid = 90000.0
        self.ask = 90001.0
        self.orders = []
        
    async def watch_ticker(self, symbol):
        await asyncio.sleep(0.5)
        self.bid += random.uniform(-1, 1)
        self.ask = self.bid + 1.0
        return {'bid': self.bid, 'ask': self.ask}

    async def watch_my_trades(self, symbol):
        while True:
            await asyncio.sleep(random.uniform(2, 5))
            yield [{
                'amount': random.uniform(0.01, 0.1),
                'price': self.bid,
                'timestamp': time.time() * 1000,
                'symbol': self.symbol
            }]

    async def watch_orders(self, symbol):
        while True:
            await asyncio.sleep(1)
            # Simulate a few open orders
            orders = []
            for i in range(5):
                orders.append({
                    'id': f'order_{i}',
                    'status': 'open',
                    'price': self.bid + random.uniform(-500, 500),
                    'amount': random.uniform(1, 10),
                    'remaining': random.uniform(0, 5),
                    'symbol': self.symbol
                })
            yield orders

class DebugTradeMonitor(TradeMonitor):
    def __init__(self):
        super().__init__()
        self.mock_exchange = MockExchange(self.symbol)
        self.exchange = self.mock_exchange
        self.trades_csv = "debug_trades.csv"
        self.orders_csv = "debug_orders.csv"
        
        # Clear files at start
        for f in [self.trades_csv, self.orders_csv, "results.csv"]:
            if os.path.exists(f):
                os.remove(f)

    def _log_to_csv(self, filename, data):
        df = pd.DataFrame([data])
        file_exists = os.path.isfile(filename)
        df.to_csv(filename, mode='a', index=False, header=not file_exists)

    async def watch_trades(self):
        async for trades in self.mock_exchange.watch_my_trades(self.symbol):
            for trade in trades:
                print(f"DEBUG: Mock trade: {trade['amount']} @ {trade['price']}")
                trade['log_time'] = datetime.now(UTC).isoformat()
                self._log_to_csv(self.trades_csv, trade)
                self.store.add_trade(trade)

    async def watch_orders(self):
        async for orders in self.mock_exchange.watch_orders(self.symbol):
            for order in orders:
                order['log_time'] = datetime.now(UTC).isoformat()
                self._log_to_csv(self.orders_csv, order)
                self.store.add_order(order)

    async def watch_bbo(self):
        while True:
            ticker = await self.mock_exchange.watch_ticker(self.symbol)
            self.store.update_bbo(ticker['bid'], ticker['ask'])

async def main():
    print("Starting Debug Monitor (logging to CSV)...")
    monitor = DebugTradeMonitor()
    monitor.compute_window = 10 
    monitor.record_window = 30
    
    await monitor.run()

if __name__ == "__main__":
    asyncio.run(main())
