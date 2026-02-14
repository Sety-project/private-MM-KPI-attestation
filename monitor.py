import asyncio
import ccxt.pro as ccxtpro
import yaml
import os
import time
import logging
from datetime import datetime, timedelta, UTC
import pandas as pd
import numpy as np
from collections import deque
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataStore:
    def __init__(self, record_window_seconds):
        self.record_window = record_window_seconds
        self.trades = deque()  # Queue of dicts
        self.orders = {}  # Dict of order_id: order_data
        self.bbo_history = deque()  # Queue of (timestamp, bid, ask)
        
    def add_trade(self, trade):
        trade['received_at'] = time.time()
        self.trades.append(trade)
        self.prune()

    def add_order(self, order):
        # Update or add the order by its ID to keep track of latest status
        order['received_at'] = time.time()
        self.orders[order['id']] = order
        # Note: We don't prune orders on every addition as it's a dict.
        # Pruning is handled in the periodic prune call.

    def update_bbo(self, bid, ask):
        self.bbo_history.append({
            'timestamp': time.time(),
            'bid': bid,
            'ask': ask,
            'mid': (bid + ask) / 2.0
        })
        self.prune()

    def prune(self):
        cutoff = time.time() - self.record_window
        
        # Efficient pruning from the left (oldest first)
        while self.trades and self.trades[0]['received_at'] < cutoff:
            self.trades.popleft()
            
        while self.bbo_history and self.bbo_history[0]['timestamp'] < cutoff:
            self.bbo_history.popleft()

        # Pruning the orders dict is more expensive, so we only do it if it grows large
        # or less frequently. For now, let's do a simple cleanup.
        if len(self.orders) > 1000:
            expired_ids = [
                oid for oid, o in self.orders.items() 
                if o['received_at'] < cutoff and o['status'] != 'open'
            ]
            for oid in expired_ids:
                del self.orders[oid]

    def get_trades_since(self, seconds):
        cutoff = time.time() - seconds
        return [t for t in self.trades if t['received_at'] > cutoff]

class TradeMonitor:
    def __init__(self, config_path='.env'):
        load_dotenv(config_path)
        with open('config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
            
        self.exchange_id = self.config['exchange_name']
        self.symbol = self.config['symbol']
        self.depths = self.config['depths']
        self.quantiles = self.config['quantiles']
        self.compute_window = self.config['compute_window']
        self.record_window = self.config['record_window']
        
        self.api_key = os.getenv('API_KEY')
        self.secret_key = os.getenv('SECRET_KEY')
        
        self.store = DataStore(self.record_window)
        self.exchange = getattr(ccxtpro, self.exchange_id)({
            'apiKey': self.api_key,
            'secret': self.secret_key,
            'enableRateLimit': True,
        })
        
        # Internal state for tracking "total size within depth" history.
        # We use a deque with maxlen for automatic circular pruning.
        self.order_size_history = {d: deque(maxlen=self.record_window) for d in self.depths}

    async def watch_trades(self):
        while True:
            try:
                trades = await self.exchange.watch_my_trades(self.symbol)
                for trade in trades:
                    logger.info(f"New trade: {trade['amount']} @ {trade['price']}")
                    self.store.add_trade(trade)
            except Exception as e:
                logger.error(f"Error watching trades: {e}")
                await asyncio.sleep(5)

    async def watch_orders(self):
        while True:
            try:
                orders = await self.exchange.watch_orders(self.symbol)
                for order in orders:
                    # logger.info(f"Order update: {order['id']} {order['status']}")
                    self.store.add_order(order)
            except Exception as e:
                logger.error(f"Error watching orders: {e}")
                await asyncio.sleep(5)

    async def watch_bbo(self):
        while True:
            try:
                ticker = await self.exchange.watch_ticker(self.symbol)
                bid = ticker['bid']
                ask = ticker['ask']
                if bid and ask:
                    self.store.update_bbo(bid, ask)
            except Exception as e:
                logger.error(f"Error watching BBO: {e}")
                await asyncio.sleep(5)
    async def sample_order_snapshots(self):
        """Samples the current order sizes within depths every second for quantile calculation."""
        while True:
            try:
                if self.store.bbo_history:
                    current_mid = self.store.bbo_history[-1]['mid']
                    open_orders = [o for o in self.store.orders.values() if o['status'] == 'open']
                    
                    for d in self.depths:
                        size_within_depth = sum(
                            o.get('remaining', o.get('amount', 0)) for o in open_orders
                            if abs(o['price']/current_mid - 1) <= d*1e-4
                        )
                        self.order_size_history[d].append(size_within_depth)
                
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in snapshot sampler: {e}")
                await asyncio.sleep(1)

    def verify_trades(self):
        trades = self.store.get_trades_since(self.compute_window)
        total_volume = sum(t['amount'] * t['price'] for t in trades)
        logger.info(f"Verify Trades: Total volume in last {self.compute_window}s: {total_volume}")
        return total_volume

    def verify_orders(self):
        """
        For each depth and quantile, computes the quantile of the total size of orders 
        within depth of the avg of best bid and offer.
        """
        if not self.store.bbo_history:
            logger.warning("No BBO history yet for verify_orders")
            return {}

        current_mid = self.store.bbo_history[-1]['mid']
        # The calculations are done based on the sampled order_size_history.
        
        results = {}
        for d in self.depths:
            data = np.array(self.order_size_history[d])
            if len(data) > 0:
                for q in self.quantiles:
                    val = np.quantile(data, q)
                    results[f"depth_{d}_q{q}"] = val
                    
        logger.info(f"Verify Orders Results: {results}")
        return results

    async def save_to_csv(self, data):
        filename = "results.csv"
        # Flatten the order_stats into the main dictionary
        row = {
            'start_ts': data['start_ts'],
            'end_ts': data['end_ts'],
            'symbol': data['symbol'],
            'total_volume': data['total_volume']
        }
        # Add order stats directly to the row
        if 'order_stats' in data:
            row.update(data['order_stats'])

        df = pd.DataFrame([row])
        file_exists = os.path.isfile(filename)
        
        # Append to CSV
        df.to_csv(filename, mode='a', index=False, header=not file_exists)
        logger.info(f"Saved results to {filename}")

    async def scheduler(self):
        # Initialize the first window start
        window_start = datetime.now(UTC)
        
        while True:
            await asyncio.sleep(self.compute_window)
            try:
                window_end = datetime.now(UTC)
                volume = self.verify_trades()
                order_stats = self.verify_orders()
                
                payload = {
                    'start_ts': window_start.isoformat(),
                    'end_ts': window_end.isoformat(),
                    'symbol': self.symbol,
                    'total_volume': volume,
                    'order_stats': order_stats
                }
                
                await self.save_to_csv(payload)
                
                # Update window_start for next iteration
                window_start = window_end
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")

    async def run(self):
        await asyncio.gather(
            self.watch_trades(),
            self.watch_orders(),
            self.watch_bbo(),
            self.sample_order_snapshots(),
            self.scheduler()
        )

if __name__ == "__main__":
    monitor = TradeMonitor()
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        pass
