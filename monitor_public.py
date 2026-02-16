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
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential, retry_if_exception_type

class ReconnectionError(Exception):
    """Custom exception to signal a retry is needed after exchange re-init."""
    pass

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PublicDataStore:
    def __init__(self, record_window_seconds):
        self.record_window = record_window_seconds
        self.trades = deque()  # Queue of dicts
        self.order_book = None  # Latest order book Snapshot
        self.last_book_update = 0
        self.bbo_history = deque()  # Queue of (timestamp, bid, ask)
        self.last_activity = time.time() 
        self.connected = True # Proactive connection status
        
    def add_trade(self, trade):
        trade['received_at'] = time.time()
        self.trades.append(trade)
        self.last_activity = time.time()
        self.prune()

    def update_order_book(self, book):
        self.order_book = book
        self.last_book_update = time.time()
        self.last_activity = time.time()

    def update_bbo(self, bid, ask):
        self.bbo_history.append({
            'timestamp': time.time(),
            'bid': bid,
            'ask': ask,
            'mid': (bid + ask) / 2.0
        })
        self.last_activity = time.time()
        self.prune()

    def prune(self):
        cutoff = time.time() - self.record_window
        
        # Efficient pruning from the left (oldest first)
        while self.trades and self.trades[0]['received_at'] < cutoff:
            self.trades.popleft()
            
        while self.bbo_history and self.bbo_history[0]['timestamp'] < cutoff:
            self.bbo_history.popleft()

    def get_trades_since(self, seconds):
        cutoff = time.time() - seconds
        return [t for t in self.trades if t['received_at'] > cutoff]

class PublicTradeMonitor:
    def __init__(self):
        with open('config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
            
        self.exchange_id = self.config['exchange_name']
        self.symbol = self.config['symbol']
        self.depths = self.config['depths']
        self.quantiles = self.config['quantiles']
        self.compute_window = self.config['compute_window']
        self.record_window = self.config['record_window']
        
        # Public monitoring might not need these, but we keep them just in case for higher rate limits
        self.api_key = os.getenv('API_KEY')
        self.secret_key = os.getenv('SECRET_KEY')
        
        self.store = PublicDataStore(self.record_window)
        self.reconnect_lock = asyncio.Lock()
        self._init_exchange()
        
        # Internal state for tracking "total size within depth" history.
        self.order_size_history = {d: deque(maxlen=self.record_window) for d in self.depths}

    def _init_exchange(self):
        exchange_config = {
            'enableRateLimit': True,
        }
        # In case we need keys for higher rate limits even for public data
        if self.api_key and self.secret_key:
            exchange_config['apiKey'] = self.api_key
            exchange_config['secret'] = self.secret_key
            
        self.exchange = getattr(ccxtpro, self.exchange_id)(exchange_config)

    async def watch_with_retry(self, method_name, *args):
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(10),
            wait=wait_exponential(multiplier=1, min=1, max=60),
            retry=retry_if_exception_type((Exception, ReconnectionError)),
            before_sleep=lambda retry_state: self._handle_retry_failure(method_name, retry_state)
        ):
            with attempt:
                method = getattr(self.exchange, method_name)
                try:
                    data = await asyncio.wait_for(method(*args), timeout=30)
                    self.store.connected = True
                    return data
                except (Exception, asyncio.CancelledError) as e:
                    self.store.connected = False
                    
                    # If this is a CancelledError, it might be because ANOTHER task 
                    # called self.exchange.close() to reset the connection.
                    error_type = type(e).__name__
                    logger.error(f"Error in {method_name}: {error_type}. Re-initializing connection...")
                    
                    async with self.reconnect_lock:
                        if method.__self__ is self.exchange:
                            try:
                                await self.exchange.close()
                            except:
                                pass
                            self._init_exchange()
                    
                    if isinstance(e, asyncio.CancelledError):
                        # Convert to an Exception so tenacity handles it
                        raise ReconnectionError(f"Task cancelled in {method_name}") from e
                    raise e

    def _handle_retry_failure(self, method_name, retry_state):
        logger.warning(f"Retrying {method_name} (Attempt {retry_state.attempt_number}/10) after error: {retry_state.outcome.exception()}")
        self.store.connected = False

    async def watch_trades(self):
        logger.info(f"Starting trade watcher for {self.symbol}")
        first = True
        while True:
            trades = await self.watch_with_retry('watch_trades', self.symbol)
            if first:
                logger.info(f"Received first trade for {self.symbol}")
                first = False
            for trade in trades:
                self.store.add_trade(trade)

    async def watch_order_book(self):
        logger.info(f"Starting order book watcher for {self.symbol}")
        first = True
        while True:
            book = await self.watch_with_retry('watch_order_book', self.symbol)
            if first:
                logger.info(f"Received first order book snapshot for {self.symbol}")
                first = False
            self.store.update_order_book(book)

    async def sample_order_snapshots(self):
        """Samples the current order book sizes within depths every second for quantile calculation."""
        logger.info("Starting order book snapshot sampler")
        while True:
            try:
                if self.store.order_book:
                    book = self.store.order_book
                    bids = book['bids'] # [[price, amount], ...]
                    asks = book['asks'] # [[price, amount], ...]
                    
                    if bids and asks:
                        # Link staleness check
                        if time.time() - self.store.last_activity > 30:
                            # Skip sampling if not heard from server
                            pass
                        else:
                            best_bid = bids[0][0]
                            best_ask = asks[0][0]
                            current_mid = (best_bid + best_ask) / 2.0
                        
                        for d in self.depths:
                            depth_ratio = d * 1e-4
                            min_price = current_mid * (1 - depth_ratio)
                            max_price = current_mid * (1 + depth_ratio)
                            
                            # Calculate value in quote currency (amount * price) for bids and asks within depth
                            bid_value = sum(amount * price for price, amount in bids if price >= min_price)
                            ask_value = sum(amount * price for price, amount in asks if price <= max_price)
                            
                            total_value_within_depth = bid_value + ask_value
                            self.order_size_history[d].append(total_value_within_depth)
                
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in snapshot sampler: {e}")
                await asyncio.sleep(1)

    def verify_trades(self):
        if not self.store.connected or time.time() - self.store.last_activity > 15:
            logger.error("Connection is down or stale (>15s). Reporting STALE_DATA.")
            return "STALE_DATA"

        trades = self.store.get_trades_since(self.compute_window)
        total_volume = sum(t['amount'] * t['price'] for t in trades)
        logger.info(f"Verify Public Trades: Collected {len(trades)} trades. Total volume in last {self.compute_window}s: {total_volume:.2f}")
        return total_volume

    def verify_orders(self):
        """
        Computes the quantile of the total size of the order book 
        within depth of the mid price.
        """
        if not self.store.connected or time.time() - self.store.last_activity > 15:
            logger.error("Connection is down or stale (>15s). Reporting STALE_DATA.")
            return {f"depth_{d}_q{q}": "STALE_DATA" for d in self.depths for q in self.quantiles}

        if not self.store.order_book:
            return {f"depth_{d}_q{q}": "NO_DATA" for d in self.depths for q in self.quantiles}

        results = {}
        for d in self.depths:
            data = np.array(self.order_size_history[d])
            if len(data) > 0:
                for q in self.quantiles:
                    val = np.quantile(data, q)
                    results[f"depth_{d}_q{q}"] = val
            else:
                for q in self.quantiles:
                    results[f"depth_{d}_q{q}"] = "NO_SAMPLES"
                    
        logger.info(f"Verify Public Orders Results: {results}")
        return results

    async def save_to_csv(self, data):
        filename = "results_public.csv"
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
        logger.info(f"Saved public results to {filename}")

    async def scheduler(self):
        # Initialize the first window start
        window_start = datetime.now(UTC)
        logger.info(f"Scheduler started. Window size: {self.compute_window}s")
        
        while True:
            await asyncio.sleep(self.compute_window)
            try:
                window_end = datetime.now(UTC)
                logger.info(f"Starting computation for window {window_start.isoformat()} to {window_end.isoformat()}")
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
        logger.info(f"Initializing monitor for {self.exchange_id} {self.symbol}")
        try:
            await asyncio.gather(
                self.watch_trades(),
                self.watch_order_book(),
                self.sample_order_snapshots(),
                self.scheduler()
            )
        finally:
            await self.exchange.close()
            logger.info("Exchange connection closed")

if __name__ == "__main__":
    monitor = PublicTradeMonitor()
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        logger.info("Monitor stopped by user")
