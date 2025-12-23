# Binance Futures Pipeline

## Features

1. **Data collection**
   - Pulls tick-by-tick trade data from Binance WebSocket (`btcusdt@trade`).
   - Validates ticks in real-time.

2. **Validation**
   - Smooths large price jumps (>5% by default).
   - Maintains rolling 5-minute max/min for quantity.
   - Adjusts ticks automatically if they exceed thresholds.

3. **OFI calculation**
   - Computes a simple **Order-Flow Imbalance (OFI)** per tick.
  
4. **Storage-Agnostic component**
   - **ParquetWriter**: appends tick data to a Parquet file (Arrow engine).
   - **TimescaleWriter**: inserts ticks into TimescaleDB hypertables.

5. **Materialized view**
   - Aggregates OFI per minute (`ofi_per_min`) for fast queries and visualizations.
   
## Design choices

### 1. **Performance**
- **Rolling Window for Validation:** Max/min quantity is tracked using a `deque` to efficiently remove old ticks and adjust unlikely price values.
- **Lightweight OFI Computation:** OFI is computed incrementally to avoid full table scans.
- **Materialized Views:** Pre-aggregates OFI per minute to speed up analytics queries.

### 2. **Scaling**
- **AsyncIO Collector + Writer:** Collector and writer run in separate asynchronous tasks, allowing high-throughput ingestion.
- **Queue Buffering:** Async queue prevents backpressure from blocking WebSocket collection.

## Assumptions

- Binance WebSocket is stable and delivers all ticks in order.
- Tick quantities and prices are roughly consistent.
- extreme outliers are smoothed by the validator with a simple criterion.
- Timezone consistency: all timestamps are stored in **UTC**.

## Limitations & Known Issues

- **Price smoothing:** Large sudden price drops or spikes are capped by a fixed percentage, but this should be made dependent on local volatility.
- **Checks:** more checks should be added to track and asses consistency of time series (e.g.: time std of price series, outlier detection, time-consistency checks,
 time-delta between consecutive ticks, comparison with other markets, ...) 
- **Single-node limitation:** The collector runs on a single process; high-frequency trading scenarios may require multiple instances.
- **Data loss:** In the case of collector crash or network disconnection, uncommitted ticks in the queue may be lost.
