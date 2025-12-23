from prometheus_client import start_http_server, Gauge, Counter

INGESTION_LAG = Gauge("ingestion_lag_seconds", "Lag of ingestion behind real-time")
QUEUE_SIZE = Gauge("queue_size", "Asyncio queue size")
OFI_RATE = Counter("ofi_rate_total", "Total OFI calculations performed")

def start_metrics_server(port=8001):
    start_http_server(port)
    print(f"Prometheus metrics server started at http://localhost:{port}")
