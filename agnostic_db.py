
from datetime import datetime

class BaseWriter:
    def insert_batch(self, rows):
        """Insert a batch of tick dictionaries into storage"""
        raise NotImplementedError()


class ParquetWriter(BaseWriter):
    def __init__(self, path="ticks.parquet"):
        self.path = path

    def insert_batch(self, rows):
        import pandas as pd
        df = pd.DataFrame(rows)
        df.to_parquet(self.path, engine="pyarrow", index=False, append=True)

class TimescaleWriter(BaseWriter):
    def __init__(self, conn):
        """
        conn: psycopg2 connection
        """
        self.conn = conn
        self._create_table()

    def _create_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS market_ticks (
                ts TIMESTAMP,
                price DOUBLE PRECISION,
                qty DOUBLE PRECISION,
                side TEXT,
                ofi DOUBLE PRECISION
            );
            SELECT create_hypertable('market_ticks', 'ts', if_not_exists => TRUE);
            """)
            self.conn.commit()

    def insert_batch(self, rows):
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO market_ticks (ts, price, qty, side, ofi)
            VALUES (%s, %s, %s, %s, %s)
            """, [(r["ts"], r["price"], r["qty"], r["side"], r["ofi"]) for r in rows])
            self.conn.commit()
