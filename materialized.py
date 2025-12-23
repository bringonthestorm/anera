from chdb import session as chs

sess = chs.Session("/Users/davidefloriello/genai/chdb_data")  

sess.query("""
CREATE MATERIALIZED VIEW IF NOT EXISTS ofi_per_min
ENGINE = SummingMergeTree
ORDER BY minute AS
SELECT
    toStartOfMinute(ts) AS minute,
    sum(ofi) AS ofi_sum
FROM market_ticks
GROUP BY minute;
""")