# Phase 3: PySpark Structured Streaming Consumer
# Reads from Kafka → aggregates → writes to Redis
# Phase 4 addition: momentum-based sentiment score per ticker

from collections import defaultdict, deque          

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, last,
    sum as spark_sum,
    to_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import redis

# ─────────────────────────────────────────────
# SECTION 1: Spark Session
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("StockSentimentStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
# SECTION 2: Define the message schema
# ─────────────────────────────────────────────
schema = StructType([
    StructField("ticker",    StringType(),  True),
    StructField("price",     DoubleType(),  True),
    StructField("volume",    LongType(),    True),
    StructField("timestamp", StringType(),  True),
])

# ─────────────────────────────────────────────
# SECTION 3: Read from Kafka
# ─────────────────────────────────────────────
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# ─────────────────────────────────────────────
# SECTION 4: Parse the JSON payload
# ─────────────────────────────────────────────
parsed = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .filter(col("ticker").isNotNull()) \
    .filter(col("price").isNotNull())

# ─────────────────────────────────────────────
# SECTION 5: Windowed aggregation
# ─────────────────────────────────────────────
windowed = parsed \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "30 seconds"),
        col("ticker")
    ) \
    .agg(
        last("price").alias("avg_price"),
        spark_sum("volume").alias("total_volume")
    )

# ─────────────────────────────────────────────
# SECTION 6: Sentiment — rolling price history
# ─────────────────────────────────────────────
# Keeps the last 5 window prices per ticker in memory.
# Each window = 30 seconds, so 5 windows ≈ 2.5 minutes of lookback.
# Lives here (module level) so it persists across all foreachBatch calls.

PRICE_HISTORY = defaultdict(lambda: deque(maxlen=5))   # ← NEW

def compute_sentiment(ticker: str, current_price: float):
    """
    Momentum-based sentiment over the last 5 price windows.

    Score = clamp(momentum_pct × 20, -100, +100)
      A 0.5% price rise  → score ≈ +10  → Bullish
      A 0.5% price drop  → score ≈ -10  → Bearish
      < ±0.05% change    → Neutral

    Returns (score: float, label: str)
    """
    history = PRICE_HISTORY[ticker]
    history.append(current_price)

    # Need at least 2 data points to compute any momentum
    if len(history) < 2:
        return 0.0, "Neutral"

    oldest_price = history[0]

    # Guard against division by zero (shouldn't happen with real prices)
    if oldest_price == 0:
        return 0.0, "Neutral"

    momentum_pct = (current_price - oldest_price) / oldest_price * 100
    score = round(max(-100.0, min(100.0, momentum_pct * 20)), 2)

    if momentum_pct > 0.05:
        label = "Bullish"
    elif momentum_pct < -0.05:
        label = "Bearish"
    else:
        label = "Neutral"

    return score, label

# ─────────────────────────────────────────────
# SECTION 7: Write aggregations + sentiment to Redis
# ─────────────────────────────────────────────
r = redis.Redis(host="redis", port=6379, decode_responses=True)

def write_to_redis(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    pipe = r.pipeline()
    log_lines = []

    for row in rows:
        ticker      = row["ticker"]
        avg_price   = round(row["avg_price"], 4)
        total_vol   = int(row["total_volume"])
        window_end  = row["window"]["end"]

        score, label = compute_sentiment(ticker, avg_price)

        # Key 1: Hash
        hash_key = f"{ticker}:latest"
        pipe.hset(hash_key, mapping={
            "avg_price":       avg_price,
            "total_volume":    total_vol,
            "window_end":      window_end.isoformat(),
            "sentiment_score": score,
            "sentiment_label": label,
        })
        pipe.expire(hash_key, 300)

        # Key 2a: Price stream
        pipe.xadd(f"{ticker}:price_stream",
                  {"price": str(avg_price)}, maxlen=100, approximate=True)

        # Key 2b: Volume stream
        pipe.xadd(f"{ticker}:volume_stream",
                  {"volume": str(total_vol)}, maxlen=100, approximate=True)

        # Key 2c: Sentiment stream
        pipe.xadd(f"{ticker}:sentiment_stream",
                  {"score": str(score), "label": label}, maxlen=100, approximate=True)

        log_lines.append(f"  {ticker}: ${avg_price}  {label} (score={score})")

    pipe.execute()
    print(f"[batch {batch_id}] Wrote {len(rows)} window(s) to Redis")
    for line in log_lines:
        print(line)

# ─────────────────────────────────────────────
# SECTION 8: Start the streaming query
# ─────────────────────────────────────────────
query = windowed.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_redis) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

print("✅ Streaming query started. Waiting for data from Kafka...")
query.awaitTermination()