"""
Phase 2: Kafka Producer for Live Stock Price Sentiment Monitor

Fetches real-time stock prices via yfinance and publishes
structured JSON messages to the Kafka topic `stock-prices`.

Runs on the HOST machine (outside Docker).
Kafka reachable at localhost:9092 via the PLAINTEXT listener.
"""

import json
import time
import logging
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# Configuration 

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "stock-prices"

TICKERS = ["AAPL", "TSLA", "GOOGL"]

POLL_INTERVAL_SECONDS = 10

# Logging 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Kafka Producer Setup

def create_producer(retries: int = 5, delay: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
            )
            log.info("✅ Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning(
                "⏳ Kafka not reachable (attempt %d/%d). Retrying in %ds...",
                attempt, retries, delay,
            )
            time.sleep(delay)

    raise RuntimeError(
        f"❌ Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} "
        f"after {retries} attempts. Is Docker running?"
    )

# Price Fetching 

def fetch_price(ticker: str) -> dict | None:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="1d", interval="1m")

        if not hist.empty:
            # Most recent completed 1-minute bar
            last_bar = hist.iloc[-1]
            price  = round(float(last_bar["Close"]), 4)
            volume = int(last_bar["Volume"])
        else:
            # Off-hours fallback: no intraday bars (weekend / holiday)
            log.warning("⚠️  %s: no 1m bars available, falling back to fast_info", ticker)
            info = t.fast_info
            price = info.last_price
            if price is None:
                log.warning("⚠️  %s: fast_info price also None", ticker)
                return None
            price  = round(float(price), 4)
            volume = int(info.three_month_average_volume or 0)

        return {
            "ticker":    ticker,
            "price":     price,
            "volume":    volume,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as exc:
        log.error("❌ Failed to fetch %s: %s", ticker, exc)
        return None

# Delivery Callbacks

def on_send_success(record_metadata):
    log.info(
        "📨 Sent → topic=%s  partition=%d  offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )

def on_send_error(exc):
    log.error("❌ Kafka send failed: %s", exc)

# Main Loop 

def main():
    log.info("🚀 Starting Stock Price Producer")
    log.info("   Tickers       : %s", TICKERS)
    log.info("   Topic         : %s", KAFKA_TOPIC)
    log.info("   Poll interval : %ds", POLL_INTERVAL_SECONDS)

    producer = create_producer()

    try:
        while True:
            log.info("── Polling prices ──────────────────────────────")

            for ticker in TICKERS:
                payload = fetch_price(ticker)

                if payload is None:
                    continue

                log.info("📈 %s → $%.4f  vol=%d", payload["ticker"], payload["price"], payload["volume"])

                producer.send(KAFKA_TOPIC, value=payload) \
                        .add_callback(on_send_success) \
                        .add_errback(on_send_error)

            producer.flush()

            log.info("💤 Sleeping %ds...\n", POLL_INTERVAL_SECONDS)
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        log.info("🛑 Producer stopped by user (Ctrl+C)")
    finally:
        producer.close()
        log.info("🔒 Kafka producer closed cleanly.")


if __name__ == "__main__":
    main()