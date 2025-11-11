"""
Automation utilities for historical backfills and daily realtime updates.

This module orchestrates the existing stage1 → stage4 pipeline and exposes
two command line entrypoints:

- historical: run a full backfill across a range of timeline keys.
- daily: fetch and process the most recent news for today only.

Example:
    python automation.py historical --start-key 500 --end-key 1000
    python automation.py daily --keys 500 501 502 --symbol ACB
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from tqdm import tqdm
from vnstock3 import Vnstock

from stage1 import url_extract as fetch_timeline_page
from stage2 import process_each_file
from stage3 import url_extract as fetch_article_page
from stage4 import NonmatchException, PostProcessing, pre_processing_page_data

try:
    import mysql.connector  # type: ignore
    from mysql.connector.connection import MySQLConnection  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    mysql = None  # type: ignore
    MySQLConnection = None  # type: ignore


BASE_DIR = Path(__file__).resolve().parent
STAGE1_DIR = BASE_DIR / "stage_1_data"
STAGE2_DIR = BASE_DIR / "stage_2_data"
STAGE3_DIR = BASE_DIR / "stage_3_data"
STAGE4_DIR = BASE_DIR / "stage_4_data"
DAILY_DIR = BASE_DIR / "daily_outputs"


def ensure_directories() -> None:
    for directory in [
        STAGE1_DIR,
        STAGE2_DIR,
        STAGE3_DIR,
        STAGE4_DIR,
        DAILY_DIR,
    ]:
        directory.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Historical backfill helpers
# ---------------------------------------------------------------------------


def download_timeline_pages(keys: Iterable[int], delay_seconds: float = 3.0) -> None:
    """
    Download timeline pages (stage 1) for the provided keys.

    Args:
        keys: Iterable of integer keys to fetch from Cafef timeline endpoint.
        delay_seconds: Optional delay between requests to avoid overwhelming the server.
    """
    ensure_directories()
    for key in keys:
        output_path = STAGE1_DIR / f"{key}.pkl"
        if output_path.exists():
            continue

        response_dict = fetch_timeline_page(key=key)
        if response_dict is None:
            continue

        with output_path.open("wb") as fp:
            pickle.dump(response_dict, fp)

        time.sleep(delay_seconds)


def build_link_catalogue() -> Path:
    """
    Process downloaded timeline pickles (stage 2) and produce a JSON file containing
    all article links.

    Returns:
        Path to the generated JSON catalogue.
    """
    ensure_directories()
    catalogue: List[Dict[str, str]] = []
    for pickle_file in sorted(STAGE1_DIR.glob("*.pkl")):
        with pickle_file.open("rb") as fp:
            data = pickle.load(fp)
        try:
            catalogue.extend(process_each_file(data))
        except Exception as exc:
            print(f"[stage2] Failed to parse {pickle_file.name}: {exc}")
            continue

    output_path = STAGE2_DIR / "links.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(catalogue, fp, indent=4, ensure_ascii=False)

    return output_path


def download_article_pages(step: int = 1000, delay_seconds: float = 1.5) -> None:
    """
    Download article detail pages (stage 3) in batches.

    Args:
        step: Number of links per batch file.
        delay_seconds: Optional delay between requests.
    """
    ensure_directories()
    links_path = STAGE2_DIR / "links.json"
    if not links_path.exists():
        raise FileNotFoundError(
            "Stage 2 catalogue not found. Run build_link_catalogue() first."
        )

    with links_path.open("r", encoding="utf-8") as fp:
        links = json.load(fp)

    total_links = len(links)
    for start in range(0, total_links, step):
        output_path = STAGE3_DIR / f"page_data_{start}.json"
        if output_path.exists():
            continue

        end = min(start + step, total_links)
        batch_links = links[start:end]

        batch_payload: List[Dict[str, str]] = []
        for entry in batch_links:
            result = fetch_article_page(url=entry["link"], key=entry["key"])
            if result is not None:
                batch_payload.append(result)
            time.sleep(delay_seconds)

        with output_path.open("w", encoding="utf-8") as fp:
            json.dump(batch_payload, fp, indent=4, ensure_ascii=False)


def preprocess_articles() -> None:
    """
    Stage 4 preprocessing: transform raw article HTML into structured corpus files.
    """
    ensure_directories()
    for json_file in sorted(STAGE3_DIR.glob("page_data_*.json")):
        output_path = STAGE4_DIR / json_file.name
        if output_path.exists():
            continue

        with json_file.open("r", encoding="utf-8") as fp:
            articles = json.load(fp)

        processed_items: List[Dict[str, str]] = []
        for article in tqdm(
            articles, desc=f"preprocess:{json_file.name}", unit="article"
        ):
            try:
                processed_items.append(
                    pre_processing_page_data(
                        page_data=article["page_data"], url=article["url"]
                    )
                )
            except (IndexError, NonmatchException) as exc:
                continue

        with output_path.open("w", encoding="utf-8") as fp:
            json.dump(processed_items, fp, indent=4, ensure_ascii=False)


def run_historical_pipeline(start_key: int, end_key: int, step: int = 1000) -> Path:
    """
    End-to-end historical pipeline covering stage1 → stage4 alignment.

    Args:
        start_key: First key (inclusive) for timeline crawling.
        end_key: Last key (exclusive) for timeline crawling.
        step: Batch size for stage 3 downloads.

    Returns:
        Path to the generated CSV file.
    """
    key_range = range(start_key, end_key)
    download_timeline_pages(key_range)
    build_link_catalogue()
    download_article_pages(step=step)
    preprocess_articles()

    engine = PostProcessing()
    aligned_df = engine.align()

    csv_path = STAGE4_DIR / "total.csv"
    aligned_df.to_csv(csv_path, index=False)

    return csv_path


# ---------------------------------------------------------------------------
# Daily realtime pipeline
# ---------------------------------------------------------------------------

@dataclass
class DailyResult:
    symbol: str
    news_events: List[Dict[str, str]]
    price_row: Optional[pd.Series]
    record: Optional[Dict[str, object]]
    output_path: Path


def fetch_daily_news(keys: Sequence[int]) -> List[Dict[str, str]]:
    """
    Retrieve and preprocess news articles for the provided timeline keys.
    Only articles that pass the keyword filter inside `pre_processing_page_data`
    are returned.
    """
    ensure_directories()
    today = date.today()
    filtered_articles: List[Dict[str, str]] = []

    for key in keys:
        response = fetch_timeline_page(key=key)
        if response is None:
            continue

        try:
            link_entries = process_each_file(response)
        except Exception as exc:
            print(f"[daily] failed to extract links for key={key}: {exc}")
            continue

        for entry in link_entries:
            article = fetch_article_page(url=entry["link"], key=entry["key"])
            if article is None:
                continue

            try:
                processed = pre_processing_page_data(
                    page_data=article["page_data"], url=article["url"]
                )
            except (IndexError, NonmatchException):
                continue

            article_date = date(
                processed["year"], processed["month"], processed["day"]
            )
            if article_date == today:
                filtered_articles.append(processed)

    return filtered_articles


def fetch_daily_price(symbol: str) -> Optional[pd.Series]:
    """
    Fetch today's price row for the given stock symbol using vnstock3.
    Returns None if market data is unavailable.
    """
    today_str = str(date.today())
    stocks = Vnstock().stock(symbol=symbol, source="TCBS")
    history = stocks.quote.history(
        start=today_str,
        end=today_str,
        interval="1D",
    )
    history = history.sort_values(by="time")

    if history.empty:
        return None
    history["time"] = pd.to_datetime(history["time"])
    return history.iloc[-1]


def compose_daily_record(
    symbol: str,
    price_row: Optional[pd.Series],
    news_events: List[Dict[str, str]],
) -> Optional[Dict[str, object]]:
    """
    Build a single-row payload matching the structure expected by the training CSV.
    Returns None when price data is unavailable.
    """
    if price_row is None:
        return None

    def _to_float(value: object) -> Optional[float]:
        if value is None:
            return None
        if pd.isna(value):
            return None
        if hasattr(value, "item"):
            return float(value.item())
        return float(value)

    timestamp = pd.to_datetime(price_row["time"]).to_pydatetime()
    merge_corpus = "\n\n".join(article["corpus"] for article in news_events) if news_events else ""

    record = {
        "symbol": symbol,
        "time": timestamp.isoformat(),
        "open": _to_float(price_row.get("open")),
        "high": _to_float(price_row.get("high")),
        "low": _to_float(price_row.get("low")),
        "close": _to_float(price_row.get("close")),
        "volume": _to_float(price_row.get("volume")),
        "year": timestamp.year,
        "month": timestamp.month,
        "day": timestamp.day,
        "merge_corpus": merge_corpus,
        "news_count": len(news_events),
        "created_at": datetime.utcnow().isoformat(),
    }

    return record


def insert_daily_row(
    record: Dict[str, object],
    table: str,
    db_config: Dict[str, object],
) -> None:
    """
    Insert the composed daily record into a MySQL table.
    """
    if mysql is None or MySQLConnection is None:  # pragma: no cover - optional dependency
        raise ImportError(
            "mysql-connector-python is required for database inserts. "
            "Install it or remove the database options."
        )

    connection: Optional[MySQLConnection] = None
    try:
        connection = mysql.connector.connect(**db_config)  # type: ignore[arg-type]
        cursor = connection.cursor()

        columns = list(record.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        column_clause = ", ".join(f"`{col}`" for col in columns)
        sql = f"INSERT INTO {table} ({column_clause}) VALUES ({placeholders})"

        values = [record[col] for col in columns]
        cursor.execute(sql, values)
        connection.commit()
    finally:
        if connection is not None:
            connection.close()


def run_daily_pipeline(
    keys: Sequence[int],
    symbol: str = "ACB",
    *,
    db_config: Optional[Dict[str, object]] = None,
) -> DailyResult:
    """
    Execute the realtime pipeline: fetch today's news and price, then persist
    the results to `daily_outputs`.
    """
    ensure_directories()
    today_str = date.today().isoformat()
    output_path = DAILY_DIR / f"{today_str}.json"

    news_events = fetch_daily_news(keys)
    price_row = fetch_daily_price(symbol=symbol)
    record = compose_daily_record(symbol=symbol, price_row=price_row, news_events=news_events)

    if db_config and record is not None:
        insert_daily_row(record, "fact_price_stock", db_config)

    payload = {
        "symbol": symbol,
        "generated_at": datetime.utcnow().isoformat(),
        "news_events": news_events,
        "price": price_row.to_dict() if price_row is not None else None,
        "record": record,
    }

    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=4, ensure_ascii=False, default=str)

    return DailyResult(
        symbol=symbol,
        news_events=news_events,
        price_row=price_row,
        record=record,
        output_path=output_path,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Automation helpers for Cafef stock/news pipeline."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    hist_parser = subparsers.add_parser("historical", help="Run full historical scrape")
    hist_parser.add_argument("--start-key", type=int, required=True)
    hist_parser.add_argument("--end-key", type=int, required=True)
    hist_parser.add_argument("--batch-size", type=int, default=1000)

    daily_parser = subparsers.add_parser("daily", help="Run daily realtime crawl")
    daily_parser.add_argument(
        "--keys",
        nargs="+",
        type=int,
        required=True,
        help="Timeline keys to inspect for today's news.",
    )
    daily_parser.add_argument("--symbol", type=str, default="ACB")
    daily_parser.add_argument("--db-host", type=str, help="MySQL host.")
    daily_parser.add_argument("--db-port", type=int, default=3306, help="MySQL port.")
    daily_parser.add_argument("--db-user", type=str, help="MySQL user.")
    daily_parser.add_argument("--db-password", type=str, help="MySQL password.")
    daily_parser.add_argument("--db-name", type=str, help="MySQL database name.")

    return parser.parse_args()


def resolve_db_config(args: argparse.Namespace) -> Optional[Dict[str, object]]:
    """
    Combine CLI arguments with environment variables to build DB config.
    Returns db_config or None if not enough info provided.
    Table name is hardcoded to 'fact_price_stock'.
    """
    def _env_or_arg(arg_value: Optional[str], env_key: str) -> Optional[str]:
        return arg_value or os.getenv(env_key)

    host = _env_or_arg(args.db_host, "MYSQL_HOST")
    user = _env_or_arg(args.db_user, "MYSQL_USER")
    password = _env_or_arg(args.db_password, "MYSQL_PASSWORD")
    database = _env_or_arg(args.db_name, "MYSQL_DATABASE")
    port = args.db_port or int(os.getenv("MYSQL_PORT", "3306"))

    # If any required field is missing, return None (optional DB insert)
    if not all([host, user, password, database]):
        return None

    db_config: Dict[str, object] = {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port,
    }
    return db_config


def main() -> None:
    args = parse_args()

    if args.command == "historical":
        csv_path = run_historical_pipeline(
            start_key=args.start_key, end_key=args.end_key, step=args.batch_size
        )
        print(f"[historical] dataset exported to {csv_path}")
    elif args.command == "daily":
        db_config = resolve_db_config(args)
        result = run_daily_pipeline(
            keys=args.keys,
            symbol=args.symbol,
            db_config=db_config,
        )
        print(f"[daily] symbol: {result.symbol}")
        print(f"[daily] news events: {len(result.news_events)} items")
        if result.price_row is not None:
            close_price = result.price_row.get("close")
            print(f"[daily] close price: {close_price}")
        else:
            print("[daily] price data unavailable for today.")
        if result.record is not None:
            print("[daily] record prepared for database insert.")
        print(f"[daily] payload saved to {result.output_path}")


if __name__ == "__main__":
    main()


