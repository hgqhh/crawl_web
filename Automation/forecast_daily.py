"""
Daily forecasting script.

Reads the latest 20 rows from fact_price_stock table, runs the model,
and inserts the prediction into fact_price_predict table.

Usage:
    python forecast_daily.py \
        --db-host localhost \
        --db-user my_user \
        --db-password secret \
        --db-name stock_db \
        --model-path model.pt

Environment variables can be used instead of CLI flags:
    MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import torch
from sentence_transformers import SentenceTransformer

from model.LSTM.modeling import LSTMModel

try:
    import mysql.connector  # type: ignore
    from mysql.connector.connection import MySQLConnection  # type: ignore
except ImportError as exc:  # pragma: no cover - optional dependency
    raise ImportError(
        "mysql-connector-python is required to run forecast_daily.py. "
        "Install it via `pip install mysql-connector-python`."
    ) from exc


DEFAULT_SEQUENCE_LENGTH = 20
EMBEDDING_MODEL = "dangvantuan/vietnamese-document-embedding"


@dataclass
class ForecastResult:
    reference_time: pd.Timestamp
    predicted_price: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily stock forecast")
    parser.add_argument("--db-host", type=str, help="MySQL host.")
    parser.add_argument("--db-port", type=int, default=3306, help="MySQL port.")
    parser.add_argument("--db-user", type=str, help="MySQL user.")
    parser.add_argument("--db-password", type=str, help="MySQL password.")
    parser.add_argument("--db-name", type=str, help="MySQL database name.")
    parser.add_argument("--model-path", type=Path, default=Path("model.pt"))
    parser.add_argument("--sequence-length", type=int, default=DEFAULT_SEQUENCE_LENGTH)
    parser.add_argument(
        "--device",
        type=str,
        choices=["auto", "cpu", "cuda"],
        default="auto",
        help="Torch device. 'auto' selects cuda if available.",
    )
    return parser.parse_args()


def resolve_db_config(args: argparse.Namespace) -> Dict[str, object]:
    """
    Combine CLI arguments with environment variables to build DB config.
    Table names are hardcoded: 'fact_price_stock' for reading, 'fact_price_predict' for writing.
    """
    def _env_or_arg(arg_value: Optional[str], env_key: str) -> Optional[str]:
        return arg_value or os.getenv(env_key)

    host = _env_or_arg(args.db_host, "MYSQL_HOST")
    user = _env_or_arg(args.db_user, "MYSQL_USER")
    password = _env_or_arg(args.db_password, "MYSQL_PASSWORD")
    database = _env_or_arg(args.db_name, "MYSQL_DATABASE")
    port = args.db_port or int(os.getenv("MYSQL_PORT", "3306"))

    missing = [name for name, value in [("host", host), ("user", user), ("password", password), ("database", database)] if not value]
    if missing:
        raise ValueError(
            f"Missing database configuration for fields: {', '.join(missing)}. "
            "Provide CLI options or environment variables."
        )

    db_config: Dict[str, object] = {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port,
    }
    return db_config


def fetch_last_days(
    db_config: Dict[str, object],
    sequence_length: int,
) -> pd.DataFrame:
    """
    Pull the latest rows ordered by time ascending. Ensures that we have the
    required number of rows for inference.
    """
    connection: Optional[MySQLConnection] = None
    try:
        connection = mysql.connector.connect(**db_config)  # type: ignore[arg-type]
        query = f"""
            SELECT *
            FROM fact_price_stock
            ORDER BY time DESC
            LIMIT {sequence_length}
        """
        df = pd.read_sql(query, connection)
    finally:
        if connection is not None:
            connection.close()

    if df.empty or len(df) < sequence_length:
        raise ValueError(
            f"Not enough rows to run inference. Required: {sequence_length}, "
            f"available: {len(df)}."
        )

    df = df.sort_values(by="time").reset_index(drop=True)
    df["time"] = pd.to_datetime(df["time"])
    return df


def load_model(model_path: Path, device_preference: str) -> Tuple[LSTMModel, torch.device]:
    if not model_path.exists():
        raise FileNotFoundError(f"Model checkpoint not found at {model_path}")

    if device_preference == "auto":
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    else:
        device = torch.device(device_preference)

    model = LSTMModel(
        input_price_dim=4,
        cell_hidden_dim=768,
        last_cfl_hidden_dim=256,
        sequence_length=DEFAULT_SEQUENCE_LENGTH,
        final_output_dim=1,
    )
    state = torch.load(model_path, map_location=device)
    model.load_state_dict(state)
    model.eval()
    model.to(device)
    return model, device


def prepare_tensors(
    df: pd.DataFrame,
    sentence_model: SentenceTransformer,
) -> Tuple[torch.Tensor, torch.Tensor, Dict[str, Dict[str, float]]]:
    """
    Replica of the training preprocessing for a single sequence.
    """
    prices = df.copy()

    price_stats = {
        "high": {"min": prices["high"].min(), "max": prices["high"].max()},
        "low": {"min": prices["low"].min(), "max": prices["low"].max()},
        "open": {"min": prices["open"].min(), "max": prices["open"].max()},
        "close": {"min": prices["close"].min(), "max": prices["close"].max()},
    }

    def _scale(column: str) -> pd.Series:
        denom = price_stats[column]["max"] - price_stats[column]["min"]
        denom = denom if denom != 0 else 1.0
        return (prices[column] - price_stats[column]["min"]) / denom

    scaled_high = _scale("high")
    scaled_low = _scale("low")
    scaled_open = _scale("open")
    scaled_close = _scale("close")

    price_tensor = torch.tensor(
        np.column_stack(
            [
                scaled_high.values,
                scaled_low.values,
                scaled_open.values,
                scaled_close.values,
            ]
        ),
        dtype=torch.float32,
    ).unsqueeze(0)  # (1, seq_len, 4)

    corpus = prices["merge_corpus"].fillna("").tolist()
    embedding_dim = 768
    event_tensor = torch.zeros((1, len(corpus), embedding_dim), dtype=torch.float32)

    non_empty_indices = [idx for idx, text in enumerate(corpus) if text.strip()]
    if non_empty_indices:
        non_empty_texts = [corpus[idx] for idx in non_empty_indices]
        embeddings = sentence_model.encode(
            non_empty_texts,
            show_progress_bar=False,
            precision="float32",
            convert_to_tensor=True,
        ).cpu()
        for tensor_idx, corpus_idx in enumerate(non_empty_indices):
            event_tensor[0, corpus_idx, :] = embeddings[tensor_idx]

    return price_tensor, event_tensor, price_stats


def insert_prediction(
    reference_time: pd.Timestamp,
    predicted_price: float,
    db_config: Dict[str, object],
) -> None:
    """
    Insert the forecast result into fact_price_predict table.
    """
    connection: Optional[MySQLConnection] = None
    try:
        connection = mysql.connector.connect(**db_config)  # type: ignore[arg-type]
        cursor = connection.cursor()

        sql = """
            INSERT INTO fact_price_predict (time, price_predict)
            VALUES (%s, %s)
        """
        values = (reference_time.to_pydatetime(), predicted_price)
        cursor.execute(sql, values)
        connection.commit()
    finally:
        if connection is not None:
            connection.close()


def run_forecast(args: argparse.Namespace) -> ForecastResult:
    db_config = resolve_db_config(args)
    sequence_length = args.sequence_length

    history_df = fetch_last_days(
        db_config=db_config,
        sequence_length=sequence_length,
    )

    reference_time = history_df.iloc[-1]["time"]

    model, device = load_model(args.model_path, args.device)
    sentence_model = SentenceTransformer(
        EMBEDDING_MODEL,
        cache_folder=".checkpoint",
        trust_remote_code=True,
        device=device,
    )

    price_tensor, event_tensor, stats = prepare_tensors(history_df, sentence_model)
    price_tensor = price_tensor.to(device)
    event_tensor = event_tensor.to(device)

    with torch.no_grad():
        scaled_pred = model(price_tensor, event_tensor)

    scaled_value = scaled_pred.cpu().numpy()[0]
    close_min = stats["close"]["min"]
    close_max = stats["close"]["max"]
    predicted_price = scaled_value * (close_max - close_min) + close_min

    result = ForecastResult(
        reference_time=reference_time,
        predicted_price=float(predicted_price),
    )

    # Insert prediction into database
    insert_prediction(
        reference_time=result.reference_time,
        predicted_price=result.predicted_price,
        db_config=db_config,
    )

    return result


def main() -> None:
    args = parse_args()
    result = run_forecast(args)
    print("time,price_predict")
    print(f"{result.reference_time.date()},{result.predicted_price:.2f}")


if __name__ == "__main__":
    main()


