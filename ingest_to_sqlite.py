from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd


DB_NAME = "ecom.db"
CSV_FILES: Dict[str, str] = {
    "products": "products.csv",
    "users": "users.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "reviews": "reviews.csv",
}

SCHEMA_DEFINITIONS: Dict[str, str] = {
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            brand TEXT NOT NULL,
            price REAL NOT NULL,
            cost REAL NOT NULL,
            inventory INTEGER NOT NULL,
            sku TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL
        )
    """,
    "users": """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone_number TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            country TEXT,
            signup_date TEXT NOT NULL,
            is_active INTEGER NOT NULL CHECK (is_active IN (0, 1))
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            ship_date TEXT,
            delivery_date TEXT,
            status TEXT NOT NULL,
            shipping_method TEXT NOT NULL,
            shipping_cost REAL NOT NULL,
            payment_method TEXT NOT NULL,
            subtotal REAL NOT NULL,
            total REAL NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        )
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            unit_price REAL NOT NULL,
            discount REAL NOT NULL CHECK (discount >= 0),
            FOREIGN KEY (order_id) REFERENCES orders (order_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products (product_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT
        )
    """,
    "reviews": """
        CREATE TABLE IF NOT EXISTS reviews (
            review_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
            title TEXT NOT NULL,
            review_text TEXT,
            review_date TEXT NOT NULL,
            verified_purchase INTEGER NOT NULL CHECK (verified_purchase IN (0, 1)),
            FOREIGN KEY (user_id) REFERENCES users (user_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products (product_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        )
    """,
}

INDEX_DEFINITIONS: Dict[str, Iterable[str]] = {
    "orders": ("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)",),
    "order_items": (
        "CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id)",
    ),
    "reviews": (
        "CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON reviews(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id)",
    ),
}

BOOLEAN_COLUMNS: Dict[str, Iterable[str]] = {
    "users": ("is_active",),
    "reviews": ("verified_purchase",),
}

LOAD_ORDER: Tuple[str, ...] = ("products", "users", "orders", "order_items", "reviews")


def resolve_path(path_str: str) -> Path:
    path = Path(path_str).expanduser().resolve()
    return path


def safe_read_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    return pd.read_csv(csv_path)


def normalize_boolean_columns(df: pd.DataFrame, boolean_cols: Iterable[str]) -> pd.DataFrame:
    normalized = df.copy()
    for column in boolean_cols:
        if column in normalized.columns:
            normalized[column] = normalized[column].astype(int)
    return normalized


def create_tables(connection: sqlite3.Connection) -> None:
    with connection:
        for ddl in SCHEMA_DEFINITIONS.values():
            connection.execute(ddl)


def create_indexes(connection: sqlite3.Connection) -> None:
    with connection:
        for ddl_list in INDEX_DEFINITIONS.values():
            for ddl in ddl_list:
                connection.execute(ddl)


def insert_dataframe(connection: sqlite3.Connection, table: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    columns = frame.columns.tolist()
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(columns)
    sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
    records = list(frame.itertuples(index=False, name=None))
    with connection:
        connection.executemany(sql, records)


def enforce_foreign_keys(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys = ON;")


def ingest(database: Path, csv_dir: Path) -> None:
    csv_dir = csv_dir.resolve()
    database.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(database) as conn:
        enforce_foreign_keys(conn)
        try:
            create_tables(conn)
            for table in LOAD_ORDER:
                csv_path = csv_dir / CSV_FILES[table]
                frame = safe_read_csv(csv_path)
                frame = normalize_boolean_columns(frame, BOOLEAN_COLUMNS.get(table, ()))
                insert_dataframe(conn, table, frame)
            create_indexes(conn)
        except Exception:
            conn.rollback()
            raise


def main() -> None:
    database_path = resolve_path(DB_NAME)
    csv_directory = resolve_path(".")
    ingest(database_path, csv_directory)


if __name__ == "__main__":
    main()

