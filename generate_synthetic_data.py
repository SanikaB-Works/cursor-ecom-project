from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
from faker import Faker


def seeded_faker(seed: int) -> Faker:
    fake = Faker()
    Faker.seed(seed)
    return fake


def generate_counts(rng: random.Random) -> Tuple[int, int, int, int, int]:
    num_products = rng.randint(60, 100)
    num_users = rng.randint(70, 100)
    num_orders = rng.randint(55, 90)
    num_reviews = rng.randint(50, 80)
    return num_products, num_users, num_orders, num_reviews, num_orders * 4


def generate_products(n: int, rng: np.random.Generator, fake: Faker) -> pd.DataFrame:
    categories = [
        "Electronics",
        "Home & Kitchen",
        "Beauty",
        "Sports",
        "Books",
        "Toys",
        "Health",
        "Clothing",
    ]
    brands = [
        "Acme",
        "Globex",
        "Innova",
        "Zenith",
        "Nimbus",
        "Vertex",
        "Pulse",
        "Voyage",
    ]
    records = []
    for product_id in range(1, n + 1):
        category = rng.choice(categories)
        base_price = rng.uniform(10, 400)
        price = round(base_price + rng.uniform(-5, 25), 2)
        records.append(
            {
                "product_id": product_id,
                "name": f"{rng.choice(brands)} {fake.word().title()}",
                "category": category,
                "brand": rng.choice(brands),
                "price": price,
                "cost": round(price * rng.uniform(0.4, 0.7), 2),
                "inventory": rng.integers(25, 500),
                "sku": f"SKU-{product_id:05d}",
                "created_at": fake.date_time_between(start_date="-2y", end_date="-1y"),
            }
        )
    return pd.DataFrame(records)


def generate_users(n: int, rng: np.random.Generator, fake: Faker) -> pd.DataFrame:
    records = []
    for user_id in range(1, n + 1):
        profile = fake.simple_profile()
        records.append(
            {
                "user_id": user_id,
                "first_name": profile["name"].split()[0],
                "last_name": profile["name"].split()[-1],
                "email": fake.unique.email(),
                "phone_number": fake.phone_number(),
                "address": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "postal_code": fake.postcode(),
                "country": "USA",
                "signup_date": fake.date_time_between(start_date="-2y", end_date="now"),
                "is_active": rng.choice([True, True, True, False]),
            }
        )
    return pd.DataFrame(records)


def generate_orders(
    n: int,
    users: pd.DataFrame,
    rng: np.random.Generator,
    fake: Faker,
) -> pd.DataFrame:
    statuses = ["Processing", "Shipped", "Delivered", "Cancelled", "Returned"]
    records = []
    for order_id in range(1, n + 1):
        user_id = int(rng.choice(users["user_id"]))
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        ship_date = order_date + timedelta(days=int(rng.integers(1, 10)))
        delivery_date = ship_date + timedelta(days=int(rng.integers(1, 7)))
        status = rng.choice(statuses, p=[0.25, 0.25, 0.35, 0.05, 0.1])
        if status in {"Cancelled", "Returned"}:
            delivery_date = None
        records.append(
            {
                "order_id": order_id,
                "user_id": user_id,
                "order_date": order_date,
                "ship_date": ship_date if status != "Cancelled" else None,
                "delivery_date": delivery_date,
                "status": status,
                "shipping_method": rng.choice(
                    ["Standard", "Expedited", "Two-Day", "Overnight"], p=[0.5, 0.2, 0.2, 0.1]
                ),
                "shipping_cost": round(float(rng.uniform(0, 25)), 2),
                "payment_method": rng.choice(["Credit Card", "PayPal", "Gift Card", "Apple Pay"]),
                "subtotal": 0.0,
                "total": 0.0,
            }
        )
    return pd.DataFrame(records)


def generate_order_items(
    orders: pd.DataFrame,
    products: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    records = []
    item_id = 1
    product_lookup = products.set_index("product_id")
    for _, order in orders.iterrows():
        num_items = int(rng.integers(1, 6))
        product_ids = rng.choice(
            products["product_id"],
            size=num_items,
            replace=False if len(products) >= num_items else True,
        )
        for product_id in product_ids:
            product = product_lookup.loc[product_id]
            quantity = int(rng.integers(1, 5))
            unit_price = round(float(product["price"]) * rng.uniform(0.95, 1.05), 2)
            records.append(
                {
                    "order_item_id": item_id,
                    "order_id": int(order["order_id"]),
                    "product_id": int(product_id),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount": round(unit_price * quantity * rng.uniform(0, 0.15), 2),
                }
            )
            item_id += 1
    return pd.DataFrame(records)


def reconcile_order_totals(orders: pd.DataFrame, order_items: pd.DataFrame) -> pd.DataFrame:
    item_totals = (
        order_items.assign(line_total=lambda df: (df["unit_price"] * df["quantity"]) - df["discount"])
        .groupby("order_id")["line_total"]
        .sum()
        .round(2)
    )
    orders = orders.copy()
    orders["subtotal"] = orders["order_id"].map(item_totals).fillna(0)
    orders["total"] = (orders["subtotal"] + orders["shipping_cost"]).round(2)
    return orders


def generate_reviews(
    order_items: pd.DataFrame,
    orders: pd.DataFrame,
    rng: np.random.Generator,
    fake: Faker,
    target_count: int,
) -> pd.DataFrame:
    merged = order_items.merge(orders[["order_id", "user_id", "order_date"]], on="order_id")
    if len(merged) < target_count:
        sample = merged
    else:
        sample = merged.sample(target_count, random_state=rng.integers(0, 1_000_000))
    records = []
    for review_id, (_, row) in enumerate(sample.iterrows(), start=1):
        review_date = row["order_date"] + timedelta(days=int(rng.integers(3, 30)))
        records.append(
            {
                "review_id": review_id,
                "user_id": int(row["user_id"]),
                "product_id": int(row["product_id"]),
                "rating": int(rng.integers(1, 6)),
                "title": fake.sentence(nb_words=6),
                "review_text": fake.paragraph(nb_sentences=3),
                "review_date": review_date,
                "verified_purchase": True,
            }
        )
    return pd.DataFrame(records)


def save_dataframe(df: pd.DataFrame, filename: str) -> None:
    df.to_csv(Path(filename), index=False)


def main() -> None:
    seed = 42
    random.seed(seed)
    np.random.seed(seed)
    fake = seeded_faker(seed)
    python_rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    num_products, num_users, num_orders, num_reviews, _ = generate_counts(python_rng)

    products = generate_products(num_products, np_rng, fake)
    users = generate_users(num_users, np_rng, fake)
    orders = generate_orders(num_orders, users, np_rng, fake)
    order_items = generate_order_items(orders, products, np_rng)
    orders = reconcile_order_totals(orders, order_items)
    reviews = generate_reviews(order_items, orders, np_rng, fake, num_reviews)

    save_dataframe(products, "products.csv")
    save_dataframe(users, "users.csv")
    save_dataframe(orders, "orders.csv")
    save_dataframe(order_items, "order_items.csv")
    save_dataframe(reviews, "reviews.csv")


if __name__ == "__main__":
    main()