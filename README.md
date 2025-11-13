# E-commerce Data Pipeline Assignment

## Project Summary
This assignment demonstrates a complete mini data pipeline implemented with Cursor AI. It showcases the generation of a realistic synthetic e-commerce dataset, automated ingestion into a relational SQLite database, execution-ready SQL join queries, and a GitHub workflow suitable for rapid iteration and review.

## Tech Stack
- Python
- pandas
- Faker
- SQLite
- Cursor IDE
- Git
- GitHub

## Project Structure
- `generate_synthetic_data.py` – creates five synthetic e-commerce CSV files (products, users, orders, order items, reviews)
- `ingest_sqlite.py` – ingests the CSV files into a SQLite database named `ecom.db`
- `queries.sql` – contains optimized SQL join queries for reporting and analysis
- `*.csv` – generated synthetic datasets for each e-commerce entity
- `ecom.db` – finalized SQLite database ready for analytics workloads

## How to Run
1. **Install dependencies** – use the installation command below.
2. **Generate data** – run `python generate_synthetic_data.py` to create the CSV files.
3. **Ingest database** – run `python ingest_sqlite.py` to load the CSVs into `ecom.db`.
4. **Query data** – execute the statements in `queries.sql` against `ecom.db` using your preferred SQLite client.

## Features
- Realistic synthetic data wired with proper foreign key relationships.
- Automated SQLite schema creation with referential integrity.
- Multi-table join queries designed for common analytics use cases.
- Clean, modular, and production-quality Python code.

## Installation Commands
```bash
pip install pandas faker
```

## Outputs
- Generated CSV files: `products.csv`, `users.csv`, `orders.csv`, `order_items.csv`, `reviews.csv`
- SQLite database file: `ecom.db`
- Database tables: `products`, `users`, `orders`, `order_items`, `reviews`
- SQL query collection: `queries.sql` ready for execution