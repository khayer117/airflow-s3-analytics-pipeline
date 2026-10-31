"""
Generate synthetic website log CSV files for pipeline testing.

Usage:
    python generate_sample_data.py --files 5 --rows 1000
    python generate_sample_data.py --files 3 --rows 500 --start-date 2024-03-01 --users 200
"""

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# Weighted country pool: dominant countries appear more often
COUNTRIES = [
    ("US", 20), ("IN", 15), ("GB", 10), ("DE", 7), ("BR", 7),
    ("CA", 5),  ("FR", 5),  ("AU", 4),  ("JP", 4),  ("MX", 4),
    ("ID", 3),  ("PH", 3),  ("TR", 3),  ("NG", 2),  ("PK", 2),
    ("AR", 2),  ("ZA", 2),  ("KR", 1),  ("IT", 1),  ("ES", 1),
]

COUNTRY_POOL = [code for code, weight in COUNTRIES for _ in range(weight)]


def random_timestamp(day: date) -> str:
    """Return a random ISO-8601 timestamp within the given day."""
    seconds = random.randint(0, 86399)
    dt = datetime(day.year, day.month, day.day) + timedelta(seconds=seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_file(path: Path, day: date, rows: int, user_pool_size: int) -> None:
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "timestamp", "country"])
        for _ in range(rows):
            writer.writerow([
                random.randint(1, user_pool_size),
                random_timestamp(day),
                random.choice(COUNTRY_POOL),
            ])


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic website log CSV files.")
    parser.add_argument("--files", type=int, default=2, help="Number of files to generate (default: 2)")
    parser.add_argument("--rows", type=int, default=1000, help="Rows per file (default: 1000)")
    parser.add_argument("--start-date", default="2024-01-01", help="Start date YYYY-MM-DD (default: 2024-01-01)")
    parser.add_argument("--users", type=int, default=500, help="User pool size — controls DAU density (default: 500)")
    parser.add_argument("--output-dir", default="sample_data", help="Output directory (default: sample_data)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    out = Path(args.output_dir)
    out.mkdir(exist_ok=True)

    for i in range(args.files):
        day = start + timedelta(days=i)
        filename = out / f"website_logs_{day}.csv"
        generate_file(filename, day, args.rows, args.users)
        print(f"  created {filename}  ({args.rows} rows, date={day})")

    print(f"\nDone — {args.files} file(s) in '{out}/'")


if __name__ == "__main__":
    main()
