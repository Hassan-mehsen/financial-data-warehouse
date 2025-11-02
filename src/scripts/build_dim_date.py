"""
generate_dim_date.py
---------------------

Generate a complete date dimension (dim_date) CSV file to be used as a dbt seed.

This script builds a continuous calendar table, computes date-related attributes
(year, month, quarter, weekday, weekend flag), and exports the result to the
`dbt_financial_market/seeds` directory for loading via `dbt seed`.

Output
-------
- File: `dbt_financial_market/seeds/dim_date.csv`
- Columns:
    - date_key (int, surrogate key in YYYYMMDD format)
    - date (DATE)
    - month (int)
    - year (int)
    - fiscal_quarter (int, 1..4)
    - day_of_week (int, 1=Monday .. 7=Sunday)
    - is_weekend (bool)

"""

from datetime import datetime
from pathlib import Path
import pandas as pd
import argparse


def build_dim_date(start_date, end_date) -> pd.DataFrame:
    """Generate a full date dimension DataFrame."""

    # generate dates for a specific range
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    # create a dataframe, with extending dates in rows
    df = pd.DataFrame({"date": dates})

    # derived columns form date
    df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["day_of_week"] = df["date"].dt.day_of_week + 1
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["fiscal_quarter"] = df["date"].dt.quarter
    df["is_weekend"] = df["day_of_week"] > 5
    df["date"] = df["date"].dt.date

    # reorder the df
    df = df[["date_key", "date", "month", "year", "fiscal_quarter", "day_of_week", "is_weekend"]]

    # print(df)
    return df


def export_csv(df: pd.DataFrame, filename: str) -> None:
    """Export dataframe to seeds directory"""

    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "dbt_financial_market" / "seeds"
    output_path = data_dir / filename

    df.to_csv(output_path, index=False)
    print(f"{filename} generated at: {output_path}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    if end_date < start_date:
        raise ValueError("end_date must be greater than start_date")

    df = build_dim_date(start_date=start_date, end_date=end_date)
    export_csv(df, "dim_date.csv")
