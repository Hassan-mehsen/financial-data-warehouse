"""
This module defines all RAW tables of the financial warehouse.
Each table stores the full JSON list returned by an FMP API call.
Data is inserted as-is, with UTC ingestion timestamps handled by PostgreSQL.
"""

from sqlalchemy import Table, Column, Integer, text, JSON, TIMESTAMP, MetaData

metadata_raw = MetaData(schema="raw")


def create_raw_table(name: str) -> Table:
    """Create a raw table to store full JSON list responses from API call"""
    return Table(
        name,
        metadata_raw,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("json_list", JSON, nullable=False),
        Column(
            "ingestion_ts",
            TIMESTAMP(timezone=False),
            server_default=text("timezone('UTC', now())"),
        ),
    )


# Tables defenition
company_profile = create_raw_table(name="company_profile")
all_shares_float = create_raw_table(name="shares_float")
stock_batch_quotes = create_raw_table(name="stock_quotes")
stock_price_changes = create_raw_table(name="price_changes")

income_stmt = create_raw_table(name="income_stmt")
balance_stmt = create_raw_table(name="balance_sheet_stmt")
cash_stmt = create_raw_table(name="cash_flow_stmt")

dividends = create_raw_table(name="company_dividends")
earnings = create_raw_table(name="earnings_reports")
splits = create_raw_table(name="stock_splits")
