"""
marts.company_id_map table:
--------------------------

This table provides a persistent mapping between each unique company symbol
and its auto-incremented company_id.

- The table structure is created and versioned by Alembic to guarantee
  the auto-incremental primary key behavior.

- The incremental insertion logic is controlled by dbt through a model
  configured with:
      materialized = 'incremental'
      on_schema_change = 'ignore'
      full_refresh = False
  ensuring that the table remains stable and persistent over time.

- Each company symbol is unique, and a UTC timestamp is automatically
  generated upon insertion for audit and traceability purposes.

Usage:
    The integer-based company_id is used as a surrogate key across marts
    dimension and fact tables. Integer joins are significantly faster and
    more efficient than string-based joins (on symbol), reducing query
    execution time and improving overall BI performance.
"""

from sqlalchemy import Table, Column, Integer, String, TIMESTAMP, text, MetaData

metadata_marts = MetaData(schema="marts")

company_id_map = Table(
    "company_id_map",
    metadata_marts,
    Column("company_id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String(50), unique=True, nullable=False),
    Column(
        "created_at",
        TIMESTAMP(timezone=False),
        server_default=text("timezone('UTC', now())"),
    ),
)
