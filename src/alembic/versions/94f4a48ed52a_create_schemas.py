"""create schemas

Revision ID: 94f4a48ed52a
Revises: 
Create Date: 2025-10-18 17:51:26.664192

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '94f4a48ed52a'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    op.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    op.execute("CREATE SCHEMA IF NOT EXISTS marts;")


def downgrade() -> None:
    pass
