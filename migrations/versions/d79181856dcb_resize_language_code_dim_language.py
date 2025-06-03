"""resize_language_code_dim_language

Revision ID: d79181856dcb
Revises: 3f31a8b3b20e
Create Date: 2025-04-28 10:31:07.786658

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd79181856dcb'
down_revision: Union[str, None] = '3f31a8b3b20e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
