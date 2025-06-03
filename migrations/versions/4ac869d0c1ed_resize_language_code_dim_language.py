"""resize_language_code_dim_language

Revision ID: 4ac869d0c1ed
Revises: d79181856dcb
Create Date: 2025-04-28 10:41:40.561101

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4ac869d0c1ed'
down_revision: Union[str, None] = 'd79181856dcb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
