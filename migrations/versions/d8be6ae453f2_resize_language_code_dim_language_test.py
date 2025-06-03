"""resize_language_code_dim_language_test

Revision ID: d8be6ae453f2
Revises: 4ac869d0c1ed
Create Date: 2025-04-28 10:42:43.954374

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd8be6ae453f2'
down_revision: Union[str, None] = '4ac869d0c1ed'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
