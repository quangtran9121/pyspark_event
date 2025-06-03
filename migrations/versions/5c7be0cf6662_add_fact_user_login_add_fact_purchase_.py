"""add fact_user_login add fact_purchase_fail_report

Revision ID: 5c7be0cf6662
Revises: d8be6ae453f2
Create Date: 2025-04-29 07:38:16.854736

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5c7be0cf6662'
down_revision: Union[str, None] = 'd8be6ae453f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
