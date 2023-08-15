"""create product tables

Revision ID: 42b5a33459ef
Revises: 
Create Date: 2023-08-15 13:20:25.273630

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "42b5a33459ef"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "temp",
        sa.Column("id", sa.Integer, nullable=False, primary_key=True),
        sa.Column("iso2_code", sa.String(length=2), nullable=False),
        sa.Column("adm0_name", sa.String(length=128)),
        sa.Column("adm1_name", sa.String(length=128)),
        sa.Column("adm2_name", sa.String(length=128)),
        sa.Column("adm1_id", sa.String(length=128)),
        sa.Column("adm2_id", sa.String(length=128)),
        sa.Column("product", sa.String(), nullable=False),
        sa.Column("scenario", sa.String(), nullable=False),
        sa.Column("month", sa.String(), nullable=False),
        sa.Column("mean_raw", sa.Float()),
        sa.Column("min_raw", sa.Float()),
        sa.Column("max_raw", sa.Float()),
        sa.Column("uploaded_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
        schema="public",
    )

    op.create_table(
        "tmin",
        sa.Column("id", sa.Integer, nullable=False, primary_key=True),
        sa.Column("iso2_code", sa.String(length=2), nullable=False),
        sa.Column("adm0_name", sa.String(length=128)),
        sa.Column("adm1_name", sa.String(length=128)),
        sa.Column("adm2_name", sa.String(length=128)),
        sa.Column("adm1_id", sa.String(length=128)),
        sa.Column("adm2_id", sa.String(length=128)),
        sa.Column("product", sa.String(), nullable=False),
        sa.Column("scenario", sa.String(), nullable=False),
        sa.Column("month", sa.String(), nullable=False),
        sa.Column("mean_raw", sa.Float()),
        sa.Column("min_raw", sa.Float()),
        sa.Column("max_raw", sa.Float()),
        sa.Column("uploaded_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
        schema="public",
    )

    op.create_table(
        "tmax",
        sa.Column("id", sa.Integer, nullable=False, primary_key=True),
        sa.Column("iso2_code", sa.String(length=2), nullable=False),
        sa.Column("adm0_name", sa.String(length=128)),
        sa.Column("adm1_name", sa.String(length=128)),
        sa.Column("adm2_name", sa.String(length=128)),
        sa.Column("adm1_id", sa.String(length=128)),
        sa.Column("adm2_id", sa.String(length=128)),
        sa.Column("product", sa.String(), nullable=False),
        sa.Column("scenario", sa.String(), nullable=False),
        sa.Column("month", sa.String(), nullable=False),
        sa.Column("mean_raw", sa.Float()),
        sa.Column("min_raw", sa.Float()),
        sa.Column("max_raw", sa.Float()),
        sa.Column("uploaded_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
        schema="public",
    )

    op.create_table(
        "prec",
        sa.Column("id", sa.Integer, nullable=False, primary_key=True),
        sa.Column("iso2_code", sa.String(length=2), nullable=False),
        sa.Column("adm0_name", sa.String(length=128)),
        sa.Column("adm1_name", sa.String(length=128)),
        sa.Column("adm2_name", sa.String(length=128)),
        sa.Column("adm1_id", sa.String(length=128)),
        sa.Column("adm2_id", sa.String(length=128)),
        sa.Column("product", sa.String(), nullable=False),
        sa.Column("scenario", sa.String(), nullable=False),
        sa.Column("month", sa.String(), nullable=False),
        sa.Column("mean_raw", sa.Float()),
        sa.Column("min_raw", sa.Float()),
        sa.Column("max_raw", sa.Float()),
        sa.Column("uploaded_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
        schema="public",
    )

    op.create_table(
        "bio",
        sa.Column("id", sa.Integer, nullable=False, primary_key=True),
        sa.Column("iso2_code", sa.String(length=2), nullable=False),
        sa.Column("adm0_name", sa.String(length=128)),
        sa.Column("adm1_name", sa.String(length=128)),
        sa.Column("adm2_name", sa.String(length=128)),
        sa.Column("adm1_id", sa.String(length=128)),
        sa.Column("adm2_id", sa.String(length=128)),
        sa.Column("product", sa.String(), nullable=False),
        sa.Column("scenario", sa.String(), nullable=False),
        sa.Column("month", sa.String(), nullable=False),
        sa.Column("mean_raw", sa.Float()),
        sa.Column("min_raw", sa.Float()),
        sa.Column("max_raw", sa.Float()),
        sa.Column("uploaded_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
        schema="public",
    )


def downgrade() -> None:
    pass
