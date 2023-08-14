"""create package_sharing_policy table

Revision ID: 8c3d7f7ae37d
Revises: 
Create Date: 2023-08-07 00:58:07.680039

"""
from alembic import op
import sqlalchemy as sa
from ckan import model
from ckan.model import types

# revision identifiers, used by Alembic.
revision = '8c3d7f7ae37d'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'package_sharing_policy',
        sa.Column(
            'package_id',
            sa.UnicodeText,
            sa.ForeignKey(model.package_table.columns['id']),
            primary_key=True,
            default=types.make_uuid,
        ),
        sa.Column('allowed', sa.Boolean),
        sa.Column('handle', sa.UnicodeText, nullable=True),
    )


def downgrade():
    op.drop_table('package_sharing_policy')
