"""replace package sharing policy handle with name

Revision ID: 5645daacca80
Revises: 8c3d7f7ae37d
Create Date: 2023-08-18 12:09:22.590777

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5645daacca80'
down_revision = '8c3d7f7ae37d'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('package_sharing_policy') as batch:
        batch.drop_column('handle')
        batch.add_column(sa.Column('handle', sa.UnicodeText, nullable=True))
        batch.drop_constraint('package_sharing_policy_package_id_fkey', type_='foreignkey')
        batch.create_foreign_key('package_sharing_policy_package_id_fkey', 'package', ondelete='CASCADE')


def downgrade():
    pass
