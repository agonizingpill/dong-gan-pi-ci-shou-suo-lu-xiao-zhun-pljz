"""Install the initial laboratory schema and upgrade earlier databases in place."""
from sqlalchemy import inspect, text

from src import models
from src.db import engine

# Columns and enum variants introduced after the first snapshot. ``create_all`` only
# creates missing tables, so databases seeded before reference revocation existed need
# these applied explicitly.
COLUMNS = [
    ('calibration_releases', 'affected_reference', 'VARCHAR(160) NULL'),
    ('calibration_releases', 'affected_reason', 'VARCHAR(400) NULL'),
    ('calibration_releases', 'affected_at', 'DATETIME NULL'),
    ('shrinkage_samples', 'superseded', 'TINYINT(1) NOT NULL DEFAULT 0'),
]
ENUMS = [
    ('calibration_releases', 'state',
     "ENUM('queued','running','failed','awaiting_verification','retest_required',"
     "'verified','held','released','suspended') NOT NULL"),
    ('dry_batches', 'state',
     "ENUM('collecting','reference','calibrating','review','released','revoked') NOT NULL"),
]


def upgrade():
    inspector = inspect(engine)
    with engine.begin() as connection:
        for table, column, definition in COLUMNS:
            existing = {item['name'] for item in inspector.get_columns(table)}
            if column not in existing:
                connection.execute(text(f'ALTER TABLE {table} ADD COLUMN {column} {definition}'))
        for table, column, definition in ENUMS:
            current = connection.scalar(text(
                'SELECT COLUMN_TYPE FROM information_schema.COLUMNS '
                'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table AND COLUMN_NAME = :column'
            ), {'table': table, 'column': column}) or ''
            missing = [variant for variant in definition.replace('ENUM(', '').split(')')[0].split(',')
                       if variant.strip("' ") and variant not in current]
            if missing:
                connection.execute(text(f'ALTER TABLE {table} MODIFY COLUMN {column} {definition}'))


if __name__ == '__main__':
    models.Base.metadata.create_all(engine)
    upgrade()
