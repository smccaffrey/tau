"""Alembic environment.

Tau runs migrations itself at startup (``tau.core.database.create_tables``), passing an
open connection in ``config.attributes["connection"]``. To use the alembic CLI directly,
set ``sqlalchemy.url`` to a *sync* URL, e.g. ``sqlite:///tau.db``.
"""

from alembic import context
from sqlalchemy import engine_from_config, pool

import tau.models  # noqa: F401  (register every table on Base.metadata)
from tau.core.database import Base

config = context.config
target_metadata = Base.metadata


def _run(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=True,  # SQLite cannot ALTER most things; batch mode rebuilds tables
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


connection = config.attributes.get("connection")
if connection is not None:
    _run(connection)
else:
    engine = engine_from_config(
        config.get_section(config.config_ini_section) or {},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with engine.connect() as conn:
        _run(conn)
