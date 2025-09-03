from alembic.script import ScriptDirectory


def test_only_single_head_revision_in_migrations():
    # Based on https://blog.jerrycodes.com/multiple-heads-in-alembic-migrations/
    script = ScriptDirectory(dir="alembic")

    # This will raise if there are multiple heads
    script.get_current_head()
