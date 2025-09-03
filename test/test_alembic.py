import dotenv

from pytest_alembic.tests import (  # noqa:F401 importing these will get them collected and run with the rest of our test suite
    test_model_definitions_match_ddl,
    test_single_head_revision,
    test_up_down_consistency,
    test_upgrade,
)

from alembic.script import ScriptDirectory

dotenv.load_dotenv()


def test_only_single_head_revision_in_migrations():
    # Based on https://blog.jerrycodes.com/multiple-heads-in-alembic-migrations/
    script = ScriptDirectory(dir="alembic")

    # This will raise if there are multiple heads
    script.get_current_head()
