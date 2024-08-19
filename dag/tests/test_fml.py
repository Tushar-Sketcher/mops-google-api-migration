import pytest


@pytest.fixture
def airflow_env():
    from airflow.utils import db
    from airflow.models import Variable
    db.initdb()
    Variable.set('env', 'stage')


def test_import_dag(airflow_env):
    1 == 1
