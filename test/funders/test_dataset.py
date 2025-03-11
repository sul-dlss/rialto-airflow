from rialto_airflow.funders.dataset import is_federal, is_federal_grid_id


def test_is_federal():
    assert is_federal("Defense Advanced Research Projects Agency (DARPA)") is True


def test_is_not_federal():
    assert is_federal("Stanford University") is False


def test_is_federal_no_acronym():
    assert is_federal("Defense Advanced Research Projects Agency") is True


def test_is_federal_case():
    assert is_federal("DEFENSE AdvANCED RESEARCH ProjeCTS Agency") is True


def test_is_federal_grid_id():
    assert is_federal_grid_id("grid.239134.e") is True


def test_is_not_federal_grid_id():
    assert is_federal_grid_id("12XU") is False
