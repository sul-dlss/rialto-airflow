from rialto_airflow.funders.ror_grid_dataset import (
    convert_grid_to_ror,
    convert_ror_to_grid,
)


def test_grid_to_ror():
    assert convert_grid_to_ror("grid.168010.e") == "https://ror.org/00f54p054"


def test_ror_to_grid():
    assert convert_ror_to_grid("https://ror.org/00f54p054") == "grid.168010.e"


def test_grid_to_ror_not_found():
    assert convert_grid_to_ror("00000") is None


def test_ror_to_grid_no_found():
    assert convert_ror_to_grid("abcdef") is None
