from rialto_airflow import website


def test_build(snapshot, test_session):
    site_dir = website.build(snapshot)
    assert site_dir
    assert (site_dir / "index.html").is_file()
    assert (site_dir / "data" / "openaccess.json").is_file()
    assert (site_dir / "data" / "publications.json").is_file()
