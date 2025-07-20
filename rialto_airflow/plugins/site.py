from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, send_from_directory
from flask_appbuilder import expose, BaseView

site_dir = Path("data/website").absolute()


class SiteView(BaseView):
    @expose("/")
    def my_files(self):
        return send_from_directory(site_dir, "index.html")


view = SiteView()

bp = Blueprint(
    "site_plugin",
    __name__,
    static_folder=site_dir,
    static_url_path="/static/site/",
)


class AirflowSitePlugin(AirflowPlugin):
    name = "site_plugin"
    admin_views = [view]
    flask_blueprints = [bp]
    admin_views = [{"name": "Site View", "category": "Site Plugin", "view": view}]
    appbuilder_menu_items = [
        {
            "name": "Site",
            "href": "/static/site/index.html",
        }
    ]
