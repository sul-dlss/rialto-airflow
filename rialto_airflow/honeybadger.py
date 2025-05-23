from honeybadger import honeybadger  # type: ignore
from airflow.models import Variable
import logging

honeybadger.configure(
    api_key=Variable.get("honeybadger_api_key"),
    environment=Variable.get("honeybadger_env"),
    force_sync=True,
)  # type: ignore


def task_failure_notify(context):
    task = context["task"].task_id
    logging.error(f"Task {task} failed.")
    honeybadger.notify(
        error_class="Task failure",
        error_message=f"Task {task} failed in {context.get('task_instance_key_str')}",
        context=context,
    )


def default_args():
    return {
        "email": [Variable.get("email_address_for_errors")],
        "on_failure_callback": task_failure_notify,
        "email_on_failure": True,
        "email_on_retry": False,
    }
