# # An example DAG showing how to interact with Google Sheets and Google Drive using Airflow.
# # Pull these examples into your own DAGs as needed

# import datetime

# from airflow.decorators import dag, task
# from airflow.models import Variable

# from rialto_airflow.google import (
#     clear_google_sheet,
#     append_rows_to_google_sheet,
#     replace_file_in_google_drive,
#     upload_file_to_google_drive,
# )

# gcp_conn_id = Variable.get("google_connection")
# google_drive_id = Variable.get("google_drive_id")
# google_sheet_id = Variable.get(
#     "orcid_stats_sheet_id", default_var="1smq7H5wuGBzTMsO3sAGyPd-Uz8BBlECX1xGd_0pwTrY"
# )  # setup in Airflow Admin -> Variables OR via vault/puppet environment variables


# @dag(
#     # schedule=@weekly,
#     start_date=datetime.datetime(2024, 1, 1),
#     catchup=False,
# )
# def google():
#     @task()
#     def replace_in_google():
#         replace_file_in_google_drive(
#             "/opt/airflow/rialto_airflow/dags/google.py",
#             "1k-y2iAlf57UOnmR3rRnHzqx-T4owff1N",
#         )

#     @task()
#     def upload_to_google():
#         upload_file_to_google_drive(
#             "/opt/airflow/rialto_airflow/dags/harvest.py",
#             google_drive_id,
#         )

#     @task()
#     def clear_sheet():
#         clear_google_sheet(google_sheet_id)

#     @task()
#     def append_to_sheet():
#         append_rows_to_google_sheet(
#             google_sheet_id,
#             [
#                 [
#                     "Hello",
#                     "World",
#                     datetime.date.today().isoformat(),
#                     datetime.datetime.now().isoformat(),
#                 ]
#             ],
#         )

#     replace_in_google()
#     upload_to_google()
#     clear_sheet() >> append_to_sheet()


# google()
