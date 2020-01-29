import json
import pathlib
import posixpath
import airflow
import requests
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, \
    DataProcPySparkOperator

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from hooks.LaunchHook import LaunchHook
from operators.LaunchToGcsOperator import LaunchToGcsOperator
from operators.http_to_gcs_operator import HttpToGcsOperator

args = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10)
}

dag = DAG(
    dag_id="use_case",
    default_args=args,
    description="Running ETL for the use case",
    schedule_interval="0 0 * * *"
)

# def _connect(**context):
#     return LaunchHook.get_conn()
#
#
# def _download_rocket_launches(ds, tomorrow_ds, **context):
#     query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
#     result_path = f"/tmp/rocket_launches/ds={ds}"
#     pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
#     response = requests.get(query)
#     print(f"response was {response}")
#
#     with open(posixpath.join(result_path, "launches.json"), "w") as f:
#         print(f"Writing to file {f.name}")
#         f.write(response.text)
#
#
# query = "?startdate={ds}&enddate={tomorrow_ds}"

fetch_exchange_rates = HttpToGcsOperator(task_id="fetch_exchange_rates",
                                         http_conn_id="exchange_rates",
                                         gcs_bucket="europe-west1-training-airfl-a98394bc-bucket",
                                         gcs_path="data/use_case_ivan/exchange_rates.json",
                                         endpoint="history?start_at=2018-01-01&end_at=2018-01-04&symbols=EUR&base=GBP",
                                         dag=dag)

create_dataproc_cluster = DataprocClusterCreateOperator(task_id="create_dataproc_cluster",
                                                        num_workers=2,
                                                        cluster_name="my-dataproc-cluster",
                                                        project_id="airflowbolcom-jan2829-b51a8ad2",
                                                        region='europe-west4',
                                                        dag=dag)

arguments = [
    'gs://europe-west1-training-airfl-a98394bc-bucket/data/ivan_postgress_*.json', #input_properties
    'gs://europe-west1-training-airfl-a98394bc-bucket/data/use_case_ivan/exchange_rates.json', #input_currencies
    'gs://europe-west1-training-airfl-a98394bc-bucket/use_case_output', #target_path
    'EUR', #target_currency
    '{{ ds }}',#target_date
]

run_spark = DataProcPySparkOperator(task_id="run_spark",
                                    main="gs://europe-west1-training-airfl-a98394bc-bucket/build_statistics.py",
                                    cluster_name="my-dataproc-cluster",
                                    region='europe-west4',
                                    arguments=arguments,
                                    dag=dag)
# delete_dataproc_cluster = DataprocClusterDeleteOperator(task_id="delete_dataproc_cluster",
#                                                         cluster_name="my-dataproc-cluster",
#                                                         project_id="airflowbolcom-jan2829-b51a8ad2",
#                                                         region='europe-west4',
#                                                         dag=dag)
# fetch_exchange_rates
# fetch_exchange_rates >> create_dataproc_cluster
# fetch_exchange_rates >> create_dataproc_cluster >> run_spark >> delete_dataproc_cluster
fetch_exchange_rates >> create_dataproc_cluster >> run_spark
# write_response_to_gcs = LaunchToGcsOperator(task_id="write_response_to_gcs",
#                                             python_callable=_connect,
#                                             provide_context=True,
#                                             query=query,
#                                             ds="{{ds}}",
#                                             ds_tomorrow="{{ds_tomorrow}}",
#                                             postgres_conn_id="gdd_connection",
#                                             export_format="json",
#                                             bucket='europe-west1-training-airfl-a98394bc-bucket',
#                                             filename='data/ivan_postgress_{{execution_date}}.json',
#                                             dag=dag)


# def _print_stats(ds, **context):
#     with open(f"/tmp/rocket_launches/ds={ds}/launches.json") as f:
#         data = json.load(f)
#         rockets_launched = [launch["name"] for launch in data["launches"]]
#         rockets_str = ""
#
#         if rockets_launched:
#             rockets_str = f" ({' & '.join(rockets_launched)})"
#             print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")
#         else:
#             print(f"No rockets found in {f.name}")
#
#
# download_rocket_launches = PythonOperator(
#     task_id="download_rocket_launches",
#     python_callable=_download_rocket_launches,
#     provide_context=True,
#     dag=dag
# )
#
# print_stats = PythonOperator(
#     task_id="print_stats",
#     python_callable=_print_stats,
#     provide_context=True,
#     dag=dag
# )
#
# download_rocket_launches >> print_stats
