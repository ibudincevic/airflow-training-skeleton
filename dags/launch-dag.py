import json
import pathlib
import posixpath
import airflow
import requests

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from hooks.LaunchHook import LaunchHook
from operators.LaunchToGcsOperator import LaunchToGcsOperator

args = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10)
}

dag = DAG(
    dag_id="download_rocket_launches",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *"
)


def _connect(**context):
    return LaunchHook.get_conn()


def _download_rocket_launches(ds, tomorrow_ds, **context):
    query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
    result_path = f"/tmp/rocket_launches/ds={ds}"
    pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
    response = requests.get(query)
    print(f"response was {response}")

    with open(posixpath.join(result_path, "launches.json"), "w") as f:
        print(f"Writing to file {f.name}")
        f.write(response.text)


query = "?startdate={ds}&enddate={tomorrow_ds}"

write_response_to_gcs = LaunchToGcsOperator(task_id="write_response_to_gcs",
                                            python_callable=_connect,
                                            provide_context=True,
                                            query=query,
                                            ds="{{ds}}",
                                            ds_tomorrow="{{ds_tomorrow}}",
                                            postgres_conn_id="gdd_connection",
                                            export_format="json",
                                            bucket='europe-west1-training-airfl-a98394bc-bucket',
                                            filename='data/ivan_postgress_{{execution_date}}.json',
                                            dag=dag

                                            )


def _print_stats(ds, **context):
    with open(f"/tmp/rocket_launches/ds={ds}/launches.json") as f:
        data = json.load(f)
        rockets_launched = [launch["name"] for launch in data["launches"]]
        rockets_str = ""

        if rockets_launched:
            rockets_str = f" ({' & '.join(rockets_launched)})"
            print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")
        else:
            print(f"No rockets found in {f.name}")


download_rocket_launches = PythonOperator(
    task_id="download_rocket_launches",
    python_callable=_download_rocket_launches,
    provide_context=True,
    dag=dag
)

print_stats = PythonOperator(
    task_id="print_stats",
    python_callable=_print_stats,
    provide_context=True,
    dag=dag
)

download_rocket_launches >> print_stats
