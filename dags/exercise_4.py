# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise_4',
    default_args=args,
    schedule_interval="45 13 * * 1,3,5",
    dagrun_timeout=timedelta(minutes=60),
)


def _print_exec_date(execution_date, **context):
    print(execution_date)


print_exec_date = PythonOperator(
    task_id="python_exec_date",
    python_callable=_print_exec_date,
    provide_context=True,
    dag=dag
)
wait_1 = BashOperator(
    task_id='wait_1',
    bash_command='sleep 1',
    dag=dag,
)
wait_5 = BashOperator(
    task_id='wait_5',
    bash_command='sleep 5',
    dag=dag,
)
wait_10 = BashOperator(
    task_id='wait_10',
    bash_command='sleep 10',
    dag=dag,
)

the_end = DummyOperator(
    task_id='the_end',
    dag=dag,
)
print_exec_date >> [wait_1, wait_5, wait_10]
wait_1 >> the_end
wait_5 >> the_end
wait_10 >> the_end
# task2 = DummyOperator(
#     task_id='task2',
#     dag=dag,
# )
# task3 = DummyOperator(
#     task_id='task3',
#     dag=dag,
# )
# task4 = DummyOperator(
#     task_id='task4',
#     dag=dag,
# )
# task5 = DummyOperator(
#     task_id='task5',
#     dag=dag,
# )
# task1 >> task2
# task2 >> task3
# task2 >> task4
# [task3, task4] >> task5


# # [START howto_operator_bash]
# run_this = BashOperator(
#     task_id='run_after_loop',
#     bash_command='echo 1',
#     dag=dag,
# )
# # [END howto_operator_bash]

# run_this >> run_this_last
#
# for i in range(3):
#     task = BashOperator(
#         task_id='runme_' + str(i),
#         bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
#         dag=dag,
#     )
#     task >> run_this
#
# # [START howto_operator_bash_template]
# also_run_this = BashOperator(
#     task_id='also_run_this',
#     bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
#     dag=dag,
# )
# # [END howto_operator_bash_template]
# also_run_this >> run_this_last
