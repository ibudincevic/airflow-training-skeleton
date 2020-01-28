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
import datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise_5',
    default_args=args,
    schedule_interval="45 13 * * 1,3,5",
    dagrun_timeout=timedelta(minutes=60),
)


# def _print_weekday( **context):
#     print("The day of the week is: ", datetime.datetime.today().weekday())

def _get_weekday(execution_date, **context):
    return days_dict[execution_date.strftime("%a")]


print_weekday = PythonOperator(
    task_id="print_weekday",
    python_callable=_get_weekday,
    provide_context=True,
    dag=dag
)

branching = BranchPythonOperator(task_id = "branching",
                                 python_callable = _get_weekday,
                                 provide_context = True,
                                 dag = dag)

final_task = DummyOperator(
    task_id='final_task',
    dag=dag,
)

# days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
days = ["Mon", "Tue", "Wed"]
days_dict = {"Mon": 'email_joe', "Tue": 'email_alice', "Wed": 'email_bob'}
for day in days:
    # branching >> DummyOperator(task_id=day, dag=dag) >> final_task
    print("The day is {}, so we will execute task {}".format(day, days_dict[day]))
    branching >> DummyOperator(task_id=days_dict[day], dag=dag) >> final_task

email_joe = BashOperator(
    task_id='email_joe',
    bash_command='echo "email Joe"',
    dag=dag,
)
email_alice = BashOperator(
    task_id='email_alice',
    bash_command='echo "email Alice"',
    dag=dag,
)
email_bob = BashOperator(
    task_id='email_bob',
    bash_command='echo "email Bob"',
    dag=dag,
)
print_weekday >> branching
[email_joe, email_alice, email_bob] >> final_task
# wait_5 = BashOperator(
#     task_id='wait_5',
#     bash_command='sleep 5',
#     dag=dag,
# )
# wait_10 = BashOperator(
#     task_id='wait_10',
#     bash_command='sleep 10',
#     dag=dag,
# )
#
#
# email_joe >> final_task
# email_alice >> final_task
# email_bob >> final_task
# print_weekday >> [wait_1, wait_5, wait_10]
# wait_1 >> the_end
# wait_5 >> the_end
# wait_10 >> the_end
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
