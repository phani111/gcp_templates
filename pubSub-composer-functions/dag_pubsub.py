"""This is a DAG file to conduct Airflow with our built PubSub operator.
   Process is to pull from a pubsub subscription hourly. Messages are then processed
   with a python script. Py script is called from bash operator to provide sys.argv input

   Knowledge on this matter is thanks to many contributors on Git in tackling this pubsub for airflow challenge.
   Of particular interest for this DAG file:
   https://github.com/apache/airflow/blob/master/airflow/contrib/example_dags/example_pubsub_flow.py 

   Note a custom operator is built in Plugins folder as Composer uses an older Airflow version not supporting
   the pubsub operator package in airflow.contrib library."""

import base64
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates
# When importing a custom class/plugin -- have the file in Plugins folder.
#   Then, call as such: from <file> import <class>
from pullOperator import PubSubHook, PubSubPullSensor

project = 'your-project-id'  # Change this to your own GCP project_id
topic = 'example-topic'  # Cloud Pub/Sub topic
subscription = 'subscription-to-example-topic'  # Cloud Pub/Sub subscription

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(2),
    'email': ['willjspangler@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'project': project,
    'topic': topic,
    'subscription': subscription,
}
py_path = '/users/user/dags/dependencies/pyjob.py'

bash_cmd = ("{% for m in task_instance.xcom_pull(task_ids='pull-messages') %}" +
            "python {} m".format(py_path) +
            "{% endfor %}")

with DAG('pull_message_process', default_args=default_args,
         schedule_interval=datetime.timedelta(days=1)) as dag:

    t1 = PubSubPullSensor(task_id='pull-messages')
    t2 = BashOperator(task_id='invoke-py-script', bash_cmd=bash_cmd)

    t1 >> t2
