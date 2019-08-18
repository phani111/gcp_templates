"""This is a DAG file to conduct Airflow with our built PubSub operator.
   Py script is called from bash operator to provide sys.argv input

   Dataflow jobs can be scheduled with Airflow in a couple of ways.
   The easiest way is to execute the source file with BashOperator.

   To do so, place the pipeline file in the same directory, or below as the Dag Bag.
   Accordingly, also place the requirements file in the same directory that the pipeline can reference.
   Ensure that runner=DataflowRunner is specified, as well as any other arguments to put in.

   Then, execute the below code.

   Also note that the python version of composer environment should match the version of pipeline.
"""

import base64
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates

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
