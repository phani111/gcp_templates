"""Utilizing PubSub in Airflow is an incredibly valuable process to schedule pull or publish jobs.
   Unfortunately, it is also a surprisingly very difficult process to setup a subscriber.
   A publisher is quite easy and can just be done in py script with a python operator.

   For a subscriber, I have not found luck in setting it in my py scripts.
   Instead, I have found it best to create a PubSub operator that is called in the beginning of the DAG
   file. Then, a bash operator can be used below which calls an executable python script with a
   sys.argv input of the pubsub messages pulled in the pubsub operator above.
   I haven't found an easier way that is reliable, but this atleast works, mostly...

   There are a few resources online on how to build a PubSub Operator. However, it seems with Cloud
   Composer using an older version of Airflow, it is necessary to build a PubSub Hook as the pre-made
   versions on Git are not compatible as a plugin yet. (Composer uses Airflow 1.09, PubSub comes in 1.10)

   Source code found from Josh Laird: https://medium.com/@joshlaird/ensuring-youtube-data-integrity-
   using-data-transfer-files-with-gcps-pub-sub-composer-20aee17928a4

   Pull Sensor is found from Apache website:
   https://airflow.readthedocs.io/en/stable/_modules/airflow/contrib/sensors/pubsub_sensor.html """

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from apiclient import errors
from googleapiclient.discovery import build
import ast

def _format_subscription(project, subscription):
    return 'projects/{}/subscriptions/{}'.format(project, subscription)

def _format_topic(project, topic):
    return 'projects/{}/topics/{}'.format(project, topic)

class PubSubException(Exception):
    pass

class PubSubHook(GoogleCloudBaseHook):
    """Hook to access GCP PubSub. Links to project defined in connection function"""
    def __init__(self):
        super(PubSubHook, self).__init__(gcp_conn_id, delegate_to=delegate_to)

    def get_conn(self):
        """Establishes client connection as a service object"""
        http_authorized = self.authorize()
        return build('pubsub', 'v1', http=http_authorized, cache_discovery=False)

    def pull(self, project, subscription, max_messages, return_immediately=False):
        """Synchronous non-streaming pull executable by call.
           For an airflow job, you would be interval batch pulls, not streaming.
           In the event you do want streaming in Airflow, utilize a PullSensor."""
        service = self.get_conn()
        full_subscription = _format_subscription(project, subscription)
        body = {"maxMessages": max_messages, "returnImmediately": return_immediately}

        try:
            response = service.projects().subscriptions().pull(
                subscription=full_subscription, body=body).execute()
            return response.get('receivedMessages:', [])
        except errors.HttpError as e:
            raise PubSubException("Error pulling message from {} ".format(full_subscription), e)

    def acknowledge(self, project, subscription, ack_ids):
        service = self.get_conn()
        full_subscription = _format_subscription(project, subscription)

        try:
            response = service.projects().subscriptions().acknowledge(
                subscription=full_subscription, body={"ackIds": ack_ids}).execute()
        except errors.HttpError as e:
            raise PubSubException("Error acknowledging {} messages pulled from {} ".format(
                len(ack_ids), full_subscription), e)

    def decode_pubsub(self, pubsub_message):
        """Decode the PubSub message that was pulled. Then, we will use ast module for
           a literal eval to have a dynamic and reliable-enough data type conversion to work with."""
        decoded = pubsub_message.get('message').get('data').decode('base64')
        return ast.literal_eval(decoded)

    def execute(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        messages = hook.pull(project=self.project, subscription=self.subscription,
                             max_messages=self.max_messages, return_immediately=self.return_immediately)


class PubSubPullSensor(BaseSensorOperator):
    """Pulls messages from a PubSub subscription and passes them through XCom.

    This sensor operator will pull up to ``max_messages`` messages from the
    specified PubSub subscription. When the subscription returns messages,
    the poke method's criteria will be fulfilled and the messages will be
    returned from the operator and passed through XCom for downstream tasks.

    If ``ack_messages`` is set to True, messages will be immediately
    acknowledged before being returned, otherwise, downstream tasks will be
    responsible for acknowledging them.

    ``project`` and ``subscription`` are templated so you can use
    variables in them.
    """
    template_fields = ['project', 'subscription']

    ui_color = '#ff7f50'

    def __init__(
        self,
        project,
        subscription,
        max_messages=5,
        return_immediately=False,
        ack_messages=False,
        gcp_conn_id='google_cloud_default',
        delegate_to=None,
        *args,
        **kwargs):

        super(PubSubPullSensor, self).__init__(*args, **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.project = project
        self.subscription = subscription
        self.max_messages = max_messages
        self.return_immediately = return_immediately
        self.ack_messages = ack_messages

        self._messages = None

    def execute(self, context):
        """Overridden to allow messages to be passed"""
        super(PubSubPullSensor, self).execute(context)
        return self._messages


    def poke(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)
        self._messages = hook.pull(
            self.project, self.subscription, self.max_messages,
            self.return_immediately)
        if self._messages and self.ack_messages:
            if self.ack_messages:
                ack_ids = [m['ackId'] for m in self._messages if m.get('ackId')]
                hook.acknowledge(self.project, self.subscription, ack_ids)
        return self._messages
