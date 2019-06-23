"""This file contains code and use case for making a Publisher and 
   Asynch/Synchronous Pull Subscribers, or push subscribers.
   Refer to the readme.txt in this directory to learn more about PubSub
   and when it is useful to use.
   Big thanks to GCP Documentation pages for putting these methods together."""

from google.cloud import pubsub_v1
import random
import time
import logging
import multiprocessing
import base64
import json

class PubSub:
    """Code basics for working with Pubsub. These functions can be used in various
       platforms such as Dataflow, Cloud Functions, App Engine, and others.
       The pubsub_v1 module is used to set up Client connections. Note that
       publishing and subscribing utilize different client connections."""

    def __init__(self):
        self.project = 'myproject'  # GCP Project of PubSub
        self.topic = 'topic1'  # Topic name routes messages to that grouping
        self.asynch_sub = 'asynch_sub'  # Will use for asynchronous pull
        self.synch_sub = 'synch_sub'  # Will use for synchronous pull
        self.push_sub = 'push_sub'  # Will use for push subscription

    def publish(self):
        """Publishing a message to PubSub can take in any data format, but will have
           to be encoded into a byte string. It is important to note there is a 10mb
           size limit per message, so it is best to keep messages to individual elements
           rather than larger scale dataframes. We will use a json example below, which
           is a common use case."""

        # Optional) Configure batch settings for publishing. For optimal performance it is advised to
        # ignore this step. However, it can be useful for setting controls on data throughput such
        # as only publishing once a data size is met, or time is met. Below is 1kb or 1sec controls.

        batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=1024, max_latency=1)

        # 1) Setup publish client connection. Then, assign the topic path to publish to.

        publisher = pubsub_v1.PublisherClient()
        # publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        pub_path = publisher.topic_path(project_id=self.project, topic_name=self.topic)

        # 2) Below we create a callback lambda function. The callback function gets called after
        # a message is 'processed'. There will be more importance placed on it for pulling messages.
        # For publishing messages, they are not totally necessary, but it is best practice to atleast
        # keep a callback function for logging purposes, which I outline below.
        # I use lambda out of dislike for def functions within functions, but that works too.
        # Note that printing in lambda as below is only supported in python3.

        pub_callback = lambda message: (print("Publishing message to {} threw exception: ".format(
                    self.topic, message.exception())) if message.exception(timeout=30)
                                        else print(message.result()))

        # 3) With callback and paths defined above, this is where we actually publish.
        # As stated before, we will use simple json values, but this part is where we can get creative in
        # data sources such as reading tables, having processing output, or putting at end of a pipeline.

        rando_money = lambda: "I got ${} dollars!".format(random.randint(1, 100))
        for n in range(1,10):  # publish 10 messages
            """First convert to unicode which can then be converted to REQUIRED byte string format. After
               converting to bytes, we publish message to topic and trigger callback function for logging."""
            data = {'whatever_key': n, 'dollars': rando_money()}
            msg = u'{}'.format(data)
            msg = msg.encode('utf-8')
            message = publisher.publish(pub_path, data=msg)
            message.add_done_callback(callback=pub_callback)

        # 4) Keep main thread from exiting so messages can be processed in background
        while True:
            time.sleep(60)

    def asynch_pull(self):
        """Asynchronous pull is the recommended strategy for pulling messages as it allows for higher throughput
           by using a long running listener. This will automatically pull messages as long as sub is online, and
           also take in queued messages from if it went offline."""
        # 1) Set up client connection for subscriptions
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(project_id=self.project, subscription_name=self.asynch_sub)

        # 2) Utilize a callback method to log and acknowledge message.
        # Acknowledging a message is necessary to take it out of queue.
        # Cloud Functions and Dataflow have an auto acknowledgement, so this is only necessary for other tools.
        # You can as well process the message within the callback as shown below, by decoding data.
        def asynch_callback(message):
            print('Received message: {}'.format(message))
            data = json.loads(message.decode('utf-8'))
            data = base64.b64decode(data['data'])
            message.ack()

        # 3) define the pull method by setting subscription path and callback function to perform.
        pull = subscriber.subscribe(subscription_path=sub_path, callback=asynch_callback)

        # 4) For error handling, we use a try / except with the pull method
        try:
            pull.result(timeout=30)
        except Exception as e:
            print('Listening for messages on {} threw exception: {}'.format(sub_path, e))

        # 5) Same as before, keep thread open to listen to messages on topic.
        print('Listening for messages on {}'.format(sub_path))
        while True:
            time.sleep(60)

    def synch_pull(self):
        """Synchronous pulls are useful for when you want more controlled pulls in your operation, as in
           when you want to trigger pulls. This is useful for when you are using PubSub in a continuous
           running application or process but want scheduled pull triggers. In this sense, synchronous
           better aligns with a pull method, oppose to asynchronous aligning with Streaming Pull.
           Multi-threading is used below because synchronous pulls should be performed in parallel
           to maximize amount of messages being processed at once. To explain, having 5 processes pulling
           10 messages at once will perform quicker then one processor pulling 50 messages at once."""

        final_data = []  # collect a list of message data as we pull.

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id=self.project, subscription_name=self.synch_sub)

        NUM_MESSAGES, ACK_DEADLINE = 10, 30

        # The subscriber pulls a specific number of messages.
        response = subscriber.pull(subscription_path, max_messages=NUM_MESSAGES)

        multiprocessing.log_to_stderr()
        logger = multiprocessing.get_logger()
        logger.setLevel(logging.INFO)

        def worker(msg):
            """Simulates a long-running process."""
            RUN_TIME = random.randint(1, 60)
            logger.info('{}: Running {} for {}s'.format(
                time.strftime("%X", time.gmtime()), msg.message.data, RUN_TIME))
            time.sleep(RUN_TIME)

        # processes store process as key and ack id and message as values.
        # message has a subcomponent, 'data', which we want to gather for processing.
        processes = dict()
        for message in response.received_messages:
            process = multiprocessing.Process(target=worker, args=(message,))
            processes[process] = (message.ack_id, message.message.data)
            process.start()

        while processes:
            for process in list(processes):
                ack_id, msg_data = processes[process]
                # If the process is still running, reset the ack deadline as
                # specified by ACK_DEADLINE once every while as specified
                # by SLEEP_TIME.
                if process.is_alive():
                    # `ack_deadline_seconds` must be between 10 to 600.
                    subscriber.modify_ack_deadline(
                        subscription_path,
                        [ack_id],
                        ack_deadline_seconds=ACK_DEADLINE)
                    logger.info('{}: Reset ack deadline for {} for {}s'.format(
                        time.strftime("%X", time.gmtime()),
                        msg_data, ACK_DEADLINE))

                    final_data.append(msg_data)

                # If the processs is finished, acknowledges using `ack_id`.
                else:
                    subscriber.acknowledge(subscription_path, [ack_id])
                    logger.info("{}: Acknowledged {}".format(
                        time.strftime("%X", time.gmtime()), msg_data))
                    processes.pop(process)

            # If there are still processes running, sleeps the thread.
            # Need to have a sleep to keep thread open.
            if processes:
                time.sleep(10)

        print("Received and acknowledged {} messages. Done.".format(NUM_MESSAGES))
        return final_data
