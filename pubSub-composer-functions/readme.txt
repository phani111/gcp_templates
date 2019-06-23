This directory contains helpful information and source material for working with PubSub.
Topics include: basic publisher and subscriber setup, different types of subscribers, creating a PubSub operator to integrate with Airflow, and using PubSub in Airflow.

About PubSub

PubSub is a data messaging bus system on GCP. When applications or databases need to communicate with one another to transfer data, they can do so in messages.
A messaging bus is an optimized solution to messaging systems that increases consistency, scalability, and expanse of data transfer by having a bus platform in place to handle message distribution instead of direct communication between platforms.
Any data type can be transferred through PubSub with a 10mb limit per message (there are other quotas/limits too). A message will need the data transferred to a byte string, and metadata will also accompany the message data.
Messages can be stored up to 7 days in a topic. When acknowledged, a message is by default removed. Generally, each message should be thought of as one entity/observation.

Elements to PubSub

Topic - A category of messages for applications to communicate with.
Publisher - An application(s) that writes messages to a topic. 
Subscriber - An application/DB that listens to a topic for messages to receive.
Push Subscribe - high throughput message receival to take in immediately as available. Misses messages when offline.
Pull Subscribe - controlled message receival to take in messages on command.
How they connect - Every topic needs at least one publisher. There can be 0+ subscribers per topic. 

When to Use PubSub

PubSub works well in many scenarios, but here are some prime use cases: 
1) A device/application takes in live feed data with high throughput and you need low latency. Have pubsub transfer the data to a pipeline for processing or direct to a DB for storage.
2) An intermittent step in a workflow between processes to hold or transfer data 

PubSub with Airflow

Apache Airflow has a supported PubSub Operator library for import in v1.10 which can be found on Git.
However, using GCP Cloud Composer with Airflow only supports modules up to v1.09. Because Composer uses an older version of Airflow, the PubSub library is not supported.
So, a user has to copy and paste the Git PubSub Operator code into a script and put that script in their Plugins folder.
The script should be accompanied with an empty __init__.py file. Then, in the DAG file you do "from <file> import <class> from Plugin folder.
