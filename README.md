Exercise
========

As part of the demo application you must be able to send events to a Kafka topic (a producer) which will then be read by a Kafka consumer application that you've written.

The consumer application must then store the consumed data to an Aiven PostgreSQL database.

Even though this is a small concept program, returned homework should include
tests and proper packaging. If your tests require Kafka and PostgreSQL
services, for simplicity your tests can assume those are already running,
instead of integrating Aiven service creation and deleting.

Testing
==========

Start process with producer:

`python main.py --producer`

Start another process with consumer:

`python main.py --consumer`






