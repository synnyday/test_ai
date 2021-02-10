Exercise
========

As part of the demo application you must be able to send events to a Kafka topic (a producer) which will then be read by a Kafka consumer application that you've written.

The consumer application must then store the consumed data to an Aiven PostgreSQL database.

Even though this is a small concept program, returned homework should include
tests and proper packaging. If your tests require Kafka and PostgreSQL
services, for simplicity your tests can assume those are already running,
instead of integrating Aiven service creation and deleting.

To complete the homework please register to Aiven at https://console.aiven.io/signup.html at which point you'll automatically be given $300 worth of credits to play around with. This should be enough for a few hours use of our services. If you need more credits to complete your homework, please contact us.

We accept homework exercises written in Python, Go, Java and Node.JS. Aiven predominantly uses Python itself, so we give out bonus points for candidates completing the homework in Python.

Automatic tests for the application are not mandatory, but again we'd like to see at least a description of how the application could be tested.

Testing
==========

Start process with producer:

`python main.py --producer`

Start another process with consumer:

`python main.py --consumer`






