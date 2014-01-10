kafka-s3-consumer
=================

Store batched Kafka messages in S3.

Build
-----

    make

Run
---

    java -jar kafka-s3-consumer-1.0.jar <props>

or (for EC2 instances)

    java -Djava.io.tmpdir=/mnt -jar kafka-s3-consumer-1.0.jar

Debian Packaging
----------------

If you want to create a Debian package, you'll need [fpm](https://github.com/jordansissel/fpm).
Then, just run:

    make deb
