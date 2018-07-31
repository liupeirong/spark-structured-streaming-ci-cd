[![Build Status](https://travis-ci.com/liupeirong/spark-structured-streaming-ci-cd.svg?branch=master)](https://travis-ci.com/liupeirong/spark-structured-streaming-ci-cd)

# spark-structured-streaming-ci-cd
Spark structured streaming with unit tests integrated with Travis CI

To build this jar, run ```mvn package``` if you have Spark installed locally and running. Otherwise, run ```mvn package -DskipTests``` to skip the tests.

To run this jar, ```spark-submit --master spark://your-host-name:7077 --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.2,com.typesafe:config:1.3.1 --class o.paige.Main target/sparkss-1.0-SNAPSHOT.jar```
