# spark-streaming
Example streaming app using Spark w/ direct instrumentation / visualization

## Thanks to br4nd0n
This project is inspired from : https://github.com/br4nd0n/spark-streaming

## Dependencies:
 * Redis needs to be running locally on default port 6379.
 * Use "nc -lk 1337" to create local TCP server
 * http://localhost:4040/ to see Spark dashboard
 * Be sure you have spark scala installed
  *This project is a maven project using SBT : be sure to have sbt installed

## To run:
* sbt compile
* sbt run spark.example.SparkRunner

