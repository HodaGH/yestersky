#!/bin/bash

# Recursively searches a within a directory for CSV files. These files should have air-traffic data
# from The OpenSky Network: https://opensky-network.org/datasets/states/
# Files are then ingested for messages and sent to Kafka

export CLASSPATH=$CONFLUENT_HOME/share/java/kafka/*:/path/to/kafka-avro-serializer-3.3.0.jar\
	:$CONFLUENT_HOME/share/java/confluent-common/*:$CONFLUENT_HOME/share/java/schema-registry/*\
	:/directory/containing/processAirData


find $1 -name *.csv -exec java -cp $CLASSPATH processAirData.ReadOpenSkyFile {} \;
