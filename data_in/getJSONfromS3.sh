#!/bin/bash

# Processes OpenSky Network scrapes that have been stored in S3.
# Scrapes are taken from: https://opensky-network.org/api/states/all
# Files are ingested and sent to Kafka

export CLASSPATH=$CONFLUENT_HOME/share/java/kafka/*:/path/to/kafka-avro-serializer-3.3.0.jar\
	:$CONFLUENT_HOME/share/java/confluent-common/*:$CONFLUENT_HOME/share/java/schema-registry/*\
	:/directory/containing/processAirData


dir=s3://openskydata/crawl/
tempdir=./temp/

flist=(`aws s3 ls $dir | awk '{print $4}'`)

readFile () {
    aws s3 cp $dir$1 $tempdir
    java -cp $CLASSPATH processAirData.ReadOpenSkyFile $tempdir$1
    rm $tempdir$1
}

for i in "${flist[@]}"
do
        readFile $i &
done
