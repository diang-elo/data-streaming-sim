#!/bin/bash

echo '************************************************'
echo '** Cleaning Kafka topology                    **'
echo '**                                            **'
echo '************************************************'


#location of kafka-logs and zookeper-data directories on local system
rm -r ./kafka-logs
rm -r ./zookeeper-data



echo '************************************************'
echo "Kafka topology is reset."
echo '************************************************'
