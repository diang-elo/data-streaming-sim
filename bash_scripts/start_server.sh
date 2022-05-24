
#!/bin/bash

echo '************************************************'
echo '** Kafka server launch script starting        **'
echo '** Launching...                               **'
echo '************************************************'


echo '************************************************'
echo "Starting Zookeeper server."
echo '************************************************'

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties &

echo '************************************************'
echo "Starting Kafka server."
echo '************************************************'

.\bin\windows\kafka-server-start.bat .\config\server.properties &
