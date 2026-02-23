@echo off
set KAFKA_HOME=C:\kafka\kafka_2.13-3.6.0
set BOOTSTRAP=localhost:9092

echo Creating Kafka topics...

%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server %BOOTSTRAP% --topic sensor-events --partitions 3 --replication-factor 1 --if-not-exists

%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server %BOOTSTRAP% --topic valid-events --partitions 3 --replication-factor 1 --if-not-exists

%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server %BOOTSTRAP% --topic invalid-events --partitions 3 --replication-factor 1 --if-not-exists

echo Done. Existing topics:
%KAFKA_HOME%\bin\windows\kafka-topics.bat --list --bootstrap-server %BOOTSTRAP%