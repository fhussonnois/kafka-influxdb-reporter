Kafka InfluxDB Metrics Reporter
=============================================================

[Apache Kafka](http://kafka.apache.org/) is a high-throughput, distributed, publish-subscribe messaging system.

Simple library to report kafka metrics into InfluxDB.

## Metrics

This library will create a new measurement for each metric type.

```
SHOW measurements;

name: measurements
------------------
name
BrokerTopicMetrics
ControllerStats
DelayedOperationPurgatory
GroupMetadataManager
KafkaController
KafkaRequestHandlerPool
KafkaServer
Log
LogCleaner
LogCleanerManager
Partition
Processor
ReplicaFetcherManager
ReplicaManager
RequestChannel
RequestMetrics
SessionExpireListener
SocketServer
Throttler
jvm
```

## Examples

```sql
SELECT time, brokerId, 1MinuteRate FROM BrokerTopicMetrics WHERE metric = 'MessagesInPerSec' AND time > now() - 15m;

SELECT * FROM RequestMetrics WHERE metric = 'TotalTimeMs' AND request = 'Produce';
```

## Installation

1. Build this project using `mvn clean package`
2. Add `kafka-influxdb-metrics-reporter-0.4.0-shaded.jar` to the `libs/` directory of your kafka broker installation.
3. Configure the broker (see the configuration section below).
4. Restart the broker.

## Configuration

Add the following configuration to server.properties of each or your kafka broker. 

```
kafka.metrics.reporters=com.github.fhuss.kafka.influxdb.KafkaInfluxMetricsReporter
kafka.influxdb.metrics.reporter.enabled=true
```
    
| **Property name**                        | **Description**                     | **Default**       |
| -----------------------------------------| ------------------------------------| ------------------|
| **kafka.influxdb.metrics.address**       | InfluxDB address                    | {"localhost:8086"}|
| **kafka.influxdb.metrics.database**      | The database used to store metrics  | {"kafka"}         |
| **kafka.influxdb.metrics.username**      | Username for authentication         | {"root"}          |
| **kafka.influxdb.metrics.password**      | Password for authentication         | {"root"}          |
| **kafka.influxdb.metrics.consistency**   | -                                   |                   |
| **kafka.influxdb.metrics.retention**     | -                                   |                   |
| **kafka.influxdb.metrics.tags**          | Custom Additional Tags: comma separated <br>&nbsp;&nbsp;*key=value*<br> pairs | |
| _Advanced Configuration_ |
| **kafka.metrics.polling.interval.secs**  | Change the reporting frequency      | 10                |
| **kafka.influxdb.metrics.omit**          | Comma-separated list of metric names to omit |          |
| **kafka.influxdb.polling.interval.quantize** | If set to **true**, then reports will be emitted on quantized boundaries. For example: if *kafka.metrics.polling.interval.secs* is 10, then reports will be emitted at 0, 10, 20, 30, 40, 50 seconds past the minute. <br>Otherwise they will be emitted every 10 seconds after initialisation.| false |
| _Multiple InfluxDB_ |
| **kafka.influxdb.metrics.address.N**     | See below                           | value from kafka.influxdb.metrics.address |
| **kafka.influxdb.metrics.database.N**    | See below                           | value from kafka.influxdb.metrics.database |
| **kafka.influxdb.metrics.username.N**    | See below                           | value from kafka.influxdb.metrics.username |
| **kafka.influxdb.metrics.password.N**    | See below                           | value from kafka.influxdb.metrics.password |

## Using Multiple InfluxDBs
If you want to send the same measurements to more than one InfluxDB for resilience these can be
configured by specifying numerically qualified address, database, username and/or password fields. 
You can partially specify these details, and configuration will fall back to the unqualified fields.

All other fields (tags, retention etc) are assumed to be consistent across all databases so qualified
versions will be ignored.

For example, if you have 2 hosts set up, sharing the same authorization details and database name, you _might_ 
expect to use a configuration which includes:

| |
|-------------------------------------------------|   
| kafka.influxdb.metrics.address.1=server_1.monitoring_subnet.internal |
| kafka.influxdb.metrics.address.2=server_2.monitoring_subnet.internal |
| kafka.influxdb.metrics.database=influx_dbname |
| kafka.influxdb.metrics.username=service_writer |
| kafka.influxdb.metrics.password=writer_password |
| |
The numeric qualifiers start at 1, and are incremental. If there is a gap in the properties, the scanning
for more config will stop. So, if you have address.1, address.2 and address.4, you should only expect to
see _2_ servers set up (directing measurements to address.1 and address.2)

Refer to the InfluxDBMetricsConfigTest for more examples of supported config. 

## Metrics measures:

By default all stats are reported. You can disabled some with the following properties.

| **Property name**                                 |
| --------------------------------------------------|
| kafka.influxdb.measure.enabled.count=true         |
| kafka.influxdb.measure.enabled.meanRate=true      |
| kafka.influxdb.measure.enabled.m1Rate=true        |
| kafka.influxdb.measure.enabled.m5Rate=true        |
| kafka.influxdb.measure.enabled.m15Rate=true       |
| kafka.influxdb.measure.enabled.min=true           |
| kafka.influxdb.measure.enabled.max=true           |
| kafka.influxdb.measure.enabled.mean=true          |
| kafka.influxdb.measure.enabled.sum=true           |
| kafka.influxdb.measure.enabled.stddev=true        |
| kafka.influxdb.measure.enabled.median=true        |
| kafka.influxdb.measure.enabled.p50=true           |
| kafka.influxdb.measure.enabled.p75=true           |
| kafka.influxdb.measure.enabled.p95=true           |
| kafka.influxdb.measure.enabled.p98=true           |
| kafka.influxdb.measure.enabled.p99=true           |
| kafka.influxdb.measure.enabled.p999=true          |

## Example Grafana Dashboard

![Grafana](./dashboards/Grafana-Kafka-Cluster-Overview.png)

## Versions
* 0.4.0: Support redundant Influx Database configuration with full backward compatibility on previous config; Add basic unit tests
* 0.3.0: Add option to snap reporting time to a predictable (quantized) period, e.g. on 0, 10, 20, ... seconds past the top of the minute 
* 0.2.0-rc0: POST Truncation bug fix, resolve 400 from Influx where ClusterID sent as numeric
* 0.1.0: Initial version

## Contributions
Any contribution is welcome

## Others Kafka Reporters

Thanks to developers who made the first reporters.

- [Graphite](https://github.com/damienclaveau/kafka-graphite)
- [Elasticsearch](https://github.com/be-hase/kafka-elasticsearch-metrics-reporter)

## Licence
Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License