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
2. Add `kafka-influxdb-metrics-reporter-0.1.0-shaded.jar` to the `libs/` directory of your kafka broker installation.
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
| **kafka.influxdb.metrics.tags**          | Custom Additional Tags              |                   |

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