/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.influxdb;

import com.yammer.metrics.Metrics;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class KafkaInfluxMetricsReporter implements KafkaMetricsReporter, KafkaInfluxMetricsReporterMBean {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInfluxMetricsReporter.class);
    private static final String DEFAULT_NAME = "infuxdb-reporter";

    private boolean initialized = false;

    private boolean running = false;

    private InfluxReporter reporter;

    @Override
    public void init(VerifiableProperties props) {

        if(!initialized) {
            KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);

            InfluxDBMetricsConfig config = new InfluxDBMetricsConfig(props);
            config.addTag("brokerId", props.getString("broker.id"));

            this.reporter = new InfluxReporter(Metrics.defaultRegistry(), DEFAULT_NAME
                    ,new InfluxDBClient(config), new MetricsPredicate(config.getPredicates()));

            if (props.getBoolean(InfluxDBMetricsConfig.KAFKA_INFLUX_METRICS_ENABLE, false)) {
                initialized = true;
                startReporter(metricsConfig.pollingIntervalSecs());
                LOG.info("KafkaInfluxMetricsReporter initialized.");
            }
        }
    }

    @Override
    public void startReporter(long pollingPeriodInSeconds) {
        if (initialized && !running) {
            reporter.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
            running = true;
            LOG.info(String.format("Started KafkaInfluxMetricsReporter with polling period %d seconds", pollingPeriodInSeconds));
        }
    }

    @Override
    public void stopReporter() {

    }

    @Override
    public String getMBeanName() {
        return "kafka:type=" + KafkaInfluxMetricsReporter.class.getName();
    }
}
