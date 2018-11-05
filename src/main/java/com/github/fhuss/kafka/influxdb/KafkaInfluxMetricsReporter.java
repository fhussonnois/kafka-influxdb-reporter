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

/**
 * Mixin class which satisfies the KafkaMetricsReporter lifecycle and handles calls via JMX.<p>
 *
 * Inspection of <a href="https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/metrics/KafkaMetricsReporter.scala">KafkaMetricsReporter.scala</a>
 * suggests that the {@link #startReporter(long)} is not explicitly called - the lifecycle is triggered
 * by (no-arg) Construction followed by a call to {@link #init(VerifiableProperties)}.<p>
 *
 * if Kafka is started with the reporting disabled, the properties could be changed via JMX and {@link #startReporter(long)}
 * called via JMX, but this seems unlikely in a production system?
 */
public class KafkaInfluxMetricsReporter implements KafkaMetricsReporter, KafkaInfluxMetricsReporterMBean {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInfluxMetricsReporter.class);
    private static final String DEFAULT_NAME = "infuxdb-reporter";

    private boolean initialized = false;

    private boolean running = false;

    private InfluxReporter reporter;
    private boolean quantizeReportingPeriods;

    @Override
    public void init(VerifiableProperties props) {

        if(!initialized) {
            KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);

            InfluxDBMetricsConfig config = new InfluxDBMetricsConfig(props);
            config.addTag("brokerId", props.getString("broker.id"));
            this.quantizeReportingPeriods = config.quantizeReportingPeriod();

            this.reporter = new InfluxReporter(Metrics.defaultRegistry(), DEFAULT_NAME,
                    new InfluxDBClient(config).init(), new MetricsPredicate(config.getPredicates()),config);

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
            final long nowInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            final long delayInSeconds = pollingPeriodInSeconds - nowInSeconds % pollingPeriodInSeconds;
            if (quantizeReportingPeriods && delayInSeconds > 0) {
                // the default reporter starts in "pollingPeriodInSeconds" time, and every "pollingPeriod..."
                // thereafter. We don't want this, because we want to have reporting start consistently
                // across all nodes in the cluster - if the period is (say) 60 seconds, then we want to wait
                // until the second hand reaches 0 and *then* start. If we're unlucky and wait for a full
                // 59 seconds before invoking start, then it will look like we've missed a minute of data
                // after a node restart, but that will appear as a "bump" in the following minute.
                LOG.info("Delaying startup for {}s", delayInSeconds);

                reporter.start(delayInSeconds, pollingPeriodInSeconds);
            } else  {
                reporter.start(pollingPeriodInSeconds, pollingPeriodInSeconds);
            }
            LOG.info(String.format("Starting KafkaInfluxMetricsReporter in %d seconds, with polling period %d seconds", delayInSeconds, pollingPeriodInSeconds));
            // deliberately always say "running", even if there is a deferred start to avoid repeated restart
            running = true;
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
