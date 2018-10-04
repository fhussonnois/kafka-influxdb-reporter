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

import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static com.github.fhuss.kafka.influxdb.MetricsPredicate.Measures.*;

class InfluxReporter extends AbstractPollingReporter implements MetricProcessor<InfluxReporter.Context> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxReporter.class);

    private static final MetricPredicate DEFAULT_METRIC_PREDICATE = MetricPredicate.ALL;

    private List<Point> nextBatchPoints;

    private InfluxDBClient client;

    private Clock clock;

    private Context context;

    private MetricsPredicate predicate;

    private VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();

    private final InfluxDBMetricsConfig config;

    /**
     * Creates a new {@link AbstractPollingReporter} instance.
     **/
    InfluxReporter(MetricsRegistry registry, String name, InfluxDBClient client, MetricsPredicate predicate, InfluxDBMetricsConfig config) {
        super(registry, name);
        this.client = client;
        this.clock = Clock.defaultClock();
        this.predicate = predicate;
        this.context = new Context() {
            @Override
            public long getTime() {
                return InfluxReporter.this.clock.time();
            }
        };
        this.config = config;
    }

    @Override
    public void run() {
        try {
            this.nextBatchPoints = new LinkedList<>();
            printRegularMetrics(context);
            processVirtualMachine(vm, context);
            this.client.write(nextBatchPoints);
        } catch (Exception e) {
            LOG.error("Cannot send metrics to InfluxDB {}", e);
        }
    }

    private void printRegularMetrics(final Context context) {
        for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(DEFAULT_METRIC_PREDICATE).entrySet()) {
            for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null && !config.omit(subEntry.getKey())) {
                    try {
                        metric.processWith(this, subEntry.getKey(), context);
                    } catch (Exception e) {
                        LOG.error("Error printing regular metrics: {}", e);
                    }
                }
            }
        }
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Context context) throws Exception {
        Point point = buildPoint(name, context);
        addMeteredFields(meter, point);
        nextBatchPoints.add(point);
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Context context) throws Exception {
        Point point = buildPoint(name, context);
        filterOrAddField(count, point, counter.count());
        nextBatchPoints.add(point);
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Context context) throws Exception {
        final Snapshot snapshot = histogram.getSnapshot();
        Point point = buildPoint(name, context);
        addSummarizableFields(histogram, point);
        addSnapshotFields(snapshot, point);
        nextBatchPoints.add(point);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Context context) throws Exception {
        final Snapshot snapshot = timer.getSnapshot();

        Point point = buildPoint(name, context);
        addSummarizableFields(timer, point);
        addMeteredFields(timer, point);
        addSnapshotFields(snapshot, point);
        filterOrAddField(count, point, timer.count());
        nextBatchPoints.add(point);
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Context context) throws Exception {

        Point point = buildPoint(name, context);
        Object fieldValue = gauge.value();
        String fieldName = value.label();
        if (name.getName().equals("ClusterId") && name.getType().equals("KafkaServer"))
            point.addField("name", fieldValue.toString());

        else if (name.getType().equals("ReplicaFetcherManager"))
            point.addField(fieldName, Double.valueOf(fieldValue.toString()));

        else if( fieldValue instanceof Float)
            point.addField(fieldName, (Float)fieldValue);
        else if( fieldValue instanceof Double)
            point.addField(fieldName, (Double)fieldValue);
        else if( fieldValue instanceof Long)
            point.addField(fieldName, (Long)fieldValue);
        else if( fieldValue instanceof Integer)
            point.addField(fieldName, (Integer)fieldValue);
        else if( fieldValue instanceof String)
            point.addField(fieldName, (String)fieldValue);
        else
            return;
        nextBatchPoints.add(point);
    }


    private void addSummarizableFields(Summarizable m, Point point) {
        filterOrAddField(max, point, m.max());
        filterOrAddField(mean, point, m.mean());
        filterOrAddField(min, point, m.min());
        filterOrAddField(stddev, point, m.stdDev());
        filterOrAddField(sum, point, m.sum());
    }

    private void addMeteredFields(Metered m, Point point) {
        filterOrAddField(m1Rate, point, m.oneMinuteRate());
        filterOrAddField(m5Rate, point, m.fiveMinuteRate());
        filterOrAddField(m15Rate, point, m.fifteenMinuteRate());
        filterOrAddField(meanRate, point, m.meanRate());
        filterOrAddField(count, point, m.count());

    }

    private void addSnapshotFields(Snapshot m, Point point) {
        filterOrAddField(median, point, m.getMedian());
        filterOrAddField(p75, point, m.get75thPercentile());
        filterOrAddField(p95, point, m.get95thPercentile());
        filterOrAddField(p98, point, m.get98thPercentile());
        filterOrAddField(p99, point, m.get99thPercentile());
        filterOrAddField(p999, point, m.get999thPercentile());
    }

    public void processVirtualMachine(VirtualMachineMetrics vm, Context context) throws Exception  {
        Point point = new Point("JVM", context.getTime());
        point.addField("memory.heap_used", vm.heapUsed());
        point.addField("memory.heap_usage", vm.heapUsage());
        point.addField("memory.non_heap_usage", vm.nonHeapUsage());

        point.addField("daemon_thread_count", vm.daemonThreadCount());
        point.addField("thread_count", vm.threadCount());
        point.addField("uptime", vm.uptime());
        point.addField("fd_usage", vm.fileDescriptorUsage());

        nextBatchPoints.add(point);
    }

    private void filterOrAddField(MetricsPredicate.Measures measure, Point point, double value) {
        if (predicate.isEnable(measure)) point.addField(measure.label(), value);
    }

    private Point buildPoint(MetricName name, Context context) {
        Point pb = new Point(name.getType(), context.getTime())
                .addTag("metric", name.getName())
                .addField("group", name.getGroup());

        if( name.hasScope() ) {
            String scope = name.getScope();

            List<String> scopes = Arrays.asList(scope.split("\\."));
            if( scopes.size() % 2 == 0) {
                Iterator<String> iterator = scopes.iterator();
                while (iterator.hasNext()) {
                    pb.addTag(iterator.next(), iterator.next());
                }
            }
            else pb.addTag("scope", scope);
        }
        return pb;
    }

    public interface Context {

        long getTime( );
    }
}
