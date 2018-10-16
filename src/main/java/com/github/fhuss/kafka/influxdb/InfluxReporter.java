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

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractReporter;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.fhuss.kafka.influxdb.MetricsPredicate.Measures.*;

/**
 * Kafka {@link AbstractReporter} which can process different types of metrics by adding them to a
 * local "batch" of {@link Point}s ready for transmission to InfluxDB. While this is effectively an
 * {@link com.yammer.metrics.reporting.AbstractPollingReporter} it does not extend it directly
 * because the {@code AbstractPollingReporter} has a reporting time drift due to its use of
 * {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)} in {@code start()}.
 * This class uses {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)} (see
 * docs for the differences).
 */
class InfluxReporter extends AbstractReporter implements MetricProcessor<InfluxReporter.Context>, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxReporter.class);

    private static final MetricPredicate DEFAULT_METRIC_PREDICATE = MetricPredicate.ALL;

    private final List<Point> nextBatchPoints = new LinkedList<>();

    private final InfluxDBClient client;

    private final Clock clock;

    private final Context context;

    private final MetricsPredicate predicate;

    private final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();

    private final InfluxDBMetricsConfig config;

    private final ScheduledExecutorService executor;

    /**
     * Creates a new {@link AbstractReporter} instance.
     **/
    InfluxReporter(MetricsRegistry registry, String name, InfluxDBClient client, MetricsPredicate predicate, InfluxDBMetricsConfig config) {
        super(registry);
        this.executor = registry.newScheduledThreadPool(1, name);
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

    /**
     * Starts the reporter polling at the given period.
     * @param delayInSeconds     time period
     * @param periodInSeconds    the amount of time between polls
     */
    void start(long delayInSeconds, long periodInSeconds) {
        executor.scheduleAtFixedRate(this, delayInSeconds, periodInSeconds, TimeUnit.SECONDS);
    }

    /**
     * Shuts down the reporter polling, waiting the specific amount of time for any current polls to
     * complete.
     *
     * @param timeout    the maximum time to wait
     * @param unit       the unit for {@code timeout}
     * @throws InterruptedException if interrupted while waiting
     */
    public void shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        executor.shutdown();
        if (timeout >= 0) {
            executor.awaitTermination(timeout, unit);
        }
        super.shutdown();
    }

    @Override
    public void shutdown() {
        try {
            shutdown(-1, null);
        } catch (InterruptedException e) {
            // can't happen with a timeout of -1
            LOG.error("unexpected {}", e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            printRegularMetrics(context);
            processVirtualMachine(vm, context);
            this.client.write(nextBatchPoints);
            this.nextBatchPoints.clear();
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
