package com.github.fhuss.kafka.influxdb;

import kafka.utils.VerifiableProperties;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.junit.Test;

import com.github.fhuss.kafka.influxdb.InfluxDBClient.ClientConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.github.fhuss.kafka.influxdb.InfluxDBMetricsConfig.KAFKA_INFLUX_METRICS_CONNECT_CONFIG;
import static java.util.Arrays.stream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class InfluxDBClientTest {


    // can't really do this as an @Before, because we want to set it up with different config
    private InfluxDBClient setup(Properties props, List<BoundRequestBuilder> requests) {
        InfluxDBMetricsConfig config = new InfluxDBMetricsConfig(new VerifiableProperties(props));
        InfluxDBClient client = spy(new InfluxDBClient(config));

        doAnswer(invocation -> {
            requests.add(invocation.getArgumentAt(0, BoundRequestBuilder.class));
            return HAPPY_FUTURE;
        }).when(client).executeRequest(any(BoundRequestBuilder.class));

        // init will have done a "create database" at startup
        client.init();

        List<ClientConfig> targets = client.getTargets();

        assertThat("expect db create", requests.size(), is(targets.size()));
        for (int i = 0; i < targets.size(); i++) {
            // verify the targets in parallel with the requests
            InfluxDBMetricsConfig.DBTargetConfig target = targets.get(i).dbTargetConfig;
            // let's see what it looks like
            Request req = requests.get(i).build();
            assertThat(req.getUrl(), containsString(target.getConnectString()));
            assertThat(req.getUri().getQuery(), containsString("CREATE%20DATABASE"));
        }

        // make sure the queue of requests sent out is cleared ready for the test itself
        requests.clear();
        return client;
    }


    @Test
    public void testSimpleReport() {
        Properties props = new Properties();
        props.put(KAFKA_INFLUX_METRICS_CONNECT_CONFIG, "http://localhost:1");

        List<BoundRequestBuilder> requests = new ArrayList<>();
        InfluxDBClient client = setup(props, requests);
        List<ClientConfig> targets = client.getTargets();
        assertThat(targets.size(), is(1));

        Point point = new Point("measurement", System.currentTimeMillis());

        verifyMeasurement(client, requests, point);
    }


    @Test
    public void testMultipleClients() {
        Properties props = new Properties();
        props.put(KAFKA_INFLUX_METRICS_CONNECT_CONFIG+".1", "http://localhost:1");
        props.put(KAFKA_INFLUX_METRICS_CONNECT_CONFIG+".2", "http://localhost:2");

        List<BoundRequestBuilder> requests = new ArrayList<>();
        InfluxDBClient client = setup(props, requests);
        List<ClientConfig> targets = client.getTargets();
        assertThat(targets.size(), is(2));

        Point point = new Point("measurement", System.currentTimeMillis());

        verifyMeasurement(client, requests, point);
    }

    @Test
    public void testMultiplePoints() {
        Properties props = new Properties();
        props.put(KAFKA_INFLUX_METRICS_CONNECT_CONFIG+".1", "http://localhost:1");
        props.put(KAFKA_INFLUX_METRICS_CONNECT_CONFIG+".2", "http://localhost:2");

        List<BoundRequestBuilder> requests = new ArrayList<>();
        InfluxDBClient client = setup(props, requests);
        verifyMeasurement(client, requests,
                new Point("one", 1),
                new Point("two", 2),
                new Point("three", 3),
                new Point("four", 4),
                new Point("five", 5));
    }

    private void verifyMeasurement(InfluxDBClient client, List<BoundRequestBuilder> requests,
                                   Point... points) {
        List<ClientConfig> targets = client.getTargets();

        client.write(Arrays.asList(points));

        // ensure there are as many requests sent as we'd expect for the number of targets
        // the loop will ensure its one each
        assertEquals(targets.size(), requests.size());
        for (int i = 0; i < targets.size(); i++) {
            // verify *all& targets in parallel with the requests
            InfluxDBMetricsConfig.DBTargetConfig target = targets.get(i).dbTargetConfig;
            Request req = requests.get(i).build();

            assertThat(req.getUrl(), containsString(target.getConnectString()));
            assertTrue(req.getHeaders().contains(
                    "Authorization",
                    "Basic "+targets.get(0).dbTargetConfig.getBasicAuthCredentials(),
                    true));
            stream(points).forEach(point ->
                    assertThat("verify "+point.asLineProtocol()+" is present",
                            req.getStringData(), containsString(point.asLineProtocol())));

        }
    }

    private static final ListenableFuture<Boolean> HAPPY_FUTURE = new ListenableFuture<Boolean>() {
        @Override
        public void done() {
        }

        @Override
        public void abort(Throwable throwable) {
        }

        @Override
        public void touch() {
        }

        @Override
        public ListenableFuture<Boolean> addListener(Runnable runnable, Executor executor) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> toCompletableFuture() {
            return null;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true; // Explicit "done"
        }

        @Override
        public Boolean get() throws InterruptedException, ExecutionException {
            return true; // "Happy"
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return get();
        }
    };
}
