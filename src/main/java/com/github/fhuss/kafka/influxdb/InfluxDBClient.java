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

import com.github.fhuss.kafka.influxdb.InfluxDBMetricsConfig.DBTargetConfig;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.uri.Uri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

class InfluxDBClient {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBClient.class);

    private InfluxDBMetricsConfig config;

    private boolean isDatabaseCreated = false;

    private final List<ClientConfig> targets;

    /** package private for tests only */
    static class ClientConfig {
        final DBTargetConfig dbTargetConfig;
        private final AsyncHttpClient client;
        private ClientConfig(DBTargetConfig dbTargetConfig, AsyncHttpClient client) {
            this.dbTargetConfig = dbTargetConfig;
            this.client = client;
        }
    }


    /**
     * Creates a new {@link InfluxDBClient} instance.
     *
     * @param config the configuraton.
     */
    InfluxDBClient(InfluxDBMetricsConfig config) {
        this.config = config;
        this.targets = config.getDBTargetConfig().stream().map(c -> {
            DefaultAsyncHttpClientConfig.Builder builder = new DefaultAsyncHttpClientConfig.Builder();
            return new ClientConfig(c, new DefaultAsyncHttpClient(builder.build()));

        }).collect(Collectors.toList());
    }

    /** initialise the InfluxDBClient - decoupled from constructor for testing */
    InfluxDBClient init() {
        targets.forEach(this::createDatabase);
        return this;
    }

    private boolean createDatabase(ClientConfig target) {
        final String database = target.dbTargetConfig.getDatabase();
        try {
            LOG.info("Attempt to create InfluxDB database {}", database);
            this.isDatabaseCreated = executeRequest(
                    target.client
                        .prepareGet(target.dbTargetConfig.getConnectString()  + "/query")
                        .addQueryParam("q", "CREATE DATABASE " + database))
                        .get();
        } catch (Exception e) {
            LOG.error("Cannot create database {}", database, e);
        }
        return this.isDatabaseCreated;
    }

    public void write(List<Point> points) {
        targets.forEach(t -> write(t, points));
    }

    private void write(ClientConfig target, List<Point> points) {
        if (this.isDatabaseCreated || createDatabase(target)) {
            StringBuilder sb = new StringBuilder();

            for (Point point : points) {
                point.addTags(this.config.getTags()); // add additional tags before writing line.
                sb.append(point.asLineProtocol()).append("\n");
            }

            // body size logged in chars, not (UTF-8) bytes
            LOG.info("sending {} points to InfluxDB {}, body={}c",
                    points.size(), target.dbTargetConfig.getConnectString(), sb.length());

            RequestBuilder requestBuilder = new RequestBuilder()
                    .setUri(Uri.create(target.dbTargetConfig.getConnectString() + "/write"))
                    .addQueryParam("db", target.dbTargetConfig.getDatabase())
                    .setMethod("POST")
                    .addHeader("Authorization", "Basic " + target.dbTargetConfig.getBasicAuthCredentials())
                    .setBody(sb.toString());

            requestBuilder.addQueryParam("precision", "ms");
            if( this.config.getRetention() != null && !this.config.getRetention().isEmpty())
                requestBuilder.addQueryParam("rp", this.config.getRetention());
            if( this.config.getConsistency() != null )
                requestBuilder.addQueryParam("consistency", this.config.getConsistency());

            BoundRequestBuilder request = target.client.prepareRequest(requestBuilder.build());
            executeRequest(request);
        }
    }

    /** package private for tests only */
    List<ClientConfig> getTargets() {
        return targets;
    }

    ListenableFuture<Boolean> executeRequest(BoundRequestBuilder request) {
        return request.execute(new AsyncHandler<Boolean>() {
            @Override
            public void onThrowable(Throwable throwable) {
                LOG.warn("Cannot established connection to InfluxDB {}", throwable.getMessage());
            }

            @Override
            public State onBodyPartReceived(HttpResponseBodyPart httpResponseBodyPart) throws Exception {
                return null;
            }

            @Override
            public State onStatusReceived(HttpResponseStatus httpResponseStatus) throws Exception {
                int statusCode = httpResponseStatus.getStatusCode();
                if (statusCode != 200 && statusCode != 204 ) {
                    LOG.warn("Unexpected response status from InfluxDB '{}' - '{}'", statusCode, httpResponseStatus.getStatusText());
                }
                return null;
            }

            @Override
            public State onHeadersReceived(HttpResponseHeaders httpResponseHeaders) throws Exception {
                return null;
            }

            @Override
            public Boolean onCompleted() throws Exception {
                return true;
            }
        });
    }
}
