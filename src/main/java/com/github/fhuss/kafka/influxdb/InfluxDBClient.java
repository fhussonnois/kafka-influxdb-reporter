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

import org.asynchttpclient.*;
import org.asynchttpclient.uri.Uri;
import org.asynchttpclient.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class InfluxDBClient {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBClient.class);

    private InfluxDBMetricsConfig config;

    private AsyncHttpClient asyncHttpClient;

    private boolean isDatabaseCreated = false;

    private String credentials;

    /**
     * Creates a new {@link InfluxDBClient} instance.
     *
     * @param config the configuratoon.
     */
    public InfluxDBClient(InfluxDBMetricsConfig config) {
        this.config = config;

        DefaultAsyncHttpClientConfig.Builder builder = new DefaultAsyncHttpClientConfig.Builder();
        this.asyncHttpClient = new DefaultAsyncHttpClient(builder.build());

        this.credentials = Base64.encode((config.getUsername() + ":" + config.getPassword()).getBytes());

        createDatabase(this.config.getDatabase());
    }

    private boolean createDatabase(String database) {
        try {
            LOG.info("Attempt to create InfluxDB database {}", database);
            this.isDatabaseCreated = executeRequest(
                    this.asyncHttpClient
                        .prepareGet(this.config.getConnectString()  + "/query")
                        .addQueryParam("q", "CREATE DATABASE " + database))
                        .get();
        } catch (Exception e) {
            LOG.error("Cannot create database {}", database, e);
        }
        return this.isDatabaseCreated;
    }

    public void write(List<Point> points) {
        if (this.isDatabaseCreated || createDatabase(this.config.getDatabase())) {
            StringBuilder sb = new StringBuilder();

            for (Point point : points) {
                point.addTags(this.config.getTags()); // add additional tags before writing line.
                sb.append(point.asLineProtocol()).append("\n");
            }

            // body size logged in chars, not (UTF-8) bytes
            LOG.info("sending {} points to InfluxDB, body={}c", points.size(), sb.length());
            RequestBuilder requestBuilder = new RequestBuilder()
                    .setUri(Uri.create(this.config.getConnectString() + "/write"))
                    .addQueryParam("db", this.config.getDatabase())
                    .setMethod("POST")
                    .addHeader("Authorization", "Basic " + credentials)
                    .setBody(sb.toString());

            requestBuilder.addQueryParam("precision", "ms");
            if( this.config.getRetention() != null && !this.config.getRetention().isEmpty())
                requestBuilder.addQueryParam("rp", this.config.getRetention());
            if( this.config.getConsistency() != null )
                requestBuilder.addQueryParam("consistency", this.config.getConsistency());

            BoundRequestBuilder request = asyncHttpClient.prepareRequest(requestBuilder.build());
            executeRequest(request);
        }
    }

    private ListenableFuture<Boolean> executeRequest(BoundRequestBuilder request) {
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
