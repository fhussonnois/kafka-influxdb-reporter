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

import com.yammer.metrics.core.MetricName;
import kafka.utils.VerifiableProperties;
import org.asynchttpclient.util.Base64;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class InfluxDBMetricsConfig {

    private static final List<String> RETENTION_POLICIES = Arrays.asList("one", "quorum", "all", "any", "");

    static final String KAFKA_INFLUX_METRICS_CONNECT_CONFIG     = "kafka.influxdb.metrics.address";
    private static final String KAFKA_INFLUX_METRICS_DATABASE_CONFIG    = "kafka.influxdb.metrics.database";
    private static final String KAFKA_INFLUX_METRICS_USERNAME_CONFIG    = "kafka.influxdb.metrics.username";
    private static final String KAFKA_INFLUX_METRICS_PASSWORD_CONFIG    = "kafka.influxdb.metrics.password";
    private static final String KAFKA_INFLUX_METRICS_CONSISTENCY_CONFIG = "kafka.influxdb.metrics.consistency";
    private static final String KAFKA_INFLUX_METRICS_RETENTION_CONFIG   = "kafka.influxdb.metrics.retention";
    private static final String KAFKA_INFLUX_METRICS_OMIT_CONFIG        = "kafka.influxdb.metrics.omit";
    private static final String KAFKA_INFLUX_METRICS_TAGS_CONFIG        = "kafka.influxdb.metrics.tags";
    private static final String KAFKA_GRAPHITE_MEASURE_ENABLED_CONFIG   = "kafka.influxdb.measure.enabled";
    private static final String KAFKA_INFLUX_METRICS_QUANTIZE_TO_TIME_PERIOD = "kafka.influxdb.polling.interval.quantize";
    static final String KAFKA_INFLUX_METRICS_ENABLE                     = "kafka.influxdb.metrics.reporter.enabled";

    private final List<DBTargetConfig> targets = new ArrayList<>();
    private String consistency;
    private String retention;
    private Map<String, String> tags = new HashMap<>();
    private Set<String> omit = new HashSet<>();
    private Map<MetricsPredicate.Measures, Boolean> predicates = new HashMap<>();
    private boolean quantizeReportPeriod;

    static final class DBTargetConfig {
        // if a mapping from "key.id" exists, return it, otherwise return the mapping of "key", falling
        // back to  "value". For example, where id=1, look for "kafka.influxdb.metrics.database.1" and
        // if it is missing, try "kafka.influxdb.metrics.database".
        private static String get(int id, VerifiableProperties props, String key, String value) {
            return props.getString(qualifiedKey(id, key), props.getString(key, value));
        }
        private static String qualifiedKey(int id, String key) {
            return key + "." + id;
        }

        // speculate about the number of DB Targets that have been configured by looking for the
        // last in an incremental sequence of the config keys supported by this class. The
        // qualified targets are "1" based, not "0" based.
        private static int deduceNumDBTargets(VerifiableProperties props) {
            int numQualifiedTarget = 0;
            for (int i = 1; ; i++) {
                // a qualified one can exist where any of its variables are specified
                if (props.containsKey(qualifiedKey(i, KAFKA_INFLUX_METRICS_CONNECT_CONFIG)) ||
                        props.containsKey(qualifiedKey(i, KAFKA_INFLUX_METRICS_DATABASE_CONFIG)) ||
                        props.containsKey(qualifiedKey(i, KAFKA_INFLUX_METRICS_USERNAME_CONFIG)) ||
                        props.containsKey(qualifiedKey(i, KAFKA_INFLUX_METRICS_PASSWORD_CONFIG))) {
                    // we've found an instance of key.id, look for the next one
                    numQualifiedTarget++;
                } else {
                    // no more instances found
                    break;
                }
            }

            // for an unqualified target to be considered independent of any qualified ones, it
            // needs to have *all* properties set.
            int numUnqualifiedTarget = props.containsKey(KAFKA_INFLUX_METRICS_CONNECT_CONFIG) &&
                    props.containsKey(KAFKA_INFLUX_METRICS_DATABASE_CONFIG) &&
                    props.containsKey(KAFKA_INFLUX_METRICS_USERNAME_CONFIG) &&
                    props.containsKey(KAFKA_INFLUX_METRICS_PASSWORD_CONFIG) ? 1 : 0;

            // finally - make sure we still allow for defaults, even if any of the rules above
            // have not been satisified
            return Math.max(1, numQualifiedTarget + numUnqualifiedTarget);
        }

        /** package private factory method for test only */
        static DBTargetConfig forTest(String connectString, String username, String password, String database) {
            return new DBTargetConfig(connectString, username, password, database);
        }

        private final String connectString;
        private final String username;
        private final String password;
        private final String database;

        /**
         * Configure this DBTargetConfig object from the properties suffixed
         * with '.N', falling back to the non-suffixed value as necessary.
         * @param id
         * @param props
         */
        private DBTargetConfig(int id, VerifiableProperties props) {
            this(get(id, props, KAFKA_INFLUX_METRICS_CONNECT_CONFIG, "http://localhost:8086"),
                    get(id, props, KAFKA_INFLUX_METRICS_USERNAME_CONFIG, "root"),
                    get(id, props, KAFKA_INFLUX_METRICS_PASSWORD_CONFIG, "root"),
                    get(id, props, KAFKA_INFLUX_METRICS_DATABASE_CONFIG, "kafka"));
        }

        private DBTargetConfig(String connectString, String username, String password, String database) {
            this.connectString = connectString;
            this.database = database;
            this.username = username;
            this.password = password;
        }

        String getConnectString() {
            return this.connectString;
        }

        String getDatabase() {
            return this.database;
        }

        String getUsername() {
            return this.username;
        }

        String getPassword() {
            return this.password;
        }

        /**
         * One-stop shop to get base64 encoded credentials for basic auth
         * @return
         */
        String getBasicAuthCredentials() {
            return Base64.encode((getUsername() + ":" + getPassword()).getBytes());
        }
    }

    /**
     * Creates a new {@link InfluxDBMetricsConfig} instance.
     *
     * @param props  The Kafka configuration.
     */
    InfluxDBMetricsConfig(VerifiableProperties props) {
        int numTargets = DBTargetConfig.deduceNumDBTargets(props);

        for (int i = 1; i <= numTargets; i++) {
            targets.add(new DBTargetConfig(i, props));
        }
        if( props.containsKey(KAFKA_INFLUX_METRICS_CONSISTENCY_CONFIG) )
            this.consistency = props.getString(KAFKA_INFLUX_METRICS_CONSISTENCY_CONFIG);
        if( props.containsKey(KAFKA_INFLUX_METRICS_TAGS_CONFIG) ) {
            this.retention = props.getString(KAFKA_INFLUX_METRICS_RETENTION_CONFIG);
            checkRetentionPolicies();
        }
        this.quantizeReportPeriod = props.getBoolean(KAFKA_INFLUX_METRICS_QUANTIZE_TO_TIME_PERIOD, false);
        setAdditionalTags(props);
        setMeasuresFilters(props);
        setOmit(props);
    }

    List<DBTargetConfig> getDBTargetConfig() {
        return targets;
    }

    private void setMeasuresFilters(VerifiableProperties props) {
        for (MetricsPredicate.Measures measure : MetricsPredicate.Measures.values()) {
            boolean enable = props.getBoolean(KAFKA_GRAPHITE_MEASURE_ENABLED_CONFIG + "." + measure.name(), true);
            this.predicates.put(measure, enable);
        }
    }

    private void setAdditionalTags(VerifiableProperties props) {
        String tags = props.getString(KAFKA_INFLUX_METRICS_TAGS_CONFIG, null);
        if( tags != null) {
            for(String tag : tags.split(",") ) {
                String[] tagAndValue = tag.split("=");
                addTag(tagAndValue[0], tagAndValue[1]);
            }
        }
    }

    private void checkRetentionPolicies() {
        if( ! RETENTION_POLICIES.contains(this.retention)) {
            throw new IllegalArgumentException(
                    String.format("Unknown retention policy '%s': %s", retention, RETENTION_POLICIES ));
        }
    }

    private void setOmit(VerifiableProperties props) {
        String omissions = props.getString(KAFKA_INFLUX_METRICS_OMIT_CONFIG, null);
        if( omissions != null ) {
            for(String metricName : omissions.split(",") ) {
                omit.add(metricName);
            }
        }
    }


    public void setConsistency(String consistency) {
        this.consistency = consistency;
    }

    public void setRetention(String retention) {
        this.retention = retention;
    }

    public void addTag(String tag, String value) {
        this.tags.put(tag, value);
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    Map<String, String> getTags() {
        return this.tags;
    }

    String getConsistency() {
        return this.consistency;
    }

    String getRetention() {
        return this.retention;
    }

    Map<MetricsPredicate.Measures, Boolean> getPredicates() {
        return predicates;
    }

    boolean omit(MetricName metric) {
        return omit.contains(metric.getName());
    }

    /** If true, then the user has asked that reporting periods are quantized, ie. snapped to
     *  a consistent value. For example if a period of 60s has been requested then all nodes will
     *  poll for their metrics at the top of the minute. If false, then there will be no (rough)
     *  synchronicity between the reporting times of each node in a cluster.
     */
    boolean quantizeReportingPeriod() {
        return this.quantizeReportPeriod;
    }
}
