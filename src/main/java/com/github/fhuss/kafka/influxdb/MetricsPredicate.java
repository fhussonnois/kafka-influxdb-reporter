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

import java.util.Map;

public class MetricsPredicate {

    public enum Measures {
        count("count"),
        meanRate("meanRate"),
        m1Rate("1MinuteRate"),
        m5Rate("5MinuteRate"),
        m15Rate("15MinuteRate"),
        min("min"),
        max("max"),
        mean("mean"),
        sum("sum"),
        stddev("stddev"),
        median("median"),
        p50("p50"),
        p75("p75"),
        p95("p95"),
        p98("p98"),
        p99("p99"),
        p999("p999"),
        value("value");

        private String label;

        Measures(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }
    private Map<Measures, Boolean> predicates;

    /**
     * Creates a new {@link MetricsPredicate} instance.
     *
     * @param predicates
     */
    MetricsPredicate(Map<Measures, Boolean> predicates) {
        this.predicates = predicates;
    }

    boolean isEnable(Measures name) {
        Boolean enable = this.predicates.get(name);
        if( enable == null)
            throw new IllegalArgumentException("Unknown measure " + name);
        return enable;
    }

}
