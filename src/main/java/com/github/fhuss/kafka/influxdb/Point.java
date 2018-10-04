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

import java.util.HashMap;
import java.util.Map;

class Point {

    private Map<String, String> fields;
    private Map<String, Object> tags;
    private String measurement;
    private long timestamp;

    public Point(String measurement, long timestamp) {
        this.measurement = measurement;
        this.timestamp = timestamp;
        this.fields = new HashMap<>();
        this.tags = new HashMap<>();
    }

    public Point addField(String field, float value) {
        this.fields.put(field, String.valueOf(value));
        return this;
    }

    public Point addField(String field, double value) {
        this.fields.put(field, Double.toString(value));
        return this;
    }

    public Point addField(String field, long value) {
        this.fields.put(field, Long.toString(value));
        return this;
    }

    public Point addField(String field, int value) {
        this.fields.put(field, Long.toString(value));
        return this;
    }

    public Point addField(String field, String value) {
        this.fields.put(field, "\"" + value + "\"");
        return this;
    }

    public Point addTag(String tag, String value) {
        this.tags.put(tag, value);
        return this;
    }

    public Point addTags(Map<String, String> tags) {
        this.tags.putAll(tags);
        return this;
    }

    public String asLineProtocol() {
        StringBuilder sb = new StringBuilder(measurement);
        for(Map.Entry<String, Object> tag : tags.entrySet()) {
            sb.append(",")
                    .append(tag.getKey())
                    .append("=")
                    .append(tag.getValue());
        }
        sb.append(" ");

        boolean f = true;
        for(Map.Entry<String, String> field : fields.entrySet()) {
            if(!f) sb.append(",");
            sb.append(field.getKey())
                    .append("=")
                    .append(field.getValue());
            f = false;
        }
        sb.append(" ").append(timestamp);

        return sb.toString();
    }
}