package com.github.fhuss.kafka.influxdb;

import com.github.fhuss.kafka.influxdb.InfluxDBMetricsConfig.DBTargetConfig;
import kafka.utils.VerifiableProperties;
import org.junit.Test;

import java.util.List;
import java.util.Properties;


import static org.junit.Assert.assertEquals;


//
// NOTE: These tests explicitly specify the text strings expected to be found in
//       kafka server properties instead of referring to the constants defined in
//       main classes to avoid accidentally changing the text without breaking the
//       tests.
public class InfluxDBMetricsConfigTest {

    // Verify that DB config as specified in previous releases still works!
    @Test
    public void testConfigBackwardCompatibility() {
        Properties props = new Properties();
        props.put("kafka.influxdb.metrics.address", "http://localhost.localdomain");
        props.put("kafka.influxdb.metrics.database", "database");
        props.put("kafka.influxdb.metrics.username", "username");
        props.put("kafka.influxdb.metrics.password", "password");


        InfluxDBMetricsConfig config = new InfluxDBMetricsConfig(new VerifiableProperties(props));
        List<DBTargetConfig> targets = config.getDBTargetConfig();
        assertEquals(targets.size(), 1);
        DBTargetConfig target = targets.get(0);
        assertEquals("http://localhost.localdomain", target.getConnectString());
        assertEquals("database", target.getDatabase());
        assertEquals("username", target.getUsername());
        assertEquals("password", target.getPassword());
    }

    // Can you just specify a host name, and if so does it pick up appropriate defaults?
    @Test
    public void testSingleton() {
        Properties props = new Properties();
        props.put("kafka.influxdb.metrics.address", "http://localhost.localdomain");


        InfluxDBMetricsConfig config = new InfluxDBMetricsConfig(new VerifiableProperties(props));
        List<DBTargetConfig> targets = config.getDBTargetConfig();
        assertEquals(targets.size(), 1);
        DBTargetConfig target = targets.get(0);
        assertEquals("http://localhost.localdomain", target.getConnectString());
        assertEquals("root", target.getUsername());
        assertEquals("root", target.getPassword());
        assertEquals("kafka", target.getDatabase());
    }


    // fully specified "old style" influx and a qualified .1 will both exist side by side
    @Test
    public void testQualifiedAndPlain() {
        Properties props = new Properties();
        props.put("kafka.influxdb.metrics.address.1", "http://localhost.localdomain:111");
        props.put("kafka.influxdb.metrics.database.1", "database.1");
        props.put("kafka.influxdb.metrics.username.1", "username.1");
        props.put("kafka.influxdb.metrics.password.1", "password.1");

        props.put("kafka.influxdb.metrics.address", "http://localhost.localdomain");
        props.put("kafka.influxdb.metrics.database", "database");
        props.put("kafka.influxdb.metrics.username", "username");
        props.put("kafka.influxdb.metrics.password", "password");


        InfluxDBMetricsConfig config = new InfluxDBMetricsConfig(new VerifiableProperties(props));
        List<DBTargetConfig> targets = config.getDBTargetConfig();
        assertEquals(targets.size(), 2);

        DBTargetConfig target = targets.get(0);
        assertEquals("http://localhost.localdomain:111", target.getConnectString());
        assertEquals("database.1", target.getDatabase());
        assertEquals("username.1", target.getUsername());
        assertEquals("password.1", target.getPassword());

        target = targets.get(1);
        assertEquals("http://localhost.localdomain", target.getConnectString());
        assertEquals("database", target.getDatabase());
        assertEquals("username", target.getUsername());
        assertEquals("password", target.getPassword());

    }

    // simplest configuration where just the hostname is explicitly called out, otherwise
    // all fallback params are used between both targets
    @Test
    public void testClonedInfluxHosts() {
        Properties props = new Properties();
        props.put("kafka.influxdb.metrics.address.1", "http://localhost.localdomain:111");
        props.put("kafka.influxdb.metrics.address.2", "http://localhost.localdomain:222");
        props.put("kafka.influxdb.metrics.database", "database");
        props.put("kafka.influxdb.metrics.username", "username");
        props.put("kafka.influxdb.metrics.password", "password");


        InfluxDBMetricsConfig config = new InfluxDBMetricsConfig(new VerifiableProperties(props));
        List<DBTargetConfig> targets = config.getDBTargetConfig();
        assertEquals(targets.size(), 2);

        DBTargetConfig target = targets.get(0);
        assertEquals("http://localhost.localdomain:111", target.getConnectString());
        assertEquals("database", target.getDatabase());
        assertEquals("username", target.getUsername());
        assertEquals("password", target.getPassword());

        target = targets.get(1);
        assertEquals("http://localhost.localdomain:222", target.getConnectString());
        assertEquals("database", target.getDatabase());
        assertEquals("username", target.getUsername());
        assertEquals("password", target.getPassword());
    }

    // verify that a "gap" in qualifier means the search stops
    @Test
    public void testConfigScanStopsAtGap() {
        Properties props = new Properties();
        props.put("kafka.influxdb.metrics.address.1", "http://localhost.localdomain:111");
        // there is no ".2", so there will be no unqualified target, and .3 and .4 will be ignored
        props.put("kafka.influxdb.metrics.address.3", "http://localhost.localdomain:333");
        props.put("kafka.influxdb.metrics.address.4", "http://localhost.localdomain:444");
        props.put("kafka.influxdb.metrics.database", "database");
        props.put("kafka.influxdb.metrics.username", "username");
        props.put("kafka.influxdb.metrics.password", "password");


        InfluxDBMetricsConfig config = new InfluxDBMetricsConfig(new VerifiableProperties(props));
        List<DBTargetConfig> targets = config.getDBTargetConfig();
        assertEquals(targets.size(), 1);

        DBTargetConfig target = targets.get(0);
        assertEquals("http://localhost.localdomain:111", target.getConnectString());
        assertEquals("database", target.getDatabase());
        assertEquals("username", target.getUsername());
        assertEquals("password", target.getPassword());
    }
}
