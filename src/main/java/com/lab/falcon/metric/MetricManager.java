package com.lab.falcon.metric;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.lab.falcon.util.HostHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Aporl on 2014/11/28.
 */

public class MetricManager {

    public static final String METRICS_HOST = "com.lab.falcon.metrics.host";
    public static final String METRICS_HOST_TAG = "com.lab.falcon.metrics.host_tag";
    public static final String METRICS_TAG = "com.lab.falcon.metrics.tags";
    public static final String METRICS_REPORT_INTERVAL = "com.lab.falcon.metrics.report.interval.seconds";
    public static final String METRICS_ENABLE_TAG = "com.lab.falcon.metrics.tags.enable";
    // whether report to falcon agent
    public static final String METRICS_ACTUAL_REPORT = "com.lab.falcon.metrics.actual.report";
    // falcon agent counter receiver
    public static final String PERFCOUNTER_AGENT_NAME = "com.lab.falcon.metrics.agent.url";
    // report metric types
    public static final String COUNTER_NAME_MAP = "com.lab.falcon.metrics.counters";
    public static final String PERFCOUNTER_AGENT = "http://127.0.0.1:1988/v1/push";
    public static final String COUNTER_NAME_MAP_DEFAULT;
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricManager.class);
    private static final String TAIL_COUNTER = "counter";
    private static final String TAIL_TIMER = "timer";

    static {
        COUNTER_NAME_MAP_DEFAULT = CounterReporter.T_TIMER + ":" +
                CounterReporter.COUNT + "," +
                CounterReporter.M_RATE + "," +
                CounterReporter.M1_RATE + "," +
                CounterReporter.M5_RATE + "," +
                CounterReporter.MEAN + "," +
                CounterReporter.P75 + "," +
                CounterReporter.P95 + "," +
                CounterReporter.P99 + "," +
                CounterReporter.P999 + "," +
                CounterReporter.STD_DEV + ";" +

                CounterReporter.T_METER + ":" +
                CounterReporter.COUNT + "," +
                CounterReporter.M1_RATE;
    }

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private ScheduledReporter reporter;
    private Clock clock = Clock.defaultClock();

    private MetricManager() {
        try {
            String hostTag = System.getProperty(METRICS_HOST_TAG, "");
            String ip = HostHelper.getLocalHostIp();
            String name = Strings.isNullOrEmpty(hostTag) ? ip : ip + ":" + hostTag;
            String tags = System.getProperty(METRICS_TAG);
            reporter = CounterReporter.forRegistry(metricRegistry)
                    .name(System.getProperty(METRICS_HOST, name))
                    .agentUrl(System.getProperty(PERFCOUNTER_AGENT_NAME, PERFCOUNTER_AGENT))
                    .actualReport(System.getProperty(METRICS_ACTUAL_REPORT, "false").equalsIgnoreCase("true"))
                    .typeSetsMap(genCounterMap(System.getProperty(COUNTER_NAME_MAP, COUNTER_NAME_MAP_DEFAULT)))
                    .tags(tags)
                    .enableTag(System.getProperty(METRICS_ENABLE_TAG, "false").equalsIgnoreCase("true"))
                    .build();
            int reportIntervalSeconds = Integer.valueOf(System.getProperty(METRICS_REPORT_INTERVAL, "60"));
            reporter.start(reportIntervalSeconds < 30 ? 30 : reportIntervalSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.error("CounterReporter started failed : {}", Throwables.getStackTraceAsString(e));
        }
    }

    public static MetricManager getInstance() {
        return MetricManagerHolder.INSTANCE;
    }

    public static void main(String[] args) {
        MetricManager metricManager = MetricManager.getInstance();
        System.out.println(metricManager.genCounterMap("x:a,b;y:d"));
        System.out.println(metricManager.genCounterMap("x:a,;y:;,;:a,;:,;a,;"));
        System.out.println(metricManager.genCounterMap(COUNTER_NAME_MAP_DEFAULT));
    }

    private Map<String, Set<String>> genCounterMap(String value) {
        Map<String, Set<String>> map = new ConcurrentHashMap<String, Set<String>>();
        String[] tokens = value.split(";");
        for (String token : tokens) {
            String[] kvs = token.split(":");
            if (kvs.length < 2) {
                LOGGER.error("Parse Metrics Counter over [{}] failed", token);
                continue;
            }
            if (kvs[0].length() < 1) {
                LOGGER.error("Parse Metrics Counter over [{}] failed", token);
                continue;
            }
            String[] vals = kvs[1].split(",");
            if (vals.length < 1) {
                LOGGER.error("Parse Metrics Counter over [{}] failed, value counter < 1", kvs[1]);
            }
            map.put(kvs[0], Sets.newConcurrentHashSet(Sets.newHashSet(vals)));
            LOGGER.info("Config Metrics Counter Map as {}", map);
        }
        return map;
    }

    /***
     * mark call count
     */
    public void mark(MetricView view, String... tags) {
        for (String tag : tags) {
            metricRegistry.meter(getTargetName(view, tag, TAIL_COUNTER)).mark();
        }
    }

    public void mark(String... tags) {
        for (String tag : tags) {
            metricRegistry.meter(getTargetName(tag, TAIL_COUNTER)).mark();
        }
    }

    /***
     * mark call count and latency
     */
    public void time(MetricView view, long start, String... tags) {
        long nano = clock.getTick() - start;
        for (String tag : tags) {
            metricRegistry.timer(getTargetName(view, tag, TAIL_TIMER)).update(nano, TimeUnit.NANOSECONDS);
        }
    }

    public void time(long start, String... tags) {
        long nano = clock.getTick() - start;
        for (String tag : tags) {
            metricRegistry.timer(getTargetName(tag, TAIL_TIMER)).update(nano, TimeUnit.NANOSECONDS);
        }
    }

    private String getTargetName(String tag, String tail) {
        return String.format("%s-%s", tag, tail);
    }

    private String getTargetName(MetricView view, String tag, String tail) {
        if (view == null) {
            return String.format("view=unknown,%s-%s", tag, tail);
        }
        return String.format("view=%s,%s-%s", view, tag, tail);
    }


    public long getTick() {
        return clock.getTick();
    }

    public double getOneMinuteRate(MetricView view, String tag) {
        return metricRegistry.timer(getTargetName(view, tag, TAIL_TIMER)).getOneMinuteRate();
    }

    public void close() {
        reporter.close();
    }

    private static class MetricManagerHolder {

        static MetricManager INSTANCE = new MetricManager();
    }
}
