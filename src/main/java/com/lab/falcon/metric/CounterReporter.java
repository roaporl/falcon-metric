package com.lab.falcon.metric;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.*;
import com.google.common.base.Throwables;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by Aporl on 2014/11/28.
 */
public class CounterReporter extends ScheduledReporter {

    public static final String T_GAUGE = "gauge";
    public static final String T_TIMER = "timer";
    public static final String T_METER = "meter";
    public static final String T_HISTOGRAM = "histogram";

    public static final String COUNT = "-count";
    public static final String M_RATE = "-meanRate-QPS";
    public static final String M1_RATE = "-1minRate-QPS";
    public static final String M5_RATE = "-5minRate-QPS";
    public static final String M15_RATE = "-15minRate-QPS";
    public static final String MEDIAN = "-median-COST";
    public static final String P75 = "-75th-COST";
    public static final String P95 = "-95th-COST";
    public static final String P99 = "-99th-COST";
    public static final String P999 = "-999th-COST";
    public static final String MEAN = "-mean-COST";
    public static final String STD_DEV = "-stdDev-COST";
    private static final Logger LOGGER = LoggerFactory.getLogger(CounterReporter.class);
    private static final String ENDPOINT = "endpoint";
    private static final String METRIC = "metric";
    private static final String TIMESTAMP = "timestamp";
    private static final String STEP = "step";
    private static final String VALUE = "value";
    private static final String COUNTER_TYPE = "counterType";
    private static final String TAGS = "tags";
    private final String name;
    private final String agentUrl;
    private final boolean actualReport;
    private final Map<String, Set<String>> typeSetsMap;
    private final String tags;
    private final int stepSeconds;
    private final boolean enableTag;

    /**
     * Creates a new {@link ScheduledReporter} instance.
     *
     * @param registry the {@link MetricRegistry} containing the metrics this reporter will report
     * @param name     the reporter's name
     * @param filter   the filter for which metrics to report
     */
    protected CounterReporter(MetricRegistry registry, String name, String agentUrl, MetricFilter filter,
                              TimeUnit rateUnit, TimeUnit durationUnit, boolean actualReport,
                              Map<String, Set<String>> typeSetsMap,
                              String tags, int stepSeconds, boolean enableTag) throws IOException {
        super(registry, name, filter, rateUnit, durationUnit);
        this.name = name;
        this.agentUrl = agentUrl;
        this.actualReport = actualReport;
        this.typeSetsMap = typeSetsMap;
        this.tags = tags;
        this.stepSeconds = stepSeconds;
        this.enableTag = enableTag;
    }

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        long now = System.currentTimeMillis() / 1000;
        try {
            JSONArray jsonArray = new JSONArray();
            collectMeters(jsonArray, now, meters);
            collectTimers(jsonArray, now, timers);
            if (!jsonArray.isEmpty()) {
                reportToAgent(jsonArray.toJSONString());
            }
        } catch (Exception e) {
            LOGGER.error("Report metrics failed : {}", Throwables.getStackTraceAsString(e));
        }
    }

    private void collectTimers(JSONArray jsonArray, long now, SortedMap<String, Timer> timers) {
        Set<String> timerSet = typeSetsMap.get(T_TIMER);
        if (timerSet != null) {
            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                String timerName = entry.getKey();
                Timer timer = entry.getValue();
                Snapshot snapshot = timer.getSnapshot();
                if (timerSet.contains(COUNT)) {
                    jsonArray.add(getJSONGauge(timerName + COUNT, now, timer.getCount()));
                }
                if (timerSet.contains(M_RATE)) {
                    jsonArray.add(getJSONGauge(timerName + M_RATE, now, convertRate(timer.getMeanRate())));
                }
                if (timerSet.contains(M1_RATE)) {
                    jsonArray.add(getJSONGauge(timerName + M1_RATE, now, convertRate(timer.getOneMinuteRate())));
                }
                if (timerSet.contains(M5_RATE)) {
                    jsonArray.add(getJSONGauge(timerName + M5_RATE, now, convertRate(timer.getFiveMinuteRate())));
                }
                if (timerSet.contains(M15_RATE)) {
                    jsonArray.add(getJSONGauge(timerName + M15_RATE, now, convertRate(timer.getFifteenMinuteRate())));
                }

                if (timerSet.contains(P75)) {
                    jsonArray.add(getJSONGauge(timerName + P75, now, convertDuration(snapshot.get75thPercentile())));
                }
                if (timerSet.contains(P95)) {
                    jsonArray.add(getJSONGauge(timerName + P95, now, convertDuration(snapshot.get95thPercentile())));
                }
                if (timerSet.contains(P99)) {
                    jsonArray.add(getJSONGauge(timerName + P99, now, convertDuration(snapshot.get99thPercentile())));
                }
                if (timerSet.contains(P999)) {
                    jsonArray.add(getJSONGauge(timerName + P999, now, convertDuration(snapshot.get999thPercentile())));
                }
                if (timerSet.contains(MEDIAN)) {
                    jsonArray.add(getJSONGauge(timerName + MEDIAN, now, convertDuration(snapshot.getMedian())));
                }
                if (timerSet.contains(MEAN)) {
                    jsonArray.add(getJSONGauge(timerName + MEAN, now, convertDuration(snapshot.getMean())));
                }
                if (timerSet.contains(STD_DEV)) {
                    jsonArray.add(getJSONGauge(timerName + STD_DEV, now, convertDuration(snapshot.getStdDev())));
                }
            }
        }
    }

    private void collectMeters(JSONArray jsonArray, long nowSeconds, SortedMap<String, Meter> meters) {
        Set<String> meterSet = typeSetsMap.get(T_METER);
        if (meterSet != null) {
            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                String meterName = entry.getKey();
                Meter meter = entry.getValue();
                if (meterSet.contains(COUNT)) {
                    jsonArray.add(getJSONGauge(meterName + COUNT, nowSeconds, meter.getCount()));
                }
                if (meterSet.contains(M_RATE)) {
                    jsonArray.add(getJSONGauge(meterName + M_RATE, nowSeconds, convertRate(meter.getMeanRate())));
                }
                if (meterSet.contains(M1_RATE)) {
                    jsonArray.add(getJSONGauge(meterName + M1_RATE, nowSeconds, convertRate(meter.getOneMinuteRate())));
                }
                if (meterSet.contains(M5_RATE)) {
                    jsonArray.add(getJSONGauge(meterName + M5_RATE, nowSeconds, convertRate(meter.getFiveMinuteRate())));
                }
                if (meterSet.contains(M15_RATE)) {
                    jsonArray.add(getJSONGauge(meterName + M15_RATE, nowSeconds, convertRate(meter.getFifteenMinuteRate())));
                }
            }
        }
    }

    private void reportToAgent(String content) {
        if (!actualReport) {
            return;
        }
        CloseableHttpClient client = HttpClients.createDefault();
        try {
            HttpPost postRequest = new HttpPost(agentUrl);
            StringEntity input = new StringEntity(content);
            input.setContentType("application/json");
            postRequest.setEntity(input);
            client.execute(postRequest);
            LOGGER.debug("Post to {} with gauge counters : {}", agentUrl, content);
        } catch (Exception e) {
            LOGGER.error("Report to agent failed, {}", Throwables.getStackTraceAsString(e));
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                LOGGER.error("Client close failed : {}", Throwables.getStackTraceAsString(e));
            }
        }
    }

    private JSONObject getJSONGauge(String metricName, long timestamp, double value) {
        JSONObject json = new JSONObject();
        json.put(ENDPOINT, name);
        if (enableTag) {
            json.put(TAGS, tags);
        }
        json.put(METRIC, metricName);
        json.put(TIMESTAMP, timestamp);
        json.put(STEP, stepSeconds);
        json.put(VALUE, value);
        json.put(COUNTER_TYPE, "GAUGE");
        return json;
    }

    public static class Builder {

        private final MetricRegistry registry;
        private String name;
        private String agentUrl;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private boolean actualReport;
        private Map<String, Set<String>> typeSetsMap;
        private boolean enableTag;
        private String tags;
        private int stepSeconds = 60;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder agentUrl(String agentUrl) {
            this.agentUrl = agentUrl;
            return this;
        }

        public Builder actualReport(boolean actualReport) {
            this.actualReport = actualReport;
            return this;
        }

        public Builder typeSetsMap(Map<String, Set<String>> typeSetsMap) {
            this.typeSetsMap = typeSetsMap;
            return this;
        }

        public Builder tags(String tags) {
            this.tags = tags;
            return this;
        }

        public Builder enableTag(boolean enableTag) {
            this.enableTag = enableTag;
            return this;
        }

        public Builder stepSeconds(int stepSeconds) {
            this.stepSeconds = stepSeconds;
            return this;
        }

        public CounterReporter build() throws IOException {
            return new CounterReporter(registry, name, agentUrl, filter, rateUnit, durationUnit, actualReport, typeSetsMap,
                    tags, stepSeconds, enableTag);
        }
    }
}
