package com.lab.falcon.metric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Aporl on 2014/11/28.
 */
public class RateLimiter {

    private final Map<String, Double> rulesMap = new ConcurrentHashMap<String, Double>();
    private final MetricManager metricManager;

    public RateLimiter(MetricManager metricManager) {
        this.metricManager = metricManager;
    }

    public void addRule(String uri, double rate) {
        rulesMap.put(uri, rate);
    }

    private Double getRateInternal(String uri) {
        return rulesMap.get(uri);
    }

    /**
     * Clear all rules.
     */
    public void clear() {
        rulesMap.clear();
    }

    /**
     * @param uri request uri
     * @return true if real rate <= ruled rate.
     */
    public boolean accept(MetricView view, String uri) {
        double currentRate = metricManager.getOneMinuteRate(view, uri);
        Double ruleRate = getRateInternal(uri);
        return ruleRate == null || currentRate <= ruleRate;
    }

    // return 0 if rule not exists.
    public double getRate(String uri) {
        Double ruleRate = getRateInternal(uri);
        return ruleRate != null ? ruleRate : 0;
    }
}
