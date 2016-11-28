package com.lab.falcon.metric;

/**
 * Created by Aporl on 2014/11/28.
 */
public enum MetricView {
    SERVER((byte) 0, "server"), CLIENT((byte) 1, "client");

    private final byte value;
    private final String name;

    MetricView(byte value, String name) {
        this.value = value;
        this.name = name;
    }

    public byte getValue() {
        return value;
    }

    public String getName() {
        return name;
    }
}
