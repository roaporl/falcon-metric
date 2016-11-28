package com.lab.falcon.proxy;

import com.lab.falcon.metric.MetricView;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * Created by Aporl on 2014/11/28.
 */
public class ProxyHelper {

    @SuppressWarnings("unchecked")
    public static <T> T getProxyService(T impl, MetricView view, String header) {
        T realService = impl;
        ClassLoader classLoader = impl.getClass().getClassLoader();
        Class[] ifaces = impl.getClass().getInterfaces();
        if (ifaces.length == 0) {
            throw new RuntimeException("no interface annotated");
        }
        InvocationHandler proxyHandler = new InvocationMetricProxy(impl, realService, view, ifaces, header);
        impl = (T) Proxy.newProxyInstance(classLoader, ifaces, proxyHandler);
        return impl;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getProxyService(T impl, Class<?> iface, MetricView view, String header) {
        T realService = impl;
        ClassLoader classLoader = impl.getClass().getClassLoader();
        InvocationHandler proxyHandler = new InvocationMetricProxy(impl, realService, view, new Class[]{iface}, header);
        impl = (T) Proxy.newProxyInstance(classLoader, new Class[]{iface}, proxyHandler);
        return impl;
    }

    public <T> T proxyService(T impl, MetricView view) {
        return getProxyService(impl, view, null);
    }


    public <T> T proxyService(T impl, Class<T> iface, MetricView view) {
        return getProxyService(impl, iface, view, null);
    }

    public <T> T proxyService(T impl, MetricView view, String header) {
        return getProxyService(impl, view, header);
    }


    public <T> T proxyService(T impl, Class<T> iface, MetricView view, String header) {
        return getProxyService(impl, iface, view, header);
    }
}