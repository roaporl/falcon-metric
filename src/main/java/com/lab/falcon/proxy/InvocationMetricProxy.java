package com.lab.falcon.proxy;

/**
 * Created by Aporl on 2014/11/28.
 */

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.lab.falcon.metric.MetricManager;
import com.lab.falcon.metric.MetricView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InvocationMetricProxy implements InvocationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(InvocationMetricProxy.class);
    private static final String EXCEPTION_METRIC_PREFIX = "EXCEPTION_METRIC_PREFIX";
    private static final String METHOD_METRIC_PREFIX = "METHOD_METRIC_PREFIX";
    private static final String ALL_METHODS_UNEXPECTED_FAIL = "ALL_METHODS_UNEXPECTED_FAIL";
    private Set<Method> validMethods = null;
    private Object serviceImpl;
    private Set<Class<?>> ignoreExceptions = new HashSet<Class<?>>();
    private Map<Method, Set<Class<?>>> declaredExceptions = new HashMap<Method, Set<Class<?>>>();
    private MetricView view;
    private MetricManager metricManager = MetricManager.getInstance();
    private String header = "";

    public InvocationMetricProxy(Object serviceImpl, Object realServiceImpl, MetricView view, Class<?>[] ifaces, String header) {
        this.view = view;
        this.serviceImpl = serviceImpl;
        this.validMethods = getValidMethods(ifaces);
        MetricIgnoreException ignored = realServiceImpl.getClass().getAnnotation(MetricIgnoreException.class);
        if (ignored != null) {
            this.ignoreExceptions.addAll(Arrays.asList(ignored.value()));
        }
        this.header = header != null ? header : serviceImpl.getClass().getName();
    }

    private Set<Method> getValidMethods(Class<?>[] ifaces) {
        Set<Method> methodSet = new HashSet<Method>();
        for (Class<?> iface : ifaces) {
            methodSet.addAll(Arrays.asList(iface.getDeclaredMethods()));
        }
        return methodSet;
    }


    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
        boolean isValidMethod = validMethods.contains(method);
        if (metricManager == null) {
            return method.invoke(serviceImpl, args);
        }

        long start = metricManager.getTick();
        try {
            return method.invoke(serviceImpl, args);
        } catch (InvocationTargetException ite) {
            Throwable t = (ite.getCause() != null) ? ite.getCause() : ite;
            logException(t, method, isValidMethod, true);
            throw t;
        } catch (Throwable r) {
            logException(r, method, isValidMethod, true);
            throw r;
        } finally {
            if (isValidMethod) {
                metricManager.time(view, start, header + method.getName());
            }
        }
    }

    private void logException(Throwable t, Method method, boolean incrementMetric, boolean isFirstException) {
        if (isFirstException && isUserDeclaredException(method, t)) {
            LOGGER.info("User defined exception was thrown.", t);
        }

        //report all the failures
        if (metricManager == null) {
            return;
        }

        if (isFirstException && this.ignoreExceptions.contains(t.getClass())) {
            return;
        }

        //report unexpected failures.
        if (t != null) {
            metricManager.mark(view, ALL_METHODS_UNEXPECTED_FAIL);
            metricManager.mark(view, EXCEPTION_METRIC_PREFIX + t.getClass().getName());
            if (incrementMetric) {
                metricManager.mark(view, METHOD_METRIC_PREFIX + method.getName() + "_fail");
            }
            logException(t.getCause(), method, false, false);
        }
    }

    private boolean isUserDeclaredException(final Method method, Throwable t) {
        synchronized (method) {
            Set<Class<?>> exceptions = declaredExceptions.get(method);
            if (exceptions == null) {
                exceptions = Sets.newConcurrentHashSet(Arrays.asList(method.getExceptionTypes()));
//        exceptions.remove(TException.class);
                declaredExceptions.put(method, exceptions);
            }
            return exceptions.contains(t.getClass());
        }
    }

}
