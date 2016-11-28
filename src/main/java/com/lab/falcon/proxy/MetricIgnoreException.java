package com.lab.falcon.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by Aporl on 2014/11/28.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MetricIgnoreException {

    /**
     * The Class of exception to be ignored.
     */
    Class<?>[] value();
}