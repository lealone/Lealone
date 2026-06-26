/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lealone.server.servlet.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation used to declare a listener for various types of event, in a given web application context.<br>
 * <br>
 * The class annotated MUST implement one, (or more), of the following interfaces:
 * {@link com.lealone.server.servlet.http.HttpSessionAttributeListener}, {@link com.lealone.server.servlet.http.HttpSessionListener},
 * {@link com.lealone.server.servlet.ServletContextAttributeListener}, {@link com.lealone.server.servlet.ServletContextListener},
 * {@link com.lealone.server.servlet.ServletRequestAttributeListener}, {@link com.lealone.server.servlet.ServletRequestListener} or
 * {@link com.lealone.server.servlet.http.HttpSessionIdListener} <br>
 * E.g. <code>@WebListener</code><br>
 * <code>public TestListener implements ServletContextListener {</code><br>
 *
 * @since Servlet 3.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface WebListener {

    /**
     * @return description of the listener, if present
     */
    String value() default "";
}
