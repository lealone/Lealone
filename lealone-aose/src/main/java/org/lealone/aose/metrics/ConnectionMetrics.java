/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.aose.metrics;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

/**
 * Metrics for {@link OutboundTcpConnection}.
 */
public class ConnectionMetrics {
    /** Total number of timeouts happened on this node */
    public static final Meter totalTimeouts = Metrics.newMeter(
            DefaultNameFactory.createMetricName("Connection", "TotalTimeouts", null), "total timeouts",
            TimeUnit.SECONDS);

    /** Number of timeouts for specific IP */
    public final Meter timeouts;

    private final MetricNameFactory factory;

    /**
     * Create metrics for given connection pool.
     *
     * @param ip IP address to use for metrics label
     */
    public ConnectionMetrics(InetAddress ip) {
        // ipv6 addresses will contain colons, which are invalid in a JMX ObjectName
        String address = ip.getHostAddress().replaceAll(":", ".");

        factory = new DefaultNameFactory("Connection", address);

        timeouts = Metrics.newMeter(factory.createMetricName("Timeouts"), "timeouts", TimeUnit.SECONDS);
    }

    public void release() {
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("Timeouts"));
    }
}
