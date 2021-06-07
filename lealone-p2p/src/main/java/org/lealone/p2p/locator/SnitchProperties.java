/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.p2p.util.FileUtils;

public class SnitchProperties {
    private static final Logger logger = LoggerFactory.getLogger(SnitchProperties.class);
    public static final String RACKDC_PROPERTY_FILENAME = "lealone-rackdc.properties";

    private final Properties properties;

    public SnitchProperties() {
        properties = new Properties();
        InputStream stream = null;
        String configURL = System.getProperty(RACKDC_PROPERTY_FILENAME);
        try {
            URL url;
            if (configURL == null)
                url = SnitchProperties.class.getClassLoader().getResource(RACKDC_PROPERTY_FILENAME);
            else if (!RACKDC_PROPERTY_FILENAME.equalsIgnoreCase(configURL)) // 方便从测试或运行资源中找
                url = SnitchProperties.class.getClassLoader().getResource(configURL);
            else
                url = new URL(configURL);

            stream = url.openStream(); // catch block handles potential NPE
            properties.load(stream);
        } catch (Exception e) {
            // do not throw exception here, just consider this an incomplete or an empty property file.
            logger.warn("Unable to read {}", ((configURL != null) ? configURL : RACKDC_PROPERTY_FILENAME));
        } finally {
            FileUtils.closeQuietly(stream);
        }
    }

    /**
     * Get a snitch property value or return defaultValue if not defined.
     */
    public String get(String propertyName, String defaultValue) {
        return properties.getProperty(propertyName, defaultValue);
    }
}
