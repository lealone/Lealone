/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.cassandra.config;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ConfigurationLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SeedProviderDef;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;

import com.codefollower.lealone.cassandra.server.CassandraTcpServer;

//改编自org.apache.cassandra.config.YamlConfigurationLoader
public class CassandraConfigurationLoader implements ConfigurationLoader {
    private static final Logger logger = LoggerFactory.getLogger(YamlConfigurationLoader.class);

    private final static String DEFAULT_CONFIGURATION = "cassandra.yaml";

    private CassandraTcpServer cassandraTcpServer;

    /**
     * Inspect the classpath to find storage configuration file
     */
    private URL getStorageConfigURL() throws ConfigurationException {
        String configUrl = System.getProperty("cassandra.config");
        if (configUrl == null)
            configUrl = DEFAULT_CONFIGURATION;

        URL url;
        try {
            url = new URL(configUrl);
            url.openStream().close(); // catches well-formed but bogus URLs
        } catch (Exception e) {
            ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
            url = loader.getResource(configUrl);
            if (url == null)
                throw new ConfigurationException("Cannot locate " + configUrl);
        }

        return url;
    }

    public Config loadConfig() throws ConfigurationException {
        InputStream input = null;
        CassandraConfig config = null;
        try {
            URL url = getStorageConfigURL();
            logger.info("Loading settings from {}", url);
            try {
                input = url.openStream();
            } catch (IOException e) {
                // getStorageConfigURL should have ruled this out
                throw new AssertionError(e);
            }
            Constructor constructor = new Constructor(CassandraConfig.class);
            TypeDescription seedDesc = new TypeDescription(SeedProviderDef.class);
            seedDesc.putMapPropertyType("parameters", String.class, String.class);
            constructor.addTypeDescription(seedDesc);
            Yaml yaml = new Yaml(constructor);
            config = (CassandraConfig) yaml.load(input);
        } catch (YAMLException e) {
            throw new ConfigurationException("Invalid yaml", e);
        } finally {
            FileUtils.closeQuietly(input);
        }

        cassandraTcpServer = new CassandraTcpServer(config);
        cassandraTcpServer.start();

        return config;
    }
}
