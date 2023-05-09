/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.main.config;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.IOUtils;
import org.lealone.main.config.Config.MapPropertyTypeDef;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.MissingProperty;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

public class YamlConfigLoader implements ConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(YamlConfigLoader.class);

    private final static String DEFAULT_CONFIGURATION = "lealone.yaml";

    private static URL getConfigURL() throws ConfigException {
        String configUrl = Config.getProperty("config");
        if (configUrl == null)
            configUrl = DEFAULT_CONFIGURATION;
        URL url;
        try {
            url = new URL(configUrl);
            url.openStream().close(); // catches well-formed but bogus URLs
        } catch (Exception e) {
            try {
                File file = new File(configUrl).getCanonicalFile();
                url = file.toURI().toURL();
                url.openStream().close();
                return url;
            } catch (Exception e2) {
            }
            url = YamlConfigLoader.class.getClassLoader().getResource(configUrl);
        }
        return url;
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
    }

    @Override
    public Config loadConfig() throws ConfigException {
        if (isYamlAvailable()) {
            return YamlLoader.loadConfig();
        } else {
            logger.info("Use default config");
            return null;
        }
    }

    private static boolean isYamlAvailable() {
        try {
            Class.forName("org.yaml.snakeyaml.Yaml");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // 避免对yaml的强依赖
    private static class YamlLoader {
        private static Config loadConfig() throws ConfigException {
            URL url = getConfigURL();
            if (url == null) {
                logger.info("Use default config");
                return null;
            }
            try {
                logger.info("Loading config from {}", url);
                byte[] configBytes;
                try (InputStream is = url.openStream()) {
                    configBytes = IOUtils.toByteArray(is);
                } catch (IOException e) {
                    // getConfigURL should have ruled this out
                    throw new AssertionError(e);
                }

                YamlConstructor configConstructor = new YamlConstructor(Config.class);
                addTypeDescription(configConstructor);

                MissingPropertiesChecker propertiesChecker = new MissingPropertiesChecker();
                configConstructor.setPropertyUtils(propertiesChecker);
                Yaml yaml = new Yaml(configConstructor);
                yaml.addImplicitResolver(YamlConstructor.ENV_TAG, YamlConstructor.ENV_FORMAT, "$");
                Config result = yaml.loadAs(new ByteArrayInputStream(configBytes), Config.class);
                propertiesChecker.check();
                return result;
            } catch (YAMLException e) {
                throw new ConfigException("Invalid yaml", e);
            }
        }

        private static void addTypeDescription(Constructor configConstructor) {
            TypeDescription mapPropertyTypeDesc = new TypeDescription(MapPropertyTypeDef.class);
            mapPropertyTypeDesc.addPropertyParameters("parameters", String.class, String.class);
            configConstructor.addTypeDescription(mapPropertyTypeDesc);
        }
    }

    private static class MissingPropertiesChecker extends PropertyUtils {
        private final Set<String> missingProperties = new HashSet<>();

        public MissingPropertiesChecker() {
            setSkipMissingProperties(true);
        }

        @Override
        public Property getProperty(Class<? extends Object> type, String name) {
            Property result = super.getProperty(type, name);
            if (result instanceof MissingProperty) {
                missingProperties.add(result.getName());
            }
            return result;
        }

        public void check() throws ConfigException {
            if (!missingProperties.isEmpty()) {
                throw new ConfigException("Invalid yaml. " //
                        + "Please remove properties " + missingProperties + " from your lealone.yaml");
            }
        }
    }
}
