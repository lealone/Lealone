/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.main.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.env.EnvScalarConstructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;

public class YamlConstructor extends EnvScalarConstructor {

    // EnvScalarConstructor 不支持 ${LEALONE_HOME}/data 这种语法，所以这里用新的正则表达式实现
    public static final Pattern ENV_FORMAT = Pattern.compile(
            "^\\$\\{\\s*((?<name>\\w+)((?<separator>:?(-|\\?))(?<value>\\S+)?)?)\\s*\\}(?<suffix>\\X*)$");

    public YamlConstructor(Class<? extends Object> theRoot) {
        super(new TypeDescription(theRoot), null, new LoaderOptions());
        this.yamlConstructors.put(ENV_TAG, new ConstructEnv());
    }

    private class ConstructEnv extends AbstractConstruct {
        @Override
        public Object construct(Node node) {
            String val = constructScalar((ScalarNode) node);
            Matcher matcher = ENV_FORMAT.matcher(val);
            matcher.matches();
            String name = matcher.group("name");
            String value = matcher.group("value");
            String separator = matcher.group("separator");
            String suffix = matcher.group("suffix");
            return apply(name, separator, value != null ? value : "", getEnv(name)) + suffix;
        }
    }
}
