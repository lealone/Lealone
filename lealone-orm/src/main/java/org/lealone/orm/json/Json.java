/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package org.lealone.orm.json;

import org.lealone.orm.json.jackson.JacksonFactory;
import org.lealone.orm.json.spi.JsonCodec;
import org.lealone.orm.json.spi.JsonFactory;
import org.lealone.orm.json.util.ServiceHelper;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Json {

    /**
     *
     */
    public static final JsonCodec CODEC = JsonFactory.INSTANCE.codec();

    /**
     * Load the factory with the {@code ServiceLoader}, when no factory is found then a factory
     * using Jackson will be returned.
     * <br/>
     * When {@code jackson-databind} is available then a codec using it will be used otherwise
     * the codec will only use {@code jackson-core} and provide best effort mapping.
     */
    public static org.lealone.orm.json.spi.JsonFactory load() {
        org.lealone.orm.json.spi.JsonFactory factory = ServiceHelper
                .loadFactoryOrNull(org.lealone.orm.json.spi.JsonFactory.class);
        if (factory == null) {
            factory = JacksonFactory.INSTANCE;
        }
        return factory;
    }

    /**
     * Encode a POJO to JSON using the underlying Jackson mapper.
     *
     * @param obj a POJO
     * @return a String containing the JSON representation of the given POJO.
     * @throws EncodeException if a property cannot be encoded.
     */
    public static String encode(Object obj) throws EncodeException {
        return CODEC.toString(obj);
    }

    /**
     * Encode a POJO to JSON with pretty indentation, using the underlying Jackson mapper.
     *
     * @param obj a POJO
     * @return a String containing the JSON representation of the given POJO.
     * @throws EncodeException if a property cannot be encoded.
     */
    public static String encodePrettily(Object obj) throws EncodeException {
        return CODEC.toString(obj, true);
    }

    /**
     * Decode a given JSON string to a POJO of the given class type.
     * @param str the JSON string.
     * @param clazz the class to map to.
     * @param <T> the generic type.
     * @return an instance of T
     * @throws DecodeException when there is a parsing or invalid mapping.
     */
    public static <T> T decodeValue(String str, Class<T> clazz) throws DecodeException {
        return CODEC.fromString(str, clazz);
    }

    /**
     * Decode a given JSON string.
     *
     * @param str the JSON string.
     *
     * @return a JSON element which can be a {@link JsonArray}, {@link JsonObject}, {@link String}, ...etc if the content is an array, object, string, ...etc
     * @throws DecodeException when there is a parsing or invalid mapping.
     */
    public static Object decodeValue(String str) throws DecodeException {
        return decodeValue(str, Object.class);
    }
}
