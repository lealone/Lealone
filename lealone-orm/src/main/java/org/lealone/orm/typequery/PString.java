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
package org.lealone.orm.typequery;

/**
 * String property.
 *
 * @param <R> the root query bean type
 */
public class PString<R> extends PBaseComparable<R, String> {

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root query bean instance
     */
    public PString(String name, R root) {
        super(name, root);
    }

    /**
     * Construct with additional path prefix.
     */
    public PString(String name, R root, String prefix) {
        super(name, root, prefix);
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R ieq(String value) {
        expr().ieq(name, value);
        return root;
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R iequalTo(String value) {
        expr().ieq(name, value);
        return root;
    }

    /**
     * Like - include '%' and '_' placeholders as necessary.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R like(String value) {
        expr().like(name, value);
        return root;
    }

    /**
     * Starts with - uses a like with '%' wildcard added to the end.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R startsWith(String value) {
        expr().startsWith(name, value);
        return root;
    }

    /**
     * Ends with - uses a like with '%' wildcard added to the beginning.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R endsWith(String value) {
        expr().endsWith(name, value);
        return root;
    }

    /**
     * Contains - uses a like with '%' wildcard added to the beginning and end.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R contains(String value) {
        expr().contains(name, value);
        return root;
    }

    /**
     * Case insensitive like.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R ilike(String value) {
        expr().ilike(name, value);
        return root;
    }

    /**
     * Case insensitive starts with.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R istartsWith(String value) {
        expr().istartsWith(name, value);
        return root;
    }

    /**
     * Case insensitive ends with.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R iendsWith(String value) {
        expr().iendsWith(name, value);
        return root;
    }

    /**
     * Case insensitive contains.
     *
     * @param value the equal to bind value
     * @return the root query bean instance
     */
    public R icontains(String value) {
        expr().icontains(name, value);
        return root;
    }

    /**
     * Add a full text "Match" expression.
     * <p>
     * This means the query will automatically execute against the document store (ElasticSearch).
     * </p>
     *
     * @param value the match expression
     */
    public R match(String value) {
        expr().match(name, value);
        return root;
    }
}
