/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.orm.Model;

/**
 * String property.
 */
public class PString<M extends Model<M>> extends PBaseComparable<M, String> {

    private String value;

    public PString(String name, M model) {
        super(name, model);
    }

    private PString<M> P(M model) {
        return this.<PString<M>> getModelProperty(model);
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M ieq(String value) {
        return iequalTo(value);
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M iequalTo(String value) {
        return expr().ieq(name, value);
    }

    /**
     * Like - include '%' and '_' placeholders as necessary.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M like(String value) {
        return expr().like(name, value);
    }

    /**
     * Starts with - uses a like with '%' wildcard added to the end.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M startsWith(String value) {
        return expr().startsWith(name, value);
    }

    /**
     * Ends with - uses a like with '%' wildcard added to the beginning.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M endsWith(String value) {
        return expr().endsWith(name, value);
    }

    /**
     * Contains - uses a like with '%' wildcard added to the beginning and end.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M contains(String value) {
        return expr().contains(name, value);
    }

    /**
     * Case insensitive like.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M ilike(String value) {
        return expr().ilike(name, value);
    }

    /**
     * Case insensitive starts with.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M istartsWith(String value) {
        return expr().istartsWith(name, value);
    }

    /**
     * Case insensitive ends with.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M iendsWith(String value) {
        return expr().iendsWith(name, value);
    }

    /**
     * Case insensitive contains.
     *
     * @param value the equal to bind value
     * @return the model bean instance
     */
    public M icontains(String value) {
        return expr().icontains(name, value);
    }

    /**
     * Add a full text "Match" expression.
     * <p>
     * This means the query will automatically execute against the document store (ElasticSearch).
     * </p>
     *
     * @param value the match expression
     */
    public M match(String value) {
        return expr().match(name, value);
    }

    public final M set(String value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueString.get(value));
        }
        return model;
    }

    @Override
    public M set(Object value) {
        return set(value.toString());
    }

    public final String get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        map.put(getName(), value);
    }

    @Override
    protected void deserialize(Object v) {
        value = (String) v;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getString();
    }
}
