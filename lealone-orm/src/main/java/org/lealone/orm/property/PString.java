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
 *
 * @param <R> the root model bean type
 */
public class PString<R> extends PBaseComparable<R, String> {

    private String value;

    public PString(String name, R root) {
        super(name, root);
    }

    private PString<R> P(Model<?> model) {
        return this.<PString<R>> getModelProperty(model);
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R ieq(String value) {
        return iequalTo(value);
    }

    /**
     * Case insensitive is equal to.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R iequalTo(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).iequalTo(value);
        }
        expr().ieq(name, value);
        return root;
    }

    /**
     * Like - include '%' and '_' placeholders as necessary.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R like(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).like(value);
        }
        expr().like(name, value);
        return root;
    }

    /**
     * Starts with - uses a like with '%' wildcard added to the end.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R startsWith(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).startsWith(value);
        }
        expr().startsWith(name, value);
        return root;
    }

    /**
     * Ends with - uses a like with '%' wildcard added to the beginning.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R endsWith(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).endsWith(value);
        }
        expr().endsWith(name, value);
        return root;
    }

    /**
     * Contains - uses a like with '%' wildcard added to the beginning and end.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R contains(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).contains(value);
        }
        expr().contains(name, value);
        return root;
    }

    /**
     * Case insensitive like.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R ilike(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).ilike(value);
        }
        expr().ilike(name, value);
        return root;
    }

    /**
     * Case insensitive starts with.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R istartsWith(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).istartsWith(value);
        }
        expr().istartsWith(name, value);
        return root;
    }

    /**
     * Case insensitive ends with.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R iendsWith(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).iendsWith(value);
        }
        expr().iendsWith(name, value);
        return root;
    }

    /**
     * Case insensitive contains.
     *
     * @param value the equal to bind value
     * @return the root model bean instance
     */
    public R icontains(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).icontains(value);
        }
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
        Model<?> model = getModel();
        if (model != root) {
            return P(model).match(value);
        }
        expr().match(name, value);
        return root;
    }

    public final R set(String value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueString.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(value.toString());
    }

    public final String get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
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
