/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm;

import java.math.BigDecimal;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.lealone.db.value.Value;

/**
 * A property used in type query.
 *
 * @param <M> the type of the owning model bean
 */
@SuppressWarnings("unchecked")
public abstract class ModelProperty<M extends Model<M>> {

    protected final String name;
    protected final M model;

    protected String fullName;

    /**
     * Construct with a property name and root instance.
     *
     * @param name the name of the property
     * @param model the model bean instance
     */
    public ModelProperty(String name, M model) {
        this.name = name;
        this.model = model;
    }

    public String getDatabaseName() {
        return model.getDatabaseName();
    }

    public String getSchemaName() {
        return model.getSchemaName();
    }

    public String getTableName() {
        return model.getTableName();
    }

    @Override
    public String toString() {
        return name;
    }

    protected M getModel() {
        return (M) model.maybeCopy();
    }

    protected <P> P getModelProperty(M model) {
        return (P) model.getModelProperty(name);
    }

    private ModelProperty<M> P(M model) {
        return this.<ModelProperty<M>> getModelProperty(model);
    }

    /**
     * Internal method to return the underlying expression builder.
     */
    protected ExpressionBuilder<?> expr() {
        return model.peekExprBuilder();
    }

    /**
     * Is null.
     */
    public M isNull() {
        M m = getModel();
        if (m != model) {
            return P(m).isNull();
        }
        expr().isNull(name);
        return model;
    }

    /**
     * Is not null.
     */
    public M isNotNull() {
        M m = getModel();
        if (m != model) {
            return P(m).isNotNull();
        }
        expr().isNotNull(name);
        return model;
    }

    /**
     * Order by ascending on this property.
     */
    public M asc() {
        M m = getModel();
        if (m != model) {
            return P(m).asc();
        }
        expr().orderBy(name, false);
        return model;
    }

    /**
     * Order by descending on this property.
     */
    public M desc() {
        M m = getModel();
        if (m != model) {
            return P(m).desc();
        }
        expr().orderBy(name, true);
        return model;
    }

    /**
     * Return the property name.
     */
    public String getName() {
        return name;
    }

    protected String getFullName() {
        if (fullName == null) {
            fullName = getSchemaName() + "." + getTableName() + "." + name;
        }
        return fullName;
    }

    public final M eq(ModelProperty<?> p) {
        M m = getModel();
        if (m != model) {
            return P(m).eq(p);
        }
        expr().eq(name, p);
        return model;
    }

    public M set(Object value) {
        return model;
    }

    protected void serialize(Map<String, Object> map) {
    }

    protected void deserialize(Object v) {
    }

    // map存放的是查询结果集某一条记录各个字段的值
    protected void deserialize(HashMap<String, Value> map) {
        Value v = map.get(getFullName());
        if (v != null) {
            deserialize(v);
        }
    }

    // 子类不需要再对参数v做null判断
    protected abstract void deserialize(Value v);

    /**
     * Helper method to check if two objects are equal.
     */
    @SuppressWarnings({ "rawtypes" })
    protected static boolean areEqual(Object obj1, Object obj2) {
        if (obj1 == null) {
            return (obj2 == null);
        }
        if (obj2 == null) {
            return false;
        }
        if (obj1 == obj2) {
            return true;
        }
        if (obj1 instanceof BigDecimal) {
            // Use comparable for BigDecimal as equals
            // uses scale in comparison...
            if (obj2 instanceof BigDecimal) {
                Comparable com1 = (Comparable) obj1;
                return (com1.compareTo(obj2) == 0);
            } else {
                return false;
            }
        }
        if (obj1 instanceof URL) {
            // use the string format to determine if dirty
            return obj1.toString().equals(obj2.toString());
        }
        return obj1.equals(obj2);
    }
}
