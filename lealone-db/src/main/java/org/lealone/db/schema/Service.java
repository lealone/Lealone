package org.lealone.db.schema;

import org.lealone.common.trace.Trace;
import org.lealone.db.DbObjectType;

public class Service extends SchemaObjectBase {

    private String packageName;
    private String implementBy;

    public Service(Schema schema, int id, String name) {
        super(schema, id, name, Trace.SCHEMA);
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.SERVICE;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getImplementBy() {
        return implementBy;
    }

    public void setImplementBy(String implementBy) {
        this.implementBy = implementBy;
    }

    @Override
    public String getCreateSQL() {
        // TODO Auto-generated method stub
        return null;
    }

}
