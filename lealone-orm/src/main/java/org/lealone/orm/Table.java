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
package org.lealone.orm;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.db.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.orm.typequery.PArray;
import org.lealone.orm.typequery.PBigDecimal;
import org.lealone.orm.typequery.PBoolean;
import org.lealone.orm.typequery.PByte;
import org.lealone.orm.typequery.PDouble;
import org.lealone.orm.typequery.PFloat;
import org.lealone.orm.typequery.PInteger;
import org.lealone.orm.typequery.PLong;
import org.lealone.orm.typequery.PShort;
import org.lealone.orm.typequery.PSqlDate;
import org.lealone.orm.typequery.PString;
import org.lealone.orm.typequery.PTime;
import org.lealone.orm.typequery.PTimestamp;
import org.lealone.orm.typequery.PUuid;

public class Table {

    private final org.lealone.db.table.Table dbTable;
    private final Database db;

    public Table(String url, String tableName) {
        db = new Database(url);
        dbTable = db.getDbTable(tableName);
        dbTable.getTemplateRow();
    }

    Table(Database db, org.lealone.db.table.Table dbTable) {
        this.db = db;
        this.dbTable = dbTable;
    }

    public void save(Object bean) {
        dbTable.getColumns();
    }

    public void save(Object... beans) {
        for (Object o : beans) {
            save(o);
        }
    }

    public boolean delete(Object bean) {
        return false;
    }

    org.lealone.db.table.Table getDbTable() {
        return dbTable;
    }

    ServerSession getSession() {
        return db.getSession();
    }

    public Database getDatabase() {
        return db;
    }

    public void genJavaCode(String path, String packageName) {
        StringBuilder buff = new StringBuilder();
        StringBuilder methods = new StringBuilder();
        String className = CamelCaseHelper.toCamelFromUnderscore(dbTable.getName());
        className = Character.toUpperCase(className.charAt(0)) + className.substring(1);

        String qclassName = 'Q' + className;
        StringBuilder qbuff = new StringBuilder();
        StringBuilder qfields = new StringBuilder();
        StringBuilder qinit = new StringBuilder();
        HashSet<String> qimportSet = new HashSet<>();

        String queryTypePackageName = PInteger.class.getPackage().getName();
        int queryTypePackageNameLength = queryTypePackageName.length();

        buff.append("package ").append(packageName).append(";\r\n");
        buff.append("\r\n");
        buff.append("import org.lealone.orm.Table;\r\n");
        buff.append("\r\n");
        buff.append("/**\r\n");
        buff.append(" * Model bean for table '").append(dbTable.getName()).append("'.\r\n");
        buff.append(" *\r\n");
        buff.append(" * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.\r\n");
        buff.append(" */\r\n");
        buff.append("public class ").append(className).append(" {\r\n");
        buff.append("\r\n");
        buff.append("    public static ").append(className).append(" create(String url) {\r\n");
        buff.append("        Table t = new Table(url, \"").append(dbTable.getName()).append("\");\r\n");
        buff.append("        return new ").append(className).append("(t);\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");
        buff.append("    private Table _t_;\r\n");
        buff.append("\r\n");
        for (Column c : dbTable.getColumns()) {
            int type = c.getType();
            String typeClassName = DataType.getTypeClassName(type);
            if (typeClassName.startsWith("java.lang.")) {
                typeClassName = typeClassName.substring(10);
            }

            String queryTypeClassName = getQueryTypeClassName(type);
            queryTypeClassName = queryTypeClassName.substring(queryTypePackageNameLength + 1);
            qimportSet.add(queryTypeClassName);
            String columnName = CamelCaseHelper.toCamelFromUnderscore(c.getName());
            String columnNameFirstUpperCase = Character.toUpperCase(columnName.charAt(0)) + columnName.substring(1);
            buff.append("    private ").append(typeClassName).append(" ").append(columnName).append(";\r\n");

            qfields.append("    public final ").append(queryTypeClassName).append('<').append(qclassName).append("> ")
                    .append(columnName).append(";\r\n");

            // 例如: this.id = new PLong<>("id", this);
            qinit.append("        this.").append(columnName).append(" = new ").append(queryTypeClassName)
                    .append("<>(\"").append(columnName).append("\", this);\r\n");

            // setter
            methods.append("\r\n");
            methods.append("    public ").append(className).append(" set").append(columnNameFirstUpperCase).append('(')
                    .append(typeClassName);
            methods.append(' ').append(columnName).append(") {\r\n");
            methods.append("        this.").append(columnName).append(" = ").append(columnName).append("; \r\n");
            methods.append("        return this;\r\n");
            methods.append("    }\r\n");

            // getter
            methods.append("\r\n");
            methods.append("    public ").append(typeClassName).append(" get").append(columnNameFirstUpperCase)
                    .append("() { \r\n");
            methods.append("        return ").append(columnName).append("; \r\n");
            methods.append("    }\r\n");
        }
        buff.append("\r\n");
        buff.append("    public ").append(className).append("() {\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");
        buff.append("    private ").append(className).append("(Table t) {\r\n");
        buff.append("        this._t_ = t;\r\n");
        buff.append("    }\r\n");
        buff.append(methods);
        buff.append("\r\n");
        buff.append("    public void save() {\r\n");
        buff.append("        _t_.save(this);\r\n");
        buff.append("    }\r\n");
        buff.append("\r\n");
        buff.append("    public boolean delete() {\r\n");
        buff.append("       return _t_.delete(this);\r\n");
        buff.append("    }\r\n");
        buff.append("}\r\n");
        // System.out.println(buff);

        // 查询器类
        qbuff.append("package ").append(packageName).append(";\r\n\r\n");
        qbuff.append("import org.lealone.orm.Query;\r\n");
        qbuff.append("import org.lealone.orm.Table;\r\n\r\n");
        for (String i : qimportSet) {
            qbuff.append("import ").append(queryTypePackageName).append('.').append(i).append(";\r\n");
        }
        qbuff.append("\r\n");
        qbuff.append("/**\r\n");
        qbuff.append(" * Query bean for model '").append(className).append("'.\r\n");
        qbuff.append(" *\r\n");
        qbuff.append(" * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.\r\n");
        qbuff.append(" */\r\n");
        // 例如: public class QCustomer extends Query<Customer, QCustomer> {
        qbuff.append("public class ").append(qclassName).append(" extends Query<").append(className).append(", ")
                .append(qclassName).append("> {\r\n");
        qbuff.append("\r\n");
        qbuff.append("    public static ").append(qclassName).append(" create(String url) {\r\n");
        qbuff.append("        Table t = new Table(url, \"").append(dbTable.getName()).append("\");\r\n");
        qbuff.append("        return new ").append(qclassName).append("(t);\r\n");
        qbuff.append("    }\r\n");
        qbuff.append("\r\n");
        qbuff.append(qfields);
        qbuff.append("\r\n");
        qbuff.append("    private ").append(qclassName).append("(Table t) {\r\n");
        qbuff.append("        super(t);\r\n");
        qbuff.append("        setRoot(this);\r\n");
        qbuff.append("\r\n");
        qbuff.append(qinit);
        qbuff.append("    }\r\n");
        qbuff.append("}\r\n");
        // System.out.println(qbuff);

        if (!path.endsWith(File.separator))
            path = path + File.separator;
        path = path.replace('/', File.separatorChar);
        path = path + packageName.replace('.', File.separatorChar) + File.separatorChar;
        try {
            if (!new File(path).exists()) {
                new File(path).mkdirs();
            }
            BufferedOutputStream modelFile = new BufferedOutputStream(new FileOutputStream(path + className + ".java"));
            modelFile.write(buff.toString().getBytes(Charset.forName("UTF-8")));
            modelFile.close();

            BufferedOutputStream qFile = new BufferedOutputStream(new FileOutputStream(path + qclassName + ".java"));
            qFile.write(qbuff.toString().getBytes(Charset.forName("UTF-8")));
            qFile.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to genJavaCode, path = " + path);
        }
    }

    private static String getQueryTypeClassName(int type) {
        switch (type) {
        case Value.BOOLEAN:
            return PBoolean.class.getName();
        case Value.BYTE:
            return PByte.class.getName();
        case Value.SHORT:
            return PShort.class.getName();
        case Value.INT:
            return PInteger.class.getName();
        case Value.LONG:
            return PLong.class.getName();
        case Value.DECIMAL:
            return PBigDecimal.class.getName();
        case Value.TIME:
            return PTime.class.getName();
        case Value.DATE:
            return PSqlDate.class.getName();
        case Value.TIMESTAMP:
            return PTimestamp.class.getName();
        case Value.BYTES:
            // "[B", not "byte[]";
            return byte[].class.getName(); // TODO
        case Value.UUID:
            return PUuid.class.getName();
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            return PString.class.getName();
        case Value.BLOB:
            // "java.sql.Blob";
            throw DbException.throwInternalError("type=" + type); // return java.sql.Blob.class.getName(); // TODO
        case Value.CLOB:
            // "java.sql.Clob";
            throw DbException.throwInternalError("type=" + type); // return java.sql.Clob.class.getName(); // TODO
        case Value.DOUBLE:
            return PDouble.class.getName();
        case Value.FLOAT:
            return PFloat.class.getName();
        case Value.NULL:
            return null;
        case Value.JAVA_OBJECT:
            // "java.lang.Object";
            throw DbException.throwInternalError("type=" + type); // return Object.class.getName(); // TODO
        case Value.UNKNOWN:
            // anything
            return Object.class.getName();
        case Value.ARRAY:
            return PArray.class.getName();
        case Value.RESULT_SET:
            throw DbException.throwInternalError("type=" + type); // return ResultSet.class.getName(); // TODO
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }
}
