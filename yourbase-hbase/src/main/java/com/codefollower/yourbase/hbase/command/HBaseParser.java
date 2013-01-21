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
package com.codefollower.yourbase.hbase.command;

import java.util.ArrayList;
import com.codefollower.yourbase.command.Parser;
import com.codefollower.yourbase.command.Prepared;
import com.codefollower.yourbase.command.dml.Delete;
import com.codefollower.yourbase.command.dml.Insert;
import com.codefollower.yourbase.command.dml.Select;
import com.codefollower.yourbase.command.dml.Update;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.dbobject.Schema;
import com.codefollower.yourbase.dbobject.table.Column;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.dbobject.table.TableFilter;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.hbase.command.ddl.CreateColumnFamily;
import com.codefollower.yourbase.hbase.command.ddl.CreateHBaseTable;
import com.codefollower.yourbase.hbase.command.ddl.Options;
import com.codefollower.yourbase.hbase.command.dml.HBaseDelete;
import com.codefollower.yourbase.hbase.command.dml.HBaseInsert;
import com.codefollower.yourbase.hbase.command.dml.HBaseSelect;
import com.codefollower.yourbase.hbase.command.dml.HBaseUpdate;
import com.codefollower.yourbase.hbase.dbobject.table.HBaseTable;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.util.New;

public class HBaseParser extends Parser {

    public HBaseParser(Session session) {
        super(session);
    }

    @Override
    protected Prepared parseCreate() {
        if (readIf("HBASE")) {
            return parseCreateHBaseTable();
        } else {
            return super.parseCreate();
        }
    }

    private CreateHBaseTable parseCreateHBaseTable() {
        read("TABLE");
        boolean ifNotExists = readIfNoExists();
        String tableName = readIdentifierWithSchema();
        CreateHBaseTable command = new CreateHBaseTable(session, getSchema(), tableName);

        command.setIfNotExists(ifNotExists);
        Schema schema = getSchema();
        if (readIf("(")) {
            if (!readIf(")")) {
                Options options;
                ArrayList<String> splitKeys;
                do {
                    options = parseOptions();
                    if (options != null)
                        command.setOptions(options);

                    splitKeys = parseSplitKeys();
                    if (splitKeys != null)
                        command.setSplitKeys(splitKeys);

                    if (readIf("COLUMN")) {
                        read("FAMILY");
                        String cfName = readUniqueIdentifier();
                        options = null;
                        if (readIf("(") && !readIf(")")) {
                            do {
                                options = parseOptions();
                                if (options == null)
                                    parseColumn(schema, command, tableName, cfName);
                            } while (readIfMore());
                        }
                        CreateColumnFamily cf = new CreateColumnFamily(session, cfName);
                        cf.setOptions(options);
                        command.addCreateColumnFamily(cf);
                    }
                } while (readIfMore());
            }
        }

        return command;
    }

    private Options parseOptions() {
        if (readIf("OPTIONS")) {
            read("(");
            ArrayList<String> optionNames = New.arrayList();
            ArrayList<String> optionValues = New.arrayList();
            do {
                optionNames.add(readUniqueIdentifier());
                read("=");
                optionValues.add(readString());
            } while (readIfMore());

            return new Options(session, optionNames, optionValues);
        }

        return null;
    }

    private ArrayList<String> parseSplitKeys() {
        if (readIf("SPLIT")) {
            read("KEYS");
            read("(");
            ArrayList<String> keys = New.arrayList();
            do {
                keys.add(readString());
            } while (readIfMore());

            if (keys.size() == 0)
                keys = null;
            return keys;
        }

        return null;
    }

    public Insert createInsert(Session session) {
        return new HBaseInsert(session);
    }

    public Update createUpdate(Session session) {
        return new HBaseUpdate(session);
    }

    public Delete createDelete(Session session) {
        return new HBaseDelete(session);
    }

    public Select createSelect(Session session) {
        return new HBaseSelect(session);
    }

    protected Column parseColumn(Table table) {
        String columnName = readColumnIdentifier();

        if (table instanceof HBaseTable) {
            HBaseTable t = (HBaseTable) table;
            if (t.getRowKeyName().equalsIgnoreCase(columnName))
                return t.getRowKeyColumn();

            String columnFamilyName = null;
            if (readIf(".")) {
                columnFamilyName = columnName;
                if (currentTokenType != IDENTIFIER) {
                    throw DbException.getSyntaxError(sqlCommand, parseIndex, "identifier");
                }
                columnName = currentToken;
                read();
            }
            return t.getColumn(columnFamilyName, columnName, currentPrepared instanceof Insert);
        }

        if (database.getSettings().rowId && Column.ROWID.equals(columnName)) {
            return table.getRowIdColumn();
        }
        return table.getColumn(columnName);
    }

    protected Column readTableColumn(TableFilter filter) {
        String tableAlias = null;
        String columnName = readColumnIdentifier();
        if (readIf(".")) {
            if (filter.getTable() instanceof HBaseTable) {
                String columnFamilyName = columnName;
                columnName = readColumnIdentifier();
                if (readIf(".")) {
                    tableAlias = columnFamilyName;
                    columnFamilyName = columnName;
                    columnName = readColumnIdentifier();
                    if (readIf(".")) {
                        String schema = tableAlias;
                        tableAlias = columnFamilyName;
                        columnFamilyName = columnName;
                        columnName = readColumnIdentifier();
                        if (readIf(".")) {
                            String catalogName = schema;
                            schema = tableAlias;
                            tableAlias = columnFamilyName;
                            columnFamilyName = columnName;
                            columnName = readColumnIdentifier();
                            if (!equalsToken(catalogName, database.getShortName())) {
                                throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, catalogName);
                            }
                        }
                        if (!equalsToken(schema, filter.getTable().getSchema().getName())) {
                            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schema);
                        }
                    }
                    if (!equalsToken(tableAlias, filter.getTableAlias())) {
                        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
                    }
                }
                Column c = ((HBaseTable) filter.getTable()).getColumn(columnFamilyName, columnName,
                        currentPrepared instanceof Insert);
                if (columnFamilyName != null)
                    c.setColumnFamilyName(columnFamilyName);
                return c;
            }
            tableAlias = columnName;
            columnName = readColumnIdentifier();
            if (readIf(".")) {
                String schema = tableAlias;
                tableAlias = columnName;
                columnName = readColumnIdentifier();
                if (readIf(".")) {
                    String catalogName = schema;
                    schema = tableAlias;
                    tableAlias = columnName;
                    columnName = readColumnIdentifier();
                    if (!equalsToken(catalogName, database.getShortName())) {
                        throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, catalogName);
                    }
                }
                if (!equalsToken(schema, filter.getTable().getSchema().getName())) {
                    throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schema);
                }
            }
            if (!equalsToken(tableAlias, filter.getTableAlias())) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
            }
        }
        if (database.getSettings().rowId) {
            if (Column.ROWID.equals(columnName)) {
                return filter.getRowIdColumn();
            }
        }
        return filter.getTable().getColumn(columnName);
    }
}
