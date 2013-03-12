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
package com.codefollower.lealone.hbase.command;

import java.util.ArrayList;

import com.codefollower.lealone.command.Command;
import com.codefollower.lealone.command.Parser;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.command.ddl.AlterSequence;
import com.codefollower.lealone.command.dml.Delete;
import com.codefollower.lealone.command.dml.Insert;
import com.codefollower.lealone.command.dml.Select;
import com.codefollower.lealone.command.dml.Update;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.Sequence;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.command.ddl.AlterSequenceNextValueMargin;
import com.codefollower.lealone.hbase.command.ddl.CreateColumnFamily;
import com.codefollower.lealone.hbase.command.ddl.CreateHBaseTable;
import com.codefollower.lealone.hbase.command.ddl.Options;
import com.codefollower.lealone.hbase.command.dml.HBaseDelete;
import com.codefollower.lealone.hbase.command.dml.HBaseInsert;
import com.codefollower.lealone.hbase.command.dml.HBaseSelect;
import com.codefollower.lealone.hbase.command.dml.HBaseUpdate;
import com.codefollower.lealone.hbase.command.dml.InTheRegion;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.engine.HBaseDatabase;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.util.New;

public class HBaseParser extends Parser {

    public HBaseParser(Session session) {
        super(session);
    }

    @Override
    protected Prepared parsePrepared(char first) {
        if (first == 'I' || first == 'i') {
            if (readIf("IN") && readIf("THE") && readIf("REGION")) {
                return parseInTheRegion();
            }
        }
        return null;
    }

    private Prepared parseInTheRegion() {
        String regionName = readString();
        Prepared p = parsePrepared();
        return new InTheRegion(session, regionName, p);
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

    @Override
    protected AlterSequence parseAlterSequence() {
        String sequenceName = readIdentifierWithSchema();
        Sequence sequence = getSchema().getSequence(sequenceName);
        if (readIf("NEXT")) {
            readIf("VALUE");
            readIf("MARGIN");
            return new AlterSequenceNextValueMargin(session, sequence.getSchema(), sequence);
        }
        AlterSequence command = new AlterSequence(session, sequence.getSchema());
        command.setSequence(sequence);
        if (readIf("RESTART")) {
            read("WITH");
            command.setStartWith(readExpression());
        }
        if (readIf("INCREMENT")) {
            read("BY");
            command.setIncrement(readExpression());
        }
        return command;
    }

    @Override
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

    @Override
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

    @Override
    protected Table readTableOrView(String tableName) {
        try {
            return super.readTableOrView(tableName);
        } catch (Exception e) {
            ((HBaseDatabase) database).refreshMetaTable();
            return super.readTableOrView(tableName);
        }
    }

    @Override
    public Command prepareCommand(String sql, boolean isLocal) {
        Command command = super.prepareCommand(sql, isLocal);
        if (isLocal)
            return command;
        //sql有可能在本机执行，也可能需要继续转发给其他节点
        command = new CommandProxy(this, sql, command);
        return command;
    }

    @Override
    public Insert createInsert(Session session) {
        return new HBaseInsert(session);
    }

    @Override
    public Update createUpdate(Session session) {
        return new HBaseUpdate(session);
    }

    @Override
    public Delete createDelete(Session session) {
        return new HBaseDelete(session);
    }

    @Override
    public Select createSelect(Session session) {
        return new HBaseSelect(session);
    }

}
