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
package org.lealone.hbase.command;

import java.util.ArrayList;

import org.lealone.command.Command;
import org.lealone.command.Parser;
import org.lealone.command.Prepared;
import org.lealone.command.ddl.AlterSequence;
import org.lealone.command.ddl.CreateTable;
import org.lealone.command.ddl.DefineCommand;
import org.lealone.command.dml.Delete;
import org.lealone.command.dml.Insert;
import org.lealone.command.dml.Merge;
import org.lealone.command.dml.Select;
import org.lealone.command.dml.Update;
import org.lealone.constant.ErrorCode;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.Sequence;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.Table;
import org.lealone.dbobject.table.TableFilter;
import org.lealone.engine.Session;
import org.lealone.hbase.command.ddl.AlterSequenceNextValueMargin;
import org.lealone.hbase.command.ddl.CreateColumnFamily;
import org.lealone.hbase.command.ddl.CreateHBaseTable;
import org.lealone.hbase.command.ddl.DefineCommandWrapper;
import org.lealone.hbase.command.ddl.Options;
import org.lealone.hbase.command.dml.HBaseDelete;
import org.lealone.hbase.command.dml.HBaseInsert;
import org.lealone.hbase.command.dml.HBaseMerge;
import org.lealone.hbase.command.dml.HBaseSelect;
import org.lealone.hbase.command.dml.HBaseUpdate;
import org.lealone.hbase.engine.HBaseConstants;
import org.lealone.hbase.engine.HBaseDatabase;
import org.lealone.hbase.engine.HBaseSession;
import org.lealone.hbase.util.HBaseUtils;
import org.lealone.message.DbException;
import org.lealone.util.New;

public class HBaseParser extends Parser {
    private final HBaseSession session;

    public HBaseParser(Session session) {
        super(session);
        this.session = (HBaseSession) session;
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
        String[] regionNames = parseRegionNames();
        Prepared p = parsePrepared();
        p.setLocalRegionNames(regionNames);
        return p;
    }

    private String[] parseRegionNames() {
        ArrayList<String> regionNames = New.arrayList();
        do {
            regionNames.add(readString());
        } while (readIf(","));
        return regionNames.toArray(new String[regionNames.size()]);
    }

    @Override
    protected Prepared parseCreate() {
        if (readIf("HBASE")) {
            read("TABLE");
            return parseCreateTable(false, false, true, true);
        } else {
            return super.parseCreate();
        }
    }

    @Override
    protected void parseTableDefinition(Schema schema, CreateTable createTable, String tableName) {
        CreateHBaseTable command = (CreateHBaseTable) createTable;
        Options options;
        ArrayList<String> splitKeys;

        do {
            DefineCommand c = parseAlterTableAddConstraintIf(tableName, schema);
            if (c != null) {
                command.addConstraintCommand(c);
            } else {
                //TABLE级别的选项参数，支持下面两种语法:
                //OPTIONS(...)
                //TABLE OPTIONS(...)
                options = parseTableOptions();
                if (options != null) {
                    command.setTableOptions(options);
                    continue;
                }

                splitKeys = parseSplitKeys();
                if (splitKeys != null) {
                    command.setSplitKeys(splitKeys);
                    continue;
                }

                if (readIf("COLUMN")) {
                    read("FAMILY");
                    //COLUMN FAMILY OPTIONS(...)语法
                    options = parseOptions();
                    if (options != null) {
                        command.setDefaultColumnFamilyOptions(options);
                    } else {
                        //COLUMN FAMILY定义
                        String cfName = readUniqueIdentifier();
                        options = null;
                        if (readIf("(") && !readIf(")")) {
                            do {
                                options = parseOptions();
                                if (options == null)
                                    parseColumn(schema, command, tableName, cfName);
                            } while (readIfMore());
                        }
                        CreateColumnFamily cf = new CreateColumnFamily(session, cfName, options);
                        command.addCreateColumnFamily(cf);
                    }
                } else {
                    parseColumn(schema, command, tableName, null);
                }
            }
        } while (readIfMore());
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

    private Options parseTableOptions() {
        if (readIf("TABLE")) {
            Options options = parseOptions();
            if (options == null)
                throw getSyntaxError();
            else
                return options;
        } else {
            return parseOptions();
        }
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
        if (table.supportsColumnFamily()) {
            String columnName = readColumnIdentifier();
            if (database.getSettings().rowKey && Column.ROWKEY.equals(columnName))
                return table.getRowKeyColumn();
            else if (database.getSettings().rowId && Column.ROWID.equals(columnName))
                return table.getRowIdColumn();

            String columnFamilyName = null;
            if (readIf(".")) {
                columnFamilyName = columnName;
                columnName = readColumnIdentifier();
            }
            return table.getColumn(columnFamilyName, columnName, currentPrepared instanceof Insert);
        } else {
            return super.parseColumn(table);
        }
    }

    //只用于update语句
    @Override
    protected Column readTableColumn(TableFilter filter) {
        Table t = filter.getTable();
        if (!t.supportsColumnFamily())
            return super.readTableColumn(filter);

        //完整的语法是: catalogName(也就是数据库名).tableAlias.columnFamilyName.columnName
        //如果没有使用表别名，tableAlias实际上就是原始表名
        String columnFamilyName = null;
        String columnName = readColumnIdentifier();
        if (readIf(".")) {
            columnFamilyName = columnName;
            columnName = readColumnIdentifier();
            if (readIf(".")) {
                String tableAlias = columnFamilyName;
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
        }
        if (database.getSettings().rowId) {
            if (Column.ROWID.equals(columnName)) {
                return filter.getRowIdColumn();
            }
        }

        if (columnFamilyName == null) {
            return t.getColumn(columnName);
        } else if (!t.doesColumnFamilyExist(columnFamilyName)) {
            //当columnFamilyName不存在时，有可能是想使用简化的tableAlias.columnName语法
            if (!equalsToken(columnFamilyName, filter.getTableAlias())) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, columnFamilyName);
            }

            return t.getColumn(columnName);
        } else {
            return t.getColumn(columnFamilyName, columnName, false);
        }
    }

    @Override
    protected Table readTableOrView(String tableName) {
        try {
            return super.readTableOrView(tableName);
        } catch (Exception e) {
            ((HBaseDatabase) database).refreshDDLRedoTable();
            return super.readTableOrView(tableName);
        }
    }

    @Override
    protected Prepared parse(String sql) {
        Prepared p = super.parse(sql);

        //TODO 
        //1. Master只允许处理DDL
        //2. RegionServer碰到DDL时都转给Master处理
        //        if (session.isMaster() && !(p instanceof DefineCommand) && !(p instanceof TransactionCommand)) {
        //            throw new RuntimeException("Only DDL SQL allowed in master: " + sql);
        //        }

        if (p instanceof DefineCommand) {
            p = new DefineCommandWrapper(session, (DefineCommand) p, sql);
        }
        return p;
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

    @Override
    public Merge createMerge(Session session) {
        return new HBaseMerge(session);
    }

    @Override
    public Command createCommand(Prepared p, String sql) {
        if (HBaseUtils.getConfiguration().getBoolean( //
                HBaseConstants.COMMAND_RETRYABLE, HBaseConstants.DEFAULT_COMMAND_RETRYABLE))
            return new RetryableCommand(this, sql, p);
        else
            return super.createCommand(p, sql);
    }

    @Override
    public CreateHBaseTable createTable(Session session, Schema schema) {
        return new CreateHBaseTable(session, schema);
    }
}
