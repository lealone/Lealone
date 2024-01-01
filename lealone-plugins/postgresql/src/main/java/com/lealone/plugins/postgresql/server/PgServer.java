/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.postgresql.server;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.ScriptReader;
import com.lealone.common.util.Utils;
import com.lealone.db.Database;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.result.Result;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.net.WritableChannel;
import com.lealone.plugins.postgresql.sql.expression.function.PgFunctionFactory;
import com.lealone.server.AsyncServer;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.SQLStatement;

public class PgServer extends AsyncServer<PgServerConnection> {

    private static final Logger logger = LoggerFactory.getLogger(PgServer.class);

    public static final String PG_VERSION = "8.2.23";
    public static final String PG_CATALOG_FILE = "/com/lealone/plugins/postgresql/resources/pg_catalog.sql";
    public static final int DEFAULT_PORT = 5432;

    private final HashSet<Integer> typeSet = new HashSet<>();
    private boolean trace;

    @Override
    public String getType() {
        return PgServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        trace = Boolean.parseBoolean(config.get("trace"));
    }

    @Override
    public synchronized void start() {
        super.start();

        // 创建默认的 postgres 数据库
        String sql = "CREATE DATABASE IF NOT EXISTS postgres" //
                + " PARAMETERS(DEFAULT_SQL_ENGINE='" + PgServerEngine.NAME + "')";
        LealoneDatabase.getInstance().getSystemSession().executeUpdateLocal(sql);

        // 创建默认的 postgres 用户
        sql = "CREATE USER IF NOT EXISTS postgres PASSWORD 'postgres' ADMIN";
        Database db = LealoneDatabase.getInstance().findDatabase("postgres");
        if (!db.isInitialized())
            db.init();
        db.getSystemSession().executeUpdateLocal(sql);

        // 注册内置函数工厂
        PgFunctionFactory.register();
    }

    @Override
    protected int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected PgServerConnection createConnection(WritableChannel writableChannel, Scheduler scheduler) {
        return new PgServerConnection(this, writableChannel, scheduler);
    }

    @Override
    protected void beforeRegister(PgServerConnection conn, Scheduler scheduler) {
        conn.setProcessId(getConnectionSize());
    }

    public void trace(String msg) {
        if (trace)
            logger.info(msg);
    }

    public void traceError(Throwable e) {
        logger.error("", e);
    }

    public boolean getTrace() {
        return trace;
    }

    public HashSet<Integer> getTypeSet() {
        return typeSet;
    }

    public void checkType(int type) {
        if (!typeSet.contains(type)) {
            logger.info("Unsupported type: " + type);
        }
    }

    public void createBuiltInSchemas(ServerSession session) throws SQLException {
        synchronized (this) {
            // better would be: set the database to exclusive mode
            Schema schema = session.getDatabase().findSchema(session, "PG_CATALOG");
            if (schema == null || schema.getTableOrView(session, "PG_VERSION") != null) {
                PgServer.installPgCatalog(session, false);
            }
            Result r = session.executeQueryLocal("SELECT * FROM PG_CATALOG.PG_VERSION");
            if (!r.next() || r.currentRow()[0].getInt() < 2) {
                // installation incomplete, or old version
                PgServer.installPgCatalog(session, false);
            } else {
                // version 2 or newer: check the read version
                int versionRead = r.currentRow()[1].getInt();
                if (versionRead > 2) {
                    throw DbException.throwInternalError("Incompatible PG_VERSION");
                }
            }
            r.close();
        }
        session.executeUpdateLocal("set search_path = public, pg_catalog");
        HashSet<Integer> typeSet = getTypeSet();
        if (typeSet.isEmpty()) {
            Result r = session.executeQueryLocal("SELECT OID FROM PG_CATALOG.PG_TYPE");
            while (r.next()) {
                typeSet.add(r.currentRow()[0].getInt());
            }
            r.close();
        }
    }

    public static void installPgCatalog(ServerSession session, boolean trace) throws SQLException {
        String schemaName = session.getCurrentSchemaName();
        session.setCurrentSchemaName("PG_CATALOG");
        Reader r = null;
        try {
            r = Utils.getResourceAsReader(PgServer.PG_CATALOG_FILE);
            ScriptReader reader = new ScriptReader(r);
            while (true) {
                String sql = reader.readStatement();
                if (sql == null) {
                    break;
                }
                if (trace)
                    logger.info("execute sql: " + sql);
                PreparedSQLStatement stmt = session.prepareStatementLocal(sql);
                if (SQLStatement.NO_OPERATION == stmt.getType())
                    continue;
                if (stmt.isQuery())
                    session.executeQueryLocal(stmt);
                else
                    session.executeUpdateLocal(stmt);
            }
            reader.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Can not read pg_catalog resource");
        } finally {
            session.setCurrentSchemaName(schemaName);
            IOUtils.closeSilently(r);
        }
    }
}
