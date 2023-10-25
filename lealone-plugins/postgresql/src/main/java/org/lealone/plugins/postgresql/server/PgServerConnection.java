/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Properties;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.PluginManager;
import org.lealone.db.session.ServerSession;
import org.lealone.net.NetBuffer;
import org.lealone.net.WritableChannel;
import org.lealone.plugins.postgresql.server.handler.AuthPacketHandler;
import org.lealone.plugins.postgresql.server.handler.CommandPacketHandler;
import org.lealone.plugins.postgresql.server.handler.PacketHandler;
import org.lealone.server.AsyncServerConnection;
import org.lealone.server.Scheduler;
import org.lealone.server.SessionInfo;
import org.lealone.sql.SQLEngine;

public class PgServerConnection extends AsyncServerConnection {

    private final PgServer server;
    private final Scheduler scheduler;
    private ServerSession session;
    private SessionInfo si;
    private boolean stop;
    private boolean initDone;
    private int processId;
    private PacketHandler packetHandler;

    protected PgServerConnection(PgServer server, WritableChannel writableChannel, Scheduler scheduler) {
        super(writableChannel, true);
        this.server = server;
        this.scheduler = scheduler;
        // 需要先认证，然后再切换到CommandPacketHandler
        packetHandler = new AuthPacketHandler(server, this);
    }

    public void setProcessId(int id) {
        this.processId = id;
    }

    public int getProcessId() {
        return processId;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void closeSession(SessionInfo si) {
    }

    @Override
    public int getSessionCount() {
        return 1;
    }

    public void initDone() {
        initDone = true;
    }

    public void stop() {
        stop = true;
    }

    public void createSession(String databaseName, String userName, String password)
            throws SQLException {
        Properties info = new Properties();
        info.put("MODE", PgServerEngine.NAME);
        info.put("USER", userName);
        info.put("PASSWORD", password);
        info.put("DEFAULT_SQL_ENGINE", PgServerEngine.NAME);
        String url = Constants.URL_PREFIX + Constants.URL_TCP + server.getHost() + ":" + server.getPort()
                + "/" + databaseName;
        ConnectionInfo ci = new ConnectionInfo(url, info);
        ci.setRemote(false);
        session = (ServerSession) ci.createSession();
        si = new SessionInfo(scheduler, this, session, -1, -1);
        scheduler.addSessionInfo(si);
        session.setSQLEngine(PluginManager.getPlugin(SQLEngine.class, PgServerEngine.NAME));

        packetHandler.setSession(session, si); // 旧的设置一次
        packetHandler = new CommandPacketHandler(server, this);
        packetHandler.setSession(session, si); // 新的再设置一次

        server.createBuiltInSchemas(session);
    }

    @Override
    public void close() {
        if (session == null)
            return;
        try {
            stop = true;
            session.close();
            server.trace("Close");
            super.close();
        } catch (Exception e) {
            server.traceError(e);
        }
        session = null;
        server.removeConnection(this);
    }

    private final ByteBuffer packetLengthByteBufferInitDone = ByteBuffer.allocateDirect(5);

    @Override
    public ByteBuffer getPacketLengthByteBuffer() {
        if (initDone)
            return packetLengthByteBufferInitDone;
        else
            return packetLengthByteBuffer;
    }

    @Override
    public int getPacketLength() {
        int len;
        if (initDone) {
            packetLengthByteBufferInitDone.get();
            len = packetLengthByteBufferInitDone.getInt();
            packetLengthByteBufferInitDone.flip();
        } else {
            len = packetLengthByteBuffer.getInt();
            packetLengthByteBuffer.flip();
        }
        return len - 4;
    }

    @Override
    public void handle(NetBuffer buffer) {
        // postgresql执行一条sql要分成5个包: Parse Bind Describe Execute Sync
        // 执行到Execute这一步时会异步提交sql，此时不能继续处理Sync包，否则客户端会提前收到Sync的响应，但sql的结果还看不到
        if (si == null)
            handlePacket(buffer);
        else
            si.submitTask(new PgTask(this, buffer));
    }

    void handlePacket(NetBuffer buffer) {
        if (!buffer.isOnlyOnePacket())
            DbException.throwInternalError("NetBuffer must be OnlyOnePacket");
        if (stop)
            return;
        int x;
        if (initDone) {
            x = packetLengthByteBufferInitDone.get();
            packetLengthByteBufferInitDone.clear();
            if (x < 0) {
                stop = true;
                return;
            }
        } else {
            x = 0;
        }
        packetHandler.handle(buffer, x);
    }
}
