/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mongo.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.mongo.server.bson.command.BsonCommand;
import org.lealone.mongo.server.bson.command.legacy.LCDelete;
import org.lealone.mongo.server.bson.command.legacy.LCInsert;
import org.lealone.mongo.server.bson.command.legacy.LCQuery;
import org.lealone.mongo.server.bson.command.legacy.LCUpdate;
import org.lealone.net.NetBuffer;
import org.lealone.net.NetBufferOutputStream;
import org.lealone.net.WritableChannel;
import org.lealone.server.AsyncServerConnection;
import org.lealone.server.Scheduler;
import org.lealone.server.SessionInfo;

public class MongoServerConnection extends AsyncServerConnection {

    private static final Logger logger = LoggerFactory.getLogger(MongoServerConnection.class);
    private static final boolean DEBUG = BsonCommand.DEBUG;

    private final BsonDocumentCodec codec = new BsonDocumentCodec();
    private final DecoderContext decoderContext = DecoderContext.builder().build();
    private final EncoderContext encoderContext = EncoderContext.builder().build();

    private final HashMap<UUID, ServerSession> sessions = new HashMap<>();

    @SuppressWarnings("unused")
    private final HashMap<String, LinkedList<PooledSession>> pooledSessionsMap = new HashMap<>();

    private final HashMap<String, SessionInfo> sessionInfoMap = new HashMap<>();

    private final MongoServer server;
    private final Scheduler scheduler;
    private final int connectionId;

    public MongoServerConnection(MongoServer server, WritableChannel channel, Scheduler scheduler,
            int connectionId) {
        super(channel, true);
        this.server = server;
        this.scheduler = scheduler;
        this.connectionId = connectionId;
    }

    @Override
    public void closeSession(SessionInfo si) {
    }

    @Override
    public int getSessionCount() {
        return sessionInfoMap.size();
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public HashMap<UUID, ServerSession> getSessions() {
        return sessions;
    }

    public SessionInfo getSessionInfo(BsonDocument doc) {
        Database db = BsonCommand.getDatabase(doc);
        SessionInfo si = sessionInfoMap.get(db.getName());
        if (si == null) {
            si = new SessionInfo(scheduler, this, getPooledSession(db), -1, -1);
            sessionInfoMap.put(db.getName(), si);
            scheduler.addSessionInfo(si);
        }
        return si;
    }

    public PooledSession getPooledSession(Database db) {
        return new PooledSession(db, BsonCommand.getUser(db), 0, this);
        // LinkedList<PooledSession> pooledSessions = pooledSessionsMap.get(db.getName());
        // PooledSession ps = null;
        // if (pooledSessions == null) {
        // pooledSessions = new LinkedList<>();
        // pooledSessionsMap.put(db.getName(), pooledSessions);
        // } else {
        // ps = pooledSessions.pollFirst();
        // }
        // if (ps == null)
        // ps = new PooledSession(db, BsonCommand.getUser(db), 0, this);
        // scheduler.setCurrentSession(ps);
        // return ps;
    }

    public void addPooledSession(PooledSession ps) {
        // LinkedList<PooledSession> pooledSessions = pooledSessionsMap.get(ps.getDatabase().getName());
        // if (pooledSessions != null && pooledSessions.size() < 8)
        // pooledSessions.add(ps);
    }

    @Override
    public void handleException(Exception e) {
        server.removeConnection(this);
    }

    @Override
    public void close() {
        super.close();
        for (ServerSession s : sessions.values()) {
            s.close();
        }
        sessions.clear();
        sessionInfoMap.clear();
    }

    private void sendMessage(byte[] data) {
        try (NetBufferOutputStream out = new NetBufferOutputStream(writableChannel, data.length,
                scheduler.getDataBufferFactory())) {
            out.write(data);
            out.flush(false);
        } catch (IOException e) {
            logger.error("Failed to send message", e);
        }
    }

    @Override
    public int getPacketLength() {
        int length = (packetLengthByteBuffer.get() & 0xff);
        length |= (packetLengthByteBuffer.get() & 0xff) << 8;
        length |= (packetLengthByteBuffer.get() & 0xff) << 16;
        length |= (packetLengthByteBuffer.get() & 0xff) << 20;
        return length - 4;
    }

    @Override
    public void handle(NetBuffer buffer) {
        if (!buffer.isOnlyOnePacket()) {
            DbException.throwInternalError("NetBuffer must be OnlyOnePacket");
        }
        int requestId = 0;
        int opCode = 0;
        try {
            ByteBuffer byteBuffer = buffer.getByteBuffer();
            ByteBufferBsonInput input = new ByteBufferBsonInput(new ByteBufNIO(byteBuffer));
            requestId = input.readInt32();
            int responseTo = input.readInt32();
            opCode = input.readInt32();
            if (DEBUG)
                logger.info("opCode: {}, requestId: {}, responseTo: {}", opCode, requestId, responseTo);
            switch (opCode) {
            case 2013:
                handleMessage(input, requestId, responseTo, buffer);
                break;
            case 2012:
                handleCompressedMessage(input, requestId, responseTo, buffer);
                break;
            case 2001:
                LCUpdate.execute(input, this);
                break;
            case 2002:
                LCInsert.execute(input, this);
                break;
            case 2004:
                LCQuery.execute(input, requestId, this);
                break;
            case 2006:
                LCDelete.execute(input, this);
                break;
            default:
                logger.warn("Unknow opCode: {}", opCode);
            }
        } catch (Throwable e) {
            logger.error("Failed to handle packet", e);
            sendError(null, requestId, e);
        } finally {
            if (opCode != 2013)
                buffer.recycle();
        }
    }

    // TODO 参考com.mongodb.internal.connection.Compressor
    private void handleCompressedMessage(ByteBufferBsonInput input, int requestId, int responseTo,
            NetBuffer buffer) {
        input.readInt32(); // originalOpcode
        input.readInt32(); // uncompressedSize
        input.readByte(); // compressorId
        input.close();
        sendResponse(requestId);
    }

    private void handleMessage(ByteBufferBsonInput input, int requestId, int responseTo,
            NetBuffer buffer) {
        input.readInt32(); // flagBits
        int type = input.readByte();
        BsonDocument response = null;
        switch (type) {
        case 0: {
            handleCommand(input, requestId, buffer);
            return;
        }
        case 1: {
            break;
        }
        default:
        }
        input.close();
        sendResponse(requestId, response);
    }

    private void handleCommand(ByteBufferBsonInput input, int requestId, NetBuffer buffer) {
        BsonDocument doc = decode(input);
        if (DEBUG)
            logger.info("command: {}", doc.toJson());
        SessionInfo si = getSessionInfo(doc);
        MongoTask task = new MongoTask(this, input, doc, si, requestId, buffer);
        si.submitTask(task);
    }

    public void sendResponse(int requestId) {
        BsonDocument document = new BsonDocument();
        BsonCommand.setWireVersion(document);
        BsonCommand.setOk(document);
        BsonCommand.setN(document, 1);
        sendResponse(requestId, document);
    }

    public void sendResponse(int requestId, BsonDocument document) {
        BasicOutputBuffer out = new BasicOutputBuffer();
        out.writeInt32(0);
        out.writeInt32(requestId);
        out.writeInt32(requestId);
        out.writeInt32(1);

        out.writeInt32(0);
        out.writeInt64(0);
        out.writeInt32(0);
        out.writeInt32(1);

        encode(out, document);

        out.writeInt32(0, out.getPosition());
        sendMessage(out.toByteArray());
        out.close();
    }

    @Override
    public void sendError(Session session, int requestId, Throwable t) {
        logger.error("send error", t);
        BsonDocument document = new BsonDocument();
        BsonCommand.setWireVersion(document);
        BsonCommand.append(document, "ok", 0);
        BsonCommand.setN(document, 1);
        BsonCommand.append(document, "code", DbException.convert(t).getErrorCode());
        BsonCommand.append(document, "errmsg", t.getMessage());
        sendResponse(requestId, document);
    }

    public BsonDocument decode(ByteBufferBsonInput input) {
        BsonBinaryReader reader = new BsonBinaryReader(input);
        return codec.decode(reader, decoderContext);
    }

    private void encode(BasicOutputBuffer out, BsonDocument document) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(out);
        codec.encode(bsonBinaryWriter, document, encoderContext);
    }
}
