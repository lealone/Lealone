/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server;

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
import org.lealone.docdb.server.command.BCAggregate;
import org.lealone.docdb.server.command.BCDelete;
import org.lealone.docdb.server.command.BCFind;
import org.lealone.docdb.server.command.BCInsert;
import org.lealone.docdb.server.command.BCOther;
import org.lealone.docdb.server.command.BCUpdate;
import org.lealone.docdb.server.command.BsonCommand;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetBuffer;
import org.lealone.net.NetBufferOutputStream;
import org.lealone.net.WritableChannel;
import org.lealone.server.Scheduler;

public class DocDBServerConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(DocDBServerConnection.class);
    private static final boolean DEBUG = BsonCommand.DEBUG;

    private final BsonDocumentCodec codec = new BsonDocumentCodec();
    private final DecoderContext decoderContext = DecoderContext.builder().build();
    private final EncoderContext encoderContext = EncoderContext.builder().build();

    private final HashMap<UUID, ServerSession> sessions = new HashMap<>();

    @SuppressWarnings("unused")
    private final HashMap<String, LinkedList<PooledSession>> pooledSessionsMap = new HashMap<>();

    private final DocDBServer server;
    private final Scheduler scheduler;
    private final int connectionId;

    protected DocDBServerConnection(DocDBServer server, WritableChannel channel, Scheduler scheduler,
            int connectionId) {
        super(channel, true);
        this.server = server;
        this.scheduler = scheduler;
        this.connectionId = connectionId;
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
    }

    private void sendErrorMessage(Throwable e) {
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

    private final ByteBuffer packetLengthByteBuffer = ByteBuffer.allocateDirect(4);

    @Override
    public ByteBuffer getPacketLengthByteBuffer() {
        return packetLengthByteBuffer;
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
        try {
            ByteBuffer byteBuffer = buffer.getByteBuffer();
            ByteBufferBsonInput input = new ByteBufferBsonInput(new ByteBufNIO(byteBuffer));
            int requestID = input.readInt32();
            int responseTo = input.readInt32();
            int opCode = input.readInt32();
            if (DEBUG)
                logger.info("scheduler: {}", Thread.currentThread().getName());
            if (DEBUG)
                logger.info("opCode: {}, requestID: {}, responseTo: {}", opCode, requestID, responseTo);
            switch (opCode) {
            case 2013: {
                handleMessage(input, requestID, responseTo);
                break;
            }
            case 2004: {
                handleQuery(input, requestID, responseTo, opCode);
                break;
            }
            default:
            }
        } catch (Throwable e) {
            logger.error("Failed to handle packet", e);
            sendErrorMessage(e);
        } finally {
            buffer.recycle();
        }
    }

    private void handleMessage(ByteBufferBsonInput input, int requestID, int responseTo) {
        input.readInt32(); // flagBits
        int type = input.readByte();
        BsonDocument response = null;
        switch (type) {
        case 0: {
            response = handleCommand(input);
            break;
        }
        case 1: {
            break;
        }
        default:
        }
        input.close();
        sendResponse(requestID, response);
    }

    private BsonDocument handleCommand(ByteBufferBsonInput input) {
        BsonDocument doc = decode(input);
        if (DEBUG)
            logger.info("command: {}", doc.toJson());
        String command = doc.getFirstKey().toLowerCase();
        switch (command) {
        case "insert":
            return BCInsert.execute(input, doc, this);
        case "update":
            return BCUpdate.execute(input, doc, this);
        case "delete":
            return BCDelete.execute(input, doc, this);
        case "find":
            return BCFind.execute(input, doc, this);
        case "aggregate":
            return BCAggregate.execute(input, doc, this);
        default:
            return BCOther.execute(input, doc, this, command);
        }
    }

    private void handleQuery(ByteBufferBsonInput input, int requestID, int responseTo, int opCode) {
        input.readInt32();
        input.readCString();
        input.readInt32();
        input.readInt32();
        BsonDocument doc = decode(input);
        if (DEBUG)
            logger.info("query: {}", doc.toJson());
        while (input.hasRemaining()) {
            BsonDocument returnFieldsSelector = decode(input);
            if (DEBUG)
                logger.info("returnFieldsSelector: {}", returnFieldsSelector.toJson());
        }
        input.close();
        sendResponse(requestID);
    }

    private void sendResponse(int requestID) {
        BsonDocument document = new BsonDocument();
        BsonCommand.setWireVersion(document);
        BsonCommand.setOk(document);
        BsonCommand.setN(document, 1);
        sendResponse(requestID, document);
    }

    private void sendResponse(int requestID, BsonDocument document) {
        BasicOutputBuffer out = new BasicOutputBuffer();
        out.writeInt32(0);
        out.writeInt32(requestID);
        out.writeInt32(requestID);
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

    public BsonDocument decode(ByteBufferBsonInput input) {
        BsonBinaryReader reader = new BsonBinaryReader(input);
        return codec.decode(reader, decoderContext);
    }

    private void encode(BasicOutputBuffer out, BsonDocument document) {
        BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(out);
        codec.encode(bsonBinaryWriter, document, encoderContext);
    }
}
