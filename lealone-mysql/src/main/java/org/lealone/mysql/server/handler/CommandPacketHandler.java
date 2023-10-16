/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.server.handler;

import org.lealone.mysql.server.MySQLServerConnection;
import org.lealone.mysql.server.protocol.ErrorCode;
import org.lealone.mysql.server.protocol.ExecutePacket;
import org.lealone.mysql.server.protocol.InitDbPacket;
import org.lealone.mysql.server.protocol.PacketInput;
import org.lealone.mysql.server.protocol.PacketType;

public class CommandPacketHandler implements PacketHandler {

    private final MySQLServerConnection conn;

    public CommandPacketHandler(MySQLServerConnection conn) {
        this.conn = conn;
    }

    private String readSql(PacketInput in) {
        in.position(5);
        // 使用指定的编码来读取数据
        return in.readString("utf-8");
    }

    @Override
    public void handle(PacketInput in) {
        switch (in.type()) {
        case PacketType.COM_QUERY: {
            String sql = readSql(in).trim();
            conn.executeStatement(sql);
            break;
        }
        case PacketType.COM_STMT_PREPARE: {
            String sql = readSql(in);
            conn.prepareStatement(sql);
            break;
        }
        case PacketType.COM_STMT_EXECUTE: {
            ExecutePacket packet = new ExecutePacket();
            packet.read(in, "utf-8", conn.getSession());
            conn.executeStatement(packet);
            break;
        }
        case PacketType.COM_STMT_CLOSE:
            in.position(5);
            conn.closeStatement(in.readInt());
            break;
        case PacketType.COM_INIT_DB:
            InitDbPacket packet = new InitDbPacket();
            packet.read(in);
            conn.initDatabase(packet.database);
            conn.writeOkPacket();
            break;
        case PacketType.COM_QUIT:
            conn.close();
            break;
        case PacketType.COM_PROCESS_KILL: // 直接返回OkPacket
        case PacketType.COM_PING:
            conn.writeOkPacket();
            break;
        default:
            conn.sendErrorMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }
}
