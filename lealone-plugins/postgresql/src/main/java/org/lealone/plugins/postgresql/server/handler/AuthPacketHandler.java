/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server.handler;

import java.io.IOException;

import org.lealone.plugins.postgresql.server.PgServer;
import org.lealone.plugins.postgresql.server.PgServerConnection;

// 官方协议文档: https://www.postgresql.org/docs/15/protocol-message-formats.html
public class AuthPacketHandler extends PacketHandler {

    private String userName;
    private String databaseName;
    private String dateStyle = "ISO";

    public AuthPacketHandler(PgServer server, PgServerConnection conn) {
        super(server, conn);
    }

    @Override
    public void handle(int x) throws IOException {
        switch (x) {
        case 0:
            server.trace("Init");
            int version = readInt();
            if (version == 80877102) {
                server.trace("CancelRequest (not supported)");
                server.trace(" pid: " + readInt());
                server.trace(" key: " + readInt());
            } else if (version == 80877103) {
                server.trace("SSLRequest");
                out.write('N');
                out.flush();
            } else {
                if (server.getTrace()) {
                    server.trace("StartupMessage");
                    server.trace(" version " + version + " (" + (version >> 16) + "." + (version & 0xff)
                            + ")");
                }
                while (true) {
                    String param = readString();
                    if (param.length() == 0) {
                        break;
                    }
                    String value = readString();
                    if ("user".equals(param)) {
                        userName = value;
                    } else if ("database".equals(param)) {
                        databaseName = value;
                    } else if ("client_encoding".equals(param)) {
                        // UTF8
                        clientEncoding = value;
                    } else if ("DateStyle".equals(param)) {
                        dateStyle = value;
                    }
                    // extra_float_digits 2
                    // geqo on (Genetic Query Optimization)
                    if (server.getTrace())
                        server.trace(" param " + param + "=" + value);
                }
                conn.initDone();
                sendAuthenticationCleartextPassword();
            }
            break;
        case 'p': {
            server.trace("PasswordMessage");
            String password = readString();
            try {
                conn.createSession(databaseName, userName, password);
                sendAuthenticationOk();
            } catch (Exception e) {
                sendErrorResponse(e);
                conn.stop();
            }
            break;
        }
        default:
            if (server.getTrace())
                server.trace("Unsupported: " + x + " (" + (char) x + ")");
            break;
        }
    }

    private void sendAuthenticationCleartextPassword() throws IOException {
        startMessage('R');
        writeInt(3);
        sendMessage();
    }

    private void sendAuthenticationOk() throws IOException {
        startMessage('R');
        writeInt(0);
        sendMessage();
        sendParameterStatus("client_encoding", clientEncoding);
        sendParameterStatus("DateStyle", dateStyle);
        sendParameterStatus("integer_datetimes", "off");
        sendParameterStatus("is_superuser", "off");
        sendParameterStatus("server_encoding", "SQL_ASCII");
        sendParameterStatus("server_version", PgServer.PG_VERSION);
        sendParameterStatus("session_authorization", userName);
        sendParameterStatus("standard_conforming_strings", "off");
        // TODO PostgreSQL TimeZone
        sendParameterStatus("TimeZone", "CET");
        sendBackendKeyData();
        sendReadyForQuery();
    }

    private void sendBackendKeyData() {
        startMessage('K');
        writeInt(conn.getProcessId());
        writeInt(conn.getProcessId());
        sendMessage();
    }

    private void sendParameterStatus(String param, String value) {
        startMessage('S');
        writeString(param);
        writeString(value);
        sendMessage();
    }
}
