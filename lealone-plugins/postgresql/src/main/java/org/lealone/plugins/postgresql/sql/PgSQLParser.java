/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.sql;

import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionSetting;
import org.lealone.plugins.postgresql.sql.expression.PgVariable;
import org.lealone.sql.SQLParserBase;
import org.lealone.sql.StatementBase;
import org.lealone.sql.dml.NoOperation;
import org.lealone.sql.dml.SetSession;
import org.lealone.sql.expression.Expression;

public class PgSQLParser extends SQLParserBase {

    public PgSQLParser(ServerSession session) {
        super(session);
        this.session.setSchemaSearchPath(new String[] { "public", "pg_catalog" });
    }

    @Override
    protected Expression parseVariable() {
        read("@");
        String vname = readAliasIdentifier();
        if (vname.equalsIgnoreCase("session") || vname.equalsIgnoreCase("global")) {
            readIf(".");
            vname = readAliasIdentifier();
        }
        return new PgVariable(session, vname);
    }

    @Override
    protected boolean parseShowOther(StringBuilder buff) {
        if (readIf("CLIENT_ENCODING")) {
            buff.append("'UNICODE' AS CLIENT_ENCODING");
        } else if (readIf("DEFAULT_TRANSACTION_ISOLATION")) {
            buff.append("'read committed' AS DEFAULT_TRANSACTION_ISOLATION");
        } else if (readIf("TRANSACTION")) {
            read("ISOLATION");
            read("LEVEL");
            buff.append("'read committed' AS TRANSACTION_ISOLATION");
        } else if (readIf("DATESTYLE")) {
            buff.append("'ISO' AS DATESTYLE");
        } else if (readIf("SERVER_VERSION")) {
            buff.append("'8.1.4' AS SERVER_VERSION");
        } else if (readIf("SERVER_ENCODING")) {
            buff.append("'UTF8' AS SERVER_ENCODING");
        } else if (readIf("ALL")) {
            buff.append("* FROM INFORMATION_SCHEMA.SETTINGS");
            return true;
        } else {
            String name = readStringOrIdentifier();
            buff.append("@").append(name);
        }
        buff.append(" FROM DUAL");
        return true;
    }

    @Override
    protected StatementBase parseSetOther() {
        if (readIf("SESSION")) {
            if (readIf("CHARACTERISTICS")) {
                read("AS");
                read("TRANSACTION");
                if (readIf("ISOLATION")) {
                    read("LEVEL");
                    SetSession command = new SetSession(session,
                            SessionSetting.TRANSACTION_ISOLATION_LEVEL);
                    if (readIf("SERIALIZABLE")) {
                        command.setString("SERIALIZABLE");
                    } else if (readIf("REPEATABLE")) {
                        read("READ");
                        command.setString("REPEATABLE_READ");
                    } else if (readIf("READ")) {
                        if (readIf("COMMITTED"))
                            command.setString("READ_COMMITTED");
                        else if (readIf("UNCOMMITTED"))
                            command.setString("READ_UNCOMMITTED");
                    }
                    return command;
                } else if (readIf("READ")) {
                    if (!readIf("WRITE"))
                        read("ONLY");
                } else if (readIf("NOT")) {
                    read("DEFERRABLE");
                } else {
                    readIf("DEFERRABLE");
                }
                return new NoOperation(session);
            }
        }
        if (readIf("STATEMENT_TIMEOUT")) {
            readIfEqualOrTo();
            return new NoOperation(session);
        } else if (readIf("CLIENT_ENCODING") || readIf("CLIENT_MIN_MESSAGES")
                || readIf("JOIN_COLLAPSE_LIMIT")) {
            readIfEqualOrTo();
            read();
            return new NoOperation(session);
        } else if (readIf("DATESTYLE")) {
            readIfEqualOrTo();
            if (!readIf("ISO")) {
                String s = readString();
                if (!equalsToken(s, "ISO")) {
                    throw getSyntaxError();
                }
            }
            return new NoOperation(session);
        } else if (readIf("SEARCH_PATH")) {
            readIfEqualOrTo();
            do {
                // some PG clients will send single-quoted alias
                String s = currentTokenIsValueType() ? readString() : readUniqueIdentifier();
                if ("$user".equals(s)) {
                    continue;
                }
            } while (readIf(","));
            return new NoOperation(session);
        }
        return null;
    }
}
