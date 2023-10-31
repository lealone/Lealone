/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.sql;

import java.util.ArrayList;

import org.lealone.common.util.Utils;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionSetting;
import org.lealone.sql.SQLParserBase;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.dml.SetSession;
import org.lealone.sql.dml.TransactionStatement;

public class PgSQLParser extends SQLParserBase {

    public PgSQLParser(ServerSession session) {
        super(session);
        this.session.setSchemaSearchPath(new String[] { "public", "pg_catalog" });
    }

    @Override
    protected StatementBase parseStatement(char first) {
        StatementBase s = null;
        switch (first) {
        case 'e':
        case 'E':
            if (readIf("END")) { // END和COMMIT是同义词
                s = parseCommit();
            }
            break;
        case 'r':
        case 'R':
            if (readIf("RESET")) {
                s = parseReset();
            }
            break;
        }
        return s;
    }

    @Override
    protected StatementBase parseStart() {
        if (readIf("TRANSACTION")) {
            TransactionStatement command = new TransactionStatement(session, SQLStatement.BEGIN);
            parseTransactionMode().update();
            session.getTransaction(); // 如果上一条提交事务了重新启动事务
            return command;
        } else {
            return super.parseStart();
        }
    }

    @Override
    protected TransactionStatement parseBegin() {
        TransactionStatement command = super.parseBegin();
        parseTransactionMode().update();
        session.getTransaction(); // 如果上一条提交事务了重新启动事务
        return command;
    }

    @Override
    protected TransactionStatement parseCommit() {
        TransactionStatement command = super.parseCommit();
        if (readIf("AND")) {
            boolean startNewTransaction = true;
            if (readIf("NO")) {
                startNewTransaction = false;
            }
            read("CHAIN");
            if (startNewTransaction) {
                command.update();
                command = new TransactionStatement(session, SQLStatement.BEGIN);
            }
        }
        return command;
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
        if (readIf("TRANSACTION")) {
            if (readIf("SNAPSHOT")) {
                readStringOrIdentifier();
                return noOperation();
            }
            return parseTransactionMode();
        } else if (readIf("CONSTRAINTS")) {
            if (!readIf("ALL")) {
                do {
                    readStringOrIdentifier();
                } while (readIf(","));
            }
            if (!readIf("DEFERRED")) {
                read("IMMEDIATE");
            }
            return noOperation();
        } else {
            if (readIf("SESSION")) {
                if (readIf("CHARACTERISTICS")) {
                    read("AS");
                    read("TRANSACTION");
                    return parseTransactionMode();
                } else if (readIf("AUTHORIZATION")) {
                    if (!readIf("DEFAULT")) {
                        readStringOrIdentifier();
                    }
                    return noOperation();
                }
            } else {
                readIf("LOCAL");
            }
            if (readIf("SESSION")) {
                read("AUTHORIZATION");
                if (!readIf("DEFAULT")) {
                    readStringOrIdentifier();
                }
                return noOperation();
            } else if (readIf("ROLE")) {
                if (!readIf("NONE")) {
                    readStringOrIdentifier();
                }
                return noOperation();
            } else if (readIf("TIME")) {
                read("ZONE");
                if (readIf("LOCAL") || readIf("DEFAULT")) {
                    return noOperation();
                } else {
                    readStringOrIdentifier();
                    return noOperation();
                }
            } else if (readIf("DATESTYLE")) {
                readIfEqualOrTo();
                if (!readIf("ISO")) {
                    String s = readString();
                    if (!equalsToken(s, "ISO")) {
                        throw getSyntaxError();
                    }
                }
                return noOperation();
            } else if (readIf("SEARCH_PATH")) {
                readIfEqualOrTo();
                SetSession command = new SetSession(session, SessionSetting.SCHEMA_SEARCH_PATH);
                ArrayList<String> list = Utils.newSmallArrayList();
                do {
                    // some PG clients will send single-quoted alias
                    String s = readStringOrIdentifier();
                    if ("$user".equals(s)) {
                        continue;
                    }
                    list.add(s);
                } while (readIf(","));
                String[] schemaNames = new String[list.size()];
                list.toArray(schemaNames);
                command.setStringArray(schemaNames);
                return command;
            } else {
                return parseSetVariable();
            }
        }
    }

    private StatementBase parseTransactionMode() {
        SetSession command = null;
        while (true) {
            if (readIf("ISOLATION")) {
                read("LEVEL");
                command = new SetSession(session, SessionSetting.TRANSACTION_ISOLATION_LEVEL);
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
            } else if (readIf("READ")) {
                if (!readIf("WRITE"))
                    read("ONLY");
            } else if (readIf("NOT")) {
                read("DEFERRABLE");
            } else if (!readIf("DEFERRABLE")) {
                break;
            }
        }
        return command != null ? command : noOperation();
    }

    private StatementBase parseReset() {
        if (readIf("ALL")) {
            return noOperation();
        } else if (readIf("SESSION")) {
            read("AUTHORIZATION");
            return noOperation();
        } else if (readIf("ROLE")) {
            return noOperation();
        } else {
            readAliasIdentifier();
            return noOperation();
        }
    }
}
