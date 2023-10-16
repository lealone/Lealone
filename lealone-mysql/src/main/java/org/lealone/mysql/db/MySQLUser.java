/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.db;

import org.lealone.common.util.Utils;
import org.lealone.db.Database;
import org.lealone.db.auth.User;
import org.lealone.mysql.server.util.SecurityUtil;

public class MySQLUser extends User {

    public MySQLUser(Database database, int id, String userName, boolean systemUser) {
        super(database, id, userName, systemUser);
    }

    @Override
    public boolean validateUserPasswordHash(byte[] userPasswordHash, byte[] salt) {
        if (userPasswordHash.length == 0 && getPasswordHash().length == 0) {
            return true;
        }
        if (userPasswordHash.length != getPasswordHash().length) {
            return false;
        }
        byte[] hash = SecurityUtil.scramble411Sha1Pass(getPasswordHash(), salt);
        return Utils.compareSecure(userPasswordHash, hash);
    }
}
