/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.security.SHA256;
import com.lealone.common.util.Utils;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.Mode;
import com.lealone.db.auth.scram.ScramPasswordHash;

public class PasswordHash {

    public static void setPassword(User user, String password) {
        setPasswordLealone(user, password);
        setPasswordMongo(user, password);
        setPasswordMySQL(user, password);
        setPasswordPostgreSQL(user, password);
    }

    private static void setPasswordLealone(User user, String password) {
        char[] passwordChars = password == null ? new char[0] : password.toCharArray();
        byte[] userPasswordHash = ConnectionInfo.createUserPasswordHash(user.getName(), passwordChars);
        user.setUserPasswordHash(userPasswordHash);
    }

    private static void setPasswordMongo(User user, String password) {
        ScramPasswordHash.setPasswordMongo(user, password);
    }

    private static void setPasswordMySQL(User user, String password) {
        if (password == null || password.isEmpty()) {
            user.setSaltAndHashMySQL(new byte[0], new byte[0]);
            return;
        }
        byte[] hash = sha1(password);
        user.setSaltAndHashMySQL(new byte[0], hash);
    }

    private static void setPasswordPostgreSQL(User user, String password) {
        user.setSaltAndHashPostgreSQL(user.getSalt(), user.getPasswordHash());
    }

    private static MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw DbException.convert(e);
        }
    }

    private static byte[] sha1(String pass) {
        MessageDigest md = getMessageDigest();
        return md.digest(pass.getBytes());
    }

    public static boolean validateUserPasswordHash(User user, byte[] userPasswordHash, byte[] salt,
            Mode mode) {
        if (mode.isMongo())
            return validateUserPasswordHashMongo(user, userPasswordHash, salt);
        else if (mode.isMySQL())
            return validateUserPasswordHashMySQL(user, userPasswordHash, salt);
        else if (mode.isPostgreSQL())
            return validateUserPasswordHashPostgreSQL(user, userPasswordHash, salt);
        else
            return validateUserPasswordHashLealone(user, userPasswordHash, salt);
    }

    private static boolean validateUserPasswordHashLealone(User user, byte[] userPasswordHash,
            byte[] salt) {
        if (userPasswordHash.length == 0 && user.getPasswordHash().length == 0) {
            return true;
        }
        if (userPasswordHash.length == 0) {
            userPasswordHash = SHA256.getKeyPasswordHash(user.getName(), new char[0]);
        }
        if (salt == null)
            salt = user.getSalt();
        byte[] hash = SHA256.getHashWithSalt(userPasswordHash, salt);
        return Utils.compareSecure(hash, user.getPasswordHash());
    }

    private static boolean validateUserPasswordHashMongo(User user, byte[] userPasswordHash,
            byte[] salt) {
        if (userPasswordHash.length == 0 && user.getPasswordHashMongo().length == 0) {
            return true;
        }
        return true;
    }

    private static boolean validateUserPasswordHashMySQL(User user, byte[] userPasswordHash,
            byte[] salt) {
        if (userPasswordHash.length == 0 && user.getPasswordHashMySQL().length == 0) {
            return true;
        }
        if (userPasswordHash.length != user.getPasswordHashMySQL().length) {
            return false;
        }
        byte[] hash = scramble411Sha1Pass(user.getPasswordHashMySQL(), salt);
        return Utils.compareSecure(userPasswordHash, hash);
    }

    private static boolean validateUserPasswordHashPostgreSQL(User user, byte[] userPasswordHash,
            byte[] salt) {
        if (userPasswordHash.length == 0 && user.getPasswordHashPostgreSQL().length == 0) {
            return true;
        }
        if (userPasswordHash.length == 0) {
            userPasswordHash = SHA256.getKeyPasswordHash(user.getName(), new char[0]);
        }
        if (salt == null)
            salt = user.getSaltPostgreSQL();
        byte[] hash = SHA256.getHashWithSalt(userPasswordHash, salt);
        return Utils.compareSecure(hash, user.getPasswordHashPostgreSQL());
    }

    public static byte[] scramble411Sha1Pass(byte[] sha1Pass, byte[] seed) {
        MessageDigest md = getMessageDigest();
        byte[] pass2 = md.digest(sha1Pass);
        md.reset();
        md.update(seed);
        byte[] pass3 = md.digest(pass2);
        for (int i = 0; i < pass3.length; i++) {
            pass3[i] = (byte) (sha1Pass[i] ^ pass3[i]);
        }
        return pass3;
    }
}
