/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.auth.scram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.auth.User;

public class ScramPasswordHash {

    public static void setPasswordMongo(User user, String password) {
        setPasswordMongo(user, password, 256);
    }

    public static void setPasswordMongo(User user, String password, int mechanism) {
        try {
            SecureRandom random = new SecureRandom();
            byte[] salt = new byte[24];
            random.nextBytes(salt);
            if (mechanism == 1)
                password = createAuthenticationHash(user.getName(), password.toCharArray());
            byte[] saltedPassword = generateSaltedPassword(password, salt, 4096, "HmacSHA" + mechanism);
            user.setSaltAndHashMongo(salt, saltedPassword);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw DbException.convert(e);
        }
    }

    public static ScramPasswordData createScramPasswordData(byte[] salt, byte[] saltedPassword,
            int mechanism) {
        try {
            return newPassword(salt, saltedPassword, 4096, "SHA-" + mechanism, "HmacSHA" + mechanism);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw DbException.convert(e);
        }
    }

    private static ScramPasswordData newPassword(byte[] salt, byte[] saltedPassword, int iterations,
            String digestName, String hmacName) throws NoSuchAlgorithmException, InvalidKeyException {

        if (!hmacName.toLowerCase().startsWith("hmac")) {
            throw new IllegalArgumentException("Invalid HMAC: " + hmacName);
        }

        if (saltedPassword.length == 0)
            return new ScramPasswordData(salt, saltedPassword, saltedPassword, saltedPassword,
                    iterations);

        byte[] clientKey = computeHmac(saltedPassword, hmacName, "Client Key");
        byte[] storedKey = MessageDigest.getInstance(digestName).digest(clientKey);
        byte[] serverKey = computeHmac(saltedPassword, hmacName, "Server Key");

        return new ScramPasswordData(salt, saltedPassword, storedKey, serverKey, iterations);
    }

    private static final byte[] INT_1 = new byte[] { 0, 0, 0, 1 };

    private static byte[] generateSaltedPassword(String password, byte[] salt, int iterations,
            String hmacName) throws InvalidKeyException, NoSuchAlgorithmException {
        if (password.isEmpty())
            return new byte[0];
        Mac mac = createHmac(password.getBytes(StandardCharsets.US_ASCII), hmacName);

        mac.update(salt);
        mac.update(INT_1);
        byte[] result = mac.doFinal();

        byte[] previous = null;
        for (int i = 1; i < iterations; i++) {
            mac.update(previous != null ? previous : result);
            previous = mac.doFinal();
            for (int x = 0; x < result.length; x++) {
                result[x] ^= previous[x];
            }
        }
        return result;
    }

    // 这是MongoDB使用SCRAM-SHA-1时的特殊格式
    private static String createAuthenticationHash(String userName, char[] password) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(userName.length() + 20 + password.length);
        try {
            bout.write(userName.getBytes(StandardCharsets.UTF_8));
            bout.write(":mongo:".getBytes(StandardCharsets.UTF_8));
            bout.write(new String(password).getBytes(StandardCharsets.UTF_8));
        } catch (IOException ioe) {
            throw new RuntimeException("impossible", ioe);
        }
        return hexMD5(bout.toByteArray());
    }

    private static String hexMD5(byte[] data) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            md5.reset();
            md5.update(data);
            byte[] digest = md5.digest();

            return toHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error - this implementation of Java doesn't support MD5.");
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            String s = Integer.toHexString(0xff & b);
            if (s.length() < 2) {
                sb.append("0");
            }
            sb.append(s);
        }
        return sb.toString();
    }

    private static Mac createHmac(byte[] keyBytes, String hmacName)
            throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec key = new SecretKeySpec(keyBytes, hmacName);
        Mac mac = Mac.getInstance(hmacName);
        mac.init(key);
        return mac;
    }

    public static byte[] computeHmac(byte[] key, String hmacName, String string)
            throws InvalidKeyException, NoSuchAlgorithmException {
        Mac mac = createHmac(key, hmacName);
        mac.update(string.getBytes(StandardCharsets.US_ASCII));
        return mac.doFinal();
    }
}
