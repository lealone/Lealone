package com.codefollower.yourbase.util;

import java.io.UnsupportedEncodingException;

public class Bytes {

    /** When we encode strings, we always specify UTF8 encoding */
    public static final String UTF8_ENCODING = "UTF-8";

    /**
     * Converts a string to a UTF-8 byte array.
     * @param s string
     * @return the byte array
     */
    public static byte[] toBytes(String s) {
        try {
            return s.getBytes(UTF8_ENCODING);
        } catch (UnsupportedEncodingException e) {
            s.getBytes();
            return null;
        }
    }

    /**
     * @param b Presumed UTF-8 encoded byte array.
     * @return String made from <code>b</code>
     */
    public static String toString(final byte[] b) {
        if (b == null) {
            return null;
        }
        return toString(b, 0, b.length);
    }

    /**
     * Joins two byte arrays together using a separator.
     * @param b1 The first byte array.
     * @param sep The separator to use.
     * @param b2 The second byte array.
     */
    public static String toString(final byte[] b1, String sep, final byte[] b2) {
        return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
    }

    /**
     * This method will convert utf8 encoded bytes into a string. If
     * an UnsupportedEncodingException occurs, this method will eat it
     * and return null instead.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @param len length of utf-8 sequence
     * @return String made from <code>b</code> or null
     */
    public static String toString(final byte[] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        try {
            return new String(b, off, len, UTF8_ENCODING);
        } catch (UnsupportedEncodingException e) {
            new String(b, off, len);
            return null;
        }
    }
}