/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.util;

public class CamelCaseHelper {

    /**
     * To underscore from camel case using digits compressed true and force upper case false.
     */
    public static String toUnderscoreFromCamel(String camelCase) {
        return toUnderscoreFromCamel(camelCase, true, false);
    }

    /**
     * Convert and return the string to underscore from camel case.
     */
    public static String toUnderscoreFromCamel(String camelCase, boolean digitsCompressed,
            boolean forceUpperCase) {
        int lastUpper = -1;
        StringBuilder sb = new StringBuilder(camelCase.length() + 4);
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);

            if ('_' == c) {
                // Underscores should just be passed through
                sb.append(c);
                lastUpper = i;
            } else if (Character.isDigit(c)) {
                if (i > lastUpper + 1 && !digitsCompressed) {
                    sb.append("_");
                }
                sb.append(c);
                lastUpper = i;

            } else if (Character.isUpperCase(c)) {
                if (i > lastUpper + 1) {
                    sb.append("_");
                }
                sb.append(Character.toLowerCase(c));
                lastUpper = i;

            } else {
                sb.append(c);
            }
        }
        String ret = sb.toString();
        if (forceUpperCase) {
            ret = ret.toUpperCase();
        }
        return ret;
    }

    /**
     * To camel from underscore.
     *
     * @param underscore the underscore
     * @return the string
     */
    public static String toCamelFromUnderscore(String underscore) {
        String[] vals = underscore.split("_");
        if (vals.length == 1) {
            return isUpperCase(underscore) ? underscore.toLowerCase() : underscore;
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < vals.length; i++) {
            String lower = vals[i].toLowerCase();
            if (i > 0) {
                char c = Character.toUpperCase(lower.charAt(0));
                result.append(c);
                result.append(lower.substring(1));
            } else {
                result.append(lower);
            }
        }

        return result.toString();
    }

    private static boolean isUpperCase(String underscore) {
        for (int i = 0; i < underscore.length(); i++) {
            if (Character.isLowerCase(underscore.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static String toClassNameFromUnderscore(String underscore) {
        StringBuilder result = new StringBuilder();
        String[] vals = underscore.split("_");
        for (int i = 0; i < vals.length; i++) {
            toClassName(result, vals[i]);
        }
        return result.toString();
    }

    private static void toClassName(StringBuilder buff, String str) {
        for (int i = 0, len = str.length(); i < len; i++) {
            char c = str.charAt(i);
            if (i == 0) {
                if (Character.isLowerCase(c)) {
                    c = Character.toUpperCase(c);
                }
            } else {
                if (Character.isUpperCase(c)) {
                    c = Character.toLowerCase(c);
                }
            }
            buff.append(c);
        }
    }
}
