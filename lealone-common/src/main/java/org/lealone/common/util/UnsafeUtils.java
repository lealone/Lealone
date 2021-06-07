/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.util;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

@SuppressWarnings("restriction")
public class UnsafeUtils {
    private static final sun.misc.Unsafe UNSAFE;

    static {
        try {
            // 不能直接调用Unsafe.getUnsafe()，在Eclipse下面运行会有安全异常
            final Object maybeUnsafe = AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    try {
                        final Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                        unsafeField.setAccessible(true);
                        return unsafeField.get(null);
                    } catch (Throwable e) {
                        return e;
                    }
                }
            });
            if (maybeUnsafe instanceof Throwable) {
                throw (Throwable) maybeUnsafe;
            } else {
                UNSAFE = (sun.misc.Unsafe) maybeUnsafe;
            }
        } catch (Throwable ex) {
            throw new Error(ex);
        }
    }

    public static long objectFieldOffset(Class<?> clz, String fieldName) {
        try {
            return UNSAFE.objectFieldOffset(clz.getDeclaredField(fieldName));
        } catch (Throwable ex) {
            throw new Error(ex);
        }
    }

    public static boolean compareAndSwapObject(Object object, long fieldOffset, Object cmp, Object val) {
        return UNSAFE.compareAndSwapObject(object, fieldOffset, cmp, val);
    }
}
