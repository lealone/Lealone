/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
