/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.misc;

import java.math.BigDecimal;

import com.lealone.db.value.ValueBoolean;
import com.lealone.db.value.ValueByte;
import com.lealone.db.value.ValueDate;
import com.lealone.db.value.ValueDecimal;
import com.lealone.db.value.ValueDouble;
import com.lealone.db.value.ValueFloat;
import com.lealone.db.value.ValueInt;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueShort;
import com.lealone.db.value.ValueString;
import com.lealone.db.value.ValueTimestamp;
import com.lealone.db.value.ValueUuid;

public class JavaObjectSizeTest {

    public static void main(String[] args) {
        size(ValueBoolean.get(true));
        size(ValueByte.get((byte) 1));
        size(ValueShort.get((short) 1));
        size(ValueInt.get(1));
        size(ValueLong.get(1));
        size(ValueFloat.get(0.1F));
        size(ValueDouble.get(0.1D));

        size(ValueDate.fromDateValue(System.currentTimeMillis()));
        size(ValueDecimal.get(BigDecimal.valueOf(10L)));
        size(ValueNull.INSTANCE);
        size(ValueString.get("abc"));
        size("abc");

        size(ValueTimestamp.fromDateValueAndNanos(0, 0));
        size(ValueUuid.getNewRandom());
    }

    private static void size(Object obj) {
        // System.out.println(org.openjdk.jol.info.ClassLayout.parseInstance(obj).toPrintable());
    }
}
