/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.misc;

import java.math.BigDecimal;

import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueByte;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueDecimal;
import org.lealone.db.value.ValueDouble;
import org.lealone.db.value.ValueFloat;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueShort;
import org.lealone.db.value.ValueString;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.db.value.ValueUuid;

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
