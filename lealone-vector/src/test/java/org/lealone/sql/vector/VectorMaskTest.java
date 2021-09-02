/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

public class VectorMaskTest {

    static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;// FloatVector.SPECIES_MAX;

    public static void main(String[] args) {
        int size = SPECIES.length() * 100;
        // size = 20 * 100;
        float[] a = new float[size];
        float[] b = new float[size];

        float[] c1 = new float[size];

        boolean[] bits = new boolean[size];

        for (int i = 0; i < size; i++) {
            a[i] = (i + 1) * 1.0f;
            b[i] = (i + 1) * 10.0f;
            bits[i] = true;
        }
        // for (int i = 0; i < SPECIES.length(); i++) {
        // bits[i] = true;
        // }
        bits[0] = false;
        bits[10] = false;
        vectorComputation2(a, b, c1, bits);
        System.out.println(c1[0]);
        System.out.println(c1[10]);
    }

    static void vectorComputation2(float[] a, float[] b, float[] c, boolean[] bits) {
        int i = 0;
        int upperBound = SPECIES.loopBound(a.length);
        for (; i < upperBound; i += SPECIES.length()) {
            VectorMask<Float> mask = VectorMask.fromArray(SPECIES, bits, i);
            FloatVector va = FloatVector.fromArray(SPECIES, a, i, mask);
            FloatVector vb = FloatVector.fromArray(SPECIES, b, i, mask);
            // FloatVector vc = va.mul(va, mask).add(vb.mul(vb, mask), mask).neg();
            FloatVector vc = va.add(vb);
            vc.intoArray(c, i);
        }

        for (; i < a.length; i++) {
            c[i] = (a[i] * a[i] + b[i] * b[i]) * -1.0f;
        }
    }
}
