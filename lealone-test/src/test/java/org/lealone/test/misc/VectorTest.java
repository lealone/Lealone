package org.lealone.test.misc;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

//https://openjdk.java.net/jeps/338
//https://openjdk.java.net/jeps/414
//--add-modules jdk.incubator.vector -server
public class VectorTest {

    static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;// FloatVector.SPECIES_MAX;

    public static void main(String[] args) {
        int size = SPECIES.length() * 100;
        size = 20 * 100;
        float[] a = new float[size];
        float[] b = new float[size];

        float[] c1 = new float[size];
        // float[] c2 = new float[size];
        float[] c3 = new float[size];

        for (int i = 0; i < size; i++) {
            a[i] = (i + 1) * 1.0f;
            b[i] = (i + 1) * 10.0f;
        }

        int loop = 500000;
        for (int i = 0; i < loop; i++) {
            scalarComputation(a, b, c1);
        }
        for (int i = 0; i < loop; i++) {
            // vectorComputation(a, b, c2);
        }
        for (int i = 0; i < loop; i++) {
            vectorComputation2(a, b, c3);
        }

        long t1 = System.nanoTime();
        for (int i = 0; i < loop; i++) {
            scalarComputation(a, b, c1);
        }
        long t2 = System.nanoTime();
        System.out.println((t2 - t1) / 1000);

        long t3 = System.nanoTime();
        for (int i = 0; i < loop; i++) {
            // vectorComputation(a, b, c2);
        }
        long t4 = System.nanoTime();
        // System.out.println((t4 - t3) / 1000);

        t3 = System.nanoTime();
        for (int i = 0; i < loop; i++) {
            vectorComputation2(a, b, c3);
        }
        t4 = System.nanoTime();
        System.out.println((t4 - t3) / 1000);
    }

    static void scalarComputation(float[] a, float[] b, float[] c) {
        for (int i = 0; i < a.length; i++) {
            c[i] = (a[i] * a[i] + b[i] * b[i]) * -1.0f;
        }
    }

    static void vectorComputation(float[] a, float[] b, float[] c) {
        for (int i = 0; i < a.length; i += SPECIES.length()) {
            VectorMask<Float> m = SPECIES.indexInRange(i, a.length);
            // FloatVector va, vb, vc;
            FloatVector va = FloatVector.fromArray(SPECIES, a, i, m);
            FloatVector vb = FloatVector.fromArray(SPECIES, b, i, m);
            FloatVector vc = va.mul(va).add(vb.mul(vb)).neg();
            vc.intoArray(c, i, m);
        }
    }

    static void vectorComputation2(float[] a, float[] b, float[] c) {
        int i = 0;
        int upperBound = SPECIES.loopBound(a.length);
        for (; i < upperBound; i += SPECIES.length()) {
            // FloatVector va, vb, vc;
            FloatVector va = FloatVector.fromArray(SPECIES, a, i);
            FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
            FloatVector vc = va.mul(va).add(vb.mul(vb)).neg();
            vc.intoArray(c, i);
        }

        for (; i < a.length; i++) {
            c[i] = (a[i] * a[i] + b[i] * b[i]) * -1.0f;
        }
    }

    static void vectorComputation3(float[] a, float[] b, float[] c, VectorSpecies<Float> species) {
        int i = 0;
        int upperBound = species.loopBound(a.length);
        for (; i < upperBound; i += species.length()) {
            // FloatVector va, vb, vc;
            FloatVector va = FloatVector.fromArray(species, a, i);
            FloatVector vb = FloatVector.fromArray(species, b, i);
            FloatVector vc = va.mul(va).add(vb.mul(vb)).neg();
            vc.intoArray(c, i);
        }

        for (; i < a.length; i++) {
            c[i] = (a[i] * a[i] + b[i] * b[i]) * -1.0f;
        }
    }
}
