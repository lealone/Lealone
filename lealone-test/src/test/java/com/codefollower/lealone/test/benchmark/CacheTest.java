package com.codefollower.lealone.test.benchmark;

import com.codefollower.lealone.mvstore.cache.CacheLongKeyLIRS;
import com.codefollower.lealone.omid.tso.GuavaCache;
import com.codefollower.lealone.omid.tso.LongCache;

public class CacheTest {
    static void p(String m, long v) {
        System.out.println(m + ": " + v / 1000000 + " ms");
    }

    static void p() {
        System.out.println();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        int size = 1000;
        LongCache cache1 = new LongCache(size, 32);
        CacheLongKeyLIRS<Long> cache2 = new CacheLongKeyLIRS<Long>(size);
        GuavaCache cache3 = new GuavaCache(size);

        size = 1000;
        for (long i = 0; i < size; i++) {
            cache1.set(i, i);
        }
        for (long i = 0; i < size; i++) {
            cache2.put(i, i);
        }
        
        for (long i = 0; i < size; i++) {
            cache3.set(i, i);
        }
        long start, end;
        size = 10000;

        start = System.nanoTime();

        for (long i = 0; i < size; i++) {
            cache1.set(i, i);
        }

        end = System.nanoTime();
        p("LongCache", end - start);

        p();

        start = System.nanoTime();
        for (long i = 0; i < size; i++) {
            cache2.put(i, i);
        }
        end = System.nanoTime();
        p("CacheLongKeyLIRS()", end - start);
        
        
        p();

        start = System.nanoTime();
        for (long i = 0; i < size; i++) {
            cache3.set(i, i);
        }
        end = System.nanoTime();
        p("GuavaCache()", end - start);
    }

}
