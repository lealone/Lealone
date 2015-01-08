//package org.lealone.mvstore;
//
//import org.lealone.mvstore.type.DataType;
//
//public class MVMap2<K, V> extends MVMap<K, V> {
//
//    protected MVMap2(DataType keyType, DataType valueType) {
//        super(keyType, valueType);
//    }
//
//    @Override
//    @SuppressWarnings("unchecked")
//    public synchronized V put(K key, V value) {
//        DataUtils.checkArgument(value != null, "The value may not be null");
//        beforeWrite();
//        long v = writeVersion;
//        Page p = root;
//        p = splitRootIfNeeded(p, v);
//        Object result = put(p, v, key, value);
//        newRoot(p);
//        return (V) result;
//    }
//
//    public V put(K key, V value, long version) {
//        V old = get(key);
//        if (old == null)
//            return put(key, value);
//        return old;
//
//    }
//
//    private static class VersionedValue<V> {
//
//    }
//}
