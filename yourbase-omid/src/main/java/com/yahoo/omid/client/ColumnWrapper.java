package com.yahoo.omid.client;

import java.util.Arrays;

public class ColumnWrapper {
    private byte[] family;
    private byte[] qualifier;

    public ColumnWrapper(byte[] family, byte[] qualifier) {
        this.family = family;
        this.qualifier = qualifier;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(family);
        result = prime * result + Arrays.hashCode(qualifier);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ColumnWrapper other = (ColumnWrapper) obj;
        if (!Arrays.equals(family, other.family))
            return false;
        if (!Arrays.equals(qualifier, other.qualifier))
            return false;
        return true;
    }

}