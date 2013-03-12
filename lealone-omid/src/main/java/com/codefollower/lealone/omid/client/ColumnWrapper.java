/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.codefollower.lealone.omid.client;

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