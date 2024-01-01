/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.link;

public interface Linkable<E extends Linkable<E>> {

    void setNext(E next);

    E getNext();

}
