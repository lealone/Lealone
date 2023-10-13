/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.link;

public class LinkableBase<E extends LinkableBase<E>> implements Linkable<E> {

    public E next;

    @Override
    public void setNext(E next) {
        this.next = next;
    }

    @Override
    public E getNext() {
        return next;
    }
}
