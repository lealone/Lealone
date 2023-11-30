/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.link;

//为调度线程量身订做的链表
public class LinkableList<E extends Linkable<E>> {

    private E head;
    private E tail;
    private int size;

    public E getHead() {
        return head;
    }

    public void setHead(E head) {
        this.head = head;
    }

    public E getTail() {
        return tail;
    }

    public void setTail(E tail) {
        this.tail = tail;
    }

    public boolean isEmpty() {
        return head == null;
    }

    public int size() {
        return size;
    }

    public void decrementSize() {
        size--;
    }

    public void add(E e) {
        size++;
        if (head == null) {
            head = tail = e;
        } else {
            tail.setNext(e);
            tail = e;
        }
    }

    public void remove(E e) {
        boolean found = false;
        if (head == e) { // 删除头
            found = true;
            head = e.getNext();
            if (head == null)
                tail = null;
        } else {
            E n = head;
            E last = n;
            while (n != null) {
                if (e == n) {
                    found = true;
                    last.setNext(n.getNext());
                    break;
                }
                last = n;
                n = n.getNext();
            }
            // 删除尾
            if (tail == e) {
                found = true;
                tail = last;
            }
        }
        if (found)
            size--;
    }
}
