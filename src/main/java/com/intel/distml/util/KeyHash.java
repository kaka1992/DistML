package com.intel.distml.util;

import java.util.Iterator;

/**
 * Created by yunlong on 12/11/14.
 */
public class KeyHash implements KeyCollection {

    public int hashQuato;
    public int hashIndex;
    public long totalKeyNum;

    public KeyHash(int hashQuato, int hashIndex, long totalKeyNum) {
        this.hashQuato = hashQuato;
        this.hashIndex = hashIndex;
        this.totalKeyNum = totalKeyNum;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KeyHash)) {
            return false;
        }

        KeyHash o = (KeyHash)obj;
        return (hashQuato == o.hashQuato) && (hashIndex == o.hashIndex) && (totalKeyNum == o.totalKeyNum);
    }
/*
    public PartitionInfo partitionEqually(int hostNum) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

    @Override
    public KeyCollection[] split(int hostNum) {
        throw new UnsupportedOperationException("This method is not supported.");
    }
*/
    public int size() {
        return (int) ((totalKeyNum /hashQuato) + ((totalKeyNum % hashQuato) / hashIndex));
    }

    @Override
    public boolean contains(long key) {
        return (key < totalKeyNum) && (key % hashQuato == hashIndex);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

//    public boolean containsAll(KeyCollection keys) {
//        KeyHash keyRange = (KeyHash)keys;
//        return (keyRange.totalKeyNum >= firstKey && keyRange.firstKey <= totalKeyNum);
//    }

    @Override
    public String toString() {
        return "[KeyHash: " + hashQuato + ", " + hashIndex + ", " + totalKeyNum + "]";
    }

//    public KeyHash FetchSame(KeyHash kr) {
//        long NewFirst = kr.firstKey > this.firstKey ? kr.firstKey : this.firstKey;
//        long NewLast = kr.totalKeyNum < this.totalKeyNum ? kr.totalKeyNum : this.totalKeyNum;
//        if (NewFirst > NewLast) return null;
//        return new KeyHash(NewFirst, NewLast);
//    }

    @Override
    public KeyCollection intersect(KeyCollection keys) {

        if (keys.equals(KeyCollection.ALL)) {
            return this;
        }

        if (keys.equals(KeyCollection.EMPTY)) {
            return keys;
        }

        KeyList list = new KeyList();
        Iterator<Long> it = keys.iterator();
        while(it.hasNext()) {
            long key = it.next();
            if (contains(key)) {
                list.addKey(key);
            }
        }

        if (list.isEmpty()) {
            return KeyCollection.EMPTY;
        }

        return list;
    }


    @Override
    public Iterator<Long> iterator() {
        return new _Iterator(this);
    }

    static class _Iterator implements Iterator<Long> {

        long currentKey;
        KeyHash keys;

        public _Iterator(KeyHash keys) {
            this.keys = keys;
            this.currentKey = keys.hashIndex;
        }

        @Override
        public boolean hasNext() {
            return currentKey < keys.totalKeyNum;
        }

        @Override
        public Long next() {
            long k = currentKey;
            currentKey += keys.hashQuato;
            return k;
        }

        @Override
        public void remove() {
            throw new RuntimeException("Not supported.");
        }

    }
}
